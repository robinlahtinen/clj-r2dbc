;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.connection.lifecycle
  "Connection lifecycle and the Publisher<Result> -> row flow bridge for streaming.

  When db is a ConnectionFactory: a connection is acquired on subscription and
  closed on termination (normal completion, error, or cancellation), driven by
  Reactor's Flux/usingWhen resource operator.
  When db is already a Connection: the caller owns the lifecycle and
  Connection.close() is NOT called.

  Flatten strategy: a Publisher<Result> is flattened to a Publisher of row values
  at the Reactive Streams layer via Reactor concatMap (ordered, prefetch 1), then
  bridged to a single Missionary discrete flow with m/subscribe. The flow is
  consumed by the caller's m/reduce (a non-forking consumer).

  Why not m/?> over m/subscribe: forking a discrete flow built from m/subscribe
  with m/?> (Ambiguous) races a synchronous publisher's re-entrant onNext against
  the fork and intermittently drops a wakeup, hanging the flow under concurrency
  (reproduced on constrained cores). Consuming the single m/subscribe flow with
  m/reduce instead - and doing all flattening below the bridge with concatMap -
  removes every m/?>-over-m/subscribe site. Database fetch batching is governed
  independently by Statement.fetchSize (see statement/apply-opts!).

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.connection.publisher :as pub]
   [clj-r2dbc.impl.sql.statement :as stmt]
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Connection ConnectionFactory Statement)
   (java.util.function Function Supplier)
   (org.reactivestreams Publisher)
   (reactor.core.publisher Flux)))

(set! *warn-on-reflection* true)

(defn- rows-pub
  "Flatten a Publisher<Result> into a Publisher of row values.

  Uses Reactor concatMap (ordered, so multi-Result output preserves Result order)
  with prefetch 1, so row-level demand stays request(1) and no rows are buffered
  across the bridge - preserving fetch-size-1 backpressure. row-xf is applied
  inside each Result.map via pub/result-row-pub, while the Row's backing ByteBuf
  is guaranteed live, so it materialises an immutable value before emission."
  ^Publisher [^Publisher result-pub row-xf]
  (.concatMap (Flux/from result-pub)
              (reify Function
                (apply [_ result] (pub/result-row-pub result row-xf)))
              1))

(defn- chunked-pub
  "Wrap a Publisher of row values into a Publisher of up-to-chunk-size vectors
  via Reactor buffer (a shorter final vector is emitted on completion).

  Chunking happens below the Missionary bridge so the single m/subscribe flow is
  still consumed directly by m/reduce; a transducer (m/eduction) or m/?> over the
  bridge would reintroduce the synchronous-publisher fork race (see ns docstring).
  :chunk-size requires :builder-fn, so buffered values are materialized - safe to
  batch across the bridge (raw un-materialized Rows would not be)."
  ^Publisher [^Publisher rows ^long chunk-size]
  (.map (.buffer (Flux/from rows) (int chunk-size))
        (reify Function (apply [_ batch] (vec batch)))))

(defn result-rows-flow
  "Return a Missionary discrete flow emitting every transformed row from every
  Result produced by result-pub, flattened in order.

  result-pub (Publisher<Result>) is flattened to a Publisher of row values with
  rows-pub (Reactor concatMap), then bridged to a single discrete flow with
  m/subscribe and dispatched on m/blk (pub/async-subscribe-pub). Demand-driven
  backpressure, cancellation, and termination are handled by m/subscribe + the
  Reactor operator chain. Intended to be consumed by m/reduce (or m/eduction);
  do NOT wrap it in m/?> (see ns docstring).

  Args:
    result-pub - Publisher<Result> from Statement.execute().
    row-xf     - 1-arity fn [Row] -> value applied to each row."
  [^Publisher result-pub row-xf]
  (m/subscribe (pub/async-subscribe-pub (rows-pub result-pub row-xf))))

(defn result-chunks-flow
  "Like result-rows-flow, but emits one vector of up to chunk-size transformed
  rows per value instead of one row.

  Batching happens below the bridge via Reactor buffer (chunked-pub), so the
  single m/subscribe flow stays consumable directly by m/reduce and the consumer
  performs O(batches) transfers. Batches span the whole stream (not per-Result);
  for the single-Result common case this is identical to per-Result batching.

  Args:
    result-pub - Publisher<Result> from Statement.execute().
    row-xf     - 1-arity fn [Row] -> value applied to each row.
    chunk-size - positive number of rows per emitted vector."
  [^Publisher result-pub row-xf ^long chunk-size]
  (m/subscribe
   (pub/async-subscribe-pub (chunked-pub (rows-pub result-pub row-xf) chunk-size))))

(defn streaming-plan-flow
  "Return a Missionary discrete flow emitting transformed rows with end-to-end
  demand-driven backpressure and managed connection lifecycle.

  Connection lifecycle: when db is a ConnectionFactory, the connection is
  acquired on subscription and closed on completion/error/cancellation via
  Reactor Flux/usingWhen (which cancels create()/execute() and runs
  Connection.close() exactly once on any terminal signal). When db is already a
  Connection, the caller owns the lifecycle and close() is NOT called.

  The row Publisher is built below the Missionary bridge: usingWhen acquires the
  connection, prepares and executes the Statement, and rows-pub concatMaps each
  Result's rows; the resulting Publisher is bridged once with m/subscribe
  (dispatched on m/blk). The caller consumes the flow with m/reduce/m/eduction -
  there is no m/?> over the bridge, by design (see ns docstring). Statement
  fetchSize (applied from opts in stmt/prepare!) controls database fetch batching.
  When opts carries :chunk-size, rows are batched into vectors below the bridge
  via chunked-pub (Reactor buffer).

  Args:
    db     - ConnectionFactory or Connection (ConnectableWithOpts must be
             unwrapped by the caller via conn/resolve-connectable).
    sql    - SQL string to execute.
    params - sequential collection of bind parameters.
    opts   - options map (see execute for supported keys).
    row-xf - 1-arity fn applied to each raw R2DBC Row to produce the emitted
             immutable value (the builder, defaulting to kebab-maps)."
  [db sql params opts row-xf]
  (let [owns-conn?                                                       (not (instance? Connection db))
        chunk-size                                                       (:chunk-size opts)
        use-fn                                                           (reify Function
                                                                           (apply [_ conn]
                                                                             (let [rows (rows-pub (.execute ^Statement
                                                                                                   (stmt/prepare! conn sql params opts))
                                                                                                  row-xf)]
                                                                               (if chunk-size (chunked-pub rows (long chunk-size)) rows))))
        ^Publisher pub
        (if owns-conn?
          (Flux/usingWhen (.create ^ConnectionFactory db)
                          use-fn
                          (reify Function
                            (apply [_ conn] (.close ^Connection conn))))
          (Flux/defer (reify Supplier
                        (get [_] (.apply use-fn db)))))]
    (m/subscribe (pub/async-subscribe-pub pub))))

(comment
  (require '[clj-r2dbc :as r2])
  (def db (r2/connect "r2dbc:h2:mem:///lifecycle-repl;DB_CLOSE_DELAY=-1"))
  (m/? (m/reduce conj [] (r2/stream db "SELECT 1 AS n")))
  (m/? (r2/with-conn [conn db]
         (m/reduce conj [] (r2/stream conn "SELECT 1 AS n"))))
  (m/? (m/reduce (fn [_ _] (reduced :done)) nil (r2/stream db "SELECT 1 AS n")))
  (try (m/? (m/reduce conj [] (r2/stream db "NOT VALID SQL")))
       (catch Exception e (ex-data e))))
