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

  Decoupling is conditional on the driver (see synchronous-db?): a SYNCHRONOUS
  driver (H2 in-memory, emitting on the consumer's request thread) is wrapped with
  pub/async-subscribe-pub so emission moves to m/blk - m/subscribe drops the
  element before onComplete from a synchronous publisher otherwise. An ASYNCHRONOUS
  driver (Netty PostgreSQL/MariaDB) is consumed BARE; wrapping it would add a
  second requester thread that reorders rows. Synchronicity is probed once per
  driver and cached.

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
   (io.r2dbc.spi Connection ConnectionFactory ConnectionFactoryMetadata
                 ConnectionMetadata Statement)
   (java.util.concurrent ConcurrentHashMap)
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
  row-xf (the builder, defaulting to kebab-maps) materializes each row before it
  is buffered, so buffered values are safe to batch across the bridge (raw
  un-materialized Rows would not be)."
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

(defn- build-streaming-publisher
  "Build the row Publisher consumed by the Missionary bridge.

  When db is a ConnectionFactory, Reactor Flux/usingWhen acquires the connection
  on subscription and closes it on completion/error/cancellation (close() runs
  exactly once on any terminal signal). When db is already a Connection, the
  caller owns the lifecycle and close() is NOT called (Flux/defer). In both cases
  the Statement is prepared/executed and rows-pub concatMaps each Result's rows;
  when opts carries :chunk-size the rows are batched into vectors via chunked-pub.

  Args:
    db     - ConnectionFactory or Connection (already resolved).
    sql    - SQL string to execute.
    params - sequential collection of bind parameters.
    opts   - options map (see execute for supported keys).
    row-xf - 1-arity fn [Row] -> emitted immutable value."
  ^Publisher [db sql params opts row-xf]
  (let [chunk-size (:chunk-size opts)
        use-fn     (reify Function
                     (apply [_ conn]
                       (let [rows (rows-pub (.execute ^Statement
                                             (stmt/prepare! conn sql params opts))
                                            row-xf)]
                         (if chunk-size (chunked-pub rows (long chunk-size)) rows))))]
    (if (instance? Connection db)
      (Flux/defer (reify Supplier (get [_] (.apply use-fn db))))
      (Flux/usingWhen (.create ^ConnectionFactory db)
                      use-fn
                      (reify Function
                        (apply [_ conn] (.close ^Connection conn)))))))

(defn- driver-key
  "Stable per-driver cache key for synchronous-db?: the R2DBC product/driver name
  (e.g. \"H2\", \"PostgreSQL\"). Synchronicity is a property of the driver, so a
  single probe per name serves every factory/connection of that driver."
  [db]
  (cond
    (instance? Connection db)
    (.getDatabaseProductName ^ConnectionMetadata (.getMetadata ^Connection db))
    (instance? ConnectionFactory db)
    (.getName ^ConnectionFactoryMetadata (.getMetadata ^ConnectionFactory db))
    :else (.getName ^Class (class db))))

(defonce ^:private ^ConcurrentHashMap synchronicity-cache (ConcurrentHashMap.))

(defn- synchronous-db?
  "Return true if db's driver emits result rows on the subscriber's own thread
  (a synchronous driver such as H2 in-memory) rather than on its own event loop
  (an asynchronous Netty driver such as PostgreSQL/MariaDB).

  Probed ONCE per driver via pub/synchronous-emission? over a representative
  SELECT 1, then cached by driver name. The result decides whether
  streaming-plan-flow interposes pub/async-subscribe-pub: synchronous drivers
  need it (m/subscribe drops the penultimate element of a synchronous publisher);
  asynchronous drivers must NOT get it (the extra requester thread reorders rows).
  The first stream per driver blocks briefly on the probe; all later streams hit
  the cache. Caveat: if the first stream for a driver is on a caller-owned
  Connection (with-connection/with-transaction) with a cold cache, the probe runs
  its SELECT 1 on that connection (then cancels) - one extra read, harmless for a
  plain query but worth knowing inside a transaction."
  [db]
  (.computeIfAbsent
   synchronicity-cache
   (driver-key db)
   (reify Function
     (apply [_ _]
       (pub/synchronous-emission?
        (build-streaming-publisher db "SELECT 1" [] {} (fn [_row] true)))))))

(defn streaming-plan-flow
  "Return a Missionary discrete flow emitting transformed rows with end-to-end
  demand-driven backpressure and managed connection lifecycle.

  Connection lifecycle: when db is a ConnectionFactory, the connection is
  acquired on subscription and closed on completion/error/cancellation via
  Reactor Flux/usingWhen. When db is already a Connection, the caller owns the
  lifecycle and close() is NOT called. See build-streaming-publisher.

  The row Publisher is bridged to a single discrete flow with m/subscribe and
  consumed by the caller's m/reduce/m/eduction - never m/?> over the bridge (see
  ns docstring). Statement fetchSize (from opts in stmt/prepare!) controls
  database fetch batching; :chunk-size batches rows into vectors below the bridge.

  Decoupling is conditional on the driver's emission model (synchronous-db?,
  cached per driver): a SYNCHRONOUS driver (H2) emits on the consumer's request
  thread, which m/subscribe mishandles (drops the element before onComplete), so
  the Publisher is wrapped with pub/async-subscribe-pub to emit on m/blk instead.
  An ASYNCHRONOUS driver (Netty PostgreSQL/MariaDB) already emits on its event
  loop and is consumed BARE - wrapping it would add a competing requester thread
  that reorders rows.

  Args:
    db     - ConnectionFactory or Connection (ConnectableWithOpts must be
             unwrapped by the caller via conn/resolve-connectable).
    sql    - SQL string to execute.
    params - sequential collection of bind parameters.
    opts   - options map (see execute for supported keys).
    row-xf - 1-arity fn applied to each raw R2DBC Row to produce the emitted
             immutable value (the builder, defaulting to kebab-maps)."
  [db sql params opts row-xf]
  (let [pub (build-streaming-publisher db sql params opts row-xf)]
    (if (synchronous-db? db)
      (m/subscribe (pub/async-subscribe-pub pub))
      (m/subscribe pub))))

(comment
  (require '[clj-r2dbc :as r2])
  (def db (r2/connect "r2dbc:h2:mem:///lifecycle-repl;DB_CLOSE_DELAY=-1"))
  (m/? (m/reduce conj [] (r2/stream db "SELECT 1 AS n")))
  (m/? (r2/with-conn [conn db]
         (m/reduce conj [] (r2/stream conn "SELECT 1 AS n"))))
  (m/? (m/reduce (fn [_ _] (reduced :done)) nil (r2/stream db "SELECT 1 AS n")))
  (try (m/? (m/reduce conj [] (r2/stream db "NOT VALID SQL")))
       (catch Exception e (ex-data e))))
