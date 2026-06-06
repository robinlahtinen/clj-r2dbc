;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.connection.lifecycle
  "Connection lifecycle management for streaming plan flows.

  When db is a ConnectionFactory: connection is acquired on flow start and
  closed on termination (normal/error/cancel).
  When db is already a Connection: the caller owns the lifecycle;
  Connection.close() is NOT called.

  This namespace owns stream connection lifecycle transitions and the
  Publisher<Result> -> row flow bridge (result-rows-flow), built on Missionary's
  reactive-streams adapter (m/subscribe). Chunking and flyweight transforms are
  applied by the caller (impl/execute/stream) via m/eduction.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.debug-log :as dbg]
   [clj-r2dbc.impl.connection :as conn]
   [clj-r2dbc.impl.connection.publisher :as pub]
   [clj-r2dbc.impl.sql.statement :as stmt]
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Connection Result Statement)
   (org.reactivestreams Publisher Subscriber Subscription)))

(set! *warn-on-reflection* true)

(defn- close-conn-flow
  "Wrap inner-flow so that when its terminator fires (normal completion, error,
  or cancellation), Connection.close() is invoked once and the wrapper's
  terminator fires when the close Publisher<Void> completes.

  The wrapper passes through invoke (cancel) and deref to inner-flow unchanged;
  only the terminator is intercepted. inner-flow's signal-terminator! uses an
  AtomicBoolean to guarantee wrapped-term fires at most once."
  [inner-flow ^Connection conn]
  (fn [notifier terminator]
    (let [conn-id                                                                  (str "CC" (Integer/toHexString (System/identityHashCode conn)))
          wrapped-term
          (fn []
            (dbg/dlog conn-id " wrapped-term ENTER -> subscribe (.close conn)")
            (.subscribe ^Publisher (.close conn)
                        (reify
                          Subscriber
                          (onSubscribe [_ s]
                            (.request ^Subscription s Long/MAX_VALUE))
                          (onNext [_ _])
                          (onError [_ t]
                            (dbg/dlog conn-id " close onError -> terminator type="
                                      (.getSimpleName (class t)))
                            (terminator))
                          (onComplete [_]
                            (dbg/dlog conn-id " close onComplete -> terminator")
                            (terminator)))))]
      (inner-flow notifier wrapped-term))))

(defn- fire-and-forget-close!
  "Subscribe to Connection.close() and discard the result. Used on the error
  path between connection acquisition and row-flow construction, when no flow
  exists yet to own the close lifecycle."
  [^Connection conn]
  (try
    (.subscribe ^Publisher (.close conn)
                (reify
                  Subscriber
                  (onSubscribe [_ s]
                    (.request ^Subscription s Long/MAX_VALUE))
                  (onNext [_ _])
                  (onError [_ _])
                  (onComplete [_])))
    (catch Throwable _ nil)))

(defn result-rows-flow
  "Return a Missionary discrete flow emitting transformed rows from every Result
  produced by result-pub, flattened in order.

  Delegates row delivery to Missionary's reactive-streams bridge (m/subscribe):
  the outer Publisher<Result> is consumed one Result at a time (m/?> defaults to
  parallelism 1), and each Result's Publisher<Row> (from result-row-pub) is
  consumed one row at a time. Demand-driven backpressure, cancellation, and
  termination are handled by m/subscribe. Database round-trip batching is
  governed independently by Statement.fetchSize (see statement/apply-opts!), so
  one-row-at-a-time reactive demand does not change network fetch granularity.

  Subscriptions are dispatched on m/blk (pub/async-subscribe-pub) to keep driver
  callbacks off the consumer's ForkJoinPool threads and to avoid synchronous
  re-entrancy during flow construction.

  row-xf is applied inside result-row-pub's BiFunction while the Row's backing
  ByteBuf is guaranteed live; see publisher/result-row-pub for the safety
  argument.

  Args:
    result-pub - Publisher<Result> from Statement.execute().
    row-xf     - 1-arity fn [Row] -> value applied to each row."
  [^Publisher result-pub row-xf]
  (m/ap
   (let [^Result result (m/?> (m/subscribe (pub/async-subscribe-pub result-pub)))]
     (m/?> (m/subscribe (pub/async-subscribe-pub
                         (pub/result-row-pub result row-xf)))))))

(defn result-chunks-flow
  "Like result-rows-flow, but emits one vector of up to chunk-size transformed
  rows per value instead of one row.

  Batching happens at the Reactive Streams layer via pub/buffer-pub, so the
  downstream m/subscribe performs O(batches) transfers rather than O(rows) -
  recovering the per-batch scheduling win. Batches are per-Result: a partial
  final batch is emitted at each Result boundary (identical to per-stream
  batching for the single-Result common case).

  Args:
    result-pub - Publisher<Result> from Statement.execute().
    row-xf     - 1-arity fn [Row] -> value applied to each row.
    chunk-size - positive number of rows per emitted vector."
  [^Publisher result-pub row-xf ^long chunk-size]
  (m/ap
   (let [^Result result (m/?> (m/subscribe (pub/async-subscribe-pub result-pub)))]
     (m/?> (m/subscribe (pub/async-subscribe-pub
                         (pub/buffer-pub (pub/result-row-pub result row-xf)
                                         chunk-size)))))))

(defn streaming-plan-flow
  "Return a Missionary discrete flow emitting transformed rows with end-to-end
  demand-driven backpressure.

  Connection lifecycle: when db is a ConnectionFactory, the connection is
  acquired at flow start (via conn/acquire-connection, which subscribes
  non-blockingly and parks on the m/blk executor, not ForkJoinPool) and
  closed on completion/error/cancellation. When db is already a Connection,
  the caller owns the lifecycle.

  Structure: an m/ap whose let-bindings (connection acquire, statement prep,
  row flow construction) evaluate once before the first m/?> emission; the inner
  flow is wrapped by close-conn-flow when owned, so Connection.close() runs
  exactly once on the inner flow's terminator (the wrapper's AtomicBoolean-gated
  terminator). Cleanup rides the terminator rather than a try/finally inside
  m/ap, whose finally would re-run per emission.

  The inner flow is result-rows-flow (one row per value) or, when opts carries
  :chunk-size, result-chunks-flow (one vector of up to :chunk-size rows per
  value). Both bridge the Publisher<Result> via m/subscribe. Statement.fetchSize
  (applied from opts in stmt/prepare!) controls database fetch batching.

  Args:
    db     - ConnectionFactory or Connection (ConnectableWithOpts must be
             unwrapped by the caller via conn/resolve-connectable).
    sql    - SQL string to execute.
    params - sequential collection of bind parameters.
    opts   - options map (see execute for supported keys); :chunk-size selects
             chunked emission.
    row-xf - 1-arity fn applied to each raw R2DBC Row to produce the emitted
             value (identity in flyweight mode)."
  [db sql params opts row-xf]
  (let [owns-conn? (not (instance? Connection db))
        chunk-size (:chunk-size opts)]
    (m/ap
     (let [^Connection conn                                                                        (m/? (conn/acquire-connection db))
           wrapped
           (try
             (let [^Statement stmt (stmt/prepare! conn sql params opts)
                   inner           (if chunk-size
                                     (result-chunks-flow (.execute stmt) row-xf (long chunk-size))
                                     (result-rows-flow (.execute stmt) row-xf))]
               (if owns-conn? (close-conn-flow inner conn) inner))
             (catch Throwable t
               (dbg/dlog "SPF init error type=" (.getSimpleName (class t)))
               (when owns-conn? (fire-and-forget-close! conn))
               (throw t)))]
       (m/?> wrapped)))))

(comment
  (require '[clj-r2dbc :as r2])
  (def db (r2/connect "r2dbc:h2:mem:///lifecycle-repl;DB_CLOSE_DELAY=-1"))
  (m/? (m/reduce conj [] (r2/stream db "SELECT 1 AS n")))
  (m/? (r2/with-conn [conn db]
         (m/reduce conj [] (r2/stream conn "SELECT 1 AS n"))))
  (m/? (m/reduce (fn [_ _] (reduced :done)) nil (r2/stream db "SELECT 1 AS n")))
  (try (m/? (m/reduce conj [] (r2/stream db "NOT VALID SQL")))
       (catch Exception e (ex-data e)))
  (m/? (m/reduce conj
                 []
                 (m/ap
                  (let [_ (m/?> ##Inf (m/seed (range 50)))]
                    (m/? (m/reduce conj [] (r2/stream db "SELECT 1 AS n"))))))))
