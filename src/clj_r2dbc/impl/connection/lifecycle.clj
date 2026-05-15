;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.connection.lifecycle
  "Connection lifecycle management for streaming plan flows.

  When db is a ConnectionFactory: connection is acquired on flow start and
  closed on termination (normal/error/cancel).
  When db is already a Connection: the caller owns the lifecycle;
  Connection.close() is NOT called.

  This namespace owns stream connection lifecycle transitions.
  Flow state machines (r2dbc-row-flow, r2dbc-chunk-flow) live in impl/execute/stream.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.connection :as conn]
   [clj-r2dbc.impl.connection.publisher :as pub]
   [clj-r2dbc.impl.sql.statement :as stmt]
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Connection Statement)
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
    (let [wrapped-term
          (fn []
            (.subscribe ^Publisher (.close conn)
                        (reify
                          Subscriber
                          (onSubscribe [_ s]
                            (.request ^Subscription s Long/MAX_VALUE))
                          (onNext [_ _])
                          (onError [_ _] (terminator))
                          (onComplete [_] (terminator)))))]
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

(defn streaming-plan-flow
  "Return a Missionary discrete flow emitting transformed rows with end-to-end
  demand-driven backpressure.

  Connection lifecycle: when db is a ConnectionFactory, the connection is
  acquired at flow start (via conn/acquire-connection, which subscribes
  non-blockingly and parks on the m/blk executor, not ForkJoinPool) and
  closed on completion/error/cancellation. When db is already a Connection,
  the caller owns the lifecycle.

  Structure: an m/ap whose let-bindings (connection acquire, statement prep,
  row-pub construction, inner row-flow construction) evaluate once before the
  first m/?> emission; the inner row-flow is wrapped by close-conn-flow when
  owned, so close runs exactly once via the inner flow's signal-terminator!
  AtomicBoolean.

  row-flow-fn selects the per-row emission strategy:
    r2dbc-row-flow   - one row per deref() (default when not supplied).
    r2dbc-chunk-flow - one ArrayList per fetch-size batch per deref().

  Backpressure chain:
    consumer m/reduce -> m/?> wrapped-flow -> row-flow.deref()
      -> Subscription.request(fetch-size) -> R2DBC driver -> database cursor

  Args:
    db          - ConnectionFactory or Connection (ConnectableWithOpts must be
                  unwrapped by the caller via conn/resolve-connectable).
    sql         - SQL string to execute.
    params      - sequential collection of bind parameters.
    opts        - options map (see execute for supported keys).
    row-xf      - 1-arity fn applied to each raw R2DBC Row before buffering.
    row-flow-fn - fn [Publisher fetch-size xf] -> Missionary flow constructor."
  [db sql params opts row-xf row-flow-fn]
  (let [fetch-size (pub/resolve-fetch-size opts)
        owns-conn? (not (instance? Connection db))]
    (m/ap
     (let [^Connection conn                                                                        (m/? (conn/acquire-connection db))
           wrapped
           (try
             (let [^Statement stmt                      (stmt/prepare! conn sql params opts)
                   ^Publisher row-pub
                   (pub/result-rows-pub (.execute stmt)
                                        (volatile! nil)
                                        row-xf)
                   inner                                (row-flow-fn row-pub fetch-size identity)]
               (if owns-conn? (close-conn-flow inner conn) inner))
             (catch Throwable t
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
