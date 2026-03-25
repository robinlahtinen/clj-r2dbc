;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.conn.lifecycle
  "Connection lifecycle management for streaming plan flows.

  When db is a ConnectionFactory: connection is acquired on flow start and
  closed on termination (normal/error/cancel).
  When db is already a Connection: the caller owns the lifecycle;
  Connection.close() is NOT called.

  This namespace owns stream connection lifecycle transitions.
  Flow state machines (r2dbc-row-flow, r2dbc-chunk-flow) live in impl/exec/stream.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.conn.publisher :as pub]
   [clj-r2dbc.impl.sql.statement :as stmt])
  (:import
   (clojure.lang IDeref IFn)
   (io.r2dbc.spi Connection ConnectionFactory Statement)
   (java.util.concurrent CompletableFuture CountDownLatch)
   (java.util.concurrent.atomic AtomicBoolean)
   (missionary Cancelled)
   (org.reactivestreams Publisher Subscription)))

(set! *warn-on-reflection* true)

(defn streaming-plan-flow
  "Return a Missionary flow emitting transformed rows with end-to-end
  demand-driven backpressure.

  Connection lifecycle: when db is a ConnectionFactory, the connection is
  acquired at flow start and closed on completion/error/cancellation.
  When db is already a Connection, the caller owns the lifecycle.

  row-flow-fn selects the per-row emission strategy:
    r2dbc-row-flow   - one row per deref() (default when not supplied).
    r2dbc-chunk-flow - one ArrayList per fetch-size batch per deref().

  Backpressure chain:
    consumer m/reduce -> m/?> row-flow -> row-flow-fn.deref()
      -> Subscription.request(fetch-size) -> R2DBC driver -> database cursor

  Args:
    db          - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql         - SQL string to execute.
    params      - sequential collection of bind parameters.
    opts        - options map (see execute for supported keys).
    row-xf      - 1-arity fn applied to each raw R2DBC Row before buffering.
    row-flow-fn - fn [Publisher fetch-size xf] -> Missionary flow constructor."
  ([db sql params opts row-xf row-flow-fn]
   (let [fetch-size (pub/resolve-fetch-size opts)]
     (fn [notifier terminator]
       (let [state                                                       (Object.)
             owns-conn?                                                  (not (instance? Connection db))
             conn-ref                                                    (volatile! nil)
             conn-sub-ref                                                (volatile! nil)
             result-sub-ref                                              (volatile! nil)
             process-ref                                                 (volatile! nil)
             init-error-ref                                              (volatile! nil)
             cancel-ref                                                  (volatile! false)
             ^AtomicBoolean term-ref                                     (AtomicBoolean. false)
             init-latch                                                  (CountDownLatch. 1)
             signal-terminator!
             (fn signal-terminator []
               (when (.compareAndSet term-ref false true) (terminator)))
             close-owned-conn!                                           (fn close-owned-conn []
                                                                           (when (and owns-conn? @conn-ref)
                                                                             (pub/await-void-pub! (.close ^Connection
                                                                                                   @conn-ref))))
             wrapped-terminator                                          (fn wrapped-terminator []
                                                                           (close-owned-conn!)
                                                                           (signal-terminator!))]
         (CompletableFuture/runAsync
          (fn []
            (try (let [^Connection c (if owns-conn?
                                       (pub/first-pub-blocking
                                        (.create ^ConnectionFactory db)
                                        conn-sub-ref)
                                       db)
                       _             (vreset! conn-ref c)]
                   (if @cancel-ref
                     (do (wrapped-terminator)
                         (.countDown ^CountDownLatch init-latch))
                     (let [^Statement s                                          (stmt/prepare! c sql params opts)
                           row-pub                                               (pub/result-rows-pub (.execute s)
                                                                                                      result-sub-ref
                                                                                                      row-xf)
                           process
                           ((row-flow-fn ^Publisher row-pub fetch-size identity)
                            notifier
                            wrapped-terminator)]
                       (vreset! process-ref process)
                       (.countDown ^CountDownLatch init-latch)
                       (when @cancel-ref (.invoke ^IFn process)))))
                 (catch Throwable t
                   (vreset! init-error-ref t)
                   (try (close-owned-conn!) (catch Throwable _))
                   (.countDown ^CountDownLatch init-latch)
                   (notifier)))))
         (reify
           IFn
           (invoke [_]
             (let [[process conn-sub result-sub] (locking state
                                                   (vreset! cancel-ref true)
                                                   [@process-ref @conn-sub-ref
                                                    @result-sub-ref])]
               (when conn-sub (.cancel ^Subscription conn-sub))
               (when result-sub (.cancel ^Subscription result-sub))
               (if process
                 (.invoke ^IFn process)
                 (do (try (close-owned-conn!) (catch Throwable _))
                     (signal-terminator!))))
             nil)
           IDeref
           (deref [_]
             (cond
               @init-error-ref (throw ^Throwable @init-error-ref)
               @process-ref (.deref ^IDeref @process-ref)
               @cancel-ref (throw (Cancelled.
                                   "Streaming plan flow cancelled."))
               :else
               (do
                 (.await ^CountDownLatch init-latch)
                 (cond
                   @init-error-ref (throw ^Throwable @init-error-ref)
                   @cancel-ref (throw (Cancelled.
                                       "Streaming plan flow cancelled."))
                   @process-ref (.deref ^IDeref @process-ref)
                   :else
                   (throw
                    (IllegalStateException.
                     "streaming-plan-flow: latch released with no terminal state (invariant violated)"))))))))))))

(comment
  (require '[clj-r2dbc :as r2] '[missionary.core :as m])
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
