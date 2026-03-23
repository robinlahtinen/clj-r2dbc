;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.util
  "Internal utilities for Missionary task bridging and Reactive Streams collection.

  Provides:
    to-task      - lifts a stage return value into a Missionary task.
    void->task   - bridges a Publisher<Void> into a Missionary task.
    collect-pub  - subscribes to a Publisher and collects all emitted values into a vector.
    elapsed-ms   - returns milliseconds elapsed since a System/nanoTime snapshot.

  These are implementation details; do not use from application code."
  (:require
   [missionary.core :as m])
  (:import
   (java.util ArrayList)
   (java.util.concurrent CompletableFuture ExecutionException)
   (missionary Cancelled)
   (org.reactivestreams Publisher Subscriber Subscription)))

(set! *warn-on-reflection* true)

(defn to-task
  "Lift a pipeline stage return value into a Missionary task.

  When result is already a function (satisfies the Missionary task contract:
  a 2-arity fn returning a cancel-fn), it is returned as-is. Otherwise, the
  value is wrapped in (m/sp value) to produce a task that immediately resolves.

  Allows interceptor handlers to return either a plain context map
  (synchronous, minimal allocation) or an m/sp task (async, cancellable).

  Args:
    result - interceptor return value: a context map or a Missionary task."
  [result]
  (if (fn? result) result (m/sp result)))

(defn void->task
  "Return a Missionary task that resolves to nil when pub completes, or rejects on error.

  Used to bridge R2DBC lifecycle methods that return Publisher<Void>
  (e.g., beginTransaction, commitTransaction, rollbackTransaction,
  Connection.close) into Missionary tasks.

  m/subscribe converts the Publisher into a discrete flow; m/reduce drains
  it. Since Publisher<Void> emits no values, the result is always nil.

  Args:
    pub - Publisher<Void> to drain."
  [^Publisher pub]
  (m/reduce (fn [acc _] acc) nil (m/subscribe pub)))

(defn collect-pub
  "Return a Missionary task that subscribes to pub and collects all emitted values.

  Requests Long/MAX_VALUE from the Subscription so the Publisher can emit
  all values without back-pressure stalls. Uses CompletableFuture + m/via m/blk
  to avoid blocking the Missionary scheduler thread.

  Unwraps ExecutionException from CompletableFuture.get() so callers see the
  original exception type. On Missionary cancellation, cancels upstream.

  Args:
    pub - Publisher to collect from.

  Returns a Missionary task resolving to a vector of all emitted values."
  [^Publisher pub]
  (m/sp
   (let [acc     (ArrayList.)
         fut     (CompletableFuture.)
         sub-ref (volatile! nil)]
     (.subscribe pub
                 (reify
                   Subscriber
                   (onSubscribe [_ s]
                     (vreset! sub-ref s)
                     (.request ^Subscription s Long/MAX_VALUE))
                   (onNext [_ v] (.add acc v))
                   (onError [_ t] (.completeExceptionally fut t))
                   (onComplete [_] (.complete fut (vec acc)))))
     (try (m/? (m/via m/blk (.get ^CompletableFuture fut)))
          (catch ExecutionException ee (throw (or (.getCause ee) ee)))
          (catch Cancelled c
            (when-let [^Subscription s @sub-ref]
              (.cancel s)
              nil)
            (throw c))))))

(defn elapsed-ms
  "Return the milliseconds elapsed since the nanoTime snapshot t0 as a primitive double.

  Args:
    t0 - long value from a prior (System/nanoTime) call."
  ^double [t0]
  (/ (- (System/nanoTime) t0) 1e6))

(comment
  (def publisher-void nil)
  (def some-publisher nil)
  (m/? (to-task {:ok true}))
  (m/? (to-task (m/sp {:ok true})))
  (void->task publisher-void)
  (collect-pub some-publisher)
  (let [t0 (System/nanoTime)]
    (Thread/sleep 10)
    (elapsed-ms t0)))
