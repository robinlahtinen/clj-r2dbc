;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.exec.pipeline
  "Interceptor pipeline runner for clj-r2dbc.

  Provides:
    run-pipeline - executes an interceptor chain with enter/execute/leave phases.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.util :as util]
   [missionary.core :as m])
  (:import
   (missionary Cancelled)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def ^:private max-interceptor-depth 64)

(declare run-error-stage)

(def ^:private sync-key ::sync)

(defn- sync-interceptor? [i] (true? (get i sync-key)))

(defn- fuse-sync-interceptors
  "Group adjacent interceptors into execution groups.

  Sync runs (::sync true) are grouped together so they can run in one
  m/sp checkpoint during enter/leave phases. Async/mixed interceptors are
  returned as singleton groups to preserve existing stage behavior."
  [interceptors]
  (->> interceptors
       (partition-by sync-interceptor?)
       (mapcat (fn [grp]
                 (if (and (seq grp) (sync-interceptor? (first grp)))
                   [{:sync? true, :interceptors (vec grp)}]
                   (map (fn [i] {:sync? false, :interceptors [i]}) grp))))
       vec))

(defn- run-sync-enter-group
  "Run a contiguous sync interceptor group enter stage in one checkpoint.

  Returns {:ctx ctx' :entered entered-interceptors} on success.
  On failure, invokes run-error-stage with already-entered interceptors in the
  same order as unfused execution."
  [ctx stack interceptors]
  (m/sp (loop [ctx     ctx
               entered []
               rem     interceptors]
          (if-let [i (first rem)]
            (let [ctx' (try (m/? (util/to-task ((:enter i identity) ctx)))
                            (catch Throwable t
                              (m/?
                               (run-error-stage ctx (into stack entered) t))))]
              (recur ctx' (conj entered i) (rest rem)))
            {:ctx ctx, :entered entered}))))

(defn- run-sync-leave-group
  "Run a contiguous sync interceptor leave stage in one checkpoint.

  Args:
    ctx          - current context map.
    base-stack   - interceptors below this sync run (bottom..top order).
    interceptors - sync run in leave order (top..bottom).

  Returns {:ctx ctx', :remaining remaining-stack}.
  On failure, invokes run-error-stage with the same remaining-stack that
  unfused leave execution would have used."
  [ctx base-stack interceptors]
  (m/sp (loop [ctx ctx
               rem interceptors]
          (if-let [i (first rem)]
            (let [next-rem (rest rem)
                  ctx'     (try (m/? (util/to-task ((:leave i identity) ctx)))
                                (catch Throwable t
                                  (m/? (run-error-stage ctx
                                                        (into base-stack
                                                              (reverse next-rem))
                                                        t))))]
              (recur ctx' next-rem))
            {:ctx ctx, :remaining base-stack}))))

(defn- run-error-stage
  "Walk the error stack in reverse, invoking :error on each interceptor.

  Stack semantics:
    Called from :enter  - stack holds interceptors whose :enter succeeded
    (does NOT include the interceptor whose :enter just failed).
    Called from :leave  - stack holds interceptors whose :leave has NOT yet run
    (does NOT include the interceptor whose :leave just failed).
    Called from execute-fn - stack holds all interceptors (all :enter succeeded).

  Secondary exceptions from :error handlers: missionary.Cancelled is re-thrown
  immediately. Any other Throwable is suppressed on the original t via
  .addSuppressed so the root cause is never lost.

  Always re-throws t after the loop completes - this is what propagates
  Missionary's cancellation chain and signals failure to the outer m/sp."
  [ctx stack ^Throwable t]
  (m/sp (loop [ctx   (assoc ctx :clj-r2dbc/error t)
               stack stack]
          (if-let [i (peek stack)]
            (recur (m/? (m/sp (try (m/? (util/to-task ((:error i identity)
                                                       ctx)))
                                   (catch Throwable t2
                                     (if (instance? Cancelled t2)
                                       (throw t2)
                                       (do (.addSuppressed t t2) ctx))))))
                   (pop stack))
            (throw t)))))

(defn run-pipeline
  "Execute an interceptor chain with enter/execute/leave phases.

  Args:
    ctx          - the initial context map.
    interceptors - sequential collection of interceptor maps.
    execute-fn   - 1-arity fn [ctx] -> task; performs the core operation.
    opts         - (optional) map; :max-interceptor-depth overrides the default 64.

  Returns a Missionary task resolving to the final context map on success,
  or rejecting with the original throwable on failure.

  Pipeline phases:
    1. Enter:   each interceptor's :enter handler runs in queue order (FIFO).
    2. Execute: execute-fn is called with the context after all :enter handlers.
    3. Leave:   each interceptor's :leave handler runs in reverse order (LIFO).

  On failure in any phase, run-error-stage is invoked with the appropriate
  stack of interceptors for cleanup. The original throwable is always re-thrown
  after error handlers complete.

  Each stage is isolated in a nested m/sp:
    JVM restriction: recur cannot appear in tail position inside try.
    Cancellation:    each m/? is a Missionary cancellation checkpoint.

  Throws (synchronously):
    ex-info :clj-r2dbc/limit-exceeded when interceptor chain exceeds
    :max-interceptor-depth (default 64).

  missionary.Cancelled is never swallowed - it propagates through error handlers
  and triggers physical Subscription.cancel() on R2DBC publishers."
  ([ctx interceptors execute-fn] (run-pipeline ctx interceptors execute-fn {}))
  ([ctx interceptors execute-fn opts]
   (m/sp
    (let [interceptor-count                                                      (count interceptors)
          max-depth
          (if-let [user-val (:max-interceptor-depth opts)]
            (do (when-not (and (integer? user-val) (pos? (long user-val)))
                  (throw
                   (ex-info
                    "Invalid :max-interceptor-depth; must be a positive integer"
                    {:clj-r2dbc/error-type :invalid-argument
                     :key                  :max-interceptor-depth
                     :value                user-val})))
                (long user-val))
            (long max-interceptor-depth))]
      (when (> interceptor-count max-depth)
        (throw (ex-info "interceptor chain depth exceeded"
                        {:clj-r2dbc/error   :clj-r2dbc/limit-exceeded
                         :clj-r2dbc/context :pipeline
                         :key               :interceptors
                         :constraint        :max-interceptor-depth
                         :limit             max-depth
                         :value             interceptor-count}))))
    (let [groups (fuse-sync-interceptors interceptors)]
      (loop [ctx    ctx
             groups groups
             stack  []]
        (if-let [g (first groups)]
          (let [sync?        (:sync? g)
                interceptors (:interceptors g)]
            (if sync?
              (let [result  (m/? (run-sync-enter-group ctx stack interceptors))
                    entered (:entered result)
                    ctx     (:ctx result)]
                (recur ctx (rest groups) (into stack entered)))
              (let [i    (first interceptors)
                    ctx' (m/? (m/sp (try (m/? (util/to-task ((:enter i identity)
                                                             ctx)))
                                         (catch Throwable t
                                           (m/?
                                            (run-error-stage ctx stack t))))))]
                (recur ctx' (rest groups) (conj stack i)))))
          (let [ctx' (m/? (m/sp (try (m/? (execute-fn ctx))
                                     (catch Throwable t
                                       (m/? (run-error-stage ctx stack t))))))]
            (loop [ctx   ctx'
                   stack stack]
              (if-let [top (peek stack)]
                (if (sync-interceptor? top)
                  (let [sync-run  (loop [rem stack
                                         run []]
                                    (if-let [i (peek rem)]
                                      (if (sync-interceptor? i)
                                        (recur (pop rem) (conj run i))
                                        run)
                                      run))
                        base      (loop [rem stack
                                         n   (count sync-run)]
                                    (if (pos? n) (recur (pop rem) (dec n)) rem))
                        result    (m/?
                                   (run-sync-leave-group ctx (vec base) sync-run))
                        ctx       (:ctx result)
                        remaining (:remaining result)]
                    (recur ctx remaining))
                  (let [remaining (pop stack)
                        ctx'      (m/? (m/sp (try (m/? (util/to-task
                                                        ((:leave top identity) ctx)))
                                                  (catch Throwable t
                                                    (m/? (run-error-stage ctx
                                                                          remaining
                                                                          t))))))]
                    (recur ctx' remaining)))
                ctx)))))))))

(comment
  (def interceptors
    [{:name  ::trace
      :enter (fn [ctx] (assoc ctx :entered true))
      :leave (fn [ctx] (assoc ctx :left true))
      :error (fn [ctx] (assoc ctx :error-seen true))}])
  (m/? (run-pipeline {:x 1}
                     interceptors
                     (fn [ctx] (m/sp (assoc ctx :executed true)))))
  (try (m/? (run-pipeline {:x 1}
                          interceptors
                          (fn [_ctx]
                            (m/sp (throw (ex-info "boom" {:cause :test}))))))
       (catch Exception e (ex-data e)))
  (def async-interceptors
    [{:name  ::async-trace
      :enter (fn [ctx] (m/sp (assoc ctx :async-entered true)))
      :leave (fn [ctx] (m/sp (assoc ctx :async-left true)))}])
  (m/? (run-pipeline {}
                     async-interceptors
                     (fn [ctx] (m/sp (assoc ctx :executed true)))))
  (try (m/? (run-pipeline {}
                          (repeat 3 {})
                          (fn [ctx] (m/sp ctx))
                          {:max-interceptor-depth 2}))
       (catch Exception e (select-keys (ex-data e) [:key :limit :value])))
  (def sync-interceptors
    [{::sync true, :enter (fn [ctx] (update ctx :n (fnil inc 0)))}
     {::sync true, :enter (fn [ctx] (update ctx :n (fnil inc 0)))}])
  (m/? (run-pipeline {:n 0} sync-interceptors (fn [ctx] (m/sp ctx)))))
