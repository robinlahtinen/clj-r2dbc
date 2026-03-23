;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.test-util.virtual-time
  "Wraps missionary-testkit for use in clojure.test fixtures.

  Provides:
    with-virtual-clock - use-fixtures adapter that installs the virtual clock
    run-task           - runs a Missionary task in virtual time to completion
    advance!           - advances virtual time by a given duration in ms
    check-interleaving - thin wrapper around mt/check-interleaving

  Note: m/subscribe and m/publisher are not supported under deterministic
  mode. Tests that bridge R2DBC Publishers should use run-task! from
  clj-r2dbc.test-util.db instead."
  (:require
   [de.levering-it.missionary-testkit :as mt]))

(set! *warn-on-reflection* true)

(defn with-virtual-clock
  "Install the missionary-testkit virtual clock for the duration of each test.

  Usage:
    (use-fixtures :each vt/with-virtual-clock)

  Inside the fixture scope, m/sleep, m/timeout, and m/via-call are rebound
  to virtual-time equivalents. Use run-task or the mt/ API to drive
  scheduler execution."
  [test-fn]
  (mt/with-determinism (test-fn)))

(defn make-scheduler
  "Create a missionary-testkit TestScheduler. Re-export of mt/make-scheduler.

  Use this when you need direct scheduler control (step!, tick!, etc.).
  Most tests should prefer run-task instead.

  (make-scheduler)
  (make-scheduler {:seed 42 :trace? true})"
  ([] (mt/make-scheduler))
  ([opts] (mt/make-scheduler opts)))

(defn run-task
  "Run a Missionary task in virtual time to completion and return the result.
  Automatically create a scheduler and drive execution with auto-advancing time.

  Must be called inside with-virtual-clock (or mt/with-determinism).

  Options (optional second arg):
    :seed         - RNG seed for scheduler (default: nil = FIFO)
    :max-steps    - maximum scheduler steps (default: 100000)
    :max-time-ms  - maximum virtual time in ms (default: 60000)
    :trace?       - enable tracing (default: false)

  (run-task (m/sp :done))
  (run-task (m/sp (m/? (m/sleep 100)) :slept) {:trace? true})"
  ([task] (run-task task {}))
  ([task
    {:keys [seed max-steps max-time-ms trace?]
     :or   {max-steps 100000, max-time-ms 60000, trace? false}}]
   (let [sched (mt/make-scheduler (cond-> {:trace? trace?}
                                    seed (assoc :seed seed)))]
     (mt/run sched task {:max-steps max-steps, :max-time-ms max-time-ms}))))

(defn advance!
  "Advance virtual time by dt-ms milliseconds on the given scheduler,
  then drain all due microtasks.

  (advance! sched 100)"
  [sched dt-ms]
  (mt/advance! sched dt-ms))

(defn now-ms
  "Return the current virtual time in milliseconds from the given scheduler.

  (now-ms sched)"
  [sched]
  (mt/now-ms sched))

(defn collect
  "Collect a Missionary flow into a task yielding a vector.
  Convenience wrapper around mt/collect.

  (collect (m/seed [1 2 3]))"
  ([flow] (mt/collect flow))
  ([flow opts] (mt/collect flow opts)))

(defn check-interleaving
  "Run a task factory with many random interleavings to find concurrency bugs.

  Must be called inside with-virtual-clock (or mt/with-determinism).

  task-fn is a 0-arg function returning a fresh Missionary task.
  Mutable state (atoms, etc.) should be created inside task-fn so it
  resets between iterations.

  Options:
    :num-tests   - number of interleavings to try (default: 100)
    :seed        - base seed for reproducibility
    :property    - (fn [result] boolean) predicate for valid results

  Returns {:ok? true/false ...}. On failure, includes :schedule and :seed
  for deterministic replay via mt/replay.

  (check-interleaving
    (fn [] (m/sp (m/? (m/join vector task-a task-b))))
    {:num-tests 200 :seed 42 :property (fn [r] (= 2 (count r)))})"
  [task-fn opts]
  (mt/check-interleaving task-fn opts))
