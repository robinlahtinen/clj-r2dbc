;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.bench.pipeline-fusion-bench
  "Benchmarks for pipeline interceptor fusion at 4 and 16 interceptors."
  (:require
   [clj-r2dbc.bench.util :as bench-util]
   [clj-r2dbc.impl.exec.pipeline :as pipeline]
   [clj-r2dbc.test-util.db :as db]
   [criterium.bench :as bench]
   [criterium.measured :as measured]
   [missionary.core :as m]))

(set! *warn-on-reflection* true)

(defn- sync-interceptor
  "Return a sync interceptor that bumps :counter in ctx."
  [label]
  {:name                              label
   :clj-r2dbc.impl.exec.pipeline/sync true
   :enter                             (fn [ctx] (update ctx :counter (fnil inc 0)))
   :leave                             (fn [ctx] (update ctx :counter (fnil inc 0)))})

(defn- async-interceptor
  "Return an async (non-fused) interceptor that bumps :counter in ctx."
  [label]
  {:name  label
   :enter (fn [ctx] (update ctx :counter (fnil inc 0)))
   :leave (fn [ctx] (update ctx :counter (fnil inc 0)))})

(defn- run-pipeline!
  [interceptors]
  (db/run-task! (pipeline/run-pipeline
                 {:counter 0}
                 interceptors
                 (fn [ctx] (m/sp (update ctx :counter (fnil inc 0)))))))

(defn unfused-4-bench
  "Run the 4-interceptor async (unfused) pipeline benchmark."
  []
  (println "  unfused-4 ...")
  (let [ics (mapv #(async-interceptor (str "async-" %)) (range 4))
        m   (measured/callable (fn [] (run-pipeline! ics)))]
    (bench/bench-measured bench-util/default-bench-plan m)
    (bench-util/extract-result)))

(defn fused-4-bench
  "Run the 4-interceptor sync (fused) pipeline benchmark."
  []
  (println "  fused-4 ...")
  (let [ics (mapv #(sync-interceptor (str "sync-" %)) (range 4))
        m   (measured/callable (fn [] (run-pipeline! ics)))]
    (bench/bench-measured bench-util/default-bench-plan m)
    (bench-util/extract-result)))

(defn unfused-16-bench
  "Run the 16-interceptor async (unfused) pipeline benchmark."
  []
  (println "  unfused-16 ...")
  (let [ics (mapv #(async-interceptor (str "async-" %)) (range 16))
        m   (measured/callable (fn [] (run-pipeline! ics)))]
    (bench/bench-measured bench-util/default-bench-plan m)
    (bench-util/extract-result)))

(defn fused-16-bench
  "Run the 16-interceptor sync (fused) pipeline benchmark."
  []
  (println "  fused-16 ...")
  (let [ics (mapv #(sync-interceptor (str "sync-" %)) (range 16))
        m   (measured/callable (fn [] (run-pipeline! ics)))]
    (bench/bench-measured bench-util/default-bench-plan m)
    (bench-util/extract-result)))

(defn run-all
  []
  {:unfused-4  (unfused-4-bench)
   :fused-4    (fused-4-bench)
   :unfused-16 (unfused-16-bench)
   :fused-16   (fused-16-bench)})
