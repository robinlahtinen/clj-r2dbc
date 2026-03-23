;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.bench.row-to-map-bench
  "Benchmarks for row->map single-row and hundred-rows throughput."
  (:require
   [clj-r2dbc.bench.util :as bench-util]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.test-util.mock :as mock]
   [criterium.bench :as bench]
   [criterium.measured :as measured]))

(set! *warn-on-reflection* true)

(def ^:private col-specs
  [{:name "id", :java-type Long} {:name "name", :java-type String}
   {:name "email", :java-type String}])

(def ^:private rmd (mock/mock-row-metadata col-specs))

(def ^:private cache
  (row/build-metadata-cache rmd (fn [^String col _cm] (keyword col))))

(defn single-row-bench
  "Run a single row->map call benchmark."
  []
  (println "  single-row ...")
  (let [r (mock/mock-row
           {"id" (long 1), "name" "Alice", "email" "alice@example.com"}
           {:metadata rmd})
        m (measured/callable (fn [] (row/row->map r cache)))]
    (bench/bench-measured bench-util/default-bench-plan m)
    (bench-util/extract-result)))

(defn hundred-rows-bench
  "Run a 100-row row->map benchmark to validate O(N) scaling."
  []
  (println "  hundred-rows ...")
  (let [rows (mapv (fn [i]
                     (mock/mock-row {"id"    (long i)
                                     "name"  (str "Person-" i)
                                     "email" (str "p" i "@test.com")}
                                    {:metadata rmd}))
                   (range 100))
        m    (measured/callable (fn []
                                  (reduce (fn [^long acc r]
                                            (let [rm (row/row->map r cache)]
                                              (unchecked-add acc
                                                             (long (count rm)))))
                                          0
                                          rows)))]
    (bench/bench-measured bench-util/default-bench-plan m)
    (bench-util/extract-result)))

(defn run-all
  "Run all row-to-map benchmarks in JIT-warmup order.

  The hundred-rows bench runs first: its 100×row->map iterations per sample
  fully JIT-compile the hot path before single-row is measured,
  producing a stable and informative single-row timing."
  []
  {:hundred-rows (hundred-rows-bench), :single-row (single-row-bench)})
