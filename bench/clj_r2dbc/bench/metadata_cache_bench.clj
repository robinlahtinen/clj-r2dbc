;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.bench.metadata-cache-bench
  "Benchmarks for RowMetadataCache cold-start and hot-path read."
  (:require
   [clj-r2dbc.bench.util :as bench-util]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.test-util.mock :as mock]
   [criterium.bench :as bench]
   [criterium.measured :as measured]))

(set! *warn-on-reflection* true)

(def ^:private col-specs
  [{:name "id", :java-type Long} {:name "name", :java-type String}
   {:name "email", :java-type String} {:name "age", :java-type Long}
   {:name "active", :java-type Boolean}])

(def ^:private rmd (mock/mock-row-metadata col-specs))

(def ^:private qfn (fn [^String col _cm] (keyword col)))

(defn cold-start-bench
  "Run the build-metadata-cache cold-start benchmark."
  []
  (println "  cold-start ...")
  (bench/bench (row/build-metadata-cache rmd qfn)
               :viewer :none
               :limit-time-s 10)
  (bench-util/extract-result))

(defn hot-path-bench
  "Run the row->map benchmark with a pre-built cache (hot path)."
  []
  (println "  hot-path ...")
  (let [cache (row/build-metadata-cache rmd qfn)
        r     (mock/mock-row {"id"     (long 1)
                              "name"   "Alice"
                              "email"  "alice@example.com"
                              "age"    (long 30)
                              "active" true}
                             {:metadata rmd})
        m     (measured/callable (fn [] (row/row->map r cache)))]
    (bench/bench-measured bench-util/default-bench-plan m)
    (bench-util/extract-result)))

(defn run-all [] {:cold-start (cold-start-bench), :hot-path (hot-path-bench)})
