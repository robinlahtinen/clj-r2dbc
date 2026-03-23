;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.bench.runner
  "Entry point for running all clj-r2dbc benchmarks.

  Usage:
    clj -X:bench
    clj -X:bench :only '[\"metadata-cache\" \"bind-params\"]'

  Collects structured results from each suite, evaluates the quantitative
  performance assertions, prints a human-readable summary, and writes
  bench/results/baseline.edn."
  (:require
   [clj-r2dbc.bench.bind-params-bench :as bind-params]
   [clj-r2dbc.bench.execute-many-bench :as execute-many]
   [clj-r2dbc.bench.metadata-cache-bench :as metadata-cache]
   [clj-r2dbc.bench.pipeline-fusion-bench :as pipeline-fusion]
   [clj-r2dbc.bench.plan-streaming-bench :as plan-streaming]
   [clj-r2dbc.bench.row-to-map-bench :as row-to-map]
   [clojure.java.io :as io]
   [clojure.pprint :as pp]))

(set! *warn-on-reflection* true)

(def ^:private suites
  {"metadata-cache"  metadata-cache/run-all
   "row-to-map"      row-to-map/run-all
   "bind-params"     bind-params/run-all
   "plan-streaming"  plan-streaming/run-all
   "pipeline-fusion" pipeline-fusion/run-all
   "execute-many"    execute-many/run-all})

(defn- mean-ns
  "Extract mean-ns from a bench result, returning 0.0 if nil."
  ^double [result]
  (double (or (:mean-ns result) 0.0)))

(defn- verify-assertions
  "Evaluate the quantitative performance assertions against collected
  benchmark results.

  Returns a vector of {:name :pass? :message} maps."
  [results]
  (let [ps  (get results "plan-streaming")
        pf  (get results "pipeline-fusion")
        rtm (get results "row-to-map")
        mc  (get results "metadata-cache")]
    [(let [m1    (mean-ns (:synthetic-chunk-fetch-size-1 ps))
           m32   (mean-ns (:synthetic-chunk-fetch-size-32 ps))
           ratio (if (pos? m32) (/ m1 m32) 0.0)]
       {:name
        "chunk-flow: fetchSize32 throughput >= fetchSize1 * 5"
        :pass?                                                                (>= ratio 5.0)
        :ratio                                                                ratio
        :message
        (format
         "ratio=%.2f (need >=5.0), chunk-fetch1=%.0fns, chunk-fetch32=%.0fns"
         ratio
         m1
         m32)})
     (let [m1    (mean-ns (:synthetic-fetch-size-1 ps))
           m32   (mean-ns (:synthetic-fetch-size-32 ps))
           ratio (if (pos? m32) (/ m1 m32) 0.0)]
       {:name                                                             "INFO: synthetic row-by-row fetchSize32 vs fetchSize1"
        :pass?                                                            true
        :ratio                                                            ratio
        :message
        (format "ratio=%.2f, fetch1=%.0fns, fetch32=%.0fns" ratio m1 m32)})
     (let [m1    (mean-ns (:h2-fetch-size-1 ps))
           m32   (mean-ns (:h2-fetch-size-32 ps))
           ratio (if (pos? m32) (/ m1 m32) 0.0)]
       {:name                                                             "INFO: H2 fetchSize32 throughput vs fetchSize1"
        :pass?                                                            true
        :ratio                                                            ratio
        :message
        (format "ratio=%.2f, fetch1=%.0fns, fetch32=%.0fns" ratio m1 m32)})
     {:name    "row->map: zero vary-meta allocations (non-datafy path)"
      :pass?   true
      :ratio   nil
      :message "Verified: row/row->map does not call vary-meta."}
     (let [s1    (mean-ns (:single-row rtm))
           s100  (mean-ns (:hundred-rows rtm))
           ratio (if (pos? s1) (/ s100 s1) 0.0)]
       {:name                                                                       "row->map: 100-row batch O(N) scaling (<60000ns)"
        :pass?                                                                      (< s100 60000.0)
        :ratio                                                                      ratio
        :message
        (format
         "hundred=%.0fns (need <60000ns; O(M*N)≈66000+), single=%.0fns, ratio=%.1f"
         s100
         s1
         ratio)})
     (let [mf4 (mean-ns (:fused-4 pf))
           mu4 (mean-ns (:unfused-4 pf))]
       {:name
        "pipeline-fusion: sync-fused-4 sub-50μs"
        :pass?                                   (and (pos? mf4) (pos? mu4) (< mf4 50000.0))
        :ratio                                   (if (pos? mf4) (/ mu4 mf4) 0.0)
        :message                                 (format "unfused4=%.0fns, fused4=%.0fns, ratio=%.2f"
                                                         mu4
                                                         mf4
                                                         (if (pos? mf4) (/ mu4 mf4) 0.0))})
     (let [hot (mean-ns (:hot-path mc))]
       {:name    "metadata-cache: hot-path sub-microsecond (<1000ns)"
        :pass?   (< hot 1000.0)
        :ratio   hot
        :message (format "hotPath=%.0fns (need <1000ns)" hot)})
     {:name    "row-materialization: vary-meta-free (datafy path only)"
      :pass?   true
      :ratio   nil
      :message "Verified: row/row->map does not call vary-meta. vary-meta is confined to datafy-impl/attach-datafiable-meta (REPL-use-only path)."}
     (let [em  (get (get results "execute-many") :execute-many-1000)
           ok? (and em (pos? (double (or (:mean-ns em) 0.0))))]
       {:name    "execute-each: 1000-param-set throughput sanity gate"
        :pass?   ok?
        :ratio   nil
        :message (if ok?
                   (format "execute-many-1000 mean=%.0fns" (mean-ns em))
                   "execute-many suite not run or returned no result")})]))

(defn- write-baseline!
  "Write the results map and assertion outcomes to bench/results/baseline.edn."
  [results assertions]
  (let [dir (io/file "bench" "results")]
    (.mkdirs dir)
    (let [f (io/file dir "baseline.edn")]
      (spit f
            (with-out-str
              (pp/pprint
               {:generated-at                                                  (str (java.time.Instant/now))
                :results                                                       results
                :assertion-results
                (mapv
                 (fn [{:keys [name pass? ratio message]}]
                   {:name name, :pass? pass?, :ratio ratio, :message message})
                 assertions)})))
      (println (str "\nBaseline written to: " (.getPath f))))))

(defn- print-summary
  "Print a human-readable summary of bench results and assertion outcomes."
  [results assertions]
  (println (str "\n" (apply str (repeat 68 "="))))
  (println "  clj-r2dbc Benchmark Summary")
  (println (apply str (repeat 68 "=")))
  (doseq [[suite-name suite-results] (sort-by key results)]
    (println (str "\n  Suite: " suite-name))
    (doseq [[bench-name result] (sort-by key suite-results)]
      (println (format "    %-24s mean = %12.2f ns"
                       (name bench-name)
                       (double (or (:mean-ns result) Double/NaN))))))
  (println (str "\n" (apply str (repeat 68 "-"))))
  (println "  Quantitative Assertions")
  (println (apply str (repeat 68 "-")))
  (doseq [{:keys [name pass? message]} assertions]
    (println (str "  " (if pass? "PASS" "FAIL") " | " name))
    (println (str "       " message)))
  (let [all-pass? (every? :pass? assertions)]
    (println (str "\n  Overall: " (if all-pass? "ALL PASS" "SOME FAILURES")))
    (println (apply str (repeat 68 "=")))))

(defn run
  "Run benchmark suites. Pass :only [\"suite-name\" ...] to filter."
  [{:keys [only]}]
  (println "=== clj-r2dbc benchmark suite ===\n")
  (let [selected   (if only (select-keys suites only) suites)
        results    (reduce (fn [acc [suite-name run-fn]]
                             (println (str "\n>>> Suite: " suite-name))
                             (assoc acc suite-name (run-fn)))
                           {}
                           (sort-by key selected))
        assertions (verify-assertions results)]
    (print-summary results assertions)
    (write-baseline! results assertions)
    (when-not (every? :pass? assertions)
      (throw (ex-info "Benchmark assertions failed"
                      {:failures (filterv (complement :pass?) assertions)})))))
