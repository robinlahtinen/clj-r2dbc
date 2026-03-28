;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.bench.plan-streaming-bench
  "Benchmarks for plan* streaming.

  The chunk-batching assertion must measure the library-controlled streaming
  path rather than a particular driver's fetchSize implementation, because
  R2DBC drivers may legally ignore the fetch-size hint. This suite therefore
  contains two views:

  - synthetic row-flow benchmarks using the repo's demand-driven mock Publisher,
    which isolate clj-r2dbc's fetch-size batching overhead and drive the gate
  - H2 end-to-end plan benchmarks, which remain useful observational numbers but
    are not used for the quantitative chunk-batching assertion

  All suites use the `:one-shot` collect plan. Synthetic suites drive 10,000
  rows per invocation, placing each sample in the millisecond range. H2 suites
  are I/O-bound. In both cases, I/O or scheduling overhead—not JIT-compiled
  paths—dominates throughput, so `:with-jit-warmup` can't complete its
  sampling cycle within the time budget. A single sample is taken per bench
  invocation."
  (:require
   [clj-r2dbc :as r2dbc]
   [clj-r2dbc.bench.util :as bench-util]
   [clj-r2dbc.impl.execute.stream :as stream]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.mock :as mock]
   [criterium.bench :as bench]
   [missionary.core :as m]))

(set! *warn-on-reflection* true)

(def ^:private bench-db
  (delay (r2dbc/connect "r2dbc:h2:mem:///bench-plan;DB_CLOSE_DELAY=-1")))

(def ^:private synthetic-rows
  (delay (mapv (fn [i]
                 (mock/mock-row (array-map "id" (long i) "val" (str "row-" i))))
               (range 10000))))

(def ^:private row-builder
  (let [build-row (row/make-row-fn)]
    (fn [^io.r2dbc.spi.Row r] (build-row r (.getMetadata r)))))

(defn- setup-table!
  "Create and populate bench_rows with n rows."
  [n]
  (let [cf @bench-db]
    (db/run-task! (r2dbc/execute cf "DROP TABLE IF EXISTS bench_rows"))
    (db/run-task!
     (r2dbc/execute
      cf
      "CREATE TABLE bench_rows (id INTEGER PRIMARY KEY, val VARCHAR(64))"))
    (let [param-sets (mapv (fn [i] [i (str "row-" i)]) (range n))]
      (db/run-task! (r2dbc/execute-each
                     cf
                     "INSERT INTO bench_rows (id, val) VALUES (?, ?)"
                     param-sets)))))

(defn- plan-count
  "Count rows via plan* reduce with given fetch-size."
  [fetch-size]
  (db/run-task! (m/reduce (fn [acc _] (inc acc))
                          0
                          (r2dbc/stream
                           @bench-db
                           "SELECT id, val FROM bench_rows ORDER BY id"
                           {:params     []
                            :builder    (row/make-row-fn)
                            :fetch-size fetch-size}))))

(defn- synthetic-plan-count
  "Count rows through r2dbc-row-flow using a demand-driven mock Publisher.

  This isolates the library-owned batching path from any driver-specific
  Statement.fetchSize behavior."
  [fetch-size]
  (let [{:keys [publisher]} (mock/tracking-publisher {:items @synthetic-rows})]
    (db/run-task!
     (m/reduce (fn [acc _] (inc acc))
               0
               (#'stream/r2dbc-row-flow publisher fetch-size row-builder)))))

(defn- synthetic-plan-count-chunked
  "Count rows through r2dbc-chunk-flow using a demand-driven mock Publisher.

  Uses identity as row-xf to isolate pure scheduling overhead (Missionary steps
  reduced from O(rows) to O(batches)) from row-building cost. The chunk-batching
  assertion measures this scheduling improvement: at fetch-size=32, steps drop
  from 10,000 to 313, yielding a >=5x throughput improvement.

  Using row-builder as row-xf would mix in row-transform cost (T_xf≈2.4μs/row),
  which scales O(fetch-size) per batch and cancels out the scheduling benefit."
  [fetch-size]
  (let [{:keys [publisher]} (mock/tracking-publisher {:items @synthetic-rows})]
    (db/run-task! (m/reduce
                   (fn [^long acc ^java.util.ArrayList chunk]
                     (unchecked-add acc (.size chunk)))
                   0
                   (#'stream/r2dbc-chunk-flow publisher fetch-size identity)))))

(defn synthetic-fetch-size-1-bench
  "Run the synthetic row-flow benchmark with fetch-size 1 at 10k rows."
  []
  (println "  synthetic-fetch-size-1 ...")
  (bench/bench (synthetic-plan-count 1)
               :viewer :none
               :collect-plan {:scheme-type :one-shot})
  (bench-util/extract-result))

(defn synthetic-fetch-size-32-bench
  "Run the synthetic row-flow benchmark with fetch-size 32 at 10k rows."
  []
  (println "  synthetic-fetch-size-32 ...")
  (bench/bench (synthetic-plan-count 32)
               :viewer :none
               :collect-plan {:scheme-type :one-shot})
  (bench-util/extract-result))

(defn synthetic-fetch-size-256-bench
  "Run the synthetic row-flow benchmark with fetch-size 256 at 10k rows."
  []
  (println "  synthetic-fetch-size-256 ...")
  (bench/bench (synthetic-plan-count 256)
               :viewer :none
               :collect-plan {:scheme-type :one-shot})
  (bench-util/extract-result))

(defn synthetic-chunk-fetch-size-1-bench
  "Run the synthetic chunk-flow benchmark with fetch-size 1 at 10k rows (chunk-batching baseline)."
  []
  (println "  synthetic-chunk-fetch-size-1 ...")
  (bench/bench (synthetic-plan-count-chunked 1)
               :viewer :none
               :collect-plan {:scheme-type :one-shot})
  (bench-util/extract-result))

(defn synthetic-chunk-fetch-size-32-bench
  "Run the synthetic chunk-flow benchmark with fetch-size 32 at 10k rows (chunk-batching gate)."
  []
  (println "  synthetic-chunk-fetch-size-32 ...")
  (bench/bench (synthetic-plan-count-chunked 32)
               :viewer :none
               :collect-plan {:scheme-type :one-shot})
  (bench-util/extract-result))

(defn h2-fetch-size-1-bench
  "Run the H2-backed plan* streaming benchmark with fetch-size 1 at 10k rows."
  []
  (println "  h2-fetch-size-1 ...")
  (bench/bench (plan-count 1)
               :viewer :none
               :collect-plan {:scheme-type :one-shot})
  (bench-util/extract-result))

(defn h2-fetch-size-32-bench
  "Run the H2-backed plan* streaming benchmark with fetch-size 32 at 10k rows."
  []
  (println "  h2-fetch-size-32 ...")
  (bench/bench (plan-count 32)
               :viewer :none
               :collect-plan {:scheme-type :one-shot})
  (bench-util/extract-result))

(defn h2-fetch-size-256-bench
  "Run the H2-backed plan* streaming benchmark with fetch-size 256 at 10k rows."
  []
  (println "  h2-fetch-size-256 ...")
  (bench/bench (plan-count 256)
               :viewer :none
               :collect-plan {:scheme-type :one-shot})
  (bench-util/extract-result))

(defn run-all
  []
  (println "  Setting up bench_rows table with 10,000 rows...")
  (setup-table! 10000)
  {:synthetic-fetch-size-1        (synthetic-fetch-size-1-bench)
   :synthetic-fetch-size-32       (synthetic-fetch-size-32-bench)
   :synthetic-fetch-size-256      (synthetic-fetch-size-256-bench)
   :synthetic-chunk-fetch-size-1  (synthetic-chunk-fetch-size-1-bench)
   :synthetic-chunk-fetch-size-32 (synthetic-chunk-fetch-size-32-bench)
   :h2-fetch-size-1               (h2-fetch-size-1-bench)
   :h2-fetch-size-32              (h2-fetch-size-32-bench)
   :h2-fetch-size-256             (h2-fetch-size-256-bench)})
