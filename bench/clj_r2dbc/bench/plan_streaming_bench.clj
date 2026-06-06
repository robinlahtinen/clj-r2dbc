;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.bench.plan-streaming-bench
  "Benchmarks for stream* streaming.

  Streaming bridges R2DBC's Publisher<Result> into a Missionary discrete flow via
  Missionary's reactive-streams adapter (m/subscribe). Per-row delivery does one
  reactive transfer per row; chunked delivery (:chunk-size) batches rows into
  size-N vectors at the Reactive Streams layer (pub/buffer-pub) so the consumer
  performs O(batches) transfers.

  All suites run over a real H2 in-memory database with a 10,000-row table, using
  the public r2dbc/stream API. R2DBC drivers honor request(n) demand; the prior
  synthetic mock publisher (io.r2dbc.spi.test MockResult) does not, so it is not
  used here. I/O and scheduling overhead dominate, so `:with-jit-warmup` cannot
  complete its sampling cycle within the time budget; a single sample is taken per
  bench invocation.

  Gate: the chunk-batching assertion is recalibrated to ABSOLUTE throughput
  (chunk-32 must comfortably beat the pre-redesign 6.7ms synthetic baseline) plus
  a relative sanity check (chunk-32 faster than chunk-1). The per-row path is now
  fast enough (~0.24us/row) that the old >=5x relative gate is no longer
  meaningful: both per-row and chunked throughput improved ~5-10x in absolute
  terms over the pre-redesign baseline, which compressed the relative ratio."
  (:require
   [clj-r2dbc :as r2dbc]
   [clj-r2dbc.bench.util :as bench-util]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.test-util.db :as db]
   [criterium.bench :as bench]
   [missionary.core :as m]))

(set! *warn-on-reflection* true)

(def ^:private bench-db
  (delay (r2dbc/connect "r2dbc:h2:mem:///bench-plan;DB_CLOSE_DELAY=-1")))

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
  "Count rows via per-row stream reduce with the given fetch-size."
  [fetch-size]
  (db/run-task! (m/reduce (fn [acc _] (inc acc))
                          0
                          (r2dbc/stream
                           @bench-db
                           "SELECT id, val FROM bench_rows ORDER BY id"
                           {:params     []
                            :builder    (row/make-row-fn)
                            :fetch-size fetch-size}))))

(defn- chunk-count
  "Count rows via chunked stream reduce with the given chunk-size.

  Chunking moves batching to the Reactive Streams layer (buffer-pub), so the
  consumer performs O(batches) transfers instead of O(rows)."
  [chunk-size]
  (db/run-task! (m/reduce (fn [^long acc chunk] (unchecked-add acc (count chunk)))
                          0
                          (r2dbc/stream
                           @bench-db
                           "SELECT id, val FROM bench_rows ORDER BY id"
                           {:params     []
                            :builder    (row/make-row-fn)
                            :chunk-size chunk-size}))))

(defn h2-fetch-size-1-bench
  "Per-row H2 streaming benchmark with fetch-size 1 at 10k rows."
  []
  (println "  h2-fetch-size-1 ...")
  (bench/bench (plan-count 1)
               :viewer :none
               :collect-plan :one-shot)
  (bench-util/extract-result))

(defn h2-fetch-size-32-bench
  "Per-row H2 streaming benchmark with fetch-size 32 at 10k rows."
  []
  (println "  h2-fetch-size-32 ...")
  (bench/bench (plan-count 32)
               :viewer :none
               :collect-plan :one-shot)
  (bench-util/extract-result))

(defn h2-fetch-size-256-bench
  "Per-row H2 streaming benchmark with fetch-size 256 at 10k rows."
  []
  (println "  h2-fetch-size-256 ...")
  (bench/bench (plan-count 256)
               :viewer :none
               :collect-plan :one-shot)
  (bench-util/extract-result))

(defn h2-chunk-fetch-size-1-bench
  "Chunked H2 streaming benchmark with chunk-size 1 at 10k rows (batching baseline)."
  []
  (println "  h2-chunk-fetch-size-1 ...")
  (bench/bench (chunk-count 1)
               :viewer :none
               :collect-plan :one-shot)
  (bench-util/extract-result))

(defn h2-chunk-fetch-size-32-bench
  "Chunked H2 streaming benchmark with chunk-size 32 at 10k rows (batching gate)."
  []
  (println "  h2-chunk-fetch-size-32 ...")
  (bench/bench (chunk-count 32)
               :viewer :none
               :collect-plan :one-shot)
  (bench-util/extract-result))

(defn run-all
  []
  (println "  Setting up bench_rows table with 10,000 rows...")
  (setup-table! 10000)
  {:h2-fetch-size-1        (h2-fetch-size-1-bench)
   :h2-fetch-size-32       (h2-fetch-size-32-bench)
   :h2-fetch-size-256      (h2-fetch-size-256-bench)
   :h2-chunk-fetch-size-1  (h2-chunk-fetch-size-1-bench)
   :h2-chunk-fetch-size-32 (h2-chunk-fetch-size-32-bench)})
