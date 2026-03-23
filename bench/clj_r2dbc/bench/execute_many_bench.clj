;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.bench.execute-many-bench
  "Benchmarks for execute-each multi-binding batch execution.

  The execute-each sanity gate verifies the batch path is instrumented and
  produces measurable throughput (1000-param-set gate).

  Uses an H2 in-memory database with a simple single-column table. Both
  suites use the `:one-shot` collect plan: database I/O, not JIT-compiled
  paths, dominates execution time, so `:with-jit-warmup` can't complete its
  sampling cycle within a reasonable time budget. A single sample is taken
  per bench invocation."
  (:require
   [clj-r2dbc :as r2dbc]
   [clj-r2dbc.bench.util :as bench-util]
   [clj-r2dbc.test-util.db :as db]
   [criterium.bench :as bench]))

(set! *warn-on-reflection* true)

(def ^:private bench-db
  (delay (r2dbc/connect
          "r2dbc:h2:mem:///bench-execute-many;DB_CLOSE_DELAY=-1")))

(defn- setup-table!
  []
  (let [cf @bench-db]
    (db/run-task! (r2dbc/execute cf "DROP TABLE IF EXISTS em_rows"))
    (db/run-task!
     (r2dbc/execute
      cf
      "CREATE TABLE em_rows (id INTEGER PRIMARY KEY, val VARCHAR(64))"))))

(defn- teardown-rows!
  []
  (db/run-task! (r2dbc/execute @bench-db "DELETE FROM em_rows")))

(defn- execute-many-count
  "Insert n rows via execute-each and return the number of binding results."
  [n]
  (teardown-rows!)
  (let [param-sets (mapv (fn [i] [i (str "val-" i)]) (range n))]
    (count (:clj-r2dbc/results (db/run-task!
                                (r2dbc/execute-each
                                 @bench-db
                                 "INSERT INTO em_rows (id, val) VALUES (?, ?)"
                                 param-sets))))))

(defn execute-many-100-bench
  "Run the execute-each benchmark with 100 param-sets."
  []
  (println "  execute-many-100 ...")
  (bench/bench (execute-many-count 100)
               :viewer :none
               :collect-plan {:scheme-type :one-shot})
  (bench-util/extract-result))

(defn execute-many-1000-bench
  "Run the execute-each benchmark with 1000 param-sets."
  []
  (println "  execute-many-1000 ...")
  (bench/bench (execute-many-count 1000)
               :viewer :none
               :collect-plan {:scheme-type :one-shot})
  (bench-util/extract-result))

(defn run-all
  []
  (println "  Setting up em_rows table...")
  (setup-table!)
  {:execute-many-100  (execute-many-100-bench)
   :execute-many-1000 (execute-many-1000-bench)})
