;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.integration.h2-test
  (:require
   [clj-r2dbc :as r2dbc]
   [clj-r2dbc.integration.fixtures :as fx]
   [clj-r2dbc.row :as row]
   [clj-r2dbc.test-util.db :as db]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [missionary.core :as m]))

(set! *warn-on-reflection* true)

(defonce ^:private db (fx/h2-db))

(use-fixtures :each (fn [test-fn] (fx/reset-people-table! db) (test-fn)))

(deftest ^:integration execute-select-roundtrip-test
  (testing "execute returns maps from H2"
    (let [rows (fx/exec! db "SELECT id, name FROM people ORDER BY id" [])]
      (is (= [{:id 1, :name "Alice"} {:id 2, :name "Bob"}
              {:id 3, :name "Carol"}]
             rows)))))

(deftest ^:integration execute-one-empty-result-test
  (testing "first-row returns nil for no rows"
    (is (nil?
         (fx/exec-one! db "SELECT id, name FROM people WHERE id = ?" [999])))))

(deftest ^:integration execute-each-insert-test
  (testing "execute-each inserts parameter sets and query reflects new rows"
    (fx/exec-many! db
                   "INSERT INTO people (id, name) VALUES (?, ?)"
                   [[10 "Dan"] [11 "Eve"]])
    (let [rows
          (fx/exec! db "SELECT id FROM people WHERE id >= 10 ORDER BY id" [])]
      (is (= [{:id 10} {:id 11}] rows)))))

(deftest ^:integration execute-batch-update-counts-test
  (testing "batch returns per-statement update counts"
    (let [counts (fx/exec-batch! db
                                 ["DELETE FROM people WHERE id = 1"
                                  "DELETE FROM people WHERE id = 2"])]
      (is (= 2 (count counts)))
      (is (every? number? counts))
      (is (= [{:id 3}] (fx/exec! db "SELECT id FROM people ORDER BY id" []))))))

(deftest ^:integration plan-builder-flow-test
  (testing "stream emits materialized rows when a builder is provided"
    (let [rows (fx/plan->vec db
                             "SELECT id, name FROM people ORDER BY id"
                             []
                             {:builder row/kebab-maps})]
      (is (= [{:id 1, :name "Alice"} {:id 2, :name "Bob"}
              {:id 3, :name "Carol"}]
             rows)))))

(deftest ^:integration plan-large-result-fetch-size-test
  (testing
   "stream supports large result sets with :fetch-size and early cancellation"
    (let [param-sets (mapv (fn [i] [i (str "Person-" i)]) (range 4 10004))]
      (fx/exec-many! db
                     "INSERT INTO people (id, name) VALUES (?, ?)"
                     param-sets)
      (let [first-row (db/run-task!
                       (m/reduce (fn [_ v] (reduced v))
                                 nil
                                 (r2dbc/stream
                                  db
                                  "SELECT id, name FROM people ORDER BY id"
                                  {:builder row/kebab-maps, :fetch-size 32})))
            rows      (fx/plan->vec db
                                    "SELECT id FROM people ORDER BY id"
                                    []
                                    {:builder row/kebab-maps, :fetch-size 32})]
        (is (= {:id 1, :name "Alice"} first-row))
        (is (= 10003 (count rows)))))))
