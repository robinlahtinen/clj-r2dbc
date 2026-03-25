;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.tck.streaming-contract-test
  (:require
   [clj-r2dbc :as r2dbc]
   [clj-r2dbc.impl.sql.cursor :as cursor]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.integration.fixtures :as fx]
   [clj-r2dbc.test-util.db :as db]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [missionary.core :as m])
  (:import
   (missionary Cancelled)))

(set! *warn-on-reflection* true)

(defonce ^:private tck-db
  (r2dbc/connect "r2dbc:h2:mem:///r2dbc-tck;DB_CLOSE_DELAY=-1"))

(use-fixtures :each (fn [test-fn] (fx/reset-people-table! tck-db) (test-fn)))

(deftest ^:tck stream-large-streaming-fetch-size-test
  (testing "stream handles large result sets with fetch-size chunking"
    (let [param-sets (mapv (fn [i] [i (str "Person-" i)]) (range 4 10004))]
      (db/run-task! (r2dbc/execute-each
                     tck-db
                     "INSERT INTO people (id, name) VALUES (?, ?)"
                     param-sets))
      (let [counted (db/run-task! (m/reduce
                                   (fn [acc _] (inc acc))
                                   0
                                   (r2dbc/stream
                                    tck-db
                                    "SELECT id, name FROM people ORDER BY id"
                                    {:params     []
                                     :builder    (row/make-row-fn)
                                     :fetch-size 32})))]
        (is (= 10003 counted))))))

(deftest ^:tck stream-cancellation-mid-stream-test
  (testing "stream cancellation stops consumption mid-stream without failures"
    (let [param-sets (mapv (fn [i] [i (str "Person-" i)]) (range 4 10004))]
      (db/run-task! (r2dbc/execute-each
                     tck-db
                     "INSERT INTO people (id, name) VALUES (?, ?)"
                     param-sets))
      (let [first-100
            (db/run-task!
             (m/reduce
              (fn [acc _]
                (let [next (inc acc)] (if (= 100 next) (reduced next) next)))
              0
              (r2dbc/stream
               tck-db
               "SELECT id, name FROM people ORDER BY id"
               {:params [], :builder (row/make-row-fn), :fetch-size 32})))]
        (is (= 100 first-100))))))

(defn- insert-n-people!
  "Insert n rows with ids starting at start-id into people."
  [n start-id]
  (let [param-sets (mapv (fn [i] [i (str "Person-" i)])
                         (range start-id (+ start-id n)))]
    (db/run-task! (r2dbc/execute-each
                   tck-db
                   "INSERT INTO people (id, name) VALUES (?, ?)"
                   param-sets))))

(deftest ^:tck chunked-cancel-propagation-test
  (testing
   "stream with reduced cancels cleanly and does not leak the connection"
    (insert-n-people! 10000 4)
    (let [first-50
          (db/run-task!
           (m/reduce
            (fn [acc _] (let [n (inc acc)] (if (= 50 n) (reduced n) n)))
            0
            (r2dbc/stream
             tck-db
             "SELECT id, name FROM people ORDER BY id"
             {:params [], :builder (row/make-row-fn), :fetch-size 32})))]
      (is (= 50 first-50))
      (let [rows (:clj-r2dbc/rows (db/run-task!
                                   (r2dbc/execute
                                    tck-db
                                    "SELECT COUNT(*) AS cnt FROM people")))]
        (is (= 10003 (:cnt (first rows))))))))

(deftest ^:tck long-max-value-demand-test
  (testing "execute with unbounded demand (Long/MAX_VALUE) handles 10003 rows"
    (insert-n-people! 10000 4)
    (let [rows (:clj-r2dbc/rows
                (db/run-task!
                 (r2dbc/execute tck-db "SELECT id FROM people ORDER BY id")))]
      (is (= 10003 (count rows)))
      (is (= 1 (:id (first rows))))
      (is (= 10003 (:id (last rows)))))))

(deftest ^:tck async-cancellation-mid-stream-test
  (testing "m/race cancels stream flow without exception"
    (insert-n-people! 10000 4)
    (let [result (try (db/run-task!
                       (m/race (m/sleep 0 :timeout)
                               (m/reduce (fn [acc _] (inc acc))
                                         0
                                         (r2dbc/stream
                                          tck-db
                                          "SELECT id FROM people ORDER BY id"
                                          {:params     []
                                           :builder    (row/make-row-fn)
                                           :fetch-size 32}))))
                      (catch Cancelled _ :cancelled)
                      (catch Throwable t t))]
      (is (or (= :timeout result) (= :cancelled result) (number? result))
          (str "Unexpected result: " result)))))

(deftest ^:tck fetch-size-boundary-exact-multiple-test
  (testing
   "stream with row count that is exact multiple of fetch-size returns all rows"
    (insert-n-people! 61 4)
    (let [counted (db/run-task! (m/reduce (fn [acc _] (inc acc))
                                          0
                                          (r2dbc/stream
                                           tck-db
                                           "SELECT id FROM people ORDER BY id"
                                           {:params     []
                                            :builder    (row/make-row-fn)
                                            :fetch-size 32})))]
      (is (= 64 counted)))))

(deftest ^:tck demand-driven-backpressure-test
  (testing "stream with early reduced consumes at most a bounded number of rows"
    (insert-n-people! 10000 4)
    (let [consumed (db/run-task!
                    (m/reduce (fn [acc _]
                                (let [n (inc acc)] (if (= 50 n) (reduced n) n)))
                              0
                              (r2dbc/stream tck-db
                                            "SELECT id FROM people ORDER BY id"
                                            {:params     []
                                             :builder    (row/make-row-fn)
                                             :fetch-size 32})))]
      (is (= 50 consumed))
      (let [rows (:clj-r2dbc/rows (db/run-task!
                                   (r2dbc/execute
                                    tck-db
                                    "SELECT COUNT(*) AS cnt FROM people")))]
        (is (= 10003 (:cnt (first rows))))))))

(deftest ^:tck flyweight-streaming-test
  (testing "stream without :builder streams rows via flyweight cursor"
    (insert-n-people! 97 4)
    (let [rows (db/run-task!
                (m/reduce
                 (fn [acc cursor]
                   (conj acc
                         (row/row->map (cursor/cursor-row cursor)
                                       (cursor/cursor-cache cursor))))
                 []
                 (r2dbc/stream
                  tck-db
                  "SELECT id, name FROM people ORDER BY id"
                  {:params [], :stream-mode :flyweight, :fetch-size 32})))]
      (is (= 100 (count rows)))
      (is (= 1 (:id (first rows))))
      (is (= 100 (:id (last rows)))))))

(deftest ^:tck stochastic-backpressure-property-test
  (testing
   "stream satisfies backpressure invariant across random fetch-size/row-count"
    (let [result
          (tc/quick-check
           50
           (prop/for-all
            [n-rows (gen/choose 0 1000) fetch-sz (gen/choose 1 256)]
            (fx/reset-people-table! tck-db)
            (when (pos? n-rows)
              (let [param-sets (mapv (fn [i] [(+ 4 i) (str "Person-" (+ 4 i))])
                                     (range n-rows))]
                (db/run-task! (r2dbc/execute-each
                               tck-db
                               "INSERT INTO people (id, name) VALUES (?, ?)"
                               param-sets))))
            (let [expected (+ 3 n-rows)
                  counted  (db/run-task! (m/reduce
                                          (fn [acc _] (inc acc))
                                          0
                                          (r2dbc/stream
                                           tck-db
                                           "SELECT id FROM people ORDER BY id"
                                           {:params     []
                                            :builder    (row/make-row-fn)
                                            :fetch-size fetch-sz})))]
              (= expected counted))))]
      (is (:result result)
          (str "Property failed after " (:num-tests result)
               " tests. Shrunk: " (pr-str (:shrunk result)))))))

(deftest ^:tck deterministic-backpressure-regression-test
  (testing
   "stream with n-rows=0 fetch-sz=61 returns exactly 3 seed rows (regression: volatile TOCTOU in lifecycle)"
    (fx/reset-people-table! tck-db)
    (let [counted (db/run-task! (m/reduce (fn [acc _] (inc acc))
                                          0
                                          (r2dbc/stream
                                           tck-db
                                           "SELECT id FROM people ORDER BY id"
                                           {:params     []
                                            :builder    (row/make-row-fn)
                                            :fetch-size 61})))]
      (is (= 3 counted)))))

(deftest ^:tck stochastic-early-termination-property-test
  (testing
   "stream with early reduced returns correct count across random parameters"
    (fx/reset-people-table! tck-db)
    (insert-n-people! 2000 4)
    (let [result (tc/quick-check
                  80
                  (prop/for-all
                   [stop-at (gen/choose 1 500) fetch-sz (gen/choose 1 128)]
                   (let [consumed (db/run-task!
                                   (m/reduce
                                    (fn [acc _]
                                      (let [n (inc acc)]
                                        (if (= stop-at n) (reduced n) n)))
                                    0
                                    (r2dbc/stream
                                     tck-db
                                     "SELECT id FROM people ORDER BY id"
                                     {:params     []
                                      :builder    (row/make-row-fn)
                                      :fetch-size fetch-sz})))]
                     (= stop-at consumed))))]
      (is (:result result)
          (str "Property failed after " (:num-tests result)
               " tests. Shrunk: " (pr-str (:shrunk result)))))))
