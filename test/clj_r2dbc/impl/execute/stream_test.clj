;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.execute.stream-test
  "Tests for clj-r2dbc.impl.execute.stream streaming flow.

  Uses a real H2 in-memory database because plan* bridges R2DBC Publishers via
  CompletableFuture, which is incompatible with missionary-testkit's virtual
  clock. All flows are driven with db/run-task! + m/reduce.

  Database: r2dbc:h2:mem:///stream-test (shared across tests in this namespace)
  Setup: test_table created once before all tests via use-fixtures :once."
  (:require
   [clj-r2dbc.impl.connection :as conn]
   [clj-r2dbc.impl.connection.lifecycle :as life]
   [clj-r2dbc.impl.datafy :as datafy-impl]
   [clj-r2dbc.impl.execute.stream :as stream]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.impl.sql.statement :as stmt]
   [clj-r2dbc.impl.util :as util]
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Connection Row Statement)))

(set! *warn-on-reflection* true)

(defn- get-factory
  []
  (conn/create-connection-factory* {:url "r2dbc:h2:mem:///stream-test"}))

(defn- close-conn!
  [conn]
  (db/run-task! (util/void->task (.close ^Connection conn))))

(defn- setup-plan-db!
  [keeper-conn]
  (db/drop-fixtures! keeper-conn)
  (db/insert-fixtures! keeper-conn))

(defn- warm-tracking-synchronicity!
  "Prime lifecycle/synchronous-db?'s per-driver probe for the 'tracking-mock'
  factory. That probe opens and closes one connection the first time a driver is
  streamed; priming it on a throwaway factory here means the close-counting tests
  below measure only their own stream's close, not the one-time probe's,
  regardless of test order."
  []
  (let [stmt              (mock/mock-statement
                           (mock/mock-result
                            {:rows [(mock/mock-row (array-map "id" 1))]}))
        {:keys [factory]} (mock/tracking-connection-factory {:statement stmt})]
    (db/run-task!
     (m/reduce (constantly nil) nil
               (stream/stream* factory "SELECT 1" []
                               {:builder-fn (row/make-row-fn)})))))

(use-fixtures :once
  (fn [test-fn]
    (let [cf          (get-factory)
          keeper-conn (db/run-task! (conn/acquire-connection cf))]
      (try (setup-plan-db! keeper-conn)
           (warm-tracking-synchronicity!)
           (test-fn)
           (finally (close-conn! keeper-conn))))))

(def ^:private select-all "SELECT id, name FROM test_table ORDER BY id")

(def ^:private select-none "SELECT id, name FROM test_table WHERE id = -999")

(deftest plan-empty-result-test
  (testing "plan* on empty result set completes with zero emissions"
    (let [cf     (get-factory)
          result (db/run-task!
                  (m/reduce conj [] (stream/stream* cf select-none [] {})))]
      (is (= [] result)))))

(deftest plan-rows-with-builder-fn-test
  (testing "plan* with :builder-fn emits one immutable map per row, in order"
    (let [cf                                                                    (get-factory)
          rows
          (db/run-task!
           (m/reduce
            conj
            []
            (stream/stream* cf select-all [] {:builder-fn (row/make-row-fn)})))]
      (is (= 3 (count rows)))
      (is (= 1 (:id (first rows))))
      (is (= "Alice" (:name (first rows))))
      (is (= 2 (:id (second rows))))
      (is (= "Bob" (:name (second rows))))
      (is (= 3 (:id (nth rows 2))))
      (is (= "Carol" (:name (nth rows 2)))))))

(deftest plan-builder-fn-rows-are-not-datafiable-by-default-test
  (testing
   "plan* with :builder-fn emits retained maps without execute/first-row Datafiable metadata"
    (let [cf                                                                    (get-factory)
          rows
          (db/run-task!
           (m/reduce
            conj
            []
            (stream/stream* cf select-all [] {:builder-fn (row/make-row-fn)})))]
      (is (= 3 (count rows)))
      (is (every? map? rows))
      (is (every? #(not (true? (get (meta %) datafy-impl/marker-key))) rows)))))

(deftest plan-builder-fn-distinct-instances-test
  (testing "with :builder-fn, each emitted value is a distinct persistent map"
    (let [cf                                                                    (get-factory)
          rows
          (db/run-task!
           (m/reduce
            conj
            []
            (stream/stream* cf select-all [] {:builder-fn (row/make-row-fn)})))]
      (is (= 3 (count rows)))
      (is (= 3 (count (into #{} (map #(System/identityHashCode %) rows))))))))

(deftest plan-default-builder-kebab-maps-test
  (testing
   "without :builder-fn, stream* defaults to kebab-maps and emits one immutable
   map per row, in order - the single row representation after :flyweight removal"
    (let [cf   (get-factory)
          rows (db/run-task!
                (m/reduce conj [] (stream/stream* cf select-all [] {})))]
      (is (= 3 (count rows)))
      (is (every? map? rows))
      (is (= [1 2 3] (mapv :id rows)))
      (is (= ["Alice" "Bob" "Carol"] (mapv :name rows))))))

(deftest ^:pattern-1 plan-default-builder-retention-safe-test
  (testing
   "default-builder rows are immutable per row: retaining every emitted value and
   reading it only after the stream completes still yields correct per-row data.
   Regression guard for the request-ahead corruption seen on the 2-core CI runner."
    (let [cf   (get-factory)
          rows (db/run-task!
                (m/reduce conj [] (stream/stream* cf select-all [] {})))]
      (is (= 3 (count rows)))
      (is (= [1 2 3] (mapv :id rows)))
      (is (= ["Alice" "Bob" "Carol"] (mapv :name rows))))))

(deftest plan-chunk-default-builder-test
  (testing
   "without :builder-fn, :chunk-size still streams - it only changes the emission
   unit (vectors of built values), using the same default kebab-maps builder.
   Regression guard for the decomplected :chunk-size/:builder contract."
    (let [cf     (get-factory)
          chunks (db/run-task!
                  (m/reduce conj [] (stream/stream* cf select-all [] {:chunk-size 2})))]
      (is (= 2 (count chunks)) "3 rows at chunk-size 2 -> [2 1]")
      (is (every? vector? chunks))
      (is (every? (fn [chunk] (every? map? chunk)) chunks))
      (is (= [[{:id 1 :name "Alice"} {:id 2 :name "Bob"}]
              [{:id 3 :name "Carol"}]]
             chunks)))))

(deftest plan-cancellation-test
  (testing "plan* flow can be cancelled mid-stream; connection is cleaned up"
    (let [cf                                                                    (get-factory)
          result
          (db/run-task!
           (m/reduce
            (fn [_ v] (reduced v))
            nil
            (stream/stream* cf select-all [] {:builder-fn (row/make-row-fn)})))]
      (is (map? result))
      (is (= 1 (:id result)))
      (is (= "Alice" (:name result))))))

(deftest ^:pattern-12 plan-cold-stream-resubscription-test
  (testing
   "plan* is cold - two sequential reductions on the same flow produce identical results"
    (let [cf    (get-factory)
          flow  (stream/stream* cf select-all [] {:builder-fn (row/make-row-fn)})
          rows1 (db/run-task! (m/reduce conj [] flow))
          rows2 (db/run-task! (m/reduce conj [] flow))]
      (is (= 3 (count rows1)))
      (is (= 3 (count rows2)))
      (is (= rows1 rows2)))))

(deftest ^:pattern-12 plan-cold-stream-two-independent-flows-test
  (testing
   "plan* creates independent flows - two concurrent flow instances do not share state"
    (let [cf                                                                    (get-factory)
          rows1
          (db/run-task!
           (m/reduce
            conj
            []
            (stream/stream* cf select-all [] {:builder-fn (row/make-row-fn)})))
          rows2
          (db/run-task!
           (m/reduce
            conj
            []
            (stream/stream* cf select-all [] {:builder-fn (row/make-row-fn)})))]
      (is (= 3 (count rows1)))
      (is (= 3 (count rows2)))
      (is (= rows1 rows2)))))

(deftest plan-with-direct-connection-test
  (testing
   "plan* with an existing Connection does not close it after flow completes"
    (let [cf   (get-factory)
          conn (db/run-task! (conn/acquire-connection cf))]
      (try (let [rows (db/run-task! (m/reduce conj
                                              []
                                              (stream/stream*
                                               conn
                                               select-all
                                               []
                                               {:builder-fn
                                                (row/make-row-fn)})))]
             (is (= 3 (count rows)))
             (is (some? (conn/connection-metadata* conn))))
           (finally (close-conn! conn))))))

;; result-rows-flow / result-chunks-flow are exercised against a real H2 database:
;; the r2dbc-spi-test MockResult.map publisher ignores Reactive Streams demand
;; (it floods all rows on the first request), which Missionary's m/subscribe
;; bridge - a single-slot, strictly demand-driven consumer - cannot represent.
;; Real R2DBC drivers honor request(n), so these use real Results.

(deftest result-rows-flow-multi-result-order-test
  (testing "result-rows-flow flattens rows from multiple Results in order"
    (let [cf   (get-factory)
          conn (db/run-task! (conn/acquire-connection cf))]
      (try
        (let [^Statement s (stmt/prepare!
                            conn
                            (str "SELECT id FROM test_table WHERE id <= 2 ORDER BY id;"
                                 " SELECT id FROM test_table WHERE id = 3")
                            []
                            {:fetch-size 1})
              ids          (db/run-task!
                            (m/reduce conj []
                                      (life/result-rows-flow
                                       (.execute s)
                                       (fn [^Row r] (.get r 0 Integer)))))]
          (is (= [1 2 3] ids)))
        (finally (close-conn! conn))))))

(deftest result-rows-flow-cancellation-test
  (testing "early termination stops result-rows-flow after the reduced value"
    (let [cf   (get-factory)
          conn (db/run-task! (conn/acquire-connection cf))]
      (try
        (let [^Statement s (stmt/prepare! conn "SELECT id FROM test_table ORDER BY id"
                                          [] {:fetch-size 1})
              seen         (db/run-task!
                            (m/reduce (fn [acc _]
                                        (let [n (inc acc)] (if (= 2 n) (reduced n) n)))
                                      0
                                      (life/result-rows-flow
                                       (.execute s)
                                       (fn [^Row r] (.get r 0 Integer)))))]
          (is (= 2 seen)))
        (finally (close-conn! conn))))))

(deftest result-rows-flow-error-test
  (testing "result-rows-flow propagates an upstream error"
    (let [cf   (get-factory)
          conn (db/run-task! (conn/acquire-connection cf))]
      (try
        (is (thrown? Throwable
                     (db/run-task!
                      (m/reduce conj []
                                (life/result-rows-flow
                                 (.execute (stmt/prepare! conn "SELECT nope FROM nope" [] {}))
                                 identity)))))
        (finally (close-conn! conn))))))

(deftest result-chunks-flow-batches-test
  (testing "result-chunks-flow emits size-N vectors with a short final batch"
    (let [cf   (get-factory)
          conn (db/run-task! (conn/acquire-connection cf))]
      (try
        (let [^Statement s (stmt/prepare! conn "SELECT id FROM test_table ORDER BY id"
                                          [] {:fetch-size 2})
              chunks       (db/run-task!
                            (m/reduce conj []
                                      (life/result-chunks-flow
                                       (.execute s)
                                       (fn [^Row r] (.get r 0 Integer))
                                       2)))]
          (is (= [[1 2] [3]] chunks))
          (is (every? vector? chunks)))
        (finally (close-conn! conn))))))

(deftest ^:pattern-17 plan-cancellation-closes-connection-test
  (testing "plan* closes the acquired connection when cancelled mid-stream"
    (let [result                        (mock/mock-result {:rows [(mock/mock-row (array-map "id" 1))
                                                                  (mock/mock-row (array-map "id" 2))
                                                                  (mock/mock-row (array-map "id" 3))]})
          stmt                          (mock/mock-statement result)
          {:keys [factory close-count]} (mock/tracking-connection-factory
                                         {:statement stmt})]
      (db/run-task! (m/reduce (fn [_ v] (reduced v))
                              nil
                              (stream/stream* factory
                                              "SELECT id FROM ignored"
                                              []
                                              {:builder-fn (row/make-row-fn)})))
      (is (= 1 @close-count)))))

(deftest ^:pattern-17 plan-normal-completion-closes-connection-test
  (testing "plan* closes the acquired connection after normal flow completion"
    (let [result                        (mock/mock-result {:rows [(mock/mock-row (array-map "id" 1))
                                                                  (mock/mock-row (array-map "id" 2))]})
          stmt                          (mock/mock-statement result)
          {:keys [factory close-count]} (mock/tracking-connection-factory
                                         {:statement stmt})]
      (db/run-task! (m/reduce conj
                              []
                              (stream/stream* factory
                                              "SELECT id FROM ignored"
                                              []
                                              {:builder-fn (row/make-row-fn)})))
      (is (= 1 @close-count)))))

(deftest plan-multiple-results-order-test                   ;; test 13
  (testing "stream* streams rows from multiple Result objects in order"
    ;; Real H2 returns one Result per statement in a multi-statement execute;
    ;; result-rows-flow flattens them in order.
    (let [cf   (get-factory)
          rows (db/run-task!
                (m/reduce conj []
                          (stream/stream*
                           cf
                           (str "SELECT id, name FROM test_table WHERE id <= 2 ORDER BY id;"
                                " SELECT id, name FROM test_table WHERE id = 3")
                           []
                           {:builder-fn (row/make-row-fn)})))]
      (is (= [{:id 1, :name "Alice"} {:id 2, :name "Bob"} {:id 3, :name "Carol"}]
             rows)))))
