;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.exec.stream-test
  "Tests for clj-r2dbc.impl.exec.stream streaming flow.

  Uses a real H2 in-memory database because plan* bridges R2DBC Publishers via
  CompletableFuture, which is incompatible with missionary-testkit's virtual
  clock. All flows are driven with db/run-task! + m/reduce.

  Database: r2dbc:h2:mem:///stream-test (shared across tests in this namespace)
  Setup: test_table created once before all tests via use-fixtures :once."
  (:require
   [clj-r2dbc.impl.conn.core :as conn]
   [clj-r2dbc.impl.conn.lifecycle :as lifecycle]
   [clj-r2dbc.impl.conn.publisher :as pub]
   [clj-r2dbc.impl.datafy :as datafy-impl]
   [clj-r2dbc.impl.exec.stream :as stream]
   [clj-r2dbc.impl.sql.cursor :as cursor]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.impl.util :as util]
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [missionary.core :as m])
  (:import
   (clj_r2dbc.impl.sql.cursor RowCursor)
   (clojure.lang ExceptionInfo IDeref IFn)
   (io.r2dbc.spi Connection Row)))

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

(use-fixtures :once
  (fn [test-fn]
    (let [cf          (get-factory)
          keeper-conn (db/run-task! (conn/acquire-connection cf))]
      (try (setup-plan-db! keeper-conn)
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

(deftest plan-cursor-flyweight-same-instance-test
  (testing
   "without :builder-fn, all emitted values are the same mutable RowCursor instance"
    (let [cf      (get-factory)
          cursors (db/run-task!
                   (m/reduce conj [] (stream/stream* cf select-all [] {})))]
      (is (= 3 (count cursors)))
      (is (every? #(instance? RowCursor %) cursors))
      (let [first-cursor (first cursors)]
        (is (every? #(identical? first-cursor %) cursors))))))

(deftest plan-cursor-read-immediately-test
  (testing
   "RowCursor data is correct when materialised immediately within each m/reduce step"
    (let [cf   (get-factory)
          rows (db/run-task! (m/reduce (fn [acc cursor]
                                         (conj acc
                                               (row/row->map
                                                (cursor/cursor-row cursor)
                                                (cursor/cursor-cache cursor))))
                                       []
                                       (stream/stream* cf select-all [] {})))]
      (is (= 3 (count rows)))
      (is (= 1 (:id (first rows))))
      (is (= "Alice" (:name (first rows))))
      (is (= 2 (:id (second rows))))
      (is (= 3 (:id (nth rows 2)))))))

(deftest plan-stale-row-guard-test
  (testing "retaining cursor-row across emissions throws IllegalStateException"
    (let [cf    (get-factory)
          rows  (db/run-task! (m/reduce (fn [acc cursor]
                                          (conj acc (cursor/cursor-row cursor)))
                                        []
                                        (stream/stream* cf select-all [] {})))
          stale (first rows)]
      (is (thrown-with-msg? IllegalStateException
                            #"RowCursor advanced; stale row reference."
                            (.get ^Row stale (int 0) Object))))))

(deftest ^:pattern-1 plan-generation-counter-monotonic-test
  (testing "generation counter increments exactly once per row advance"
    (let [cf   (get-factory)
          gens (db/run-task! (m/reduce
                              (fn [acc cursor]
                                (conj acc (cursor/cursor-generation cursor)))
                              []
                              (stream/stream* cf select-all [] {})))]
      (is (= 3 (count gens)))
      (is (apply < gens))
      (is (every? #(= 1 %) (map - (rest gens) gens))))))

(deftest ^:pattern-1 plan-stale-row-guard-fires-exactly-after-advance-test
  (testing
   "GenerationGuard allows field access during step; earlier guards become stale after cursor advance"
    (let [cf                                                                    (get-factory)
          guard-entries
          (db/run-task!
           (m/reduce (fn [acc cursor]
                       (let [guard (cursor/cursor-row cursor)
                             ok?   (try (.get ^Row guard (int 0) Object)
                                        true
                                        (catch IllegalStateException _ false))]
                         (conj acc {:guard guard, :ok? ok?})))
                     []
                     (stream/stream* cf select-all [] {})))]
      (is (every? :ok? guard-entries))
      (let [non-last (butlast guard-entries)]
        (is (pos? (count non-last)))
        (is (every? (fn [{:keys [guard]}]
                      (try (.get ^Row guard (int 0) Object)
                           false
                           (catch IllegalStateException _ true)))
                    non-last))))))

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

(deftest ^:pattern-18 row-flow-demand-bounded-test
  (testing
   "r2dbc-row-flow requests fetch-size batches instead of unbounded demand"
    (let [{:keys [publisher requests completed?]}                             (mock/tracking-publisher
                                                                               {:items (range 20)})
          values
          (db/run-task!
           (m/reduce conj [] (#'stream/r2dbc-row-flow publisher 4 identity)))]
      (is (= (vec (range 20)) values))
      (is (every? #(= 4 %) @requests))
      (is (<= 20 (reduce + @requests) 24))
      (is (true? @completed?)))))

(deftest row-flow-cancellation-propagates-test
  (testing
   "early termination cancels the upstream Subscription without extra demand"
    (let [{:keys [publisher requests cancelled?]} (mock/tracking-publisher
                                                   {:items (range 1000)})
          seen                                    (db/run-task! (m/reduce
                                                                 (fn [acc _]
                                                                   (let [next (inc acc)]
                                                                     (if (= 10 next) (reduced next) next)))
                                                                 0
                                                                 (#'stream/r2dbc-row-flow publisher 32 identity)))]
      (is (= 10 seen))
      (is (= [32] @requests))
      (is (true? @cancelled?)))))

(deftest row-flow-error-after-buffer-drain-test
  (testing "buffered rows are emitted before the upstream error is thrown"
    (let [seen                         (atom [])
          boom                         (ex-info "boom" {:phase :row-flow})
          {:keys [publisher requests]} (mock/tracking-publisher {:items [1 2 3]
                                                                 :error boom})]
      (is (thrown-with-msg? ExceptionInfo
                            #"boom"
                            (db/run-task!
                             (m/reduce
                              (fn [acc v] (swap! seen conj v) acc)
                              nil
                              (#'stream/r2dbc-row-flow publisher 2 identity)))))
      (is (= [1 2 3] @seen))
      (is (= [2 2] @requests)))))

(deftest row-flow-fetch-size-one-async-test
  (testing "r2dbc-row-flow with fetch-size=1 and async delivery emits all items"
    (let [{:keys [publisher requests]}                                        (mock/tracking-publisher {:items (range
                                                                                                                5)})
          values
          (db/run-task!
           (m/reduce conj [] (#'stream/r2dbc-row-flow publisher 1 identity)))]
      (is (= [0 1 2 3 4] values))
      (is (every? #(= 1 %) @requests)))))

(deftest ^:pattern-18 result-rows-demand-bounded-test
  (testing
   "result-rows-pub requests one Result at a time while rows stream in fetch-size batches"
    (let [row->map*                               (fn [^Row r] ((row/make-row-fn) r (.getMetadata r)))
          result-1                                (mock/mock-result
                                                   {:rows [(mock/mock-row (array-map "id" 1 "name" "Alice"))
                                                           (mock/mock-row (array-map "id" 2 "name" "Bob"))
                                                           (mock/mock-row (array-map "id" 3 "name" "Carol"))]})
          result-2                                (mock/mock-result
                                                   {:rows [(mock/mock-row (array-map "id" 4 "name" "Dan"))
                                                           (mock/mock-row (array-map "id" 5 "name" "Eve"))
                                                           (mock/mock-row (array-map "id" 6 "name" "Frank"))]})
          {:keys [publisher requests completed?]} (mock/tracking-publisher
                                                   {:items [result-1 result-2]})
          values                                  (db/run-task!
                                                   (m/reduce
                                                    conj
                                                    []
                                                    (#'stream/r2dbc-row-flow
                                                     (#'pub/result-rows-pub publisher (volatile! nil) row->map*)
                                                     2
                                                     identity)))]
      (is (= [{:id 1, :name "Alice"} {:id 2, :name "Bob"} {:id 3, :name "Carol"}
              {:id 4, :name "Dan"} {:id 5, :name "Eve"} {:id 6, :name "Frank"}]
             values))
      (is (= [1 1] @requests))
      (is (true? @completed?)))))

(deftest result-rows-cancellation-propagates-test
  (testing
   "early termination cancels the outer Result publisher without unbounded Result demand"
    (let [result-1                                (mock/mock-result {:rows [(mock/mock-row (array-map "id" 1))
                                                                            (mock/mock-row (array-map "id" 2))
                                                                            (mock/mock-row (array-map "id" 3))
                                                                            (mock/mock-row (array-map "id"
                                                                                                      4))]})
          result-2                                (mock/mock-result {:rows [(mock/mock-row (array-map "id" 5))
                                                                            (mock/mock-row (array-map "id"
                                                                                                      6))]})
          {:keys [publisher requests cancelled?]} (mock/tracking-publisher
                                                   {:items [result-1 result-2]})
          seen                                    (db/run-task!
                                                   (m/reduce
                                                    (fn [acc _]
                                                      (let [next (inc acc)] (if (= 2 next) (reduced next) next)))
                                                    0
                                                    (#'stream/r2dbc-row-flow
                                                     (#'pub/result-rows-pub publisher (volatile! nil) identity)
                                                     2
                                                     identity)))]
      (is (= 2 seen))
      (is (= [1] @requests))
      (is (true? @cancelled?)))))

(deftest ^:pattern-13 result-rows-cancellation-during-second-result-test
  (testing
   "cancellation after first Result prevents delivery of subsequent Results"
    (let [result-1                       (mock/mock-result {:rows [(mock/mock-row (array-map "id" 1))
                                                                   (mock/mock-row (array-map "id" 2))
                                                                   (mock/mock-row (array-map "id" 3))
                                                                   (mock/mock-row (array-map "id" 4))
                                                                   (mock/mock-row (array-map "id"
                                                                                             5))]})
          result-2                       (mock/mock-result {:rows [(mock/mock-row (array-map "id" 6))
                                                                   (mock/mock-row (array-map "id" 7))
                                                                   (mock/mock-row (array-map "id" 8))
                                                                   (mock/mock-row (array-map "id" 9))
                                                                   (mock/mock-row (array-map "id"
                                                                                             10))]})
          result-3                       (mock/mock-result {:rows [(mock/mock-row (array-map "id" 11))
                                                                   (mock/mock-row (array-map "id" 12))
                                                                   (mock/mock-row (array-map "id" 13))
                                                                   (mock/mock-row (array-map "id" 14))
                                                                   (mock/mock-row (array-map "id"
                                                                                             15))]})
          {:keys [publisher cancelled?]} (mock/tracking-publisher
                                          {:items [result-1 result-2 result-3]})
          seen                           (db/run-task!
                                          (m/reduce
                                           (fn [acc _]
                                             (let [next (inc acc)] (if (= 3 next) (reduced next) next)))
                                           0
                                           (#'stream/r2dbc-row-flow
                                            (#'pub/result-rows-pub publisher (volatile! nil) identity)
                                            5
                                            identity)))]
      (is (= 3 seen))
      (is (true? @cancelled?)))))

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
  (testing "plan* streams rows from multiple Result objects in order"
    (let [result-1 (mock/mock-result
                    {:rows [(mock/mock-row (array-map "id" 1 "name" "Alice"))
                            (mock/mock-row (array-map "id" 2 "name" "Bob"))]})
          result-2 (mock/mock-result
                    {:rows [(mock/mock-row (array-map "id" 3 "name" "Carol"))
                            (mock/mock-row (array-map "id" 4 "name" "Dan"))]})
          stmt     (mock/mock-statement [result-1 result-2])
          conn     (mock/mock-connection {:statement stmt})
          rows     (db/run-task! (m/reduce conj
                                           []
                                           (stream/stream*
                                            conn
                                            "SELECT id, name FROM ignored"
                                            []
                                            {:builder-fn (row/make-row-fn)})))]
      (is (= [{:id 1, :name "Alice"} {:id 2, :name "Bob"} {:id 3, :name "Carol"}
              {:id 4, :name "Dan"}]
             rows)))))

(deftest streaming-plan-flow-concurrent-stress-test
  (testing
   "no ISE from process-ref/notifier race under concurrent warm-pool load"
    (let [results (db/run-task!
                   (m/reduce conj
                             []
                             (m/ap (let [_i (m/?> 20 (m/seed (range 100)))]
                                     (m/?
                                      (m/reduce
                                       (fn [acc _] (inc acc))
                                       0
                                       (stream/stream*
                                        (get-factory)
                                        "SELECT id FROM test_table ORDER BY id"
                                        []
                                        {:builder-fn (row/make-row-fn)
                                         :fetch-size 1})))))))]
      (is (= 100 (count results)))
      (is (every? #(= 3 %) results)))))

(deftest streaming-plan-flow-concurrent-init-error-test
  (testing
   "no hang and correct error type when T1 throws under concurrent warm-pool load"
    (let [results (db/run-task!
                   (m/reduce
                    conj
                    []
                    (m/ap (let [_i (m/?> 20 (m/seed (range 20)))]
                            (try (m/? (m/reduce
                                       conj
                                       []
                                       (stream/stream*
                                        (get-factory)
                                        "SELECT * FROM nonexistent_table_xyz"
                                        []
                                        {:builder-fn (row/make-row-fn)
                                         :fetch-size 1})))
                                 (catch Throwable e e))))))]
      (is (= 20 (count results)))
      (is (every? #(and (instance? Throwable %)
                        (not (instance? IllegalStateException %)))
                  results)))))

(deftest streaming-plan-flow-concurrent-cancel-test
  (testing
   "no hang when stream is cancelled before first row under concurrent warm-pool load"
    (let [results (db/run-task!
                   (m/reduce conj
                             []
                             (m/ap (let [_i (m/?> 20 (m/seed (range 20)))]
                                     (m/?
                                      (m/race
                                       (m/reduce
                                        conj
                                        []
                                        (stream/stream*
                                         (get-factory)
                                         "SELECT id FROM test_table ORDER BY id"
                                         []
                                         {:builder-fn (row/make-row-fn)
                                          :fetch-size 1}))
                                       (m/sp nil)))))))]
      (is (= 20 (count results))))))

(deftest streaming-plan-flow-init-latch-race-test
  (testing
   "deref awaits init-latch when notifier fires before T1 writes process-ref"
    (let [mock-fn (fn [_row-pub _fetch-size _row-xf]
                    (fn [notifier terminator]
                      (future (notifier))
                      (Thread/sleep 50)
                      (reify
                        IFn
                        (invoke [_] nil)
                        IDeref
                        (deref [_] (terminator) ::result))))
          result  (db/run-task! (m/reduce
                                 conj
                                 []
                                 (lifecycle/streaming-plan-flow
                                  (get-factory)
                                  "SELECT id FROM test_table ORDER BY id"
                                  []
                                  {:builder-fn (row/make-row-fn)}
                                  identity
                                  mock-fn)))]
      (is (= [::result] result)))))
