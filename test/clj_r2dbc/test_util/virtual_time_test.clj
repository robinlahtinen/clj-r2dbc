;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.test-util.virtual-time-test
  "Tests for clj-r2dbc.test-util.virtual-time and clj-r2dbc.test-util.db helpers."
  (:require
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.virtual-time :as vt]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m])
  (:import
   (clojure.lang ExceptionInfo)))

(set! *warn-on-reflection* true)

(deftest with-virtual-clock-test
  (testing "installs virtual clock for test body"
    (vt/with-virtual-clock (fn [] (is (= :done (vt/run-task (m/sp :done))))))))

(deftest run-task-simple-test
  (testing "runs a simple task to completion"
    (vt/with-virtual-clock (fn [] (is (= :done (vt/run-task (m/sp :done))))))))

(deftest run-task-virtual-sleep-test
  (testing "virtual sleep completes without real delay"
    (vt/with-virtual-clock
      (fn []
        (is (= :slept (vt/run-task (m/sp (m/? (m/sleep 10000)) :slept))))))))

(deftest run-task-exception-test
  (testing "task exceptions propagate through run-task"
    (vt/with-virtual-clock
      (fn []
        (is (thrown? ExceptionInfo
                     (vt/run-task (m/sp (throw (ex-info "boom" {}))))))))))

(deftest run-task-with-seed-test
  (testing "run-task accepts scheduler options"
    (vt/with-virtual-clock
      (fn [] (is (= :ok (vt/run-task (m/sp :ok) {:seed 42, :trace? true})))))))

(deftest make-scheduler-test
  (testing "creates a scheduler for direct control"
    (vt/with-virtual-clock
      (fn [] (let [sched (vt/make-scheduler)] (is (= 0 (vt/now-ms sched))))))))

(deftest advance-test
  (testing "advances virtual time on scheduler"
    (vt/with-virtual-clock (fn []
                             (let [sched (vt/make-scheduler)]
                               (vt/advance! sched 100)
                               (is (= 100 (vt/now-ms sched))))))))

(deftest collect-test
  (testing "collects a flow to a vector"
    (vt/with-virtual-clock
      (fn [] (is (= [1 2 3] (vt/run-task (vt/collect (m/seed [1 2 3])))))))))

(deftest check-interleaving-passes-test
  (testing "check-interleaving passes when property holds"
    (vt/with-virtual-clock
      (fn []
        (let [result (vt/check-interleaving
                      (fn [] (m/sp 42))
                      {:num-tests 10, :seed 1, :property #(= 42 %)})]
          (is (:ok? result)))))))

(deftest check-interleaving-detects-bug-test
  (testing "check-interleaving detects a failing property"
    (vt/with-virtual-clock
      (fn []
        (let [result (vt/check-interleaving
                      (fn []
                        (let [a (atom 0)]
                          (m/sp (m/? (m/join (fn [& _] @a)
                                             (m/sp (let [v @a]
                                                     (m/? (m/sleep 0))
                                                     (reset! a (+ v 5))))
                                             (m/sp (let [v @a]
                                                     (m/? (m/sleep 0))
                                                     (reset! a (+ v 7))))))
                                @a)))
                      {:num-tests 100, :seed 42, :property #(= 12 %)})]
          (is (not (:ok? result))))))))

(deftest run-task!-blocking-test
  (testing "db/run-task! blocks and returns result"
    (is (= :hello (db/run-task! (m/sp :hello))))))

(deftest run-task!-exception-test
  (testing "db/run-task! re-throws task exceptions"
    (is (thrown? ExceptionInfo
                 (db/run-task! (m/sp (throw (ex-info "fail" {}))))))))

(deftest run-task!-computed-value-test
  (testing "db/run-task! returns computed values"
    (is (= 42 (db/run-task! (m/sp (* 6 7)))))))
