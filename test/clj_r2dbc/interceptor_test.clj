;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.interceptor-test
  (:require
   [clj-r2dbc.impl.execute.pipeline :as pipeline]
   [clj-r2dbc.interceptor :as interceptor]
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m])
  (:import
   (clojure.lang ExceptionInfo)
   (missionary Cancelled)))

(set! *warn-on-reflection* true)

(defn- run-pipeline
  [interceptors execute-fn ctx]
  (db/run-task! (pipeline/run-pipeline ctx interceptors execute-fn)))

(deftest logging-interceptor-enter-leave-test
  (testing "logging-interceptor attaches timing and cleans up t0 key"
    (let [ctx        {:clj-r2dbc/sql "SELECT 1"}
          execute-fn (fn [c] (m/sp (assoc c :clj-r2dbc/rows [{:id 1}])))
          out        (run-pipeline [interceptor/logging-interceptor] execute-fn ctx)]
      (is (= [{:id 1}] (:clj-r2dbc/rows out)))
      (is (nil? (::interceptor/log-t0 out))
          "t0 key should be dissoc'd by :leave"))))

(deftest transaction-interceptor-success-test
  (testing "transaction-interceptor begins and commits on success"
    (let [conn                                                                  (mock/mock-connection)
          ctx                                                                   {:r2dbc/connection conn}
          execute-fn                                                            (fn [c] (m/sp (assoc c :ok true)))
          out
          (run-pipeline [(interceptor/transaction-interceptor)] execute-fn ctx)]
      (is (true? (:ok out)))
      (is (.isBeginTransactionCalled conn))
      (is (.isCommitTransactionCalled conn))
      (is (not (.isRollbackTransactionCalled conn))))))

(deftest transaction-interceptor-error-test
  (testing "transaction-interceptor rolls back on error"
    (let [conn       (mock/mock-connection)
          ctx        {:r2dbc/connection conn}
          execute-fn (fn [_] (m/sp (throw (ex-info "boom" {}))))]
      (is
       (thrown-with-msg?
        ExceptionInfo
        #"boom"
        (run-pipeline [(interceptor/transaction-interceptor)] execute-fn ctx)))
      (is (.isBeginTransactionCalled conn))
      (is (not (.isCommitTransactionCalled conn)))
      (is (.isRollbackTransactionCalled conn)))))

(deftest transaction-interceptor-cancelled-test
  (testing "transaction-interceptor rolls back when execution is cancelled"
    (let [conn       (mock/mock-connection)
          ctx        {:r2dbc/connection conn}
          execute-fn (fn [_] (m/sp (throw (Cancelled.))))]
      (is (thrown? Cancelled
                   (run-pipeline [(interceptor/transaction-interceptor)]
                                 execute-fn
                                 ctx)))
      (is (.isBeginTransactionCalled conn))
      (is (.isRollbackTransactionCalled conn)))))

(deftest transaction-interceptor-join-test
  (testing
   "transaction-interceptor joins existing tx without begin/commit/rollback"
    (let [conn                                                                  (mock/mock-connection)
          ctx                                                                   {:r2dbc/connection conn, :clj-r2dbc/in-transaction? true}
          execute-fn                                                            (fn [c] (m/sp (assoc c :joined true)))
          out
          (run-pipeline [(interceptor/transaction-interceptor)] execute-fn ctx)]
      (is (true? (:joined out)))
      (is (not (.isBeginTransactionCalled conn)))
      (is (not (.isCommitTransactionCalled conn)))
      (is (not (.isRollbackTransactionCalled conn))))))

(deftest transaction-interceptor-join-error-test
  (testing "transaction-interceptor does not rollback on error when joining"
    (let [conn       (mock/mock-connection)
          ctx        {:r2dbc/connection conn, :clj-r2dbc/in-transaction? true}
          execute-fn (fn [_] (m/sp (throw (ex-info "boom" {}))))]
      (is
       (thrown-with-msg?
        ExceptionInfo
        #"boom"
        (run-pipeline [(interceptor/transaction-interceptor)] execute-fn ctx)))
      (is (not (.isBeginTransactionCalled conn)))
      (is (not (.isCommitTransactionCalled conn)))
      (is (not (.isRollbackTransactionCalled conn))))))

(deftest transaction-interceptor-keyword-args-test
  (testing
   "transaction-interceptor accepts keyword args and plain map identically"
    (let [from-map    (interceptor/transaction-interceptor {:isolation
                                                            :serializable})
          from-kwargs (interceptor/transaction-interceptor :isolation
                                                           :serializable)
          from-mixed  (interceptor/transaction-interceptor :isolation
                                                           :serializable
                                                           {:read-only? true})]
      (is (= (set (keys from-map)) (set (keys from-kwargs)))
          "keyword args produce same interceptor structure as plain map")
      (is (set (keys from-mixed)) "mixed form produces an interceptor map")))
  (testing
   "transaction-interceptor keyword-arg form begins and commits on success"
    (let [conn       (mock/mock-connection)
          ctx        {:r2dbc/connection conn}
          execute-fn (fn [c] (m/sp (assoc c :ok true)))
          out        (run-pipeline [(interceptor/transaction-interceptor :read-only?
                                                                         false)]
                                   execute-fn
                                   ctx)]
      (is (true? (:ok out)))
      (is (.isBeginTransactionCalled conn))
      (is (.isCommitTransactionCalled conn)))))
