;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.middleware-test
  (:require
   [clj-r2dbc.middleware :as mw]
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m])
  (:import
   (clojure.lang ExceptionInfo)
   (missionary Cancelled)))

(set! *warn-on-reflection* true)

(defn- run-mw
  [middleware execute-fn ctx]
  (db/run-task! ((middleware execute-fn) ctx)))

(deftest with-logging-redacts-params-test
  (testing "with-logging executes and leaves params unchanged"
    (let [ctx        {:clj-r2dbc/sql "SELECT 1", :clj-r2dbc/params ["secret"]}
          execute-fn (fn [c] (m/sp (assoc c :clj-r2dbc/rows [{:id 1}])))
          out        (run-mw mw/with-logging execute-fn ctx)]
      (is (= [{:id 1}] (:clj-r2dbc/rows out)))
      (is (= ["secret"] (:clj-r2dbc/params out))))))

(deftest transaction-middleware-success-test
  (testing "with-transaction begins and commits on success"
    (let [conn       (mock/mock-connection)
          ctx        {:r2dbc/connection conn}
          execute-fn (fn [c] (m/sp (assoc c :ok true)))
          out        (run-mw (mw/with-transaction) execute-fn ctx)]
      (is (true? (:ok out)))
      (is (.isBeginTransactionCalled conn))
      (is (.isCommitTransactionCalled conn))
      (is (not (.isRollbackTransactionCalled conn))))))

(deftest transaction-middleware-error-test
  (testing "with-transaction rolls back on error"
    (let [conn       (mock/mock-connection)
          ctx        {:r2dbc/connection conn}
          execute-fn (fn [_] (m/sp (throw (ex-info "boom" {}))))]
      (is (thrown-with-msg? ExceptionInfo
                            #"boom"
                            (run-mw (mw/with-transaction) execute-fn ctx)))
      (is (.isBeginTransactionCalled conn))
      (is (not (.isCommitTransactionCalled conn)))
      (is (.isRollbackTransactionCalled conn)))))

(deftest transaction-middleware-cancelled-test
  (testing "with-transaction rolls back when execution is cancelled"
    (let [conn       (mock/mock-connection)
          ctx        {:r2dbc/connection conn}
          execute-fn (fn [_] (m/sp (throw (Cancelled.))))]
      (is (thrown? Cancelled (run-mw (mw/with-transaction) execute-fn ctx)))
      (is (.isBeginTransactionCalled conn))
      (is (.isRollbackTransactionCalled conn)))))

(deftest transaction-middleware-join-test
  (testing "with-transaction joins existing tx without begin/commit/rollback"
    (let [conn       (mock/mock-connection)
          ctx        {:r2dbc/connection conn, :clj-r2dbc/in-transaction? true}
          execute-fn (fn [c] (m/sp (assoc c :joined true)))
          out        (run-mw (mw/with-transaction) execute-fn ctx)]
      (is (true? (:joined out)))
      (is (not (.isBeginTransactionCalled conn)))
      (is (not (.isCommitTransactionCalled conn)))
      (is (not (.isRollbackTransactionCalled conn))))))

(deftest with-transaction-keyword-args-test
  (testing "with-transaction accepts keyword args and plain map identically"
    (let [conn       (mock/mock-connection)
          ctx        {:r2dbc/connection conn}
          execute-fn (fn [c] (m/sp (assoc c :ok true)))]
      (let [out
            (run-mw (mw/with-transaction {:read-only? false}) execute-fn ctx)]
        (is (true? (:ok out)))
        (is (.isBeginTransactionCalled conn)))
      (let [conn2 (mock/mock-connection)
            out   (run-mw (mw/with-transaction :read-only? false)
                          execute-fn
                          {:r2dbc/connection conn2})]
        (is (true? (:ok out)))
        (is (.isBeginTransactionCalled conn2)))
      (let [conn3 (mock/mock-connection)
            out   (run-mw (mw/with-transaction :read-only? false {:name "tx1"})
                          execute-fn
                          {:r2dbc/connection conn3})]
        (is (true? (:ok out)))
        (is (.isBeginTransactionCalled conn3))))))
