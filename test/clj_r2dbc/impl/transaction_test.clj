;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.transaction-test
  (:require
   [clj-r2dbc.impl.transaction :as tx]
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing]])
  (:import
   (clojure.lang ExceptionInfo)
   (io.r2dbc.spi Batch Connection ConnectionMetadata IsolationLevel Statement TransactionDefinition ValidationDepth)
   (java.time Duration)
   (missionary Cancelled)
   (org.reactivestreams Publisher Subscriber Subscription)))

(set! *warn-on-reflection* true)

(defn- void-publisher
  "Create a Publisher<Void> that completes successfully or with error."
  [& {:keys [error]}]
  (reify
    Publisher
    (^void subscribe
      [_ ^Subscriber subscriber]
      (.onSubscribe
       subscriber
       (reify
         Subscription
         (request [_ n]
           (when (pos? (long n))
             (if error (.onError subscriber error) (.onComplete subscriber))))
         (cancel [_]))))))

(defn- make-tx-connection
  "Create a MockConnection that tracks transaction method calls."
  [begin-pub rollback-pub]
  (let [conn (mock/mock-connection)]
    (reify
      Connection
      (^Publisher beginTransaction [_] begin-pub)
      (^Publisher beginTransaction [_ ^TransactionDefinition _def] begin-pub)
      (^Publisher rollbackTransaction [_] rollback-pub)
      (^Publisher commitTransaction
        [_]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^Publisher close [_] (.close ^Connection conn))
      (^Statement createStatement
        [_ ^String sql]
        (.createStatement ^Connection conn sql))
      (^Batch createBatch
        [_]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^ConnectionMetadata getMetadata
        [_]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^IsolationLevel getTransactionIsolationLevel
        [_]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^boolean isAutoCommit
        [_]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^Publisher createSavepoint
        [_ ^String _name]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^Publisher releaseSavepoint
        [_ ^String _name]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^Publisher rollbackTransactionToSavepoint
        [_ ^String _name]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^Publisher setAutoCommit
        [_ ^boolean _ac]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^Publisher setLockWaitTimeout
        [_ ^Duration _t]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^Publisher setStatementTimeout
        [_ ^Duration _t]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^Publisher setTransactionIsolationLevel
        [_ ^IsolationLevel _level]
        (throw (UnsupportedOperationException. "not used in these tests")))
      (^Publisher validate
        [_ ^ValidationDepth _depth]
        (throw (UnsupportedOperationException. "not used in these tests"))))))

;; isolation-map tests

(deftest isolation-read-uncommitted-test
  (testing
   "isolation-map maps :read-uncommitted to IsolationLevel/READ_UNCOMMITTED"
    (is (= IsolationLevel/READ_UNCOMMITTED
           (:read-uncommitted tx/isolation-map)))))

(deftest isolation-read-committed-test
  (testing "isolation-map maps :read-committed to IsolationLevel/READ_COMMITTED"
    (is (= IsolationLevel/READ_COMMITTED (:read-committed tx/isolation-map)))))

(deftest isolation-repeatable-read-test
  (testing
   "isolation-map maps :repeatable-read to IsolationLevel/REPEATABLE_READ"
    (is (= IsolationLevel/REPEATABLE_READ
           (:repeatable-read tx/isolation-map)))))

(deftest isolation-serializable-test
  (testing "isolation-map maps :serializable to IsolationLevel/SERIALIZABLE"
    (is (= IsolationLevel/SERIALIZABLE (:serializable tx/isolation-map)))))

;; ->tx-definition tests

(deftest tx-definition-empty-opts-test
  (testing "empty tx-opts returns TransactionDefinition with all nil attributes"
    (let [^TransactionDefinition td (tx/->tx-definition {})]
      (is (instance? TransactionDefinition td))
      (is (nil? (.getAttribute td TransactionDefinition/ISOLATION_LEVEL)))
      (is (nil? (.getAttribute td TransactionDefinition/READ_ONLY)))
      (is (nil? (.getAttribute td TransactionDefinition/NAME)))
      (is (nil? (.getAttribute td TransactionDefinition/LOCK_WAIT_TIMEOUT))))))

(deftest tx-definition-valid-isolation-test
  (testing "valid :isolation keyword sets ISOLATION_LEVEL attribute"
    (let [^TransactionDefinition td (tx/->tx-definition {:isolation
                                                         :serializable})]
      (is (= IsolationLevel/SERIALIZABLE
             (.getAttribute td TransactionDefinition/ISOLATION_LEVEL))))))

(deftest tx-definition-invalid-isolation-test
  (testing "invalid :isolation keyword throws ex-info with :invalid-argument"
    (is (thrown-with-msg? ExceptionInfo
                          #"Invalid transaction isolation"
                          (tx/->tx-definition {:isolation :invalid-level})))
    (try (tx/->tx-definition {:isolation :invalid-level})
         nil
         (catch ExceptionInfo e
           (let [data (ex-data e)]
             (is (= :invalid-argument (:clj-r2dbc/error-type data)))
             (is (= :isolation (:key data)))
             (is (= :invalid-level (:value data))))))))

(deftest tx-definition-read-only-test
  (testing ":read-only? true/false sets READ_ONLY attribute"
    (let [^TransactionDefinition td-true  (tx/->tx-definition {:read-only? true})
          ^TransactionDefinition td-false (tx/->tx-definition {:read-only?
                                                               false})]
      (is (true? (.getAttribute td-true TransactionDefinition/READ_ONLY)))
      (is (false? (.getAttribute td-false TransactionDefinition/READ_ONLY))))))

(deftest tx-definition-name-test
  (testing ":name string sets NAME attribute"
    (let [^TransactionDefinition td (tx/->tx-definition {:name
                                                         "my-transaction"})]
      (is (= "my-transaction" (.getAttribute td TransactionDefinition/NAME))))))

(deftest tx-definition-lock-wait-timeout-test
  (testing ":lock-wait-timeout-ms integer converts to Duration"
    (let [^TransactionDefinition td (tx/->tx-definition {:lock-wait-timeout-ms
                                                         5000})]
      (is (instance? Duration
                     (.getAttribute td
                                    TransactionDefinition/LOCK_WAIT_TIMEOUT)))
      (is (= 5000
             (.toMillis
              ^Duration
              (.getAttribute td TransactionDefinition/LOCK_WAIT_TIMEOUT)))))))

(deftest tx-definition-combined-opts-test
  (testing "multiple options combined correctly"
    (let [^TransactionDefinition td (tx/->tx-definition
                                     {:isolation            :repeatable-read
                                      :read-only?           true
                                      :name                 "combined-tx"
                                      :lock-wait-timeout-ms 3000})]
      (is (= IsolationLevel/REPEATABLE_READ
             (.getAttribute td TransactionDefinition/ISOLATION_LEVEL)))
      (is (true? (.getAttribute td TransactionDefinition/READ_ONLY)))
      (is (= "combined-tx" (.getAttribute td TransactionDefinition/NAME)))
      (is (= 3000
             (.toMillis
              ^Duration
              (.getAttribute td TransactionDefinition/LOCK_WAIT_TIMEOUT)))))))

(deftest tx-definition-nil-lock-wait-timeout-test
  (testing "nil :lock-wait-timeout-ms returns nil Duration"
    (let [^TransactionDefinition td (tx/->tx-definition {:lock-wait-timeout-ms
                                                         nil})]
      (is (nil? (.getAttribute td TransactionDefinition/LOCK_WAIT_TIMEOUT))))))

;; begin-transaction-task tests

(deftest begin-transaction-empty-opts-test
  (testing
   "empty tx-opts calls no-arg beginTransaction and completes successfully"
    (let [pub  (void-publisher)
          conn (make-tx-connection pub pub)
          task (tx/begin-transaction-task conn {})]
      (is (nil? (db/run-task! task))))))

(deftest begin-transaction-with-opts-test
  (testing
   "non-empty tx-opts calls 1-arg beginTransaction with TransactionDefinition"
    (let [pub  (void-publisher)
          conn (make-tx-connection pub pub)
          task (tx/begin-transaction-task conn {:isolation :serializable})]
      (is (nil? (db/run-task! task))))))

(deftest begin-transaction-publisher-error-test
  (testing "Publisher<Void> error propagates through task"
    (let [error (ex-info "begin failed" {})
          pub   (void-publisher :error error)
          conn  (make-tx-connection pub pub)
          task  (tx/begin-transaction-task conn {})]
      (is
       (thrown-with-msg? ExceptionInfo #"begin failed" (db/run-task! task))))))

(deftest begin-transaction-returns-task-test
  (testing "begin-transaction-task returns a Missionary task (function)"
    (let [pub  (void-publisher)
          conn (make-tx-connection pub pub)
          task (tx/begin-transaction-task conn {})]
      (is (fn? task)))))

;; rollback-with-suppression! tests

(deftest rollback-successful-test
  (testing "successful rollback returns nil"
    (let [pub  (void-publisher)
          conn (make-tx-connection pub pub)
          root (ex-info "original error" {})
          task (tx/rollback-with-suppression! conn root)]
      (is (nil? (db/run-task! task))))))

(deftest rollback-error-suppressed-test
  (testing "rollback error suppressed on root exception"
    (let [rollback-error (ex-info "rollback failed" {})
          pub            (void-publisher :error rollback-error)
          conn           (make-tx-connection pub pub)
          root           (ex-info "original error" {})
          task           (tx/rollback-with-suppression! conn root)]
      (is (nil? (db/run-task! task)))
      (let [suppressed (vec (.getSuppressed ^Throwable root))]
        (is (= 1 (count suppressed)))
        (is (identical? rollback-error (first suppressed)))))))

(deftest rollback-error-nil-root-test
  (testing "rollback error with nil root does not throw NPE"
    (let [rollback-error (ex-info "rollback failed" {})
          pub            (void-publisher :error rollback-error)
          conn           (make-tx-connection pub pub)
          task           (tx/rollback-with-suppression! conn nil)]
      (is (nil? (db/run-task! task))))))

(deftest rollback-cancelled-rethrown-test
  (testing "Cancelled exception re-thrown immediately without suppression"
    (let [pub  (void-publisher :error (Cancelled.))
          conn (make-tx-connection pub pub)
          root (ex-info "original error" {})
          task (tx/rollback-with-suppression! conn root)]
      (is (thrown? Cancelled (db/run-task! task)))
      (is (zero? (count (.getSuppressed ^Throwable root)))))))

(deftest rollback-multiple-errors-suppressed-test
  (testing "multiple rollback errors all suppressed on same root"
    (let [rollback-error (ex-info "rollback failed" {})
          pub            (void-publisher :error rollback-error)
          conn           (make-tx-connection pub pub)
          root           (ex-info "original error" {})]
      (db/run-task! (tx/rollback-with-suppression! conn root))
      (db/run-task! (tx/rollback-with-suppression! conn root))
      (let [suppressed (vec (.getSuppressed ^Throwable root))]
        (is (= 2 (count suppressed)))
        (is (identical? rollback-error (first suppressed)))
        (is (identical? rollback-error (second suppressed)))))))
