;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.error-test
  (:require
   [clj-r2dbc.impl.sql.error :as err]
   [clj-r2dbc.test-util.db :as db]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m])
  (:import
   (clojure.lang ExceptionInfo)
   (io.r2dbc.spi R2dbcBadGrammarException R2dbcDataIntegrityViolationException R2dbcException R2dbcNonTransientException R2dbcNonTransientResourceException R2dbcPermissionDeniedException R2dbcRollbackException R2dbcTimeoutException R2dbcTransientException R2dbcTransientResourceException)
   (missionary Cancelled)))

(set! *warn-on-reflection* true)

(deftest category-bad-grammar-test
  (testing "R2dbcBadGrammarException -> :bad-grammar"
    (let [ex  (R2dbcBadGrammarException. "bad sql")
          exi (err/r2dbc-ex->ex-info ex)]
      (is (= :bad-grammar (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest category-data-integrity-violation-test
  (testing "R2dbcDataIntegrityViolationException -> :integrity"
    (let [ex  (R2dbcDataIntegrityViolationException. "constraint violation")
          exi (err/r2dbc-ex->ex-info ex)]
      (is (= :integrity (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest category-non-transient-resource-test
  (testing "R2dbcNonTransientResourceException -> :non-transient-resource"
    (let [ex  (R2dbcNonTransientResourceException. "resource failure")
          exi (err/r2dbc-ex->ex-info ex)]
      (is (= :non-transient-resource
             (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest category-permission-denied-test
  (testing "R2dbcPermissionDeniedException -> :permission"
    (let [ex  (R2dbcPermissionDeniedException. "access denied")
          exi (err/r2dbc-ex->ex-info ex)]
      (is (= :permission (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest category-rollback-test
  (testing "R2dbcRollbackException -> :rollback"
    (let [ex  (R2dbcRollbackException. "rollback")
          exi (err/r2dbc-ex->ex-info ex)]
      (is (= :rollback (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest category-timeout-test
  (testing "R2dbcTimeoutException -> :timeout"
    (let [ex  (R2dbcTimeoutException. "timed out")
          exi (err/r2dbc-ex->ex-info ex)]
      (is (= :timeout (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest category-transient-resource-test
  (testing "R2dbcTransientResourceException -> :transient-resource"
    (let [ex  (R2dbcTransientResourceException. "transient resource failure")
          exi (err/r2dbc-ex->ex-info ex)]
      (is (= :transient-resource (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest category-non-transient-test
  (testing "R2dbcNonTransientException (base) -> :non-transient"
    (let [ex  (proxy [R2dbcNonTransientException] ["non-transient error"])
          exi (err/r2dbc-ex->ex-info ex)]
      (is (= :non-transient (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest category-transient-test
  (testing "R2dbcTransientException (base) -> :transient"
    (let [ex  (proxy [R2dbcTransientException] ["transient error"])
          exi (err/r2dbc-ex->ex-info ex)]
      (is (= :transient (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest category-unknown-test
  (testing "unclassified R2dbcException -> :unknown"
    (let [ex  (proxy [R2dbcException] ["generic r2dbc error"])
          exi (err/r2dbc-ex->ex-info ex)]
      (is (= :unknown (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest ex-data-fields-test
  (testing
   "ex-info carries sql-state, error-code, and original exception as cause"
    (let [original (R2dbcBadGrammarException. "bad sql" "42000" (int 1234))
          exi      (err/r2dbc-ex->ex-info original)
          data     (ex-data exi)]
      (is (= "42000" (:sql-state data)))
      (is (= 1234 (:error-code data)))
      (is (identical? original (.getCause ^Throwable exi))))))

(deftest nil-sql-state-test
  (testing "exception with nil sql-state does not throw; key is nil in ex-data"
    (let [ex  (R2dbcBadGrammarException. "bad sql" nil (int 0))
          exi (err/r2dbc-ex->ex-info ex)]
      (is (nil? (:sql-state (ex-data exi)))))))

(deftest wrap-error-converts-r2dbc-test
  (testing
   "R2dbcException from task is converted to ExceptionInfo with correct category"
    (let [task (m/sp (throw (R2dbcTimeoutException. "timed out")))
          exi  (try (db/run-task! (err/wrap-error task))
                    nil
                    (catch ExceptionInfo e e))]
      (is (some? exi))
      (is (= :timeout (:clj-r2dbc/error-category (ex-data exi)))))))

(deftest wrap-error-cancelled-passthrough-test
  (testing "Missionary Cancelled propagates through wrap-error unchanged"
    (let [task (m/sp (throw (Cancelled.)))]
      (is (thrown? Cancelled (db/run-task! (err/wrap-error task)))))))

(deftest wrap-error-other-exception-passthrough-test
  (testing "non-R2DBC exceptions pass through wrap-error unchanged"
    (let [original (ex-info "plain error" {:key :value})
          task     (m/sp (throw original))
          caught   (try (db/run-task! (err/wrap-error task))
                        nil
                        (catch ExceptionInfo e e))]
      (is (identical? original caught)))))

(deftest wrap-error-success-test
  (testing "successful task resolves normally through wrap-error"
    (let [result (db/run-task! (err/wrap-error (m/sp 42)))]
      (is (= 42 result)))))
