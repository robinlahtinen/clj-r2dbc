;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.conn.core-test
  "Tests for clj-r2dbc.impl.conn.core lifecycle functions.

  Tests use a mix of:
  - Real H2 in-memory databases for Publisher-based tests (acquire, with-connection)
  - MockConnectionFactory/MockConnection from r2dbc-spi-test for RAII close tracking
  - Plain MockConnection for simple passthrough and metadata tests"
  (:require
   [clj-r2dbc.impl.conn.core :as conn]
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m])
  (:import
   (clojure.lang ExceptionInfo)
   (io.r2dbc.spi Connection ConnectionFactory)
   (io.r2dbc.spi.test MockConnection)
   (missionary Cancelled)))

(set! *warn-on-reflection* true)

(defn- h2-factory
  "Create an H2 in-memory ConnectionFactory with a unique database name."
  ^ConnectionFactory [db-name]
  (conn/create-connection-factory* {:url (str "r2dbc:h2:mem:///" db-name)}))

(defn- close-connection!
  "Close an R2DBC connection via its Publisher<Void>, blocking until done."
  [^Connection c]
  (db/run-task! (m/reduce (fn [_ _] nil) nil (m/subscribe (.close c)))))

(deftest create-connection-factory-test
  (testing "creates a ConnectionFactory from a URL string"
    (let [cf (conn/create-connection-factory*
              {:url "r2dbc:h2:mem:///test-create-cf"})]
      (is (instance? ConnectionFactory cf)))))

(deftest create-connection-factory-composite-url-test
  (testing "accepts composite URLs"
    (let [cf (conn/create-connection-factory*
              {:url "r2dbc:h2:mem:///test-composite"})]
      (is (instance? ConnectionFactory cf)))))

(deftest acquire-connection-from-factory-test
  (testing "acquires a Connection from a ConnectionFactory"
    (let [cf     (h2-factory "test-acquire")
          result (db/run-task! (conn/acquire-connection cf))]
      (try (is (instance? Connection result))
           (finally (when (instance? Connection result)
                      (close-connection! result)))))))

(deftest acquire-connection-passthrough-test
  (testing "returns an existing Connection directly without wrapping"
    (let [c      (mock/mock-connection)
          result (db/run-task! (conn/acquire-connection c))]
      (is (identical? c result)))))

(deftest with-connection-success-test
  (testing "acquires connection, executes body, returns result"
    (let [cf     (h2-factory "test-with-conn-success")
          seen   (atom nil)
          task   (conn/with-connection*
                   cf
                   (fn [c]
                     (m/sp (reset! seen c) (is (instance? Connection c)) :result)))
          result (db/run-task! task)]
      (is (= :result result))
      (is (some? @seen)))))

(deftest with-connection-error-test
  (testing "closes connection even when body throws"
    (let [cf   (h2-factory "test-with-conn-error")
          seen (atom nil)
          task (conn/with-connection*
                 cf
                 (fn [c]
                   (m/sp (reset! seen c) (throw (ex-info "body-error" {})))))]
      (is (thrown-with-msg? ExceptionInfo #"body-error" (db/run-task! task)))
      (is (some? @seen)))))

(deftest with-connection-existing-conn-test
  (testing "passes existing Connection directly without RAII close"
    (let [c      (mock/mock-connection)
          task   (conn/with-connection*
                   c
                   (fn [received] (m/sp (is (identical? c received)) :direct)))
          result (db/run-task! task)]
      (is (= :direct result))
      (is (not (.isCloseCalled ^MockConnection c))))))

(deftest with-connection-cancellation-test
  (testing "propagates Missionary cancellation"
    (let [cf   (h2-factory "test-with-conn-cancel")
          task (conn/with-connection* cf (fn [_c] (m/sp (throw (Cancelled.)))))]
      (is (thrown? Cancelled (db/run-task! task))))))

(deftest connection-metadata-test
  (testing "returns a plain Clojure map with product name and version"
    (let [cf   (h2-factory "test-metadata")
          task (conn/with-connection* cf
                 (fn [c]
                   (m/sp (conn/connection-metadata* c))))
          md   (db/run-task! task)]
      (is (map? md))
      (is (contains? md :db/product-name))
      (is (contains? md :db/version))
      (is (string? (:db/product-name md)))
      (is (string? (:db/version md)))
      (is (= "H2" (:db/product-name md))))))

(deftest connection-metadata-mock-test
  (testing "connection-metadata* works with mock connection"
    (let [c  (mock/mock-connection)
          md (conn/connection-metadata* c)]
      (is (map? md))
      (is (contains? md :db/product-name))
      (is (contains? md :db/version)))))

(deftest with-connection-body-result-test
  (testing "body-fn result is correctly threaded through"
    (let [cf     (h2-factory "test-body-result")
          task   (conn/with-connection*
                   cf
                   (fn [_c] (m/sp {:rows [{:id 1} {:id 2}], :count 2})))
          result (db/run-task! task)]
      (is (= {:rows [{:id 1} {:id 2}], :count 2} result)))))

(deftest with-connection-nested-tasks-test
  (testing "body-fn can execute nested Missionary tasks"
    (let [cf     (h2-factory "test-nested")
          task   (conn/with-connection* cf
                   (fn [c]
                     (m/sp
                      (let [md (conn/connection-metadata* c)
                            _  (m/? (m/sp :step2))]
                        (assoc md :extra :data)))))
          result (db/run-task! task)]
      (is (map? result))
      (is (contains? result :db/product-name))
      (is (= :data (:extra result))))))

(deftest create-factory-and-acquire-round-trip-test
  (testing "create-connection-factory* + acquire-connection round-trip"
    (let [cf            (h2-factory "test-roundtrip")
          ^Connection c (db/run-task! (conn/acquire-connection cf))]
      (try (is (instance? Connection c))
           (let [md (conn/connection-metadata* c)]
             (is (= "H2" (:db/product-name md))))
           (finally (when (instance? Connection c) (close-connection! c)))))))

(deftest with-connection-connection-valid-test
  (testing "connection is usable inside body-fn scope"
    (let [cf     (h2-factory "test-valid")
          task   (conn/with-connection*
                   cf
                   (fn [^Connection c]
                     (m/sp (let [md (.getMetadata c)]
                             {:product (.getDatabaseProductName md)}))))
          result (db/run-task! task)]
      (is (= {:product "H2"} result)))))

(deftest with-connection-raii-close-on-success-test
  (testing
   "Connection.close() is called on success path via MockConnectionFactory"
    (let [mc     (mock/mock-connection)
          cf     (mock/mock-connection-factory mc)
          task   (conn/with-connection*
                   cf
                   (fn [c] (m/sp (is (instance? Connection c)) :ok)))
          result (db/run-task! task)]
      (is (= :ok result))
      (is (.isCloseCalled ^MockConnection mc)))))

(deftest with-connection-raii-close-on-error-test
  (testing
   "Connection.close() is called on error path via MockConnectionFactory"
    (let [mc   (mock/mock-connection)
          cf   (mock/mock-connection-factory mc)
          task (conn/with-connection*
                 cf
                 (fn [_c] (m/sp (throw (ex-info "body-boom" {})))))]
      (is (thrown-with-msg? ExceptionInfo #"body-boom" (db/run-task! task)))
      (is (.isCloseCalled ^MockConnection mc)))))

(deftest with-connection-raii-close-on-cancellation-test
  (testing
   "Connection.close() is called on cancellation path via MockConnectionFactory"
    (let [mc   (mock/mock-connection)
          cf   (mock/mock-connection-factory mc)
          task (conn/with-connection* cf (fn [_c] (m/sp (throw (Cancelled.)))))]
      (is (thrown? Cancelled (db/run-task! task)))
      (is (.isCloseCalled ^MockConnection mc)))))
