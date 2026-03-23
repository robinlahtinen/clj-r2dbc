;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.test-util.mock-test
  "Tests for clj-r2dbc.test-util.mock construction helpers."
  (:require
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing]])
  (:import
   (io.r2dbc.spi Nullability)
   (io.r2dbc.spi.test MockColumnMetadata MockConnection MockConnectionFactory MockResult MockStatement)))

(set! *warn-on-reflection* true)

(deftest mock-type-test
  (testing "creates a MockType with correct java class and name"
    (let [t (mock/mock-type String "VARCHAR")]
      (is (= String (.getJavaType t)))
      (is (= "VARCHAR" (.getName t)))))
  (testing "works with numeric types"
    (let [t (mock/mock-type Integer "INTEGER")]
      (is (= Integer (.getJavaType t)))
      (is (= "INTEGER" (.getName t))))))

(deftest mock-column-metadata-test
  (testing "creates column metadata with name and java-type"
    (let [cm (mock/mock-column-metadata {:name "id", :java-type Integer})]
      (is (= "id" (.getName cm)))
      (is (= Integer (.getJavaType cm)))))
  (testing "defaults nullability to UNKNOWN"
    (let [cm (mock/mock-column-metadata {:name "x", :java-type String})]
      (is (= Nullability/UNKNOWN (.getNullability cm)))))
  (testing "accepts explicit nullability"
    (let [cm (mock/mock-column-metadata {:name        "x"
                                         :java-type   String
                                         :nullability Nullability/NULLABLE})]
      (is (= Nullability/NULLABLE (.getNullability cm)))))
  (testing "accepts precision and scale"
    (let [cm (mock/mock-column-metadata
              {:name "price", :java-type BigDecimal, :precision 10, :scale 2})]
      (is (= 10 (.getPrecision cm)))
      (is (= 2 (.getScale cm))))))

(deftest mock-row-metadata-test
  (testing "creates metadata from column spec maps"
    (let [rm (mock/mock-row-metadata [{:name "id", :java-type Integer}
                                      {:name "name", :java-type String}])]
      (is (= 2 (count (.getColumnMetadatas rm))))
      (is (= "id" (.getName (.getColumnMetadata rm 0))))
      (is (= "name" (.getName (.getColumnMetadata rm 1))))))
  (testing "accepts MockColumnMetadata instances directly"
    (let [cm (mock/mock-column-metadata {:name "col1", :java-type Long})
          rm (mock/mock-row-metadata [cm])]
      (is (= 1 (count (.getColumnMetadatas rm))))
      (is (instance? MockColumnMetadata (.getColumnMetadata rm 0)))))
  (testing "supports lookup by column name"
    (let [rm (mock/mock-row-metadata [{:name "id", :java-type Integer}])]
      (is (.contains rm "id"))
      (is (not (.contains rm "nonexistent"))))))

(deftest mock-row-test
  (testing "creates row accessible by name"
    (let [row (mock/mock-row {"id" 1, "name" "Alice"})]
      (is (= 1 (.get row "id" Long)))
      (is (= "Alice" (.get row "name" String)))))
  (testing "creates row accessible by index"
    (let [row (mock/mock-row {"id" 1, "name" "Alice"})]
      (is (= 1 (.get row (int 0) Long)))
      (is (= "Alice" (.get row (int 1) String)))))
  (testing "auto-generates row metadata"
    (let [row (mock/mock-row {"id" 1})]
      (is (some? (.getMetadata row)))
      (is (.contains (.getMetadata row) "id"))))
  (testing "accepts explicit metadata"
    (let [rm  (mock/mock-row-metadata [{:name "x", :java-type Long}])
          row (mock/mock-row {"x" 42} {:metadata rm})]
      (is (= rm (.getMetadata row)))))
  (testing "accepts explicit types"
    (let [row (mock/mock-row {"id" (int 1)} {:types {"id" Integer}})]
      (is (= (int 1) (.get row "id" Integer)))))
  (testing "handles nil values"
    (let [row (mock/mock-row {"x" nil})] (is (nil? (.get row "x" Object))))))

(deftest mock-result-test
  (testing "empty result"
    (let [result (mock/mock-result)] (is (instance? MockResult result))))
  (testing "result with update count"
    (let [result (mock/mock-result {:update-count 5})] (is (some? result))))
  (testing "result with rows"
    (let [row    (mock/mock-row {"id" 1})
          result (mock/mock-result {:rows [row]})]
      (is (some? result))))
  (testing "result with multiple rows"
    (let [r1     (mock/mock-row {"id" 1})
          r2     (mock/mock-row {"id" 2})
          result (mock/mock-result {:rows [r1 r2]})]
      (is (some? result)))))

(deftest mock-statement-test
  (testing "statement returns results on execute"
    (let [result (mock/mock-result {:update-count 1})
          stmt   (mock/mock-statement result)]
      (is (some? (.execute stmt)))))
  (testing "statement accepts multiple results"
    (let [r1   (mock/mock-result {:update-count 1})
          r2   (mock/mock-result {:update-count 2})
          stmt (mock/mock-statement [r1 r2])]
      (is (some? (.execute stmt))))))

(deftest mock-batch-test
  (testing "batch returns results on execute"
    (let [result (mock/mock-result {:update-count 1})
          batch  (mock/mock-batch result)]
      (.add batch "INSERT INTO t VALUES (1)")
      (is (= ["INSERT INTO t VALUES (1)"] (.getSqls batch)))
      (is (some? (.execute batch))))))

(deftest mock-connection-test
  (testing "connection with statement"
    (let [stmt (mock/mock-statement (mock/mock-result))
          conn (mock/mock-connection {:statement stmt})]
      (is (instance? MockStatement (.createStatement conn "SELECT 1")))
      (is (= "SELECT 1" (.getCreateStatementSql conn)))))
  (testing "connection defaults to valid=true"
    (let [conn (mock/mock-connection)] (is (instance? MockConnection conn))))
  (testing "close tracking"
    (let [conn (mock/mock-connection)]
      (is (not (.isCloseCalled conn)))
      (.block (.close conn))
      (is (.isCloseCalled conn)))))

(deftest mock-connection-factory-test
  (testing "factory returns connection from create()"
    (let [conn (mock/mock-connection)
          cf   (mock/mock-connection-factory conn)]
      (is (instance? MockConnectionFactory cf))
      (is (instance? MockConnection (.block (.create cf)))))))

(deftest mock-blob-test
  (testing "blob wraps byte data"
    (let [blob (mock/mock-blob (.getBytes "hello" "UTF-8"))]
      (is (some? blob))
      (is (some? (.stream blob))))))

(deftest mock-clob-test
  (testing "clob wraps text data"
    (let [clob (mock/mock-clob "hello world")]
      (is (some? clob))
      (is (some? (.stream clob))))))

(deftest mock-out-parameters-test
  (testing "out parameters with string identifiers"
    (let [op (mock/mock-out-parameters {"status" String} {"status" "OK"})]
      (is (= "OK" (.get op "status" String))))))

(deftest mock-full-stack-test
  (testing
   "full mock stack: factory -> connection -> statement -> result -> row"
    (let [row    (mock/mock-row {"id" 1, "name" "Alice"})
          result (mock/mock-result {:rows [row]})
          stmt   (mock/mock-statement result)
          conn   (mock/mock-connection {:statement stmt})
          cf     (mock/mock-connection-factory conn)]
      (is (instance? MockConnectionFactory cf))
      (let [resolved-conn (.block (.create cf))]
        (is (instance? MockConnection resolved-conn)))
      (let [resolved-stmt (.createStatement conn "SELECT * FROM users")]
        (is (instance? MockStatement resolved-stmt)))
      (is (= 1 (.get row "id" Long)))
      (is (= "Alice" (.get row "name" String)))
      (is (.contains (.getMetadata row) "id"))
      (is (.contains (.getMetadata row) "name")))))
