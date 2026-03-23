;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.row-test
  (:require
   [clj-r2dbc.impl.datafy :as datafy-impl]
   [clj-r2dbc.row :as row]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.core.protocols :as core-p]
   [clojure.test :refer [deftest is testing]])
  (:import
   (clojure.lang ExceptionInfo)
   (io.r2dbc.spi Parameters Row Statement)
   (java.nio ByteBuffer)))

(set! *warn-on-reflection* true)

(defn- recording-statement
  [calls]
  (reify
    Statement
    (execute [_]
      (throw (UnsupportedOperationException. "not used in row tests")))
    (^Statement bind
      [this ^int i ^Object v]
      (swap! calls conj [:bind i v])
      this)
    (^Statement bind
      [this ^String name ^Object v]
      (swap! calls conj [:bind-name name v])
      this)
    (^Statement bindNull
      [this ^int i ^Class t]
      (swap! calls conj [:bind-null i t])
      this)
    (^Statement bindNull
      [this ^String name ^Class t]
      (swap! calls conj [:bind-null-name name t])
      this)
    (^Statement add [this] this)
    (^Statement fetchSize [this ^int _n] this)
    (^Statement returnGeneratedValues
      [this ^"[Ljava.lang.String;" _cols]
      this)))

(deftest kebab-maps-test
  (testing "kebab-maps emits unqualified kebab-case keyword maps"
    (let [row (mock/mock-row {"FIRST_NAME" "Alice", "AGE" 30}
                             {:types {"FIRST_NAME" String, "AGE" Long}})
          m   (row/kebab-maps row (.getMetadata ^Row row))]
      (is (= {:first-name "Alice", :age 30} m)))))

(deftest raw-maps-test
  (testing "raw-maps emits unqualified raw-name keyword maps"
    (let [row (mock/mock-row {"FIRST_NAME" "Alice"}
                             {:types {"FIRST_NAME" String}})
          m   (row/raw-maps row (.getMetadata ^Row row))]
      (is (= {:FIRST_NAME "Alice"} m)))))

(deftest vectors-test
  (testing "vectors emits vectors in column order"
    (let [row (mock/mock-row (array-map "id" 1 "name" "Alice")
                             {:types {"id" Long, "name" String}})
          v   (row/vectors row (.getMetadata ^Row row))]
      (is (= [1 "Alice"] v)))))

(deftest ->qualified-kebab-maps-test
  (testing
   "->qualified-kebab-maps with :qualifier emits qualified kebab-case keys"
    (let [builder (row/->qualified-kebab-maps {:qualifier "users"})
          row     (mock/mock-row {"FIRST_NAME" "Alice"}
                                 {:types {"FIRST_NAME" String}})
          m       (builder row (.getMetadata ^Row row))]
      (is (= {:users/first-name "Alice"} m)))))

(deftest ->qualified-maps-test
  (testing "->qualified-maps with :qualifier emits qualified raw-name keys"
    (let [builder (row/->qualified-maps {:qualifier "users"})
          row     (mock/mock-row {"FIRST_NAME" "Alice"}
                                 {:types {"FIRST_NAME" String}})
          m       (builder row (.getMetadata ^Row row))]
      (is (= {:users/FIRST_NAME "Alice"} m)))))

(deftest qualified-builders-require-qualifier-test
  (testing "qualified map builders reject missing :qualifier"
    (is (thrown-with-msg? ExceptionInfo
                          #"Missing required :qualifier"
                          (row/->qualified-kebab-maps {})))
    (is (thrown-with-msg? ExceptionInfo
                          #"Missing required :qualifier"
                          (row/->qualified-maps {})))))

(deftest ->qualified-kebab-maps-keyword-args-test
  (testing "->qualified-kebab-maps accepts keyword args and trailing map"
    (let [row* (mock/mock-row {"FIRST_NAME" "Alice"}
                              {:types {"FIRST_NAME" String}})]
      (is (= {:users/first-name "Alice"}
             ((row/->qualified-kebab-maps {:qualifier "users"})
              row*
              (.getMetadata ^Row row*))))
      (is (= {:users/first-name "Alice"}
             ((row/->qualified-kebab-maps :qualifier "users")
              row*
              (.getMetadata ^Row row*))))
      (is (= {:users/first-name "Alice"}
             ((row/->qualified-kebab-maps :qualifier "users" {})
              row*
              (.getMetadata ^Row row*)))))))

(deftest ->qualified-maps-keyword-args-test
  (testing "->qualified-maps accepts keyword args and trailing map"
    (let [row* (mock/mock-row {"FIRST_NAME" "Alice"}
                              {:types {"FIRST_NAME" String}})]
      (is (= {:users/FIRST_NAME "Alice"}
             ((row/->qualified-maps {:qualifier "users"})
              row*
              (.getMetadata ^Row row*))))
      (is (= {:users/FIRST_NAME "Alice"}
             ((row/->qualified-maps :qualifier "users")
              row*
              (.getMetadata ^Row row*)))))))

(deftest parameter-nil-test
  (testing "Parameter nil binds null Object"
    (let [calls (atom [])
          stmt  (recording-statement calls)]
      (row/-bind nil stmt 0)
      (is (= [[:bind-null 0 Object]] @calls)))))

(deftest parameter-byte-array-test
  (testing "Parameter byte[] binds ByteBuffer"
    (let [calls (atom [])
          stmt  (recording-statement calls)
          data  (byte-array [1 2 3])]
      (row/-bind data stmt 1)
      (let [[op idx value] (first @calls)]
        (is (= :bind op))
        (is (= 1 idx))
        (is (instance? ByteBuffer value))))))

(deftest parameter-typed-nil-test
  (testing "Parameter Parameters/in(Class) binds typed null"
    (let [calls (atom [])
          stmt  (recording-statement calls)]
      (row/-bind (Parameters/in String) stmt 2)
      (is (= [[:bind-null 2 String]] @calls)))))

(deftest parameter-default-object-test
  (testing "Parameter default object extension binds the value"
    (let [calls (atom [])
          stmt  (recording-statement calls)]
      (row/-bind 42 stmt 3)
      (is (= [[:bind 3 42]] @calls)))))

(deftest attach-datafiable-meta-test
  (testing "attach-datafiable-meta attaches marker/navigation/datafy metadata"
    (let [row                                                                   {:id 1}
          connectable                                                           :db
          nav-fn                                                                (fn [_] :ok)
          out
          (datafy-impl/attach-datafiable-meta row connectable {:nav-fn nav-fn})
          m                                                                     (meta out)
          df                                                                    (get m `core-p/datafy)]
      (is (true? (get m datafy-impl/marker-key)))
      (is (= connectable (get m datafy-impl/connectable-key)))
      (is (fn? (get m datafy-impl/nav-fn-key)))
      (is (fn? df))
      (is (= out (df out))))))

(deftest attach-datafiable-meta-non-iobj-test
  (testing "attach-datafiable-meta returns non-IObj values unchanged"
    (let [n 42] (is (= n (datafy-impl/attach-datafiable-meta n :db {}))))))
