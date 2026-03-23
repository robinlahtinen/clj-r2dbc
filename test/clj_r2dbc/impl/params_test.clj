;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.params-test
  (:require
   [clj-r2dbc.impl.sql.params :as params]
   [clj-r2dbc.row :as row]
   [clojure.test :refer [deftest is testing]])
  (:import
   (io.r2dbc.spi Parameters Statement)))

(set! *warn-on-reflection* true)

(deftype CustomParam [value])

(defn- make-stmt
  [calls]
  (reify
    Statement
    (execute [_]
      (throw (UnsupportedOperationException. "not used in params tests")))
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

(deftest bind-params-returns-statement-test
  (testing "bind-params! returns the same statement instance"
    (let [stmt (make-stmt (atom []))]
      (is (identical? stmt (params/bind-params! stmt [1 "hello"]))))))

(deftest bind-params-nil-params-test
  (testing "nil params leaves stmt unchanged and returns it"
    (let [stmt (make-stmt (atom []))]
      (is (identical? stmt (params/bind-params! stmt nil))))))

(deftest bind-params-empty-params-test
  (testing "empty params vector leaves stmt unchanged and returns it"
    (let [stmt (make-stmt (atom []))]
      (is (identical? stmt (params/bind-params! stmt []))))))

(deftest bind-params-untyped-nil-test
  (testing "untyped nil binds as Object.class"
    (let [calls (atom [])
          stmt  (make-stmt calls)]
      (is (identical? stmt (params/bind-params! stmt [nil])))
      (is (= [[:bind-null 0 Object]] @calls)))))

(deftest bind-params-typed-nil-test
  (testing "typed nil uses bindNull with the explicit Class"
    (let [calls (atom [])
          stmt  (make-stmt calls)]
      (is (identical? stmt (params/bind-params! stmt [(Parameters/in String)])))
      (is (= [[:bind-null 0 String]] @calls)))))

(deftest bind-params-mixed-test
  (testing "mixed params dispatch through ReactiveParameter"
    (let [calls (atom [])
          stmt  (make-stmt calls)]
      (is (identical?
           stmt
           (params/bind-params! stmt [1 nil (Parameters/in String) "text"])))
      (is (= [[:bind 0 1] [:bind-null 1 Object] [:bind-null 2 String]
              [:bind 3 "text"]]
             @calls)))))

(deftest bind-params-monomorphic-fast-path-test
  (testing
   "String, Long, and nil use direct Statement calls (bypass row/-bind fast path)"
    (let [calls    (atom [])
          stmt     (make-stmt calls)
          observed (atom [])]
      (with-redefs [row/-bind
                    (fn [value statement index]
                      (swap! observed conj [index value])
                      (cond (nil? value)
                            (.bindNull ^Statement statement (int index) Object)
                            :else
                            (.bind ^Statement statement (int index) value))
                      statement)]
        (is (identical? stmt
                        (params/bind-params! stmt
                                             ["hello" 42 nil "world" 99]))))
      (is (= [] @observed))
      (is (= [[:bind 0 "hello"] [:bind 1 42] [:bind-null 2 Object]
              [:bind 3 "world"] [:bind 4 99]]
             @calls)))))

(deftest bind-params-custom-reactive-parameter-test
  (testing "custom type can override binding via row/Parameter"
    (let [calls (atom [])
          stmt  (make-stmt calls)]
      (extend-protocol row/Parameter
        CustomParam
        (-bind [this ^Statement s ^long i]
          (.bind s (int i) (str "custom-" (.value this)))
          s))
      (is (identical? stmt (params/bind-params! stmt [(CustomParam. "x")])))
      (is (= [[:bind 0 "custom-x"]] @calls)))))
