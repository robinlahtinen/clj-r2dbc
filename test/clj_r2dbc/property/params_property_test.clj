;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.property.params-property-test
  (:require
   [clj-r2dbc.impl.sql.params :as params]
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop])
  (:import
   (io.r2dbc.spi Parameters Statement)
   (java.nio ByteBuffer)))

(set! *warn-on-reflection* true)

(defn- recording-statement
  [calls]
  (reify
    Statement
    (execute [_]
      (throw (UnsupportedOperationException. "not used in property tests")))
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

(def ^:private scalar-gen
  (gen/one-of [(gen/choose -100000 100000) gen/string-alphanumeric
               gen/boolean]))

(def ^:private param-gen
  (gen/one-of [scalar-gen (gen/return nil)
               (gen/fmap (fn [n] (byte-array [(byte n) (byte (+ 1 n))]))
                         (gen/choose 0 120))]))

(deftest bind-params-emits-one-operation-per-param-test
  (is (:result (tc/quick-check 150
                               (prop/for-all
                                [params* (gen/vector param-gen 0 16)]
                                (let [calls (atom [])
                                      stmt  (recording-statement calls)]
                                  (params/bind-params! stmt params*)
                                  (= (count params*) (count @calls))))))))

(deftest nil-values-bind-as-object-null-test
  (is (:result (tc/quick-check 100
                               (prop/for-all
                                [prefix (gen/vector scalar-gen 0 6) suffix
                                 (gen/vector scalar-gen 0 6)]
                                (let [calls  (atom [])
                                      stmt   (recording-statement calls)
                                      params (vec (concat prefix [nil] suffix))
                                      nil-i  (count prefix)]
                                  (params/bind-params! stmt params)
                                  (= [:bind-null nil-i Object]
                                     (nth @calls nil-i))))))))

(deftest byte-arrays-are-bound-as-bytebuffer-test
  (is (:result
       (tc/quick-check
        120
        (prop/for-all
         [pos (gen/choose 0 8) data
          (gen/fmap (fn [n] (byte-array [(byte n) (byte (+ 3 n))]))
                    (gen/choose 0 120))]
         (let [calls  (atom [])
               stmt   (recording-statement calls)
               params (vec (concat (repeat pos 1) [data]))]
           (params/bind-params! stmt params)
           (let [[op idx value] (nth @calls pos)]
             (and (= :bind op) (= pos idx) (instance? ByteBuffer value)))))))))

(deftest typed-nil-parameter-binds-with-explicit-class-test
  (testing "Parameters/in(Class) for nil binds through bindNull with that class"
    (let [calls (atom [])
          stmt  (recording-statement calls)]
      (params/bind-params! stmt [(Parameters/in String)])
      (is (= [[:bind-null 0 String]] @calls)))))
