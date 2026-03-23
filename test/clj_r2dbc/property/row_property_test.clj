;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.property.row-property-test
  (:require
   [clj-r2dbc.row :as row]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is]]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop])
  (:import
   (io.r2dbc.spi Row)))

(set! *warn-on-reflection* true)

(def ^:private simple-col-gen
  (gen/fmap (fn [n] (str "col_" n)) (gen/choose 0 1000000)))

(def ^:private pairs-gen
  (gen/vector-distinct (gen/tuple simple-col-gen (gen/choose -100000 100000))
                       {:min-elements 1, :max-elements 1, :distinct-by first}))

(def ^:private cols-gen
  (gen/fmap (fn [pairs] (into (array-map) pairs)) pairs-gen))

(def ^:private qualifier-gen
  (gen/fmap (fn [s] (str "q_" s)) gen/string-alphanumeric))

(defn- row-with-types
  [cols]
  (let [types (reduce-kv (fn [acc k _v] (assoc acc k Long)) {} cols)]
    (mock/mock-row cols {:types types})))

(deftest unqualified-kebab-builder-preserves-row-count-test
  (is (:result (tc/quick-check
                150
                (prop/for-all
                 [cols cols-gen]
                 (let [row (row-with-types cols)
                       out (row/kebab-maps row (.getMetadata ^Row row))]
                   (= (count cols) (count out))))))))

(deftest unqualified-builder-preserves-row-count-test
  (is (:result (tc/quick-check
                150
                (prop/for-all [cols cols-gen]
                              (let [row (row-with-types cols)
                                    out (row/raw-maps row
                                                      (.getMetadata ^Row row))]
                                (= (count cols) (count out))))))))

(deftest vectors-builder-preserves-column-order-test
  (is (:result (tc/quick-check 120
                               (prop/for-all
                                [pairs pairs-gen]
                                (let [m   (into (array-map) pairs)
                                      row (row-with-types m)
                                      out (row/vectors row
                                                       (.getMetadata ^Row row))]
                                  (= (vec (vals m)) out)))))))

(deftest qualified-builders-emit-qualified-keys-test
  (is (:result
       (tc/quick-check
        120
        (prop/for-all
         [cols cols-gen qualifier qualifier-gen]
         (let [row           (row-with-types cols)
               kebab-builder (row/->qualified-kebab-maps {:qualifier qualifier})
               raw-builder   (row/->qualified-maps {:qualifier qualifier})
               kebab-out     (kebab-builder row (.getMetadata ^Row row))
               raw-out       (raw-builder row (.getMetadata ^Row row))
               kebab-nss     (set (keep namespace (keys kebab-out)))
               raw-nss       (set (keep namespace (keys raw-out)))]
           (and (= #{qualifier} kebab-nss) (= #{qualifier} raw-nss))))))))
