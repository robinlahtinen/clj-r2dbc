;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.property.coerce-property-test
  (:require
   [camel-snake-kebab.core :as csk]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop])
  (:import
   (io.r2dbc.spi Row)))

(set! *warn-on-reflection* true)

(def ^:private col-name-gen
  (gen/fmap (fn [[a b c]] (str a "_" b "_" c))
            (gen/tuple gen/string-alphanumeric
                       gen/string-alphanumeric
                       gen/string-alphanumeric)))

(deftest unqualified-kebab-coercion-matches-csk-test
  (is (:result (tc/quick-check
                200
                (prop/for-all
                 [col col-name-gen]
                 (= (keyword (csk/->kebab-case-string col))
                    (row/qualify-column col nil :unqualified-kebab)))))))

(deftest qualified-kebab-coercion-uses-table-namespace-test
  (is (:result (tc/quick-check
                200
                (prop/for-all
                 [col col-name-gen table
                  (gen/fmap (fn [s] (str "tbl_" s)) gen/string-alphanumeric)]
                 (= (keyword table (csk/->kebab-case-string col))
                    (row/qualify-column col table :qualified-kebab)))))))

(deftest qualified-kebab-falls-back-when-table-empty-test
  (is (:result (tc/quick-check
                160
                (prop/for-all
                 [col col-name-gen table (gen/elements [nil ""])]
                 (= (keyword (csk/->kebab-case-string col))
                    (row/qualify-column col table :qualified-kebab)))))))

(deftest metadata-cache-materializes-row-consistently-test
  (testing "row->map with a reused metadata cache returns deterministic maps"
    (let [row-a   (mock/mock-row {"FIRST_NAME" "Alice", "AGE" 30}
                                 {:types {"FIRST_NAME" String, "AGE" Long}})
          row-b   (mock/mock-row {"FIRST_NAME" "Bob", "AGE" 31}
                                 {:types {"FIRST_NAME" String, "AGE" Long}})
          rmd     (.getMetadata ^Row row-a)
          cache   (row/build-metadata-cache
                   rmd
                   (fn [^String col _]
                     (row/qualify-column col nil :unqualified-kebab)))
          map-a-1 (row/row->map row-a cache)
          map-a-2 (row/row->map row-a cache)
          map-b   (row/row->map row-b cache)]
      (is (= map-a-1 map-a-2))
      (is (= {:first-name "Alice", :age 30} map-a-1))
      (is (= {:first-name "Bob", :age 31} map-b)))))
