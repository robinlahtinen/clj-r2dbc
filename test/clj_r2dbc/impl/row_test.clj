;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.row-test
  (:require
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing]])
  (:import
   (io.r2dbc.spi ColumnMetadata Nullability Row RowMetadata)))

(set! *warn-on-reflection* true)

(deftest qualify-column-unqualified-test
  (testing ":unqualified preserves the column name as-is"
    (is (= :first_name (row/qualify-column "first_name" nil :unqualified)))))

(deftest qualify-column-unqualified-kebab-test
  (testing ":unqualified-kebab converts underscores to hyphens"
    (is (= :first-name
           (row/qualify-column "first_name" nil :unqualified-kebab)))
    (is (= :user-id (row/qualify-column "user_id" nil :unqualified-kebab)))))

(deftest qualify-column-qualified-kebab-test
  (testing ":qualified-kebab produces namespaced keyword from table and column"
    (is (= :users/first-name
           (row/qualify-column "first_name" "users" :qualified-kebab)))))

(deftest qualify-column-qualified-kebab-nil-table-test
  (testing ":qualified-kebab falls back to unqualified-kebab when table is nil"
    (is (= :first-name
           (row/qualify-column "first_name" nil :qualified-kebab)))))

(deftest qualify-column-qualified-kebab-blank-table-test
  (testing
   ":qualified-kebab falls back to unqualified-kebab when table is empty string"
    (is (= :first-name (row/qualify-column "first_name" "" :qualified-kebab)))))

(deftest build-metadata-cache-col-count-test
  (testing "col-count matches the number of columns in metadata"
    (let [rmd   (mock/mock-row-metadata [{:name "a", :java-type String}
                                         {:name "b", :java-type Long}])
          qfn   (fn [col _cm] (keyword col))
          cache (row/build-metadata-cache rmd qfn)]
      (is (= 2 (.col-count cache))))))

(deftest build-metadata-cache-keywords-test
  (testing "keys array contains the qualified keywords from col-qualify-fn"
    (let [rmd   (mock/mock-row-metadata [{:name "first_name", :java-type String}])
          qfn   (fn [col _cm] (row/qualify-column col nil :unqualified-kebab))
          cache (row/build-metadata-cache rmd qfn)]
      (is (= :first-name (aget ^"[Ljava.lang.Object;" (.keys cache) 0))))))

(deftest build-metadata-cache-java-types-test
  (testing "java-types array contains the Java class for each column"
    (let [rmd   (mock/mock-row-metadata [{:name "id", :java-type Long}])
          qfn   (fn [col _cm] (keyword col))
          cache (row/build-metadata-cache rmd qfn)]
      (is (= Long (aget ^"[Ljava.lang.Object;" (.java-types cache) 0))))))

(deftest build-metadata-cache-null-java-type-test
  (testing "column with no java-type defaults to Object in the cache"
    (let [cm    (reify
                  ColumnMetadata
                  (getName [_] "col")
                  (getJavaType [_] nil)
                  (getNullability [_] Nullability/UNKNOWN))
          rmd   (reify
                  RowMetadata
                  (getColumnMetadatas [_] [cm]))
          qfn   (fn [col _cm] (keyword col))
          cache (row/build-metadata-cache rmd qfn)]
      (is (= Object (aget ^"[Ljava.lang.Object;" (.java-types cache) 0))))))

(deftest row->map-basic-test
  (testing "row->map converts a row to a map with unqualified keyword keys"
    (let [row   (mock/mock-row {"id" 1, "name" "Alice"})
          rmd   (.getMetadata ^Row row)
          qfn   (fn [col _cm] (keyword col))
          cache (row/build-metadata-cache rmd qfn)
          m     (row/row->map row cache)]
      (is (= {:id 1, :name "Alice"} m)))))

(deftest row->map-kebab-test
  (testing "row->map with kebab qualifier converts underscores to hyphens"
    (let [row   (mock/mock-row {"first_name" "Bob"})
          rmd   (.getMetadata ^Row row)
          qfn   (fn [col _cm] (row/qualify-column col nil :unqualified-kebab))
          cache (row/build-metadata-cache rmd qfn)
          m     (row/row->map row cache)]
      (is (= {:first-name "Bob"} m)))))

(deftest row->map-nil-values-test
  (testing "nil column values appear as nil in the result map"
    (let [row   (mock/mock-row {"name" nil})
          rmd   (.getMetadata ^Row row)
          qfn   (fn [col _cm] (keyword col))
          cache (row/build-metadata-cache rmd qfn)
          m     (row/row->map row cache)]
      (is (contains? m :name))
      (is (nil? (:name m))))))

(deftest metadata-cache-reuse-test
  (testing "same cache works correctly for two different rows"
    (let [rmd   (mock/mock-row-metadata [{:name "id", :java-type Long}])
          row1  (mock/mock-row {"id" 1} {:metadata rmd, :types {"id" Long}})
          row2  (mock/mock-row {"id" 2} {:metadata rmd, :types {"id" Long}})
          qfn   (fn [col _cm] (keyword col))
          cache (row/build-metadata-cache rmd qfn)]
      (is (= {:id 1} (row/row->map row1 cache)))
      (is (= {:id 2} (row/row->map row2 cache))))))

(deftest make-row-fn-default-test
  (testing "make-row-fn with default options produces kebab-case keys"
    (let [rfn (row/make-row-fn)
          row (mock/mock-row {"first_name" "Alice"})
          rmd (.getMetadata ^Row row)
          m   (rfn row rmd)]
      (is (= {:first-name "Alice"} m)))))
