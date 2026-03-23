;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.result-test
  (:require
   [clj-r2dbc.impl.sql.result :as result]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing]])
  (:import
   (io.r2dbc.spi Result$Message Result$RowSegment Result$UpdateCount Row)))

(set! *warn-on-reflection* true)

(defn- advisory-message
  []
  (reify
    Result$Message
    (message [_] "advisory")
    (errorCode [_] 0)
    (sqlState [_] nil)))

(deftest map-result-empty-test
  (testing "empty mock result yields an empty vector"
    (let [res (db/run-task! (result/map-result (mock/mock-result) identity))]
      (is (= [] res)))))

(deftest map-result-rows-test
  (testing "two mock rows are each passed through row-fn"
    (let [row1   (mock/mock-row {"id" 1})
          row2   (mock/mock-row {"id" 2})
          r      (mock/mock-result {:rows [row1 row2]})
          row-fn (fn [^Row row] (.get row "id" Long))
          res    (db/run-task! (result/map-result r row-fn))]
      (is (= 2 (count res)))
      (is (= 1 (first res)))
      (is (= 2 (second res))))))

(deftest map-result-update-count-test
  (testing "update count segment yields {:clj-r2dbc/update-count n}"
    (let [seg (reify
                Result$UpdateCount
                (value [_] 3))
          r   (mock/mock-result {:segments [seg]})
          res (db/run-task! (result/map-result r identity))]
      (is (= [{:clj-r2dbc/update-count 3}] res)))))

(deftest map-result-message-only-test
  (testing "a single advisory message segment yields an empty vector"
    (let [r   (mock/mock-result {:segments [(advisory-message)]})
          res (db/run-task! (result/map-result r identity))]
      (is (= [] res)))))

(deftest map-result-message-not-abort-test
  (testing
   "advisory message does not abort; remaining row segments are still processed"
    (let [inner-row (mock/mock-row {"id" 42})
          row-seg   (reify
                      Result$RowSegment
                      (row [_] inner-row))
          r         (mock/mock-result {:segments [(advisory-message) row-seg]})
          row-fn    (fn [^Row row] (.get row "id" Long))
          res       (db/run-task! (result/map-result r row-fn))]
      (is (= [42] res)))))

(deftest map-result-with-row->map-test
  (testing
   "integration: mock-result with underscore columns + row->map -> kebab-case map"
    (let [row    (mock/mock-row {"first_name" "Alice"})
          rmd    (.getMetadata ^Row row)
          r      (mock/mock-result {:rows [row]})
          qfn    (fn [col _cm] (row/qualify-column col nil :unqualified-kebab))
          cache  (row/build-metadata-cache rmd qfn)
          row-fn (fn [r] (row/row->map r cache))
          res    (db/run-task! (result/map-result r row-fn))]
      (is (= [{:first-name "Alice"}] res)))))
