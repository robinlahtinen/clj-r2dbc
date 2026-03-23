;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.postgresql-test
  (:require
   [clj-r2dbc.dialect.postgresql :as pg]
   [clojure.test :refer [deftest is testing]])
  (:import
   (clojure.lang ExceptionInfo)))

(set! *warn-on-reflection* true)

(deftest pg-array-interceptor-coercion-test
  (testing "pg-array-interceptor coerces SQL arrays to vectors but keeps byte[]"
    (let [binary (byte-array [1 2 3])
          ctx    {:clj-r2dbc/rows [{:tags (into-array String ["a" "b"])
                                    :ints (int-array [10 20])
                                    :blob binary}]}
          out    ((:leave pg/pg-array-interceptor) ctx)
          row    (first (:clj-r2dbc/rows out))]
      (is (= ["a" "b"] (:tags row)))
      (is (= [10 20] (:ints row)))
      (is (instance? (class binary) (:blob row))))))

(deftest pg-json-interceptor-requires-parser-test
  (testing "pg-json-interceptor requires a :json->clj function"
    (is (thrown-with-msg? ExceptionInfo
                          #"requires :json->clj"
                          (pg/pg-json-interceptor {})))))

(deftest pg-json-interceptor-classpath-test
  (testing
   "pg-json-interceptor requires PostgreSQL driver Json codec on classpath"
    (let [has-json? (try (Class/forName "io.r2dbc.postgresql.codec.Json")
                         true
                         (catch ClassNotFoundException _ false))]
      (if has-json?
        (is (map? (pg/pg-json-interceptor {:json->clj identity})))
        (is (thrown-with-msg? ExceptionInfo
                              #"Json codec class not found"
                              (pg/pg-json-interceptor {:json->clj
                                                       identity})))))))
