;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.oracle-test
  (:require
   [clj-r2dbc.dialect.oracle :as oracle]
   [clj-r2dbc.test-util.db :as db]
   [clojure.test :refer [deftest is testing]])
  (:import
   (clojure.lang ExceptionInfo)
   (io.r2dbc.spi Blob Clob)))

(set! *warn-on-reflection* true)

(deftest oracle-lob-interceptor-enter-rewrites-oversized-params-test
  (testing
   "oracle-lob-interceptor rewrites oversized String/byte[] params to LOBs"
    (let [large-str   (apply str (repeat 40000 "x"))
          large-bytes (byte-array 40000)
          ctx         {:clj-r2dbc/params [large-str large-bytes "small"]}
          out         ((:enter oracle/oracle-lob-interceptor) ctx)
          [p0 p1 p2]  (:clj-r2dbc/params out)]
      (is (instance? Clob p0))
      (is (instance? Blob p1))
      (is (= "small" p2))
      (is (= 2 (count (::oracle/lob-handles out)))))))

(deftest oracle-lob-interceptor-with-custom-threshold-test
  (testing "oracle-lob-interceptor-with accepts custom threshold"
    (let [interceptor (oracle/oracle-lob-interceptor-with {:lob-threshold-bytes
                                                           100})
          small-str   (apply str (repeat 50 "x"))
          large-str   (apply str (repeat 200 "x"))
          ctx         {:clj-r2dbc/params [small-str large-str]}
          out         ((:enter interceptor) ctx)
          [p0 p1]     (:clj-r2dbc/params out)]
      (is (= small-str p0))
      (is (instance? Clob p1))
      (is (= 1 (count (::oracle/lob-handles out)))))))

(deftest oracle-lob-interceptor-with-default-threshold-test
  (testing "default oracle-lob-interceptor still uses 32768"
    (let [just-under (apply str (repeat 32768 "x"))
          just-over  (apply str (repeat 32769 "x"))
          ctx-under  {:clj-r2dbc/params [just-under]}
          ctx-over   {:clj-r2dbc/params [just-over]}
          out-under  ((:enter oracle/oracle-lob-interceptor) ctx-under)
          out-over   ((:enter oracle/oracle-lob-interceptor) ctx-over)]
      (is (= just-under (first (:clj-r2dbc/params out-under))))
      (is (instance? Clob (first (:clj-r2dbc/params out-over)))))))

(deftest oracle-lob-interceptor-with-invalid-threshold-test
  (testing "zero threshold throws :invalid-argument"
    (is (thrown-with-msg? ExceptionInfo
                          #"Invalid :lob-threshold-bytes"
                          (oracle/oracle-lob-interceptor-with
                           {:lob-threshold-bytes 0}))))
  (testing "negative threshold throws :invalid-argument"
    (is (thrown-with-msg? ExceptionInfo
                          #"Invalid :lob-threshold-bytes"
                          (oracle/oracle-lob-interceptor-with
                           {:lob-threshold-bytes -1})))))

(deftest oracle-lob-interceptor-cleanup-test
  (testing
   "oracle-lob-interceptor cleans up LOB handles in leave and error phases"
    (let [large-str (apply str (repeat 40000 "x"))
          ctx       {:clj-r2dbc/params [large-str]}
          entered   ((:enter oracle/oracle-lob-interceptor) ctx)
          left      (db/run-task! ((:leave oracle/oracle-lob-interceptor) entered))
          entered-2 ((:enter oracle/oracle-lob-interceptor) ctx)
          errored   (db/run-task!
                     ((:error oracle/oracle-lob-interceptor)
                      (assoc entered-2 :clj-r2dbc/error (ex-info "boom" {}))))]
      (is (not (contains? left ::oracle/lob-handles)))
      (is (not (contains? errored ::oracle/lob-handles))))))
