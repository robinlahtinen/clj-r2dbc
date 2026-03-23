;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.h2-test
  (:require
   [clj-r2dbc.dialect.h2 :as h2]
   [clojure.test :refer [deftest is testing]]))

(set! *warn-on-reflection* true)

(deftest h2-array-interceptor-coercion-test
  (testing "h2-array-interceptor coerces arrays to vectors but keeps byte[]"
    (let [binary (byte-array [1 2 3])
          ctx    {:clj-r2dbc/rows [{:tags (into-array String ["a" "b"])
                                    :ints (int-array [10 20])
                                    :blob binary}]}
          out    ((:leave h2/h2-array-interceptor) ctx)
          row    (first (:clj-r2dbc/rows out))]
      (is (= ["a" "b"] (:tags row)))
      (is (= [10 20] (:ints row)))
      (is (instance? (class binary) (:blob row))))))
