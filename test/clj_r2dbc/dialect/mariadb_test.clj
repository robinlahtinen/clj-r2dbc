;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.mariadb-test
  (:require
   [clj-r2dbc.dialect.mariadb :as mariadb]
   [clojure.test :refer [deftest is testing]]))

(set! *warn-on-reflection* true)

(deftest mariadb-type-interceptor-test
  (testing "mariadb-type-interceptor coerces Double/Float to BigDecimal"
    (let [ctx {:clj-r2dbc/rows [{:d 1.25, :f (float 2.5), :i 42}]}
          out ((:leave mariadb/mariadb-type-interceptor) ctx)
          row (first (:clj-r2dbc/rows out))]
      (is (= 1.25M (:d row)))
      (is (= 2.5M (:f row)))
      (is (= 42 (:i row))))))
