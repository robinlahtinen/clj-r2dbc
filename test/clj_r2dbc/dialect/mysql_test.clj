;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.mysql-test
  (:require
   [clj-r2dbc.dialect.mysql :as mysql]
   [clojure.test :refer [deftest is testing]]))

(set! *warn-on-reflection* true)

(deftest mysql-type-interceptor-test
  (testing "mysql-type-interceptor coerces Double/Float to BigDecimal"
    (let [ctx {:clj-r2dbc/rows [{:d 1.25, :f (float 2.5), :i 42}]}
          out ((:leave mysql/mysql-type-interceptor) ctx)
          row (first (:clj-r2dbc/rows out))]
      (is (= 1.25M (:d row)))
      (is (= 2.5M (:f row)))
      (is (= 42 (:i row))))))
