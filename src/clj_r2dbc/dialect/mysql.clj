;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.mysql
  "Optional MySQL-specific interceptor stages.

  Addresses MySQL's floating-point precision behavior, which returns Double
  and Float values that differ from BigDecimal string representations. Add
  mysql-type-interceptor to :interceptors when precise decimal handling is
  required."
  (:require
   [clj-r2dbc.impl.coerce :as impl-coerce]))

(set! *warn-on-reflection* true)

(def ^{:added "0.1"} mysql-type-interceptor
  "Interceptor that coerces floating-point values to BigDecimal in realized rows.

  Safe to include unconditionally; rows with no Double/Float values are unchanged."
  {:name  ::type-coercion
   :leave (fn [ctx]
            (update ctx
                    :clj-r2dbc/rows
                    (fn [rows] (mapv impl-coerce/coerce-numeric-row rows))))})

(comment
  (def interceptors [mysql-type-interceptor]))
