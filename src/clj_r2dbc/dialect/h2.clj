;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.h2
  "Optional H2-specific interceptor stages.

  Addresses H2's ARRAY type mapping, which returns Java arrays rather than
  Clojure persistent vectors. Add h2-array-interceptor to :interceptors when
  querying H2 ARRAY columns."
  (:require
   [clj-r2dbc.impl.coerce :as impl-coerce]))

(set! *warn-on-reflection* true)

(def ^{:added "0.1"} h2-array-interceptor
  "Interceptor that converts H2 ARRAY column values to persistent vectors.

  Binary byte[] values are preserved as-is."
  {:name  ::h2-array
   :leave (fn [ctx]
            (update ctx
                    :clj-r2dbc/rows
                    (fn [rows] (mapv impl-coerce/coerce-row-arrays rows))))})

(comment
  (def interceptors [h2-array-interceptor]))
