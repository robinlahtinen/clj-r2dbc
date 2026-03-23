;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.postgresql
  "Optional PostgreSQL-specific interceptor stages.

  Addresses two PostgreSQL type-mapping gaps: ARRAY columns (returned as Java
  arrays) and JSON/JSONB columns (returned as io.r2dbc.postgresql.codec.Json).
  Add pg-array-interceptor, pg-json-interceptor, or both to :interceptors as
  needed."
  (:require
   [clj-r2dbc.dialect.impl.postgresql :as impl-pg]
   [clj-r2dbc.impl.coerce :as impl-coerce])
  (:import
   (java.lang.reflect Method)))

(set! *warn-on-reflection* true)

(def ^{:added "0.1"} pg-array-interceptor
  "Interceptor that converts PostgreSQL array column values to persistent vectors.

  Binary byte[] values are preserved as-is."
  {:name  ::pg-array
   :leave (fn [ctx]
            (update ctx
                    :clj-r2dbc/rows
                    (fn [rows] (mapv impl-coerce/coerce-row-arrays rows))))})

(defn ^{:added "0.1"} pg-json-interceptor
  "Return an interceptor that coerces PostgreSQL JSON/JSONB column values.

  Args:
    opts       - options map.
      :json->clj - required; 1-arity fn [json-string] -> Clojure value.

  Returns an interceptor map with :name and :leave keys."
  [{:keys [json->clj]}]
  (when-not (fn? json->clj)
    (throw (ex-info "pg-json-interceptor requires :json->clj function"
                    {:clj-r2dbc/error-type :invalid-argument
                     :key                  :json->clj
                     :value                json->clj})))
  (let
   [{:keys [json-class as-string]}
    (or
     (impl-pg/load-json-reflection)
     (throw
      (ex-info
       "PostgreSQL Json codec class not found; add io.r2dbc/r2dbc-postgresql to classpath"
       {:clj-r2dbc/error-type :missing-dependency
        :dependency           "io.r2dbc/r2dbc-postgresql"})))]
    {:name  ::pg-json
     :leave (fn [ctx]
              (update ctx
                      :clj-r2dbc/rows
                      (fn [rows]
                        (mapv #(impl-pg/coerce-row-json %
                                                        json-class
                                                        ^Method as-string
                                                        json->clj)
                              rows))))}))

(comment
  (require '[clojure.edn :as edn])
  (def pg-interceptors
    [pg-array-interceptor (pg-json-interceptor {:json->clj edn/read-string})]))
