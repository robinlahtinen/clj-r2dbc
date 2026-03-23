;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.impl.postgresql
  "Internal PostgreSQL-specific utilities for clj-r2dbc.

  Provides:
    load-json-reflection - reflectively loads the io.r2dbc.postgresql.codec.Json class.
    coerce-row-json      - applies JSON decoding to matching column values in a row map.

  This namespace is an implementation detail; do not use from application code."
  (:import
   (java.lang.reflect Method)))

(set! *warn-on-reflection* true)

(defn load-json-reflection
  "Load the PostgreSQL Json codec class by reflection.

  Returns a map {:json-class Class, :as-string Method} on success, or nil when
  the class is not on the classpath (r2dbc-postgresql driver not present)."
  []
  (try (let [json-class                                               (Class/forName "io.r2dbc.postgresql.codec.Json")
             ^Method as-string
             (.getMethod json-class "asString" (into-array Class []))]
         {:json-class json-class, :as-string as-string})
       (catch ClassNotFoundException _ nil)))

(defn coerce-row-json
  "Apply JSON decoding to every column value in row that is an instance of json-class.

  Args:
    row        - row map to transform.
    json-class - the io.r2dbc.postgresql.codec.Json Class.
    as-string  - reflected Method for Json.asString().
    json->clj  - 1-arity fn [json-string] -> Clojure value.

  Returns a new map with JSON column values decoded; non-JSON values are unchanged."
  [row json-class ^Method as-string json->clj]
  (reduce-kv
   (fn [acc k v]
     (if (instance? json-class v)
       (assoc acc k (json->clj (.invoke as-string v (object-array 0))))
       (assoc acc k v)))
   {}
   row))

(comment
  (load-json-reflection))
