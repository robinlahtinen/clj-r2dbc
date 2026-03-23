;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.coerce
  "Internal type-coercion utilities shared across clj-r2dbc namespaces.

  Provides:
    byte-array-class      - the Class object for the Java byte[] type.
    parameter-type->class - resolves an R2DBC Type to a Java Class.
    java-array->clj       - converts a Java array value to a persistent vector.
    coerce-row-arrays     - applies java-array->clj to every value in a row map.
    coerce-numeric-row    - coerces Double/Float values in a row map to BigDecimal.

  This namespace is an implementation detail; do not use from application code."
  (:import
   (io.r2dbc.spi Type)))

(set! *warn-on-reflection* true)

(def byte-array-class
  "The Class object for the Java byte array type (\"[B\")."
  (Class/forName "[B"))

(defn parameter-type->class
  "Resolve an R2DBC Type descriptor to a Java Class for use with Statement.bindNull.

  Returns the Class as-is when t is already a Class, the result of
  Type.getJavaType() when t is an R2DBC Type (falling back to Object when
  getJavaType() returns nil), and Object for nil or any other type.

  Args:
    t - Class, R2DBC Type, or nil."
  ^Class [t]
  (cond (nil? t) Object
        (instance? Class t) t
        (instance? Type t) (or (.getJavaType ^Type t) Object)
        :else Object))

(defn java-array->clj
  "Convert a Java array value to a persistent Clojure vector.

  byte[] values are returned as-is (preserving binary data). nil and any other
  non-array value are returned unchanged.

  Args:
    v - any value."
  [v]
  (if (and (some? v)
           (.isArray ^Class (class v))
           (not (instance? byte-array-class v)))
    (vec v)
    v))

(defn coerce-row-arrays
  "Apply java-array->clj to every value in a row map.

  Args:
    row - map of column keyword to column value.

  Returns a new map with the same keys; byte[] column values are preserved as-is."
  [row]
  (reduce-kv (fn [acc k v] (assoc acc k (java-array->clj v))) {} row))

(defn coerce-numeric-row
  "Coerce Double and Float values in a row map to BigDecimal.

  Converts via (bigdec (str v)) to preserve the string representation of the
  floating-point value. All other values are unchanged.

  Args:
    row - map of column keyword to column value."
  [row]
  (reduce-kv (fn [acc k v]
               (assoc acc
                      k
                      (cond (instance? Double v) (bigdec (str v))
                            (instance? Float v) (bigdec (str v))
                            :else v)))
             {}
             row))

(comment
  (java-array->clj (int-array [1 2 3]))
  (java-array->clj (byte-array [1 2 3]))
  (java-array->clj nil)
  (coerce-row-arrays {:ids (int-array [1 2]), :name "Alice"})
  (coerce-numeric-row {:price 9.99, :qty 3}))
