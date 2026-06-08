;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.sql.row
  "Hot-path row-to-map conversion for clj-r2dbc.

  Two-phase row conversion pipeline:
  1. Build a RowMetadataCache once per result set using build-metadata-cache.
     This is an O(n-columns) operation that pre-computes keyword keys and Java
     types so the hot loop can use array access without per-row SPI calls.
  2. Call row->map on every row in the hot loop using the pre-built cache.
     Uses unchecked arithmetic, direct array access, and transient maps.

  Provides:
    RowMetadataCache     - deftype caching pre-computed keys and Java types.
    qualify-column       - column name to keyword, with qualifier mode support.
    build-metadata-cache - builds a RowMetadataCache from RowMetadata (once per result set).
    row->map             - converts a Row to a persistent map using a pre-built cache.
    make-row-fn          - convenience: returns a (Row, RowMetadata) -> map function.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [camel-snake-kebab.core :as csk])
  (:import
   (clojure.lang ArityException PersistentArrayMap)
   (io.r2dbc.spi ColumnMetadata Row RowMetadata)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(deftype
 ^{:doc
   "Precomputed column metadata for a result set.

   Fields:
     keys      - Object array of pre-built keyword keys.
     java-types - Object array of column Java types.
     col-count  - column count as a primitive int.

   Built once via build-metadata-cache and shared across all rows in a
   result set to eliminate per-row metadata lookups."}
 RowMetadataCache
 [^"[Ljava.lang.Object;" keys ^"[Ljava.lang.Object;" java-types
  ^int col-count])

(defn qualify-column
  "Map a column name and table name to a keyword using the given mode.

  Args:
    col-name   - the column name string.
    table-name - the table name string; may be nil.
    mode       - qualification mode keyword:
                   :unqualified       - (keyword col-name), no transformation.
                   :unqualified-kebab - (keyword (->kebab-case col-name)).
                   :qualified-kebab   - (keyword table-name (->kebab-case col-name));
                                        falls back to unqualified-kebab when
                                        table-name is nil or empty.

  Example:
    (qualify-column \"first_name\" \"users\" :qualified-kebab) ;;=> :users/first-name
    (qualify-column \"first_name\" nil       :qualified-kebab) ;;=> :first-name"
  [^String col-name ^String table-name mode]
  (case mode
    :unqualified (keyword col-name)
    :unqualified-kebab (keyword (csk/->kebab-case-string col-name))
    :qualified-kebab (if (and table-name (not (.isEmpty table-name)))
                       (keyword table-name (csk/->kebab-case-string col-name))
                       (keyword (csk/->kebab-case-string col-name)))))

(defn build-metadata-cache
  "Build a RowMetadataCache from a RowMetadata, called once per result set.

  Args:
    rmd           - the RowMetadata to read column info from.
    col-qualify-fn - 2-arity (String col-name, ColumnMetadata cm) -> keyword
                     function that produces the map key for each column.

  Java type defaults to Object when ColumnMetadata.getJavaType() returns nil.

  Example:
    (build-metadata-cache rmd (fn [col-name _cm] (keyword col-name)))"
  ^RowMetadataCache [^RowMetadata rmd col-qualify-fn]
  (let [col-metas (vec (.getColumnMetadatas rmd))
        n         (count col-metas)
        ks        (object-array n)
        types     (object-array n)]
    (dotimes [i n]
      (let [^ColumnMetadata cm (nth col-metas i)]
        (aset ks i (col-qualify-fn (.getName cm) cm))
        (aset types i (or (.getJavaType cm) Object))))
    (RowMetadataCache. ks types n)))

(defn row->map
  "Convert an R2DBC Row to a persistent Clojure map using a pre-built cache.

  This is the hot loop: uses primitive int, unchecked arithmetic, array access,
  and transient map construction for maximum throughput.

  Args:
    row   - the R2DBC Row to convert.
    cache - RowMetadataCache built with build-metadata-cache.

  Example:
    (row->map row cache) ;;=> {:id 1 :name \"Alice\"}"
  [^Row row ^RowMetadataCache cache]
  (let [n                          (.col-count cache)
        ^"[Ljava.lang.Object;" ks  (.keys cache)
        ^"[Ljava.lang.Object;" tys (.java-types cache)]
    (if (<= n 8)
      (let [arr (object-array (* 2 n))]
        (loop [i (int 0)]
          (when (< i n)
            (aset arr (unchecked-multiply 2 i) (aget ks i))
            (aset arr
                  (unchecked-inc (unchecked-multiply 2 i))
                  (.get row (int i) ^Class (aget tys i)))
            (recur (unchecked-inc i))))
        (PersistentArrayMap. arr))
      (loop [i (int 0)
             m (transient {})]
        (if (< i n)
          (recur (unchecked-inc i)
                 (assoc! m (aget ks i) (.get row (int i) ^Class (aget tys i))))
          (persistent! m))))))

(defn make-row-fn
  "Return a (Row, RowMetadata) -> map function for use with result processing.

  Caches the RowMetadataCache across invocations using RowMetadata identity
  comparison. The cache is built once per result set (when RowMetadata changes)
  and reused for all subsequent rows in the same result set.

  Args:
    opts - (optional) options map; relevant keys:
             :qualifier - :unqualified, :unqualified-kebab (default), or
                          :qualified-kebab.

  Example:
    (make-row-fn)
    ((make-row-fn) row rmd) ;;=> {:first-name \"Alice\"}"
  ([] (make-row-fn {}))
  ([{:keys [qualifier], :or {qualifier :unqualified-kebab}}]
   (let [cache-ref (volatile! nil)
         rmd-ref   (volatile! nil)
         qfn       (fn [^String col-name _cm]
                     (qualify-column col-name nil qualifier))]
     (fn [^Row row ^RowMetadata rmd]
       (let [cache (if (identical? rmd @rmd-ref)
                     @cache-ref
                     (let [c (build-metadata-cache rmd qfn)]
                       (vreset! rmd-ref rmd)
                       (vreset! cache-ref c)
                       c))]
         (row->map row cache))))))

(defn make-vector-fn
  "Return a (Row, RowMetadata) -> vector builder function.

  Builds a RowMetadataCache once per result set (keyed by RowMetadata identity)
  and reuses it for all subsequent rows. Each row is emitted as a persistent
  vector of column values in result-set order.

  Example:
    (make-vector-fn)
    ((make-vector-fn) row rmd) ;;=> [1 \"Alice\" true]"
  []
  (let [cache-ref (volatile! nil)]
    (fn [^Row r ^RowMetadata rmd]
      (let [cached @cache-ref
            cache  (if (and cached (identical? (:row-metadata cached) rmd))
                     (:cache cached)
                     (let [c (build-metadata-cache rmd
                                                   (fn [^String col _cm] col))]
                       (vreset! cache-ref {:row-metadata rmd, :cache c})
                       c))
            n      (.col-count ^RowMetadataCache cache)
            tys    ^"[Ljava.lang.Object;" (.java-types ^RowMetadataCache cache)]
        (loop [i   (int 0)
               acc (transient [])]
          (if (< i n)
            (recur (unchecked-inc i)
                   (conj! acc (.get r (int i) ^Class (aget tys i))))
            (persistent! acc)))))))

(defn map-builder-fn
  "Return a (Row, RowMetadata) -> map builder function using the supplied column key function.

  Caches the RowMetadataCache across invocations using RowMetadata identity
  comparison. The cache is built once per result set and reused for all subsequent rows.

  Args:
    col->key-fn - 2-arity (String col-name, ColumnMetadata cm) -> keyword function.

  Example:
    (map-builder-fn (fn [col _cm] (keyword col)))"
  [col->key-fn]
  (let [cache-ref (volatile! nil)]
    (fn [^Row r ^RowMetadata rmd]
      (let [cached @cache-ref
            cache  (if (and cached (identical? (:row-metadata cached) rmd))
                     (:cache cached)
                     (let [c (build-metadata-cache rmd col->key-fn)]
                       (vreset! cache-ref {:row-metadata rmd, :cache c})
                       c))]
        (row->map r cache)))))

(defn qualifier-value->ns
  "Convert a qualifier value to a namespace string.

  Args:
    qv - qualifier value; accepts String (used as-is), keyword (namespace or
         name), or symbol (name).

  Returns a namespace string, or nil for any other type."
  ^String [qv]
  (cond (string? qv) qv
        (keyword? qv) (or (namespace qv) (name qv))
        (symbol? qv) (name qv)
        :else nil))

(defn qualify-with-explicit
  "Apply an explicit qualifier to col-name, producing a qualified keyword.

  Args:
    qualifier - String, keyword, or symbol (static namespace); or a function
                of [col-name col-meta] -> namespace-value (1-arity accepted
                as fallback when the 2-arity call throws ArityException).
    col-name  - the column name string.
    col-meta  - the ColumnMetadata passed to qualifier when it is a function.
    col-xf    - (String -> String) case transformer applied to col-name before
                building the keyword (e.g., csk/->kebab-case-string or identity).

  Returns a qualified keyword.

  Throws (synchronously):
    ex-info :clj-r2dbc/error-type :invalid-argument when the resolved namespace
    is nil or blank."
  [qualifier col-name col-meta col-xf]
  (let [resolved (if (fn? qualifier)
                   (try (qualifier col-name col-meta)
                        (catch ArityException _ (qualifier col-name)))
                   qualifier)
        ns-name  (qualifier-value->ns resolved)]
    (when (or (nil? ns-name) (.isEmpty ^String ns-name))
      (throw (ex-info
              "Invalid :qualifier; expected non-empty string/keyword/symbol"
              {:clj-r2dbc/error-type :invalid-argument
               :key                  :qualifier
               :value                resolved})))
    (keyword ns-name (col-xf col-name))))

(defn ensure-qualifier
  "Validate that opts contains a non-nil :qualifier key.

  Args:
    opts - options map to check.

  Returns the :qualifier value.

  Throws (synchronously):
    ex-info :clj-r2dbc/error-type :invalid-argument when :qualifier is absent."
  [opts]
  (let [q (:qualifier opts)]
    (when-not q
      (throw (ex-info "Missing required :qualifier"
                      {:clj-r2dbc/error-type :invalid-argument
                       :key                  :qualifier
                       :value                nil})))
    q))

(comment
  (let [row nil rmd nil] ((make-row-fn) row rmd))
  (qualify-column "first_name" "users" :qualified-kebab)
  (let [rmd   nil
        rows  []
        cache (build-metadata-cache rmd (fn [col _cm] (keyword col)))]
    (mapv #(row->map % cache) rows))
  ((make-vector-fn) nil nil)
  ((map-builder-fn (fn [col _cm] (keyword col))) nil nil))
