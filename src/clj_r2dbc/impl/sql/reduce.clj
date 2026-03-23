;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.sql.reduce
  "Result reduction for clj-r2dbc execution paths.

  Provides:
    make-row-fn         - builds an AtomicReference-cached row-builder function from opts.
    collect-all-results - subscribes to Publisher<Result>, collects a flat vector.
    partition-results   - partitions a flat result vector into :clj-r2dbc/rows and
                          :clj-r2dbc/update-count.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.sql.result :as result]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.impl.util :as util]
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Result Row)
   (java.util.function Predicate)
   (org.reactivestreams Publisher)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn make-row-fn
  "Return a 1-arity (Row -> value) function.

  Builds the RowMetadataCache lazily on the first row then reuses it for
  all subsequent rows in the same result set. This is the hot-loop path.

  Args:
    opts - options map; relevant keys:
             :qualifier  - column name conversion mode; one of :unqualified-kebab
                           (default), :unqualified, or :qualified-kebab.
             :builder-fn - 2-arity (Row, RowMetadata) -> value function; when
                           present, wraps it instead of building a cache.

  Returns a 1-arity function (Row -> value)."
  [opts]
  (if-let [builder (:builder-fn opts)]
    (do (when-not (fn? builder)
          (throw (ex-info "Invalid :builder-fn; expected function"
                          {:clj-r2dbc/error-type :invalid-argument
                           :key                  :builder-fn
                           :value                (type builder)})))
        (fn row-builder [^Row r] (builder r (.getMetadata r))))
    (let [q         (:qualifier opts :unqualified-kebab)
          cache-ref (volatile! nil)]
      (fn cached-row-builder [^Row r]
        (let [rmd   (.getMetadata r)
              cache (or @cache-ref
                        (let [qfn (fn qualify-fn [^String col _]
                                    (row/qualify-column col nil q))
                              c   (row/build-metadata-cache rmd qfn)]
                          (vreset! cache-ref c)
                          c))]
          (row/row->map r cache))))))

(defn collect-all-results
  "Subscribe to result-pub (Publisher<Result>), iterate over every Result,
  and collect a flat vector of all row maps and update-count maps.

  Uses transient vector accumulation for O(N) performance across M Results x N rows.

  Args:
    result-pub  - Publisher<Result> to subscribe to.
    row-fn      - 1-arity (Row -> value) function.
    filter-pred - optional Predicate passed to Result.filter() before processing;
                  nil to skip filtering.

  Returns a Missionary task resolving to a persistent vector."
  [^Publisher result-pub row-fn filter-pred]
  (m/sp
   (let [results (m/? (util/collect-pub result-pub))]
     (loop [rs  results
            acc (transient [])]
       (if-let [[r & more] (seq rs)]
         (let [^Result r'
               (if filter-pred (.filter ^Result r ^Predicate filter-pred) r)]
           (recur more (reduce conj! acc (m/? (result/map-result r' row-fn)))))
         (persistent! acc))))))

(defn partition-results
  "Partition a flat vector of row maps and update-count maps into context keys.

  Args:
    ctx      - context map to update.
    flat-vec - flat vector of row maps and update-count maps.

  Returns ctx with :clj-r2dbc/rows set to the vector of row maps and
  :clj-r2dbc/update-count set to the sum of all UpdateCount values (absent
  when no UpdateCount segments were present)."
  [ctx flat-vec]
  (let [n (int (count flat-vec))]
    (loop [i       (int 0)
           rows    (transient [])
           uc-sum  (long 0)
           has-uc? false]
      (if (< i n)
        (let [item (nth flat-vec i)]
          (if-let [uc (:clj-r2dbc/update-count item)]
            (recur (unchecked-inc-int i) rows (+ uc-sum (long uc)) true)
            (recur (unchecked-inc-int i) (conj! rows item) uc-sum has-uc?)))
        (cond-> (assoc ctx :clj-r2dbc/rows (persistent! rows))
          has-uc? (assoc :clj-r2dbc/update-count uc-sum))))))

(comment
  (def row-fn (make-row-fn {}))
  (def qualified-row-fn (make-row-fn {:qualifier :qualified-kebab}))
  (def custom-row-fn
    (make-row-fn {:builder-fn (fn [row _meta]
                                {:raw-id (.get row "id" Integer)})}))
  (partition-results {} [{:id 1} {:id 2}])
  (partition-results {} [{:clj-r2dbc/update-count 3}])
  (partition-results {} [{:id 7} {:clj-r2dbc/update-count 1}])
  (partition-results {:sql "INSERT ..."} [{:clj-r2dbc/update-count 2}])
  (partition-results {} []))
