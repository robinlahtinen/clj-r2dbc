;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.execute
  "Core execution layer for clj-r2dbc.

  Provides:
    execute-core!   - context-map execute-fn; populates :clj-r2dbc/rows and :clj-r2dbc/update-count.
    execute!*       - main entry point; returns vector (rows, update-count map, or []).
    execute-one!*   - first item or nil.
    execute-each!*  - per-binding result vectors for multi-param-set execution.
    execute-batch!* - vector of update counts for batch SQL execution.

  Precondition: supplying both :middleware and :interceptors throws immediately.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.connection :as conn]
   [clj-r2dbc.impl.datafy :as datafy-impl]
   [clj-r2dbc.impl.execute.pipeline :as pipeline]
   [clj-r2dbc.impl.protocols :as proto]
   [clj-r2dbc.impl.sql.error :as error]
   [clj-r2dbc.impl.sql.params :as params]
   [clj-r2dbc.impl.sql.reduce :as reduce]
   [clj-r2dbc.impl.sql.result :as result]
   [clj-r2dbc.impl.sql.statement :as stmt]
   [clj-r2dbc.impl.util :as util]
   [clj-r2dbc.impl.validate :as validate]
   [missionary.core :as m])
  (:import
   (clj_r2dbc.impl.connection ConnectableWithOpts)
   (io.r2dbc.spi Batch Connection ConnectionFactory R2dbcException Statement)
   (missionary Cancelled)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def ^:private max-batch-size 10000)

(defn- query-returns-rows?
  [sql]
  (boolean (re-find #"(?is)^\s*(select|with|values|show|describe|explain)\b"
                    (or sql ""))))

(defn- execute-result-map
  [ctx db opts]
  (let [rows         (:clj-r2dbc/rows ctx [])
        rows'        (if (:datafy opts)
                       (mapv #(datafy-impl/attach-datafiable-meta % db opts) rows)
                       rows)
        update-count (:clj-r2dbc/update-count ctx)
        sql          (:clj-r2dbc/sql ctx)]
    (cond (and (seq rows') (some? update-count))
          {:clj-r2dbc/rows rows', :clj-r2dbc/update-count update-count}
          (seq rows') {:clj-r2dbc/rows rows'}
          (some? update-count) {:clj-r2dbc/update-count update-count}
          (query-returns-rows? sql) {:clj-r2dbc/rows []}
          :else {:clj-r2dbc/update-count 0})))

(defn- flat-result->execute-result
  [sql db opts flat-vec]
  (execute-result-map
   {:clj-r2dbc/sql          sql
    :clj-r2dbc/rows         (vec (remove :clj-r2dbc/update-count flat-vec))
    :clj-r2dbc/update-count (when-let [ucs (seq (keep :clj-r2dbc/update-count
                                                      flat-vec))]
                              (reduce + 0 (map long ucs)))}
   db
   opts))

(defn execute-core!
  "Execute-fn for the interceptor pipeline.

  Args:
    ctx - context map containing:
            :r2dbc/connection  - active R2DBC Connection.
            :clj-r2dbc/sql     - SQL string.
            :clj-r2dbc/params  - sequential bind parameters.
            :clj-r2dbc/opts    - options map.

  Returns a task that resolves to the updated context map with:
    :clj-r2dbc/rows         - vector of row maps (may be empty).
    :clj-r2dbc/update-count - integer sum of UpdateCount segments (absent when none).

  On R2dbcException, throws ex-info enriched with :clj-r2dbc/error-category,
  :sql-state, :error-code, :sql, and :params.
  missionary.Cancelled is never swallowed."
  [ctx]
  (let [^Connection conn (:r2dbc/connection ctx)
        sql              (:clj-r2dbc/sql ctx)
        params           (:clj-r2dbc/params ctx)
        opts             (:clj-r2dbc/opts ctx {})
        filter-pred      (:filter-segments opts)
        ^Statement s     (stmt/prepare! conn sql params opts)
        row-fn           (reduce/make-row-fn opts)]
    (m/sp (try (let [flat-vec (m/? (reduce/collect-all-results (.execute s)
                                                               row-fn
                                                               filter-pred))]
                 (reduce/partition-results ctx flat-vec))
               (catch Cancelled c (throw c))
               (catch R2dbcException ex
                 (throw (ex-info (.getMessage ex)
                                 (assoc (ex-data (error/r2dbc-ex->ex-info ex))
                                        :sql sql
                                        :params params)
                                 ex)))))))

(defn execute!*
  "Execute sql against db with the given params and opts.

  Args:
    db     - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql    - SQL string.
    params - sequential bind parameters.
    opts   - options map:
               :middleware   - seq of middleware fns (not with :interceptors).
               :interceptors - seq of interceptor maps (not with :middleware).
               :qualifier    - column keyword mode.
               :fetch-size   - calls Statement.fetchSize.
               :returning    - calls Statement.returnGeneratedValues.

  Returns a Missionary task resolving to a labeled map.

  Throws (synchronously):
    ex-info :clj-r2dbc/unsupported when both :middleware and :interceptors are present."
  [db sql params opts]
  (let [[db opts] (conn/resolve-connectable db opts)]
    (validate/check-mode! opts)
    (conn/with-connection*
      db
      (fn [conn]
        (let [ctx {:clj-r2dbc/sql    sql
                   :clj-r2dbc/params params
                   :clj-r2dbc/opts   opts
                   :r2dbc/connection conn}]
          (m/sp (let [ctx'       (if-let [interceptors (:interceptors opts)]
                                   (m/? (pipeline/run-pipeline ctx
                                                               interceptors
                                                               execute-core!
                                                               opts))
                                   (let [mw-fn (if-let [mws (:middleware opts)]
                                                 (reduce comp identity mws)
                                                 identity)]
                                     (m/? ((mw-fn execute-core!) ctx))))
                      result-map (execute-result-map ctx' db opts)]
                  result-map)))))))

(defn execute-one!*
  "Execute sql against db and return the first row or nil.

  Args:
    db     - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql    - SQL string.
    params - sequential bind parameters.
    opts   - options map (see execute!* for supported keys).

  Returns a Missionary task resolving to the first row map or nil."
  [db sql params opts]
  (let [[db opts] (conn/resolve-connectable db opts)]
    (m/sp (let [result (m/? (execute!* db sql params opts))]
            (first (:clj-r2dbc/rows result))))))

(defn execute-each!*
  "Execute sql once per param-set in param-sets via Statement.add() batching.

  Args:
    db         - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql        - SQL string.
    param-sets - sequential collection of binding-set vectors.
    opts       - options map (:qualifier, :fetch-size; not :middleware or
                 :interceptors).

  Returns a Missionary task resolving to {:clj-r2dbc/results [...]}.

  Throws (synchronously):
    ex-info :clj-r2dbc/limit-exceeded when (count param-sets) exceeds
    :max-batch-size (default 10000)."
  [db sql param-sets opts]
  (let [[db opts] (conn/resolve-connectable db opts)]
    (validate/check-mode! opts)
    (let [set-count (count param-sets)
          limit     (if-let [user-val (:max-batch-size opts)]
                      (do (when-not (and (integer? user-val) (pos? (long user-val)))
                            (throw
                             (ex-info
                              "Invalid :max-batch-size; must be a positive integer"
                              {:clj-r2dbc/error   :clj-r2dbc/invalid-value
                               :clj-r2dbc/context :execute-each
                               :key               :max-batch-size
                               :value             user-val})))
                          (long user-val))
                      (long max-batch-size))]
      (when (> set-count limit)
        (throw (ex-info "execute-each!* param-set count exceeds max-batch-size"
                        {:clj-r2dbc/error   :clj-r2dbc/limit-exceeded
                         :clj-r2dbc/context :execute-each
                         :key               :param-sets
                         :constraint        :max-batch-size
                         :limit             limit
                         :value             set-count})))
      (conn/with-connection*
        db
        (fn [^Connection conn]
          (m/sp
           (let [^Statement stmt (.createStatement conn sql)
                 sets            (vec param-sets)
                 last-idx        (dec (count sets))]
             (loop [i 0]
               (when (< i (count sets))
                 (params/bind-params! stmt (nth sets i))
                 (when (< i last-idx) (.add stmt) nil)
                 (recur (unchecked-inc i))))
             (let [row-fn  (reduce/make-row-fn opts)
                   results (m/? (util/collect-pub (.execute stmt)))]
               {:clj-r2dbc/results
                (loop [rs  results
                       acc (transient [])]
                  (if-let [[r & more] (seq rs)]
                    (let [flat-result (m/? (result/map-result r row-fn))]
                      (recur
                       more
                       (conj!
                        acc
                        (flat-result->execute-result sql db opts flat-result))))
                    (persistent! acc)))}))))))))

(defn execute-batch!*
  "Execute a batch of sql-stmts via Connection.createBatch().

  Args:
    db        - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql-stmts - sequential collection of SQL strings.
    opts      - options map.

  Returns a Missionary task resolving to {:clj-r2dbc/update-counts [...]}.
  Row segments from batch results are silently discarded."
  [db sql-stmts _opts]
  (let [[db _opts] (conn/resolve-connectable db _opts)]
    (conn/with-connection*
      db
      (fn [^Connection conn]
        (m/sp
         (let [^Batch batch (.createBatch conn)]
           (doseq [^String s sql-stmts]
             (.add batch s)
             nil)
           (let [results (m/? (util/collect-pub (.execute batch)))]
             {:clj-r2dbc/update-counts
              (loop [rs  results
                     acc (transient [])]
                (if-let [[r & more] (seq rs)]
                  (let [flat (m/? (result/map-result r identity))]
                    (recur more
                           (reduce (fn [a item]
                                     (if-let [uc (:clj-r2dbc/update-count item)]
                                       (conj! a uc)
                                       a))
                                   acc
                                   flat)))
                  (persistent! acc)))})))))))

(extend-protocol proto/Executable
  ConnectionFactory
  (-execute [db sql params opts] (execute!* db sql params opts))
  (-execute-one [db sql params opts] (execute-one!* db sql params opts))
  (-execute-each [db sql param-sets opts]
    (execute-each!* db sql param-sets opts))
  (-execute-batch [db statements opts] (execute-batch!* db statements opts))
  Connection
  (-execute [db sql params opts] (execute!* db sql params opts))
  (-execute-one [db sql params opts] (execute-one!* db sql params opts))
  (-execute-each [db sql param-sets opts]
    (execute-each!* db sql param-sets opts))
  (-execute-batch [db statements opts] (execute-batch!* db statements opts))
  ConnectableWithOpts
  (-execute [db sql params opts] (execute!* db sql params opts))
  (-execute-one [db sql params opts] (execute-one!* db sql params opts))
  (-execute-each [db sql param-sets opts]
    (execute-each!* db sql param-sets opts))
  (-execute-batch [db statements opts] (execute-batch!* db statements opts)))

(comment
  (def factory nil)
  (def logging-mw identity)
  (m/? (execute!* factory "SELECT id, name FROM users" [] {}))
  (m/? (execute!* factory "INSERT INTO users (name) VALUES ($1)" ["Bob"] {}))
  (m/? (execute!* factory "SELECT 1" [] {:middleware [logging-mw]}))
  (m/?
   (execute-each!* factory "INSERT INTO t(id) VALUES ($1)" [[1] [2] [3]] {}))
  (m/? (execute-batch!* factory ["DELETE FROM t" "DELETE FROM u"] {})))
