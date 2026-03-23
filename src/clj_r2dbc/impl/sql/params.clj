;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.sql.params
  "Statement parameter binding utilities for clj-r2dbc.

  Provides:
    bind-params! - binds a sequential of parameters to an R2DBC Statement.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.row :as row])
  (:import
   (clojure.lang IPersistentVector)
   (io.r2dbc.spi Statement)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn bind-params!
  "Bind a sequential collection of parameters to stmt by index.

  Fast paths for the three most common types (nil, String, Long) bypass protocol
  dispatch entirely. All other types fall through to clj-r2dbc.row/Parameter,
  which remains the authoritative write-path extension point.

  Args:
    stmt   - the R2DBC Statement to bind parameters to.
    params - sequential collection of parameter values; nil or empty is a no-op.

  Returns stmt for chaining. When params is nil or empty, stmt is returned unchanged.

  Example:
    (bind-params! stmt [1 \"hello\" nil])"
  ^Statement [^Statement stmt params]
  (when (seq params)
    (let [^IPersistentVector pv (if (vector? params) params (vec params))
          n                     (int (count pv))]
      (loop [i (int 0)]
        (when (< i n)
          (let [p (.nth pv i)]
            (cond (nil? p) (.bindNull stmt (int i) Object)
                  (instance? String p) (.bind stmt (int i) ^String p)
                  (instance? Long p) (.bind stmt (int i) ^Long p)
                  (instance? Integer p) (.bind stmt (int i) ^Integer p)
                  (instance? Double p) (.bind stmt (int i) ^Double p)
                  (instance? Boolean p) (.bind stmt (int i) ^Boolean p)
                  :else (row/-bind p stmt i)))
          (recur (unchecked-inc-int i))))))
  stmt)

(comment
  (require '[clj-r2dbc :as r2] '[missionary.core :as m])
  (def db (r2/connect "r2dbc:h2:mem:///params-repl;DB_CLOSE_DELAY=-1"))
  (m/?
   (r2/batch
    db
    ["CREATE TABLE IF NOT EXISTS t (id INT, name VARCHAR, score DOUBLE, active BOOLEAN)"]))
  (m/? (r2/execute db
                   "INSERT INTO t VALUES ($1, $2, $3, $4)"
                   {:params [1 "Alice" 9.5 true]}))
  (m/? (r2/execute db
                   "INSERT INTO t VALUES ($1, $2, $3, $4)"
                   {:params [2 nil nil nil]}))
  (m/? (r2/execute db "SELECT * FROM t ORDER BY id"))
  (m/? (r2/execute db "SELECT $1 AS n" {:params [(int 42)]}))
  (m/? (r2/execute db "SELECT $1 AS x" {:params [3.14]})))
