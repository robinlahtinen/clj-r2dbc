;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.sql.statement
  "Statement preparation utilities for clj-r2dbc.

  Provides a single seam for statement creation and option application that is
  shared between impl/exec/core and impl/conn/lifecycle (streaming). Centralizing
  these operations here removes duplication between execute and streaming paths.

  Provides:
    apply-opts!      - apply :fetch-size and :returning opts to a Statement.
    create-statement - create a Statement from a Connection and SQL string.
    bind!            - bind a params collection to a Statement (delegates to impl/sql/params).
    prepare!         - compose create-statement + apply-opts! + bind! in one call.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.sql.params :as params])
  (:import
   (io.r2dbc.spi Connection Statement)))

(set! *warn-on-reflection* true)

(defn apply-opts!
  "Apply Statement options from opts to stmt. No-ops when keys absent.

  Args:
    stmt - the R2DBC Statement to configure.
    opts - options map; relevant keys:
             :fetch-size - calls Statement.fetchSize(int); must be >= 0 and
                           <= Integer/MAX_VALUE.
             :returning  - calls Statement.returnGeneratedValues(String...).

  Returns nil; call for side effects only."
  [^Statement stmt opts]
  (when-let [fs (:fetch-size opts)]
    (when (neg? (long fs))
      (throw (ex-info "Invalid :fetch-size; must be >= 0"
                      {:clj-r2dbc/error-type :invalid-argument
                       :key                  :fetch-size
                       :value                fs})))
    (when (> (long fs) Integer/MAX_VALUE)
      (throw (ex-info "fetch-size exceeds Integer/MAX_VALUE"
                      {:clj-r2dbc/error-type :bound-exceeded
                       :key                  :fetch-size
                       :limit                Integer/MAX_VALUE
                       :value                fs})))
    (.fetchSize stmt (int fs)))
  (when-let [ret (:returning opts)]
    (.returnGeneratedValues stmt
                            ^"[Ljava.lang.String;" (into-array String ret)))
  nil)

(defn create-statement
  "Create an R2DBC Statement from conn and sql.

  Args:
    conn - an active R2DBC Connection; must be non-nil.
    sql  - the SQL string; must be non-nil.

  Returns the created Statement."
  ^Statement [^Connection conn ^String sql]
  (assert conn "conn must not be nil - validated at boundary")
  (assert (string? sql) "sql must be a string - validated at boundary")
  (.createStatement conn sql))

(defn bind!
  "Bind params to stmt. Delegates to impl/sql/params/bind-params!.

  Args:
    stmt   - the R2DBC Statement to bind to.
    params - sequential collection of parameter values.

  Returns stmt for chaining."
  ^Statement [^Statement stmt params]
  (params/bind-params! stmt params))

(defn prepare!
  "Create a Statement, apply opts, and bind params in one call.

  Args:
    conn   - an active R2DBC Connection.
    sql    - the SQL string.
    params - sequential collection of parameter values.
    opts   - options map (see apply-opts! for supported keys).

  Returns the prepared Statement."
  ^Statement [^Connection conn ^String sql params opts]
  (doto (create-statement conn sql) (bind! params) (apply-opts! opts)))

(comment
  (def conn nil)
  (def stmt (prepare! conn "SELECT $1" [42] {:fetch-size 64}))
  (.execute stmt))
