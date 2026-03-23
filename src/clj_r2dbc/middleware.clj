;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.middleware
  "Functional middleware extensions for clj-r2dbc.

  Middleware are higher-order functions (HOFs) of the form:
  (fn [execute-fn] (fn [ctx] task)).

  They compose left-to-right via (reduce comp identity middleware-seq): the
  first middleware in the seq is the outermost wrapper. Unlike interceptors,
  which use a data-driven enter/leave/error model, middleware are plain
  function closures - simpler to write but without the structured error-phase
  separation that interceptors provide.

  Pass a seq of middleware fns via the :middleware option.

  Provides:
    with-logging     - logs SQL timing, row count, and errors.
    with-transaction - factory; wraps execution in begin/commit/rollback."
  (:require
   [clj-r2dbc.impl.transaction :as tx]
   [clj-r2dbc.impl.util :as util]
   [missionary.core :as m]
   [taoensso.trove :as trove])
  (:import
   (io.r2dbc.spi Connection)))

(set! *warn-on-reflection* true)

(defn ^{:added "0.1"} with-logging
  "Wrap execute-fn to log SQL execution timing and row/update counts.

  Params are intentionally omitted from logs.

  Args:
    execute-fn - 1-arity fn [ctx] -> task; the next handler in the chain.

  Returns a 1-arity fn [ctx] -> task."
  [execute-fn]
  (fn [ctx]
    (m/sp
     (let [t0 (System/nanoTime)]
       (try (let [ctx'         (m/? (execute-fn ctx))
                  rows         (:clj-r2dbc/rows ctx' [])
                  update-count (:clj-r2dbc/update-count ctx')]
              (trove/log! {:level        :info
                           :event        :clj-r2dbc/query
                           :sql          (:clj-r2dbc/sql ctx)
                           :elapsed-ms   (util/elapsed-ms t0)
                           :row-count    (count rows)
                           :update-count update-count})
              ctx')
            (catch Throwable t
              (trove/log! {:level      :error
                           :event      :clj-r2dbc/query-error
                           :sql        (:clj-r2dbc/sql ctx)
                           :elapsed-ms (util/elapsed-ms t0)
                           :error      t})
              (throw t)))))))

(defn ^{:added "0.1"} with-transaction
  "Return a middleware function that wraps execute-fn in begin/commit/rollback.

  When :clj-r2dbc/in-transaction? is already true in the context, passes
  through without modifying the transaction (join semantics).

  Args:
    tx-opts - (optional) transaction options map; defaults to {}.
                :isolation            - isolation level keyword
                                        (see impl/transaction isolation-map).
                :read-only?           - boolean; default false.
                :name                 - string savepoint or transaction name.
                :lock-wait-timeout-ms - maximum time to wait for a lock, in ms.
              Accepts a map, keyword arguments, or both.

  Returns a middleware function [execute-fn] -> [ctx] -> task.

  Example:
    (with-transaction)
    (with-transaction {:isolation :read-committed})
    (with-transaction :isolation :read-committed)"
  ([& {:as tx-opts}]
   (let [tx-opts (or tx-opts {})]
     (fn [execute-fn]
       (fn [ctx]
         (m/sp (if (:clj-r2dbc/in-transaction? ctx)
                 (m/? (execute-fn ctx))
                 (let [conn ^Connection (:r2dbc/connection ctx)]
                   (m/? (tx/begin-transaction-task conn tx-opts))
                   (try (let [ctx' (m/? (execute-fn (assoc
                                                     ctx
                                                     :clj-r2dbc/in-transaction?
                                                     true)))]
                          (m/? (util/void->task (.commitTransaction conn)))
                          ctx')
                        (catch Throwable t
                          (m/? (tx/rollback-with-suppression! conn t))
                          (throw t)))))))))))

(comment
  (require '[clj-r2dbc :as r2dbc])
  (def db (r2dbc/connect "r2dbc:h2:mem:///mw-test;DB_CLOSE_DELAY=-1"))
  (m/? (r2dbc/execute db
                      "SELECT 1 AS n"
                      {:middleware [with-logging
                                    (with-transaction {:isolation
                                                       :read-committed})]})))
