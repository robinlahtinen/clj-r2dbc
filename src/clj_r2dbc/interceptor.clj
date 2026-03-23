;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.interceptor
  "Data-driven interceptor extensions for the clj-r2dbc pipeline.

  Interceptors are plain maps with four optional phase keys:
    :enter  - called before execution with the context map; may return an
              updated context (sync) or a Missionary task (async).
    :leave  - called after successful execution in reverse enter order.
    :error  - called on failure; receives the context with :clj-r2dbc/error.
    :name   - optional identifier for debugging.

  Interceptors compose as a stack: enter runs FIFO, leave and error run LIFO.
  Pass a seq of interceptor maps via the :interceptors option.

  Provides:
    logging-interceptor     - logs SQL timing, row count, and errors.
    transaction-interceptor - factory; wraps execution in begin/commit/rollback."
  (:require
   [clj-r2dbc.impl.transaction :as tx]
   [clj-r2dbc.impl.util :as util]
   [missionary.core :as m]
   [taoensso.trove :as trove])
  (:import
   (io.r2dbc.spi Connection)))

(set! *warn-on-reflection* true)

(def ^{:added "0.1"} logging-interceptor
  "Interceptor that logs SQL execution timing and row/update counts.

  Attaches a start timestamp on :enter, logs success on :leave, and
  logs errors on :error. Params are intentionally omitted (redacted)."
  {:name  ::logging
   :enter (fn [ctx] (assoc ctx ::log-t0 (System/nanoTime)))
   :leave (fn [ctx]
            (let [rows         (:clj-r2dbc/rows ctx [])
                  update-count (:clj-r2dbc/update-count ctx)]
              (trove/log! {:level        :info
                           :event        :clj-r2dbc/query
                           :sql          (:clj-r2dbc/sql ctx)
                           :elapsed-ms   (util/elapsed-ms (::log-t0 ctx))
                           :row-count    (count rows)
                           :update-count update-count})
              (dissoc ctx ::log-t0)))
   :error (fn [ctx]
            (trove/log! {:level      :error
                         :event      :clj-r2dbc/query-error
                         :sql        (:clj-r2dbc/sql ctx)
                         :elapsed-ms (util/elapsed-ms (::log-t0 ctx))
                         :error      (:clj-r2dbc/error ctx)})
            (dissoc ctx ::log-t0))})

(defn ^{:added "0.1"} transaction-interceptor
  "Return an interceptor map that wraps execution in begin/commit/rollback.

  :enter begins a transaction. :leave commits. :error rolls back.
  When :clj-r2dbc/in-transaction? is already true in the context,
  all three phases pass through (join semantics).

  Args:
    tx-opts - (optional) transaction options map; defaults to {}.
                :isolation            - isolation level keyword
                                        (see impl/transaction isolation-map).
                :read-only?           - boolean; default false.
                :name                 - string savepoint or transaction name.
                :lock-wait-timeout-ms - maximum time to wait for a lock, in ms.
              Accepts a map, keyword arguments, or both.

  Returns an interceptor map with :name, :enter, :leave, and :error keys.

  Example:
    (transaction-interceptor)
    (transaction-interceptor {:isolation :serializable})
    (transaction-interceptor :isolation :serializable)"
  ([& {:as tx-opts}]
   (let [tx-opts (or tx-opts {})]
     {:name  ::transaction
      :enter (fn [ctx]
               (if (:clj-r2dbc/in-transaction? ctx)
                 ctx
                 (let [conn ^Connection (:r2dbc/connection ctx)]
                   (m/sp (m/? (tx/begin-transaction-task conn tx-opts))
                         (assoc ctx
                                :clj-r2dbc/in-transaction? true
                                ::tx-owner? true)))))
      :leave (fn [ctx]
               (if (::tx-owner? ctx)
                 (let [conn ^Connection (:r2dbc/connection ctx)]
                   (m/sp (m/? (util/void->task (.commitTransaction conn)))
                         (dissoc ctx ::tx-owner?)))
                 ctx))
      :error (fn [ctx]
               (if (::tx-owner? ctx)
                 (let [conn ^Connection (:r2dbc/connection ctx)
                       root (:clj-r2dbc/error ctx)]
                   (m/sp (m/? (tx/rollback-with-suppression! conn root))
                         (dissoc ctx ::tx-owner?)))
                 ctx))})))

(comment
  (require '[clj-r2dbc :as db])
  (require '[clj-r2dbc.interceptor :as i])
  (def db (db/connect "r2dbc:h2:mem:///interceptor-test;DB_CLOSE_DELAY=-1"))
  (m/? (db/execute db
                   "SELECT 1"
                   {:interceptors [i/logging-interceptor
                                   (i/transaction-interceptor)]}))
  (m/? (db/execute db
                   "UPDATE t SET x = 1"
                   {:interceptors [(i/transaction-interceptor
                                    {:isolation :serializable})]})))
