;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.transaction
  "Internal shared transaction helpers for middleware and interceptor execution.

  Centralizes TransactionDefinition construction and rollback suppression so the
  public middleware and interceptor namespaces can stay focused on their
  respective composition models."
  (:require
   [clj-r2dbc.impl.util :as util]
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Connection IsolationLevel TransactionDefinition)
   (java.time Duration)
   (missionary Cancelled)))

(set! *warn-on-reflection* true)

(def isolation-map
  "Map of Clojure keywords to R2DBC IsolationLevel constants.

  Supported keys:
    :read-uncommitted, :read-committed, :repeatable-read, :serializable.

  Used by ->tx-definition to translate the :isolation option."
  {:read-uncommitted IsolationLevel/READ_UNCOMMITTED
   :read-committed   IsolationLevel/READ_COMMITTED
   :repeatable-read  IsolationLevel/REPEATABLE_READ
   :serializable     IsolationLevel/SERIALIZABLE})

(defn ->tx-definition
  "Build a TransactionDefinition reification from a tx-opts map.

  Args:
    tx-opts - map with optional keys:
                :isolation            - isolation level keyword (see isolation-map).
                :read-only?           - boolean; default false.
                :name                 - string savepoint or transaction name.
                :lock-wait-timeout-ms - maximum time to wait for a lock, in ms.

  Returns a reified TransactionDefinition.

  Throws (synchronously):
    ex-info when :isolation is present but not a key in isolation-map."
  [tx-opts]
  (let [isolation-key (:isolation tx-opts)
        isolation     (when isolation-key
                        (or (get isolation-map isolation-key)
                            (throw (ex-info "Invalid transaction isolation"
                                            {:clj-r2dbc/error-type
                                             :invalid-argument
                                             :key                  :isolation
                                             :value                isolation-key}))))]
    (reify
      TransactionDefinition
      (getAttribute [_ attr]
        (condp = attr
          TransactionDefinition/ISOLATION_LEVEL isolation
          TransactionDefinition/READ_ONLY (:read-only? tx-opts)
          TransactionDefinition/NAME (:name tx-opts)
          TransactionDefinition/LOCK_WAIT_TIMEOUT
          (some-> (:lock-wait-timeout-ms tx-opts)
                  Duration/ofMillis)
          nil)))))

(defn begin-transaction-task
  "Return a Missionary task that begins a transaction on conn.

  When tx-opts is non-empty, builds a TransactionDefinition from the map;
  otherwise calls the no-arg beginTransaction overload.

  Args:
    conn    - active R2DBC Connection.
    tx-opts - transaction options map; pass {} for default transaction."
  [^Connection conn tx-opts]
  (if (seq tx-opts)
    (util/void->task
     (.beginTransaction conn ^TransactionDefinition (->tx-definition tx-opts)))
    (util/void->task (.beginTransaction conn))))

(defn rollback-with-suppression!
  "Return a Missionary task that rolls back the transaction on conn.

  Secondary exceptions from rollback are suppressed on root via
  .addSuppressed so the original cause is never lost.
  missionary.Cancelled is re-thrown immediately without suppression.

  Args:
    conn - active R2DBC Connection.
    root - the original Throwable causing the rollback, or nil."
  [^Connection conn ^Throwable root]
  (m/sp (try (m/? (util/void->task (.rollbackTransaction conn)))
             (catch Throwable t2
               (if (instance? Cancelled t2)
                 (throw t2)
                 (when (some? root) (.addSuppressed root t2) nil))))))

(comment
  (def conn nil)
  (m/? (begin-transaction-task conn {}))
  (m/? (begin-transaction-task conn {:isolation :serializable}))
  (m/? (rollback-with-suppression! conn (ex-info "boom" {}))))
