;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.integration.fixtures
  (:require
   [clj-r2dbc :as r2dbc]
   [clj-r2dbc.impl.connection :as conn]
   [clj-r2dbc.test-util.db :as db]
   [clojure.string :as str]
   [clojure.test :as t]
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Connection)
   (java.util.concurrent Callable FutureTask TimeUnit TimeoutException)))

(set! *warn-on-reflection* true)

(def ^:private h2-url "r2dbc:h2:mem:///r2dbc-integration;DB_CLOSE_DELAY=-1")

(defn h2-db [] (r2dbc/connect h2-url))

(defn byodb-url
  [env-key]
  (let [v (System/getenv ^String env-key)] (when-not (str/blank? v) v)))

(defn pg-db
  []
  (when-let [url (byodb-url "CLJ_R2DBC_TEST_PG_URL")] (r2dbc/connect url)))

(defn mariadb-db
  []
  (when-let [url (byodb-url "CLJ_R2DBC_TEST_MARIADB_URL")] (r2dbc/connect url)))

(defn skip-unless-db!
  [db env-key label]
  (if db
    true
    (do (t/testing (str label " unavailable")
          (t/is
           true
           (str "skipped: set " env-key " to run " label " integration tests")))
        false)))

(defn run-task! [task] (db/run-task! task))

(defn acquire-connection!
  [db]
  (run-task! (m/sp (m/? (conn/acquire-connection db)))))

(defn close-connection!
  [^Connection conn]
  (run-task! (m/reduce (fn [_ _] nil) nil (m/subscribe (.close conn)))))

(defn measure-ms
  [f]
  (let [start-ns (System/nanoTime)]
    (f)
    (/ (- (System/nanoTime) start-ns) 1000000.0)))

(defn run-with-timeout!
  "Run thunk on a daemon thread and return its result.
  Throw ex-info on timeout so host-backed tests fail fast instead of hanging
  the whole suite."
  [timeout-ms label thunk]
  (let [task    (FutureTask. ^Callable (fn [] (thunk)))
        _thread (doto (Thread. ^Runnable task (str "clj-r2dbc-timebox-" label))
                  (.setDaemon true)
                  (.start))]
    (try (.get task timeout-ms TimeUnit/MILLISECONDS)
         (catch TimeoutException _
           (.cancel task true)
           (throw (ex-info (str label " timed out after " timeout-ms "ms")
                           {:label label, :timeout-ms timeout-ms}))))))

(defn measure-ms-with-timeout!
  "Measure thunk runtime in milliseconds with a hard timeout."
  [timeout-ms label thunk]
  (let [start-ns (System/nanoTime)]
    (run-with-timeout! timeout-ms label thunk)
    (/ (- (System/nanoTime) start-ns) 1000000.0)))

(defn wait-until
  [timeout-ms interval-ms pred]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (pred)
        true
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep (long interval-ms)) (recur))
          false)))))

(defn exec!
  [db sql params]
  (:clj-r2dbc/rows (run-task! (r2dbc/execute db sql {:params params}))))

(defn exec-one!
  [db sql params]
  (run-task! (r2dbc/first-row db sql {:params params})))

(defn exec-many!
  [db sql param-sets]
  (:clj-r2dbc/results (run-task! (r2dbc/execute-each db sql param-sets {}))))

(defn exec-batch!
  [db sql-statements]
  (:clj-r2dbc/update-counts (run-task! (r2dbc/batch db sql-statements {}))))

(defn- stream-opts
  [opts params]
  (cond-> opts (some? params) (assoc :params params)))

(defn plan->vec
  [db sql params opts]
  (run-task!
   (m/reduce conj [] (r2dbc/stream db sql (stream-opts opts params)))))

(defn reset-people-table!
  [db]
  (exec! db "DROP TABLE IF EXISTS people" [])
  (exec!
   db
   "CREATE TABLE people (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL)"
   [])
  (exec! db "INSERT INTO people (id, name) VALUES (1, 'Alice')" [])
  (exec! db "INSERT INTO people (id, name) VALUES (2, 'Bob')" [])
  (exec! db "INSERT INTO people (id, name) VALUES (3, 'Carol')" [])
  nil)
