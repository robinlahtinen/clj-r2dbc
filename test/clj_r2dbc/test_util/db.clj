;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.test-util.db
  "Shared helpers for integration tests.

  Provides:
    run-task!           - parks a Missionary task on the calling thread, returns result
    run-task-timeout!   - like run-task!, but with timeout and thread dump on expiry
    insert-fixtures!    - inserts common test data
    drop-fixtures!      - drops test tables"
  (:require
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Connection)))

(set! *warn-on-reflection* true)

(defn run-task!
  "Park the calling thread on a Missionary task and return the result.
  Re-throw any exception from the task.

  This is the primary mechanism for running Missionary tasks in test code
  that needs synchronous results (clojure.test assertions).

  (run-task! (m/sp :hello))  ;=> :hello
  (run-task! (m/sp (throw (ex-info \"boom\" {}))))  ;=> throws"
  [task]
  (m/? task))

(defn run-task-timeout!
  "Like run-task!, but fails after timeout-ms with a full JVM thread dump.

  Converts silent CI kills into fast diagnostic failures by capturing all thread
  stacks when the task does not complete within the timeout.

  The task is started via its raw (success failure) protocol so its canceller is
  captured. On timeout the dump is printed FIRST (preserving the quiescent state
  for diagnosis), THEN the task is cancelled before throwing. Cancellation
  propagates through the streaming flow's Reactor usingWhen, which cancels
  create()/execute() and closes the connection — so a hung task does not leave
  orphaned flows pinning resources, and CI fails in ~timeout-ms instead of
  hanging until the job is force-cancelled.

  Args:
    timeout-ms - deadline in milliseconds before dumping stacks and throwing
    task       - Missionary task to run

  Returns the task result on success. Throws ExceptionInfo with thread dump on timeout."
  [timeout-ms task]
  (let [result    (promise)
        canceller (task (fn [v] (deliver result [::ok v]))
                        (fn [e] (deliver result [::err e])))
        res       (deref result timeout-ms ::timeout)]
    (if (= res ::timeout)
      (do
        (println "\n=== TIMEOUT after" timeout-ms "ms — JVM thread dump ===")
        (doseq [[^Thread th stack] (Thread/getAllStackTraces)]
          (println (str "\nThread: " (.getName th) " state=" (.getState th)))
          (run! #(println "  " %) stack))
        (canceller)
        (throw (ex-info "concurrent test timed out" {:timeout-ms timeout-ms})))
      (let [[kind v] res]
        (if (= kind ::err) (throw v) v)))))

(defn- subscribe-consume
  "Subscribe to an R2DBC Publisher and consume all values, returning nil
  as a Missionary task."
  [publisher]
  (m/reduce (fn [_ _] nil) nil (m/subscribe publisher)))

(defn insert-fixtures!
  "Insert common test data into the test database.

  Creates a 'test_table' with columns (id INTEGER, name VARCHAR) and
  inserts sample rows. Uses raw R2DBC SPI interop.

  For integration tests only. Requires a real database connection.

  Returns nil."
  [^Connection conn]
  (run-task!
   (m/sp
    (m/?
     (subscribe-consume
      (.execute
       (.createStatement
        conn
        "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name VARCHAR(255))"))))
    (m/?
     (subscribe-consume
      (.execute
       (.createStatement
        conn
        "INSERT INTO test_table (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')"))))
    nil)))

(defn drop-fixtures!
  "Drop test tables created by insert-fixtures!.

  For integration tests only. Requires a real database connection.

  Returns nil."
  [^Connection conn]
  (run-task! (m/sp (m/? (subscribe-consume
                         (.execute (.createStatement
                                    conn
                                    "DROP TABLE IF EXISTS test_table"))))
                   nil)))
