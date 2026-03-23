;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.test-util.db
  "Shared helpers for integration tests.

  Provides:
    run-task!        - parks a Missionary task on the calling thread, returns result
    insert-fixtures! - inserts common test data
    drop-fixtures!   - drops test tables"
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
