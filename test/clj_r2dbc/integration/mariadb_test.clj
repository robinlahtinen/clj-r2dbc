;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.integration.mariadb-test
  (:require
   [clj-r2dbc :as r2dbc]
   [clj-r2dbc.integration.fixtures :as fx]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m]))

(set! *warn-on-reflection* true)

(deftest ^:integration mariadb-byodb-smoke-test
  (let [db (fx/mariadb-db)]
    (when (fx/skip-unless-db! db "CLJ_R2DBC_TEST_MARIADB_URL" "MariaDB")
      (testing "BYODB MariaDB roundtrip"
        (fx/reset-people-table! db)
        (is (= [{:id 1, :name "Alice"} {:id 2, :name "Bob"}
                {:id 3, :name "Carol"}]
               (fx/exec! db "SELECT id, name FROM people ORDER BY id" [])))
        (fx/exec-many! db
                       "INSERT INTO people (id, name) VALUES (?, ?)"
                       [[10 "Dan"]])
        (is (= {:id 10, :name "Dan"}
               (fx/exec-one! db
                             "SELECT id, name FROM people WHERE id = 10"
                             [])))))))

(def ^:private mariadb-slow-plan-sql
  (str "WITH RECURSIVE seq(n) AS ("
       "SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5"
       ") SELECT n, SLEEP(0.03) AS slept FROM seq"))

(defn- mariadb-session-id
  [conn]
  (:conn-id (first (fx/exec! conn "SELECT CONNECTION_ID() AS conn_id" []))))

(defn- mariadb-activity
  [db conn-id]
  (first
   (fx/exec!
    db
    (str
     "SELECT command, state, info FROM information_schema.processlist WHERE id = "
     conn-id)
    [])))

(deftest ^:integration mariadb-cancellation-observed-at-server-test
  (let [db (fx/mariadb-db)]
    (when (fx/skip-unless-db! db "CLJ_R2DBC_TEST_MARIADB_URL" "MariaDB")
      (testing
       "cancelling a slow MariaDB stream clears the active server query while keeping the connection reusable"
        (let [conn (fx/acquire-connection! db)]
          (try
            (let [conn-id (mariadb-session-id conn)
                  worker  (future (fx/run-task! (m/race (m/sleep 150 :timeout)
                                                        (m/reduce
                                                         (fn [acc _] (inc acc))
                                                         0
                                                         (r2dbc/stream
                                                          conn
                                                          mariadb-slow-plan-sql
                                                          {:params     []
                                                           :fetch-size 1})))))]
              (is (true? (fx/wait-until
                          1000
                          25
                          #(when-let [activity (mariadb-activity db conn-id)]
                             (and (= "Query" (:command activity))
                                  (re-find #"SLEEP"
                                           (or (:info activity) "")))))))
              (is (= :timeout
                     (fx/run-with-timeout! 5000
                                           "mariadb cancellation worker"
                                           #(deref worker)))
                  "MariaDB cancellation race did not complete promptly")
              (is (true? (fx/wait-until
                          2000
                          50
                          #(let [activity (mariadb-activity db conn-id)]
                             (or (nil? activity)
                                 (not= "Query" (:command activity))
                                 (not (re-find #"SLEEP"
                                               (or (:info activity) ""))))))))
              (is (= {:ok 1} (fx/exec-one! conn "SELECT 1 AS ok" [])))
              (is (= {:ok 1} (fx/exec-one! db "SELECT 1 AS ok" []))))
            (finally (fx/close-connection! conn))))))))
