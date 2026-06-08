;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.integration.postgresql-test
  (:require
   [clj-r2dbc :as r2dbc]
   [clj-r2dbc.impl.sql.cursor :as cursor]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.integration.fixtures :as fx]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m]))

(set! *warn-on-reflection* true)

(deftest ^:integration postgresql-flyweight-many-rows-test
  ;; Regression guard for the flyweight ByteBuf use-after-free that only surfaces
  ;; on a real Netty driver once pooled ByteBufs are recycled (the 3-row fixtures
  ;; never trigger it). Streams many rows in flyweight mode (no :builder-fn) and
  ;; asserts every materialised value is correct - garbage from a freed ByteBuf
  ;; would corrupt :id/:name while keeping the row count.
  (let [db (fx/pg-db)]
    (when (fx/skip-unless-db! db "CLJ_R2DBC_TEST_PG_URL" "PostgreSQL")
      (testing "flyweight streaming materialises correct values across ByteBuf pool reuse"
        (let [n    20000
              sql  (str "SELECT g AS id, ('name-' || g) AS name"
                        " FROM generate_series(1, " n ") g ORDER BY g")
              rows (fx/run-task!
                    (m/reduce (fn [acc cursor]
                                (conj acc (row/row->map (cursor/cursor-row cursor)
                                                        (cursor/cursor-cache cursor))))
                              []
                              (r2dbc/stream db sql {:params [], :stream-mode :flyweight})))]
          (is (= n (count rows)))
          (is (= (range 1 (inc n)) (map :id rows)))
          (is (every? (fn [r] (= (str "name-" (:id r)) (:name r))) rows))))
      (testing "flyweight cursors are per-row immutable: retained and read after the stream"
        ;; Retains every emitted cursor, then materialises them only AFTER the
        ;; stream completes. A shared/recycled cursor would return the last row n
        ;; times (or freed-ByteBuf garbage); per-row isolation makes it correct -
        ;; the real-Netty guard for the request-ahead corruption (CI [2 2 3]).
        (let [n       5000
              sql     (str "SELECT g AS id, ('name-' || g) AS name"
                           " FROM generate_series(1, " n ") g ORDER BY g")
              cursors (fx/run-task!
                       (m/reduce conj []
                                 (r2dbc/stream db sql {:params [], :stream-mode :flyweight})))
              rows    (mapv #(row/row->map (cursor/cursor-row %) (cursor/cursor-cache %))
                            cursors)]
          (is (= n (count rows)))
          (is (= (range 1 (inc n)) (map :id rows)))
          (is (every? (fn [r] (= (str "name-" (:id r)) (:name r))) rows)))))))

(deftest ^:integration postgresql-byodb-smoke-test
  (let [db (fx/pg-db)]
    (when (fx/skip-unless-db! db "CLJ_R2DBC_TEST_PG_URL" "PostgreSQL")
      (testing "BYODB PostgreSQL roundtrip"
        (fx/reset-people-table! db)
        (is (= [{:id 1, :name "Alice"} {:id 2, :name "Bob"}
                {:id 3, :name "Carol"}]
               (fx/exec! db "SELECT id, name FROM people ORDER BY id" [])))
        (is (= {:id 2, :name "Bob"}
               (fx/exec-one! db "SELECT id, name FROM people WHERE id = 2" [])))
        (let [counts (fx/exec-batch! db
                                     ["DELETE FROM people WHERE id = 3"
                                      "DELETE FROM people WHERE id = 2"])]
          (is (= 2 (count counts)))
          (is (every? number? counts)))))))

(defn- pg-session-id
  [conn]
  (:pid (first (fx/exec! conn "SELECT pg_backend_pid() AS pid" []))))

(defn- pg-activity
  [db pid]
  (first (fx/exec! db
                   (str "SELECT state, query FROM pg_stat_activity WHERE pid = "
                        pid)
                   [])))

(def ^:private pg-slow-plan-sql
  "SELECT i, pg_sleep(0.03) AS slept FROM generate_series(1, 25) AS g(i)")

(defn- consume-slow-plan!
  [conn stop-after]
  (fx/run-task!
   (m/reduce (fn [acc _]
               (let [next (inc acc)]
                 (if (and stop-after (= stop-after next)) (reduced next) next)))
             0
             (r2dbc/stream conn pg-slow-plan-sql {:params [], :fetch-size 1}))))

(deftest ^:integration postgresql-cancellation-observed-at-server-test
  (let [db (fx/pg-db)]
    (when (fx/skip-unless-db! db "CLJ_R2DBC_TEST_PG_URL" "PostgreSQL")
      (testing
       "cancelling a slow PostgreSQL stream clears the active server query while keeping the connection reusable"
        (let [conn (fx/acquire-connection! db)]
          (try
            (let [pid    (pg-session-id conn)
                  worker (future (fx/run-task! (m/race (m/sleep 150 :timeout)
                                                       (m/reduce
                                                        (fn [acc _] (inc acc))
                                                        0
                                                        (r2dbc/stream
                                                         conn
                                                         pg-slow-plan-sql
                                                         {:params     []
                                                          :fetch-size 1})))))]
              (is (true? (fx/wait-until
                          1000
                          25
                          #(when-let [activity (pg-activity db pid)]
                             (and (= "active" (:state activity))
                                  (re-find #"pg_sleep"
                                           (or (:query activity) "")))))))
              (is (= :timeout
                     (fx/run-with-timeout! 5000
                                           "postgresql cancellation worker"
                                           #(deref worker)))
                  "PostgreSQL cancellation race did not complete promptly")
              (is (true? (fx/wait-until
                          2000
                          50
                          #(let [activity (pg-activity db pid)]
                             (or (nil? activity)
                                 (not= "active" (:state activity))
                                 (not (re-find #"pg_sleep"
                                               (or (:query activity) ""))))))))
              (is (= {:ok 1} (fx/exec-one! conn "SELECT 1 AS ok" [])))
              (is (= {:ok 1} (fx/exec-one! db "SELECT 1 AS ok" []))))
            (finally (fx/close-connection! conn))))))))

(deftest ^:integration postgresql-backpressure-early-stop-is-material-test
  (let [db (fx/pg-db)]
    (when (fx/skip-unless-db! db "CLJ_R2DBC_TEST_PG_URL" "PostgreSQL")
      (testing
       "early termination on PostgreSQL is materially faster than consuming the whole slow stream"
        (let [conn (fx/acquire-connection! db)]
          (try
            (let [full-ms  (fx/measure-ms-with-timeout!
                            15000
                            "postgresql full slow plan"
                            #(consume-slow-plan! conn nil))
                  early-ms (fx/measure-ms-with-timeout!
                            15000
                            "postgresql early slow plan"
                            #(consume-slow-plan! conn 3))]
              (is
               (< early-ms full-ms)
               (str
                "expected early termination to beat full consumption, got early="
                early-ms
                "ms full="
                full-ms
                "ms"))
              (is
               (< early-ms (/ full-ms 2.0))
               (str
                "expected a material reduction from backpressure/cancellation, got early="
                early-ms
                "ms full="
                full-ms
                "ms")))
            (finally (fx/close-connection! conn))))))))
