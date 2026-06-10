;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.integration.postgresql-test
  (:require
   [clj-r2dbc :as r2dbc]
   [clj-r2dbc.integration.fixtures :as fx]
   [clj-r2dbc.test-util.db :as tu]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m]))

(set! *warn-on-reflection* true)

(def ^:private concurrent-iterations
  "Iteration count for the concurrent stress/guard tests. Each iteration forks
  concurrent-width independent streams over an asynchronous (Netty) driver - the
  production shape - to surface any hang, loss, duplication, or misordering.
  (These moved off H2: a synchronous test-only driver does not exercise the bare
  async bridge that production runs; see lifecycle/synchronous-db?.) Kept modest:
  each forked stream opens a real connection, so peak concurrency is bounded
  (concurrent-width) to avoid overrunning the server's listen backlog."
  5)

(def ^:private concurrent-width
  "Peak number of simultaneously-open connections per concurrent test."
  4)

(def ^:private three-rows-sql
  "SELECT g AS id FROM generate_series(1, 3) g ORDER BY g")

(deftest ^:integration postgresql-default-builder-many-rows-test
  ;; Regression guard for the ByteBuf use-after-free that only surfaces on a real
  ;; Netty driver once pooled ByteBufs are recycled (the 3-row fixtures never
  ;; trigger it). Streams many rows with the default builder (kebab-maps, which
  ;; materialises each row inside Result.map while the ByteBuf is live) and
  ;; asserts every value is correct - garbage from a freed ByteBuf would corrupt
  ;; :id/:name while keeping the row count.
  (let [db (fx/pg-db)]
    (when (fx/skip-unless-db! db "CLJ_R2DBC_TEST_PG_URL" "PostgreSQL")
      (testing "default-builder streaming materialises correct values across ByteBuf pool reuse"
        (let [n    20000
              sql  (str "SELECT g AS id, ('name-' || g) AS name"
                        " FROM generate_series(1, " n ") g ORDER BY g")
              rows (fx/run-task!
                    (m/reduce conj [] (r2dbc/stream db sql {:params []})))]
          (is (= n (count rows)))
          (is (= (range 1 (inc n)) (map :id rows)))
          (is (every? (fn [r] (= (str "name-" (:id r)) (:name r))) rows))))
      (testing "emitted rows are immutable per row: retained and read after the stream"
        ;; Retains every emitted value, then reads it only AFTER the stream
        ;; completes. Freed-ByteBuf garbage or a shared/recycled value would
        ;; corrupt the data; per-row materialisation makes it correct - the
        ;; real-Netty guard for the request-ahead corruption (CI [2 2 3]).
        (let [n    5000
              sql  (str "SELECT g AS id, ('name-' || g) AS name"
                        " FROM generate_series(1, " n ") g ORDER BY g")
              rows (fx/run-task!
                    (m/reduce conj [] (r2dbc/stream db sql {:params []})))]
          (is (= n (count rows)))
          (is (= (range 1 (inc n)) (map :id rows)))
          (is (every? (fn [r] (= (str "name-" (:id r)) (:name r))) rows)))))))

(deftest ^:integration postgresql-concurrent-stress-test
  (let [pgdb (fx/pg-db)]
    (when (fx/skip-unless-db! pgdb "CLJ_R2DBC_TEST_PG_URL" "PostgreSQL")
      (testing "no hang or loss under concurrent warm-pool load (async driver)"
        (dotimes [iter concurrent-iterations]
          (let [results (tu/run-task-timeout!
                         30000
                         (m/reduce conj []
                                   (m/ap (let [_i (m/?> concurrent-width (m/seed (range 16)))]
                                           (m/?
                                            (m/reduce (fn [acc _] (inc acc)) 0
                                                      (r2dbc/stream pgdb three-rows-sql
                                                                    {:params [] :fetch-size 1})))))))]
            (is (= 16 (count results)) (str "iteration " iter))
            (is (every? #(= 3 %) results) (str "iteration " iter))))))))

(deftest ^:integration postgresql-concurrent-init-error-test
  (let [pgdb (fx/pg-db)]
    (when (fx/skip-unless-db! pgdb "CLJ_R2DBC_TEST_PG_URL" "PostgreSQL")
      (testing "no hang and correct error type when T1 throws under concurrent load"
        (dotimes [iter concurrent-iterations]
          (let [results (tu/run-task-timeout!
                         30000
                         (m/reduce conj []
                                   (m/ap (let [_i (m/?> concurrent-width (m/seed (range 8)))]
                                           (try (m/? (m/reduce conj []
                                                               (r2dbc/stream pgdb
                                                                             "SELECT * FROM nonexistent_table_xyz"
                                                                             {:params [] :fetch-size 1})))
                                                (catch Throwable e e))))))]
            (is (= 8 (count results)) (str "iteration " iter))
            (is (every? #(and (instance? Throwable %)
                              (not (instance? IllegalStateException %)))
                        results)
                (str "iteration " iter))))))))

(deftest ^:integration postgresql-concurrent-cancel-test
  (let [pgdb (fx/pg-db)]
    (when (fx/skip-unless-db! pgdb "CLJ_R2DBC_TEST_PG_URL" "PostgreSQL")
      (testing "no hang when streams race cancellation under concurrent load"
        (dotimes [iter concurrent-iterations]
          (let [results (tu/run-task-timeout!
                         30000
                         (m/reduce conj []
                                   (m/ap (let [_i (m/?> concurrent-width (m/seed (range 8)))]
                                           (m/?
                                            (m/race
                                             (m/reduce conj []
                                                       (r2dbc/stream pgdb three-rows-sql
                                                                     {:params [] :fetch-size 1}))
                                             (m/sp nil)))))))]
            (is (= 8 (count results)) (str "iteration " iter))))))))

(deftest ^:integration postgresql-concurrent-chunk-stress-test
  (let [pgdb (fx/pg-db)]
    (when (fx/skip-unless-db! pgdb "CLJ_R2DBC_TEST_PG_URL" "PostgreSQL")
      (testing "no hang, loss, or duplication under concurrent chunked streaming"
        (dotimes [iter concurrent-iterations]
          (let [results (tu/run-task-timeout!
                         30000
                         (m/reduce conj []
                                   (m/ap (let [_i (m/?> concurrent-width (m/seed (range 16)))]
                                           (m/?
                                            (m/reduce (fn [acc chunk] (+ (long acc) (count chunk))) 0
                                                      (r2dbc/stream pgdb three-rows-sql
                                                                    {:params [] :chunk-size 2})))))))]
            (is (= 16 (count results)) (str "iteration " iter))
            (is (every? #(= 3 %) results) (str "iteration " iter))))))))

(deftest ^:integration postgresql-concurrent-chunk-cancel-test
  (let [pgdb (fx/pg-db)]
    (when (fx/skip-unless-db! pgdb "CLJ_R2DBC_TEST_PG_URL" "PostgreSQL")
      (testing "no hang when a chunked stream is cancelled mid-stream under load"
        (dotimes [iter concurrent-iterations]
          (let [results (tu/run-task-timeout!
                         30000
                         (m/reduce conj []
                                   (m/ap (let [_i (m/?> concurrent-width (m/seed (range 16)))]
                                           (m/?
                                            (m/reduce (fn [_ chunk] (reduced (count chunk))) 0
                                                      (r2dbc/stream pgdb three-rows-sql
                                                                    {:params [] :chunk-size 2})))))))]
            (is (= 16 (count results)) (str "iteration " iter))
            (is (every? #(= 2 %) results) (str "iteration " iter))))))))

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
