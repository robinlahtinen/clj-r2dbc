;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.exec.core-test
  "Tests for clj-r2dbc.impl.exec.core execution layer.

  Unit tests use mock objects from clj-r2dbc.test-util.mock.
  All tasks are driven synchronously via db/run-task!.

  Note on MockResult update counts: MockResult.rowsUpdated() does not surface
  as a Result$UpdateCount segment through flatMap in the mock implementation.
  Tests needing update-count segments use raw reify segments instead."
  (:require
   [clj-r2dbc.impl.datafy :as datafy-impl]
   [clj-r2dbc.impl.exec.core :as execute]
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.core.protocols :as core-p]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m])
  (:import
   (clojure.lang ExceptionInfo)
   (io.r2dbc.spi Connection R2dbcBadGrammarException Result$UpdateCount Statement)
   (missionary Cancelled)
   (org.reactivestreams Publisher Subscription)
   (reactor.core.publisher Mono)))

(set! *warn-on-reflection* true)

(defn- uc-segment
  "Create a raw Result$UpdateCount reify segment with the given value.
  Use instead of MockResult.rowsUpdated(), which does not surface through flatMap."
  [n]
  (reify
    Result$UpdateCount
    (value [_] n)))

(defn- publisher-of
  "Return a Publisher that emits the given values then completes."
  [& items]
  (reify
    Publisher
    (subscribe [_ s]
      (.onSubscribe
       s
       (reify
         Subscription
         (request [_ _] (doseq [item items] (.onNext s item)) (.onComplete s))
         (cancel [_]))))))

(defn- failing-publisher
  "Return a Publisher that immediately fails with t."
  [t]
  (reify
    Publisher
    (subscribe [_ s]
      (.onSubscribe s
                    (reify
                      Subscription
                      (request [_ _] (.onError s t))
                      (cancel [_]))))))

(defn- reify-stmt
  "Return a Statement reify that executes with the given result-pub.
  Type hints disambiguate the overloaded bind/bindNull methods."
  ([result-pub] (reify-stmt result-pub (atom {})))
  ([result-pub tracker]
   (reify
     Statement
     (execute [_] result-pub)
     (^Statement bind [this ^int _i ^Object _v] this)
     (^Statement bind [this ^String _name ^Object _v] this)
     (^Statement bindNull [this ^int _i ^Class _t] this)
     (^Statement bindNull [this ^String _name ^Class _t] this)
     (^Statement add [this] this)
     (^Statement fetchSize
       [this ^int n]
       (swap! tracker assoc :fetch-size n)
       this)
     (^Statement returnGeneratedValues
       [this ^"[Ljava.lang.String;" cols]
       (swap! tracker assoc :returning (vec cols))
       this))))

(defn- reify-conn
  "Return a Connection reify whose createStatement returns a statement built by stmt-fn."
  [stmt-fn]
  (reify
    Connection
    (createStatement [_ _] (stmt-fn))
    (^Publisher close [_] (Mono/empty))))

(defn- empty-result-conn
  "Create a MockConnection whose statement returns an empty MockResult."
  []
  (let [result (mock/mock-result)
        stmt   (mock/mock-statement result)
        conn   (mock/mock-connection {:statement stmt})]
    conn))

(defn- execute-core-ctx
  "Build a context map for execute-core! tests."
  ([conn] (execute-core-ctx conn "SELECT 1" [] {}))
  ([conn sql params opts]
   {:r2dbc/connection conn
    :clj-r2dbc/sql    sql
    :clj-r2dbc/params params
    :clj-r2dbc/opts   opts}))

(deftest check-mode-both-throws-test
  (testing
   "throws immediately when both :middleware and :interceptors are supplied"
    (is (thrown-with-msg? ExceptionInfo
                          #"Cannot supply both"
                          (execute/execute!* nil
                                             "SELECT 1"
                                             []
                                             {:middleware   [identity]
                                              :interceptors []})))))

(deftest check-mode-middleware-only-ok-test
  (testing ":middleware alone is accepted (check-mode! does not throw)"
    (is
     (fn?
      (execute/execute!* (empty-result-conn) "SELECT 1" [] {:middleware []})))))

(deftest check-mode-interceptors-only-ok-test
  (testing ":interceptors alone is accepted (check-mode! does not throw)"
    (is (fn? (execute/execute!* (empty-result-conn)
                                "SELECT 1"
                                []
                                {:interceptors []})))))

(deftest execute-core-empty-result-test
  (testing
   "empty result yields :clj-r2dbc/rows [] with no :clj-r2dbc/update-count"
    (let [conn (empty-result-conn)
          ctx  (execute-core-ctx conn)
          ctx' (db/run-task! (execute/execute-core! ctx))]
      (is (= [] (:clj-r2dbc/rows ctx')))
      (is (not (contains? ctx' :clj-r2dbc/update-count))))))

(deftest execute-core-select-rows-test
  (testing "row segments appear in :clj-r2dbc/rows as kebab-case maps"
    (let [row    (mock/mock-row {"first_name" "Alice", "age" 30}
                                {:types {"first_name" String, "age" Long}})
          result (mock/mock-result {:rows [row]})
          stmt   (mock/mock-statement result)
          conn   (mock/mock-connection {:statement stmt})
          ctx    (execute-core-ctx conn)
          ctx'   (db/run-task! (execute/execute-core! ctx))]
      (is (= [{:first-name "Alice", :age 30}] (:clj-r2dbc/rows ctx')))
      (is (not (contains? ctx' :clj-r2dbc/update-count))))))

(deftest execute-core-builder-fn-precedence-test
  (testing ":builder-fn overrides default row mapping in execute-core!"
    (let [row     (mock/mock-row {"first_name" "Alice"}
                                 {:types {"first_name" String}})
          result  (mock/mock-result {:rows [row]})
          stmt    (mock/mock-statement result)
          conn    (mock/mock-connection {:statement stmt})
          builder (fn [r _rmd]
                    {:raw-first-name
                     (.get ^io.r2dbc.spi.Row r "first_name" String)})
          ctx     (execute-core-ctx conn
                                    "SELECT first_name FROM t"
                                    []
                                    {:builder-fn builder})
          ctx'    (db/run-task! (execute/execute-core! ctx))]
      (is (= [{:raw-first-name "Alice"}] (:clj-r2dbc/rows ctx')))
      (is (not (contains? ctx' :clj-r2dbc/update-count))))))

(deftest execute-core-update-count-test
  (testing
   "update-count segment yields :clj-r2dbc/update-count N and empty :clj-r2dbc/rows"
    (let [result (mock/mock-result {:segments [(uc-segment 7)]})
          stmt   (mock/mock-statement result)
          conn   (mock/mock-connection {:statement stmt})
          ctx    (execute-core-ctx conn)
          ctx'   (db/run-task! (execute/execute-core! ctx))]
      (is (= [] (:clj-r2dbc/rows ctx')))
      (is (= 7 (:clj-r2dbc/update-count ctx'))))))

(deftest execute-core-mixed-rows-and-update-count-test
  (testing "mixed rows and update-count segments are partitioned correctly"
    (let [row  (mock/mock-row {"id" 1} {:types {"id" Long}})
          r1   (mock/mock-result {:rows [row]})
          r2   (mock/mock-result {:segments [(uc-segment 3)]})
          stmt (mock/mock-statement [r1 r2])
          conn (mock/mock-connection {:statement stmt})
          ctx  (execute-core-ctx conn)
          ctx' (db/run-task! (execute/execute-core! ctx))]
      (is (= [{:id 1}] (:clj-r2dbc/rows ctx')))
      (is (= 3 (:clj-r2dbc/update-count ctx'))))))

(deftest execute-core-fetch-size-test
  (testing ":fetch-size option calls Statement.fetchSize without exception"
    (let [tracker   (atom {})
          empty-pub (publisher-of)
          conn      (reify-conn #(reify-stmt empty-pub tracker))
          ctx       (execute-core-ctx conn "SELECT 1" [] {:fetch-size 100})]
      (db/run-task! (execute/execute-core! ctx))
      (is (= 100 (:fetch-size @tracker))))))

(deftest execute-core-fetch-size-negative-test
  (testing "negative :fetch-size throws ex-info with :invalid-argument"
    (let [conn (empty-result-conn)
          cf   (mock/mock-connection-factory conn)]
      (try (db/run-task! (execute/execute!* cf "SELECT 1" [] {:fetch-size -1}))
           (is false "should have thrown")
           (catch ExceptionInfo e
             (is (= {:clj-r2dbc/error-type :invalid-argument
                     :key                  :fetch-size
                     :value                -1}
                    (ex-data e))))))))

(deftest execute-core-fetch-size-over-max-test
  (testing ":fetch-size > Integer/MAX_VALUE throws ex-info with :bound-exceeded"
    (let [conn         (empty-result-conn)
          cf           (mock/mock-connection-factory conn)
          too-large-fs (inc (long Integer/MAX_VALUE))]
      (try (db/run-task!
            (execute/execute!* cf "SELECT 1" [] {:fetch-size too-large-fs}))
           (is false "should have thrown")
           (catch ExceptionInfo e
             (is (= {:clj-r2dbc/error-type :bound-exceeded
                     :key                  :fetch-size
                     :limit                Integer/MAX_VALUE
                     :value                too-large-fs}
                    (ex-data e))))))))

(deftest execute-core-returning-test
  (testing
   ":returning option calls Statement.returnGeneratedValues without exception"
    (let [tracker   (atom {})
          empty-pub (publisher-of)
          conn      (reify-conn #(reify-stmt empty-pub tracker))
          ctx       (execute-core-ctx conn
                                      "INSERT INTO t (name) VALUES ($1)"
                                      ["Alice"]
                                      {:returning ["id"]})]
      (db/run-task! (execute/execute-core! ctx))
      (is (= ["id"] (:returning @tracker))))))

(deftest execute-core-r2dbc-exception-wrapped-test
  (testing
   "R2dbcException from publisher is wrapped in ex-info with :sql, :params, :clj-r2dbc/error-category"
    (let [r2ex   (R2dbcBadGrammarException. "bad grammar" "42000" (int 0))
          conn   (reify-conn #(reify-stmt (failing-publisher r2ex)))
          sql    "SELECT bad_sql"
          params [1 2]
          ctx    (execute-core-ctx conn sql params {})]
      (try (db/run-task! (execute/execute-core! ctx))
           (is false "should have thrown")
           (catch ExceptionInfo e
             (let [data (ex-data e)]
               (is (= :bad-grammar (:clj-r2dbc/error-category data)))
               (is (= sql (:sql data)))
               (is (= params (:params data)))
               (is (identical? r2ex (.getCause ^Throwable e)))))))))

(deftest execute-star-empty-result-test
  (testing "execute!* returns {:clj-r2dbc/rows []} when result is empty SELECT"
    (let [conn   (empty-result-conn)
          cf     (mock/mock-connection-factory conn)
          result (db/run-task! (execute/execute!* cf "SELECT 1" [] {}))]
      (is (= {:clj-r2dbc/rows []} result)))))

(deftest execute-star-dml-test
  (testing "execute!* returns {:clj-r2dbc/update-count N} for DML"
    (let [result (mock/mock-result {:segments [(uc-segment 5)]})
          stmt   (mock/mock-statement result)
          conn   (mock/mock-connection {:statement stmt})
          cf     (mock/mock-connection-factory conn)
          res    (db/run-task! (execute/execute!* cf "DELETE FROM t" [] {}))]
      (is (= {:clj-r2dbc/update-count 5} res)))))

(deftest execute-star-rows-test
  (testing "execute!* returns {:clj-r2dbc/rows [...]} for SELECT"
    (let [row1   (mock/mock-row {"id" 1, "name" "Alice"}
                                {:types {"id" Long, "name" String}})
          row2   (mock/mock-row {"id" 2, "name" "Bob"}
                                {:types {"id" Long, "name" String}})
          result (mock/mock-result {:rows [row1 row2]})
          stmt   (mock/mock-statement result)
          conn   (mock/mock-connection {:statement stmt})
          cf     (mock/mock-connection-factory conn)
          res    (db/run-task!
                  (execute/execute!* cf "SELECT id, name FROM t" [] {}))]
      (is (= {:clj-r2dbc/rows [{:id 1, :name "Alice"} {:id 2, :name "Bob"}]}
             res)))))

(deftest execute-star-middleware-test
  (testing "middleware wraps execute-core! and is called in order"
    (let [log  (atom [])
          mw   (fn [execute-fn]
                 (fn [ctx]
                   (m/sp (swap! log conj :before)
                         (let [result (m/? (execute-fn ctx))]
                           (swap! log conj :after)
                           result))))
          conn (empty-result-conn)
          cf   (mock/mock-connection-factory conn)
          _res (db/run-task!
                (execute/execute!* cf "SELECT 1" [] {:middleware [mw]}))]
      (is (= [:before :after] @log)))))

(deftest execute-star-interceptors-test
  (testing "interceptors run enter then leave in correct order"
    (let [log  (atom [])
          i-a  {:enter (fn [ctx] (swap! log conj :enter-a) ctx)
                :leave (fn [ctx] (swap! log conj :leave-a) ctx)}
          i-b  {:enter (fn [ctx] (swap! log conj :enter-b) ctx)
                :leave (fn [ctx] (swap! log conj :leave-b) ctx)}
          conn (empty-result-conn)
          cf   (mock/mock-connection-factory conn)
          _res (db/run-task!
                (execute/execute!* cf "SELECT 1" [] {:interceptors [i-a i-b]}))]
      (is (= [:enter-a :enter-b :leave-b :leave-a] @log)))))

(deftest execute-one-star-non-empty-test
  (testing "execute-one!* returns the first map from a non-empty result"
    (let [row1   (mock/mock-row {"id" 1} {:types {"id" Long}})
          row2   (mock/mock-row {"id" 2} {:types {"id" Long}})
          result (mock/mock-result {:rows [row1 row2]})
          stmt   (mock/mock-statement result)
          conn   (mock/mock-connection {:statement stmt})
          cf     (mock/mock-connection-factory conn)
          res    (db/run-task!
                  (execute/execute-one!* cf "SELECT id FROM t" [] {}))]
      (is (= {:id 1} res)))))

(deftest execute-one-star-empty-test
  (testing "execute-one!* returns nil when result is empty"
    (let [conn (empty-result-conn)
          cf   (mock/mock-connection-factory conn)
          res  (db/run-task!
                (execute/execute-one!* cf "SELECT 1 WHERE false" [] {}))]
      (is (nil? res)))))

(deftest execute-each-star-two-sets-test
  (testing
   "execute-each!* returns {:clj-r2dbc/results [...]} with per-binding result maps"
    (let [row1    (mock/mock-row {"id" 1} {:types {"id" Long}})
          row2    (mock/mock-row {"id" 2} {:types {"id" Long}})
          r1      (mock/mock-result {:rows [row1]})
          r2      (mock/mock-result {:rows [row2]})
          stmt    (mock/mock-statement [r1 r2])
          conn    (mock/mock-connection {:statement stmt})
          cf      (mock/mock-connection-factory conn)
          res     (db/run-task! (execute/execute-each!*
                                 cf
                                 "SELECT id FROM t WHERE id = $1"
                                 [[1] [2]]
                                 {}))
          results (:clj-r2dbc/results res)]
      (is (= 2 (count results)))
      (is (= {:clj-r2dbc/rows [{:id 1}]} (first results)))
      (is (= {:clj-r2dbc/rows [{:id 2}]} (second results))))))

(deftest execute-each-max-batch-size-guard-test
  (testing "execute-each!* rejects param-set counts above 10000"
    (let [param-sets (repeat 10001 [])]
      (try (execute/execute-each!* nil "SELECT 1" param-sets {})
           (is false "should have thrown")
           (catch ExceptionInfo e
             (is (= {:clj-r2dbc/error   :clj-r2dbc/limit-exceeded
                     :clj-r2dbc/context :execute-each
                     :key               :param-sets
                     :constraint        :max-batch-size
                     :limit             10000
                     :value             10001}
                    (ex-data e))))))))

(deftest execute-each-overridable-max-batch-size-test
  (testing ":max-batch-size 20000 allows 15000 param-sets (validation only)"
    (try (execute/execute-each!* nil
                                 "SELECT 1"
                                 (repeat 15000 [])
                                 {:max-batch-size 20000})
         (is true)
         (catch NullPointerException _e (is true))
         (catch ExceptionInfo e
           (is (not= :clj-r2dbc/limit-exceeded
                     (:clj-r2dbc/error (ex-data e)))))))
  (testing "default still rejects 10001 param-sets"
    (try (execute/execute-each!* nil "SELECT 1" (repeat 10001 []) {})
         (is false "should have thrown")
         (catch ExceptionInfo e
           (is (= :clj-r2dbc/limit-exceeded (:clj-r2dbc/error (ex-data e))))
           (is (= 10000 (:limit (ex-data e)))))))
  (testing ":max-batch-size 0 throws :invalid-argument"
    (try (execute/execute-each!* nil "SELECT 1" [] {:max-batch-size 0})
         (is false "should have thrown")
         (catch ExceptionInfo e
           (is (= :clj-r2dbc/invalid-value (:clj-r2dbc/error (ex-data e))))
           (is (= :max-batch-size (:key (ex-data e)))))))
  (testing ":max-batch-size -1 throws :invalid-argument"
    (try (execute/execute-each!* nil "SELECT 1" [] {:max-batch-size -1})
         (is false "should have thrown")
         (catch ExceptionInfo e
           (is (= :clj-r2dbc/invalid-value (:clj-r2dbc/error (ex-data e)))))))
  (testing ":max-batch-size \"foo\" throws :invalid-argument"
    (try (execute/execute-each!* nil "SELECT 1" [] {:max-batch-size "foo"})
         (is false "should have thrown")
         (catch ExceptionInfo e
           (is (= :clj-r2dbc/invalid-value (:clj-r2dbc/error (ex-data e))))))))

(deftest execute-batch-star-two-stmts-test
  (testing
   "execute-batch!* returns vector of update counts for two SQL statements"
    (let [r1                                                                    (mock/mock-result {:segments [(uc-segment 5)]})
          r2                                                                    (mock/mock-result {:segments [(uc-segment 3)]})
          batch                                                                 (mock/mock-batch [r1 r2])
          conn                                                                  (mock/mock-connection {:batch batch})
          cf                                                                    (mock/mock-connection-factory conn)
          res
          (db/run-task!
           (execute/execute-batch!* cf ["DELETE FROM t1" "DELETE FROM t2"] {}))]
      (is (= {:clj-r2dbc/update-counts [5 3]} res)))))

(deftest execute-core-cancelled-passthrough-test
  (testing
   "missionary.Cancelled from publisher propagates without R2DBC wrapping"
    (let [cancelled (Cancelled.)
          conn      (reify-conn #(reify-stmt (failing-publisher cancelled)))
          ctx       (execute-core-ctx conn)]
      (is (thrown? Cancelled (db/run-task! (execute/execute-core! ctx)))))))

(deftest execute-each-check-mode-throws-test
  (testing
   "execute-each!* throws when both :middleware and :interceptors are supplied"
    (is (thrown-with-msg? ExceptionInfo
                          #"Cannot supply both"
                          (execute/execute-each!* nil
                                                  "SELECT 1"
                                                  [[]]
                                                  {:middleware   [identity]
                                                   :interceptors []})))))

(deftest execute-core-update-count-sum-test
  (testing "update-count values across multiple Results are summed"
    (let [r1   (mock/mock-result {:segments [(uc-segment 4)]})
          r2   (mock/mock-result {:segments [(uc-segment 6)]})
          stmt (mock/mock-statement [r1 r2])
          conn (mock/mock-connection {:statement stmt})
          ctx  (execute-core-ctx conn "UPDATE t SET x = 1" [] {})
          ctx' (db/run-task! (execute/execute-core! ctx))]
      (is (= [] (:clj-r2dbc/rows ctx')))
      (is (= 10 (:clj-r2dbc/update-count ctx'))))))

(deftest execute-star-default-rows-are-plain-maps-test
  (testing "execute!* returns plain maps by default without Datafiable metadata"
    (let [row    (mock/mock-row {"id" 1, "name" "Alice"}
                                {:types {"id" Long, "name" String}})
          result (mock/mock-result {:rows [row]})
          stmt   (mock/mock-statement result)
          conn   (mock/mock-connection {:statement stmt})
          cf     (mock/mock-connection-factory conn)
          res    (db/run-task!
                  (execute/execute!* cf "SELECT id, name FROM t" [] {}))
          m      (first (:clj-r2dbc/rows res))
          md     (meta m)
          df     (get md `core-p/datafy)]
      (is (= {:id 1, :name "Alice"} m))
      (is (not (true? (get md datafy-impl/marker-key))))
      (is (nil? df)))))

(deftest execute-star-rows-are-datafiable-when-opted-in-test
  (testing "execute!* attaches Datafiable metadata when :datafy true"
    (let [row                                                                 (mock/mock-row {"id" 1, "name" "Alice"}
                                                                                             {:types {"id" Long, "name" String}})
          result                                                              (mock/mock-result {:rows [row]})
          stmt                                                                (mock/mock-statement result)
          conn                                                                (mock/mock-connection {:statement stmt})
          cf                                                                  (mock/mock-connection-factory conn)
          res
          (db/run-task!
           (execute/execute!* cf "SELECT id, name FROM t" [] {:datafy true}))
          m                                                                   (first (:clj-r2dbc/rows res))
          md                                                                  (meta m)
          df                                                                  (get md `core-p/datafy)]
      (is (= {:id 1, :name "Alice"} m))
      (is (true? (get md datafy-impl/marker-key)))
      (is (fn? df))
      (is (= m (df m))))))

(deftest execute-one-star-row-is-datafiable-when-opted-in-test
  (testing "execute-one!* preserves Datafiable metadata when :datafy true"
    (let [row    (mock/mock-row {"id" 1, "name" "Alice"}
                                {:types {"id" Long, "name" String}})
          result (mock/mock-result {:rows [row]})
          stmt   (mock/mock-statement result)
          conn   (mock/mock-connection {:statement stmt})
          cf     (mock/mock-connection-factory conn)
          m      (db/run-task! (execute/execute-one!* cf
                                                      "SELECT id, name FROM t"
                                                      []
                                                      {:datafy true}))
          md     (meta m)
          df     (get md `core-p/datafy)]
      (is (= {:id 1, :name "Alice"} m))
      (is (true? (get md datafy-impl/marker-key)))
      (is (fn? df))
      (is (= m (df m))))))
