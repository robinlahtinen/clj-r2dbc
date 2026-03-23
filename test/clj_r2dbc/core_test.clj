;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.core-test
  (:require
   [clj-r2dbc :as sut]
   [clj-r2dbc.impl.conn.core :as conn]
   [clj-r2dbc.impl.exec.core :as execute]
   [clj-r2dbc.impl.exec.stream :as stream]
   [clj-r2dbc.middleware :as middleware]
   [clj-r2dbc.row :as row]
   [clj-r2dbc.test-util.db :as db]
   [clj-r2dbc.test-util.mock :as mock]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m]))

(set! *warn-on-reflection* true)

(deftest exact-12-root-exports-test
  (testing "root namespace exports exactly the 12 required public vars"
    (let [public-vars (set (keys (ns-publics 'clj-r2dbc)))
          expected    #{'connect 'with-options 'info 'execute 'first-row 'stream
                        'execute-each 'batch 'with-connection 'with-transaction
                        'with-conn 'with-tx}]
      (is (= expected public-vars)
          (str "Expected exactly 12 exports, got: " (sort public-vars))))))

(deftest connect-delegation-test
  (testing "connect delegates to create-connection-factory* with :url merged"
    (let [called (atom nil)]
      (with-redefs [conn/create-connection-factory*
                    (fn [cfg] (reset! called cfg) :factory)]
        (is (= :factory (sut/connect "r2dbc:h2:mem:///r2dbc-test" {:x 1})))
        (is (= {:url "r2dbc:h2:mem:///r2dbc-test", :x 1} @called))))))

(deftest info-delegation-test
  (testing "info acquires a connection and delegates to connection-metadata*"
    (let [called    (atom [])
          mock-conn (mock/mock-connection {})
          mock-cf   (mock/mock-connection-factory mock-conn)]
      (with-redefs [conn/with-connection*     (fn [db f]
                                                (swap! called conj
                                                       [:with-connection db])
                                                (f mock-conn))
                    conn/connection-metadata* (fn [conn]
                                                (swap! called conj [:info conn])
                                                {:db/product-name "test"})]
        (is (= {:db/product-name "test"} (db/run-task! (sut/info mock-cf))))
        (is (= [[:with-connection mock-cf] [:info mock-conn]] @called))))))

(deftest execute-delegation-test
  (testing
   "execute delegates directly to execute!* with params lifted from opts"
    (let [called  (atom nil)
          mock-cf (mock/mock-connection-factory (mock/mock-connection {}))]
      (with-redefs [execute/execute!* (fn [db sql params opts]
                                        (reset! called [db sql params opts])
                                        :task)]
        (is (= :task
               (sut/execute mock-cf "SELECT 1" {:params [1], :fetch-size 32})))
        (is (= [mock-cf "SELECT 1" [1] {:fetch-size 32}] @called))))))

(deftest first-row-delegation-test
  (testing
   "first-row delegates directly to execute-one!* with params lifted from opts"
    (let [called  (atom nil)
          mock-cf (mock/mock-connection-factory (mock/mock-connection {}))]
      (with-redefs [execute/execute-one!* (fn [db sql params opts]
                                            (reset! called [db sql params opts])
                                            :task)]
        (is (= :task (sut/first-row mock-cf "SELECT 1" {:params [1]})))
        (is (= [mock-cf "SELECT 1" [1] {}] @called))))))

(deftest stream-delegation-test
  (testing
   "stream delegates to stream!* and installs the default immutable builder"
    (let [called  (atom nil)
          mock-cf (mock/mock-connection-factory (mock/mock-connection {}))]
      (with-redefs [stream/stream* (fn [db sql params opts]
                                     (reset! called [db sql params opts])
                                     :flow)]
        (is (= :flow (sut/stream mock-cf "SELECT 1" {:params [1]})))
        (let [[db sql params opts] @called]
          (is (= mock-cf db))
          (is (= "SELECT 1" sql))
          (is (= [1] params))
          (is (identical? row/kebab-maps (:builder-fn opts))))))))

(deftest stream-chunked-delegation-test
  (testing
   "stream delegates chunked streaming to stream* with :chunk-size in opts"
    (let [called  (atom nil)
          mock-cf (mock/mock-connection-factory (mock/mock-connection {}))]
      (with-redefs [stream/stream* (fn [db sql params opts]
                                     (reset! called [db sql params opts])
                                     (m/seed []))]
        (sut/stream mock-cf "SELECT 1" {:params [1], :chunk-size 16})
        (is (= [mock-cf "SELECT 1" [1]
                {:chunk-size 16, :builder-fn row/kebab-maps}]
               @called))))))

(deftest execute-each-delegation-test
  (testing "execute-each delegates to execute-each!*"
    (let [called  (atom nil)
          params  [[1] [2]]
          mock-cf (mock/mock-connection-factory (mock/mock-connection {}))]
      (with-redefs [execute/execute-each!* (fn [db sql param-sets opts]
                                             (reset! called [db sql param-sets
                                                             opts])
                                             :task)]
        (is (= :task
               (sut/execute-each mock-cf
                                 "INSERT INTO t(id) VALUES ($1)"
                                 params
                                 {:max-batch-size 99})))
        (is (= [mock-cf "INSERT INTO t(id) VALUES ($1)" params
                {:max-batch-size 99}]
               @called))))))

(deftest batch-delegation-test
  (testing "batch delegates to execute-batch!*"
    (let [called  (atom nil)
          stmts   ["DELETE FROM t1" "DELETE FROM t2"]
          mock-cf (mock/mock-connection-factory (mock/mock-connection {}))]
      (with-redefs [execute/execute-batch!* (fn [db statements opts]
                                              (reset! called [db statements
                                                              opts])
                                              :task)]
        (is (= :task (sut/batch mock-cf stmts)))
        (is (= [mock-cf stmts {}] @called))))))

(deftest with-connection-delegation-test
  (testing "with-connection passes {:connection conn} to the body"
    (let [called    (atom nil)
          mock-conn (mock/mock-connection {})
          mock-cf   (mock/mock-connection-factory mock-conn)]
      (with-redefs [conn/with-connection* (fn [db f]
                                            (reset! called [:with-connection
                                                            db])
                                            (f mock-conn))]
        (is (= :ok
               (sut/with-connection
                 mock-cf
                 (fn [ctx] (is (= {:connection mock-conn} ctx)) :ok))))
        (is (= [:with-connection mock-cf] @called))))))

(deftest with-transaction-delegation-test
  (testing
   "with-transaction composes the transaction middleware around the body"
    (let [called    (atom [])
          mock-conn (mock/mock-connection {})
          mock-cf   (mock/mock-connection-factory mock-conn)]
      (with-redefs [conn/with-connection*                                      (fn [db f]
                                                                                 (swap! called conj
                                                                                        [:with-connection db])
                                                                                 (f mock-conn))
                    middleware/with-transaction
                    (fn [opts]
                      (swap! called conj [:middleware opts])
                      (fn [body]
                        (fn [ctx] (swap! called conj [:ctx ctx]) (body ctx))))]
        (is (= :ok
               (sut/with-transaction
                 mock-cf
                 (fn [ctx] (swap! called conj [:body ctx]) :ok)
                 {:isolation :serializable})))
        (is (= [[:with-connection mock-cf]
                [:middleware {:isolation :serializable}]
                [:ctx {:r2dbc/connection mock-conn}]
                [:body {:connection mock-conn}]]
               @called))))))

(deftest with-options-wraps-factory-test
  (testing "with-options merges defaults and execute returns labelled results"
    (let [row*   (mock/mock-row {"id" 1} {:types {"id" Long}})
          result (mock/mock-result {:rows [row*]})
          stmt   (mock/mock-statement result)
          conn*  (mock/mock-connection {:statement stmt})
          cf     (mock/mock-connection-factory conn*)
          db*    (sut/with-options cf {:qualifier :unqualified})
          res    (db/run-task! (sut/execute db* "SELECT 1"))]
      (is (= [{:id 1}] (:clj-r2dbc/rows res))))))

(deftest with-conn-evaluates-forms-once-test
  (testing "with-conn macro expansion evaluates db and opts once"
    (let [expanded (macroexpand-1 '(clj-r2dbc/with-conn [conn db opts] :ok))]
      (is (= 1 (count (filter #{'db} (tree-seq coll? seq expanded)))))
      (is (= 1 (count (filter #{'opts} (tree-seq coll? seq expanded))))))))

(deftest with-tx-evaluates-forms-once-test
  (testing "with-tx macro expansion evaluates db and opts once"
    (let [expanded (macroexpand-1 '(clj-r2dbc/with-tx [conn db opts] :ok))]
      (is (= 1 (count (filter #{'db} (tree-seq coll? seq expanded)))))
      (is (= 1 (count (filter #{'opts} (tree-seq coll? seq expanded))))))))

(deftest with-conn-macro-aot-safety-test
  (testing "with-conn macro expands without error and uses gensyms"
    (let [expanded (macroexpand-1 '(clj-r2dbc/with-conn [c db] (use c)))]
      (is (some? expanded) "with-conn macro expands without error")
      (let [syms (filter symbol? (tree-seq coll? seq expanded))]
        (is (some #(re-find #"__\d+" (name %)) syms)
            "Expansion uses gensyms")))))

(deftest with-tx-macro-aot-safety-test
  (testing "with-tx macro expands without error and uses gensyms"
    (let [expanded (macroexpand-1 '(clj-r2dbc/with-tx [c db] (use c)))]
      (is (some? expanded) "with-tx macro expands without error")
      (let [syms (filter symbol? (tree-seq coll? seq expanded))]
        (is (some #(re-find #"__\d+" (name %)) syms)
            "Expansion uses gensyms")))))

;; Keyword-argument calling convention

(deftest connect-keyword-args-test
  (testing
   "connect accepts keyword args and a trailing map identically to a plain map"
    (let [calls (atom [])]
      (with-redefs [conn/create-connection-factory*
                    (fn [cfg] (swap! calls conj cfg) :factory)]
        ;; plain map (regression)
        (sut/connect "r2dbc:h2:mem:///db" {:x 1})
        ;; keyword args
        (sut/connect "r2dbc:h2:mem:///db" :x 1)
        ;; mixed: keyword arg + trailing map
        (sut/connect "r2dbc:h2:mem:///db" :x 1 {:y 2})
        (is (= [{:url "r2dbc:h2:mem:///db", :x 1}
                {:url "r2dbc:h2:mem:///db", :x 1}
                {:url "r2dbc:h2:mem:///db", :x 1, :y 2}]
               @calls))))))

(deftest with-options-keyword-args-test
  (testing
   "with-options accepts keyword args and trailing map identically to a plain map"
    (let [row*   (mock/mock-row {"id" 42} {:types {"id" Long}})
          result (mock/mock-result {:rows [row*]})
          stmt   (mock/mock-statement result)
          conn*  (mock/mock-connection {:statement stmt})
          cf     (mock/mock-connection-factory conn*)]
      ;; plain map (regression)
      (let [db* (sut/with-options cf {:fetch-size 128})] (is (some? db*)))
      ;; keyword args
      (let [db* (sut/with-options cf :fetch-size 128)] (is (some? db*)))
      ;; mixed: keyword arg + trailing map
      (let [db* (sut/with-options cf :fetch-size 128 {:qualifier :unqualified})]
        (is (some? db*))))))

(deftest execute-keyword-args-test
  (testing
   "execute accepts keyword args and trailing map identically to a plain map"
    (let [calls   (atom [])
          mock-cf (mock/mock-connection-factory (mock/mock-connection {}))]
      (with-redefs [execute/execute!* (fn [_ _ params opts]
                                        (swap! calls conj [params opts])
                                        :task)]
        ;; plain map (regression)
        (sut/execute mock-cf "SELECT $1" {:params [1], :fetch-size 32})
        ;; keyword args
        (sut/execute mock-cf "SELECT $1" :params [1] :fetch-size 32)
        ;; mixed: keyword pairs + trailing map
        (sut/execute mock-cf "SELECT $1" :params [1] {:fetch-size 32})
        (is (= [[[1] {:fetch-size 32}] [[1] {:fetch-size 32}]
                [[1] {:fetch-size 32}]]
               @calls))))))

(deftest first-row-keyword-args-test
  (testing "first-row accepts keyword args identically to a plain map"
    (let [calls   (atom [])
          mock-cf (mock/mock-connection-factory (mock/mock-connection {}))]
      (with-redefs [execute/execute-one!* (fn [_ _ params opts]
                                            (swap! calls conj [params opts])
                                            :task)]
        (sut/first-row mock-cf "SELECT $1" {:params [1]})
        (sut/first-row mock-cf "SELECT $1" :params [1])
        (sut/first-row mock-cf "SELECT $1" :params [1] {:fetch-size 64})
        (is (= [[[1] {}] [[1] {}] [[1] {:fetch-size 64}]] @calls))))))

(deftest stream-keyword-args-test
  (testing "stream accepts keyword args identically to a plain map"
    (let [calls   (atom [])
          mock-cf (mock/mock-connection-factory (mock/mock-connection {}))]
      (with-redefs [stream/stream* (fn [_ _ params opts]
                                     (swap! calls conj
                                            [params (:fetch-size opts)])
                                     :flow)]
        (sut/stream mock-cf "SELECT 1" {:fetch-size 128})
        (sut/stream mock-cf "SELECT 1" :fetch-size 128)
        (sut/stream mock-cf "SELECT 1" :fetch-size 64 {:params [1]})
        (is (= [[[] 128] [[] 128] [[1] 64]] @calls))))))

(deftest execute-each-keyword-args-test
  (testing "execute-each accepts keyword args identically to a plain map"
    (let [calls   (atom [])
          params  [[1] [2]]
          mock-cf (mock/mock-connection-factory (mock/mock-connection {}))]
      (with-redefs [execute/execute-each!*
                    (fn [_ _ _ opts] (swap! calls conj opts) :task)]
        (sut/execute-each mock-cf
                          "INSERT INTO t VALUES ($1)"
                          params
                          {:max-batch-size 99})
        (sut/execute-each mock-cf
                          "INSERT INTO t VALUES ($1)" params
                          :max-batch-size 99)
        (sut/execute-each mock-cf
                          "INSERT INTO t VALUES ($1)"
                          params
                          :max-batch-size
                          99
                          {:fetch-size 32})
        (is (= [{:max-batch-size 99} {:max-batch-size 99}
                {:max-batch-size 99, :fetch-size 32}]
               @calls))))))

(deftest with-transaction-keyword-args-test
  (testing "with-transaction accepts keyword args identically to a plain map"
    (let [calls     (atom [])
          mock-conn (mock/mock-connection {})
          mock-cf   (mock/mock-connection-factory mock-conn)]
      (with-redefs [conn/with-connection*       (fn [_ f] (f mock-conn))
                    middleware/with-transaction (fn [opts]
                                                  (swap! calls conj opts)
                                                  (fn [body]
                                                    (fn [ctx] (body ctx))))]
        ;; plain map (regression)
        (sut/with-transaction mock-cf (fn [_] :ok) {:isolation :serializable})
        ;; keyword args
        (sut/with-transaction mock-cf (fn [_] :ok) :isolation :serializable)
        ;; mixed
        (sut/with-transaction mock-cf
          (fn [_] :ok)
          :isolation
          :read-committed
          {:read-only? true})
        (is (= [{:isolation :serializable} {:isolation :serializable}
                {:isolation :read-committed, :read-only? true}]
               @calls))))))
