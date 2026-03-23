;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.datafy-test
  (:require
   [clj-r2dbc.datafy :as datafy]
   [clj-r2dbc.impl.datafy :as datafy-impl]
   [clj-r2dbc.test-util.db :as db]
   [clojure.datafy :as d]
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m])
  (:import
   (clojure.lang ExceptionInfo)))

(set! *warn-on-reflection* true)

(deftest nav-task-defaults-to-nil-test
  (testing "nav-task yields nil when row has no clj-r2dbc datafy metadata"
    (is (nil? (db/run-task! (datafy/nav-task {:id 1} :id 1))))))

(deftest nav-task-delegates-to-nav-fn-test
  (testing "nav-task delegates to metadata nav-fn"
    (let [row (datafy-impl/attach-datafiable-meta {:id 1}
                                                  :db
                                                  {:nav-fn (fn [_] {:id 2})})]
      (is (= {:id 2} (db/run-task! (datafy/nav-task row :friend-id 2)))))))

(deftest nav-timeout-test
  (testing "nav throws ex-info timeout when nav-task does not complete in time"
    (let [row (datafy-impl/attach-datafiable-meta
               {:id 1}
               :db
               {:nav-fn (fn [_] (m/sp (m/? (m/sleep 100)) :late))})]
      (try (datafy/nav row :x 1 {:nav-timeout-ms 5})
           (is false "should have thrown")
           (catch ExceptionInfo e
             (is (= :nav (:clj-r2dbc/timeout (ex-data e)))))))))

(deftest navigable-extension-test
  (testing
   "requiring clj-r2dbc.datafy enables clojure.datafy/nav on marked rows"
    (let [row (datafy-impl/attach-datafiable-meta {:id 1}
                                                  :db
                                                  {:nav-fn (fn [_] :ok)})]
      (is (= :ok (d/nav row :k :v))))))
