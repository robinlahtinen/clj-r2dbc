;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.exec.pipeline-test
  "Tests for clj-r2dbc.impl.exec.pipeline interceptor runner."
  (:require
   [clj-r2dbc.impl.exec.pipeline :as pipeline]
   [clj-r2dbc.test-util.virtual-time :as vt]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [missionary.core :as m])
  (:import
   (clojure.lang ExceptionInfo)
   (missionary Cancelled)))

(set! *warn-on-reflection* true)

(use-fixtures :each vt/with-virtual-clock)

(defn- simple-execute-fn
  "A minimal execute-fn that marks the context as executed."
  [ctx]
  (m/sp (assoc ctx :executed true)))

(defn- recording-interceptor
  "Create an interceptor that records enter/leave/error calls to an atom.

  The atom accumulates vectors of [phase name] entries."
  [n log-atom]
  {:name  n
   :enter (fn [ctx]
            (swap! log-atom conj [:enter n])
            (assoc ctx (keyword (str "entered-" n)) true))
   :leave (fn [ctx]
            (swap! log-atom conj [:leave n])
            (assoc ctx (keyword (str "left-" n)) true))
   :error (fn [ctx] (swap! log-atom conj [:error n]) ctx)})

(deftest enter-leave-ordering-test
  (testing "interceptors enter in queue order, leave in reverse (LIFO)"
    (let [log  (atom [])
          i-a  (recording-interceptor "A" log)
          i-b  (recording-interceptor "B" log)
          i-c  (recording-interceptor "C" log)
          task (pipeline/run-pipeline {} [i-a i-b i-c] simple-execute-fn)]
      (vt/run-task task)
      (is (= [[:enter "A"] [:enter "B"] [:enter "C"] [:leave "C"] [:leave "B"]
              [:leave "A"]]
             @log)))))

(deftest error-handler-reverse-order-test
  (testing "error handlers called in reverse stack order on execute-fn failure"
    (let [log          (atom [])
          i-a          (recording-interceptor "A" log)
          i-b          (recording-interceptor "B" log)
          i-c          (recording-interceptor "C" log)
          failing-exec (fn [_ctx] (m/sp (throw (ex-info "exec-fail" {}))))
          task         (pipeline/run-pipeline {} [i-a i-b i-c] failing-exec)]
      (is (thrown? ExceptionInfo (vt/run-task task)))
      (is (= [[:enter "A"] [:enter "B"] [:enter "C"] [:error "C"] [:error "B"]
              [:error "A"]]
             @log)))))

(deftest enter-failure-error-stack-test
  (testing
   "when :enter fails, error stage runs on previously succeeded interceptors only"
    (let [log    (atom [])
          i-a    (recording-interceptor "A" log)
          i-b    (recording-interceptor "B" log)
          i-fail {:name  "C"
                  :enter (fn [_ctx]
                           (swap! log conj [:enter "C"])
                           (throw (ex-info "enter-C-fail" {})))
                  :error (fn [ctx] (swap! log conj [:error "C"]) ctx)}
          task   (pipeline/run-pipeline {} [i-a i-b i-fail] simple-execute-fn)]
      (is (thrown? ExceptionInfo (vt/run-task task)))
      (is (= [[:enter "A"] [:enter "B"] [:enter "C"] [:error "B"] [:error "A"]]
             @log)))))

(deftest leave-failure-error-stack-test
  (testing
   "when :leave fails, error stage runs on remaining stack (not including failed)"
    (let [log  (atom [])
          i-a  (recording-interceptor "A" log)
          i-b  {:name  "B"
                :enter (fn [ctx]
                         (swap! log conj [:enter "B"])
                         (assoc ctx :entered-B true))
                :leave (fn [_ctx]
                         (swap! log conj [:leave "B"])
                         (throw (ex-info "leave-B-fail" {})))
                :error (fn [ctx] (swap! log conj [:error "B"]) ctx)}
          i-c  (recording-interceptor "C" log)
          task (pipeline/run-pipeline {} [i-a i-b i-c] simple-execute-fn)]
      (is (thrown? ExceptionInfo (vt/run-task task)))
      (is (= [[:enter "A"] [:enter "B"] [:enter "C"] [:leave "C"] [:leave "B"]
              [:error "A"]]
             @log)))))

(deftest execute-fn-failure-test
  (testing "execute-fn throws, error stage runs on full stack"
    (let [log          (atom [])
          i-a          (recording-interceptor "A" log)
          i-b          (recording-interceptor "B" log)
          failing-exec (fn [_ctx] (m/sp (throw (ex-info "exec-boom" {}))))
          task         (pipeline/run-pipeline {} [i-a i-b] failing-exec)]
      (is (thrown? ExceptionInfo (vt/run-task task)))
      (is (= [[:enter "A"] [:enter "B"] [:error "B"] [:error "A"]] @log)))))

(deftest cancelled-passthrough-test
  (testing "missionary.Cancelled propagates through error handling"
    (let [log  (atom [])
          i-a  (recording-interceptor "A" log)
          task (pipeline/run-pipeline {}
                                      [i-a]
                                      (fn [_ctx] (m/sp (throw (Cancelled.)))))]
      (is (thrown? Cancelled (vt/run-task task)))
      (is (= [[:enter "A"] [:error "A"]] @log)))))

(deftest cancelled-in-error-handler-test
  (testing "missionary.Cancelled in :error handler is re-thrown immediately"
    (let [log  (atom [])
          i-a  {:name  "A"
                :enter (fn [ctx] (swap! log conj [:enter "A"]) ctx)
                :error (fn [ctx] (swap! log conj [:error "A"]) ctx)}
          i-b  {:name                                                          "B"
                :enter                                                         (fn [ctx] (swap! log conj [:enter "B"]) ctx)
                :error
                (fn [_ctx] (swap! log conj [:error "B"]) (throw (Cancelled.)))}
          task (pipeline/run-pipeline
                {}
                [i-a i-b]
                (fn [_ctx] (m/sp (throw (ex-info "original" {})))))]
      (is (thrown? Cancelled (vt/run-task task)))
      (is (= [[:enter "A"] [:enter "B"] [:error "B"]] @log)))))

(deftest secondary-error-suppression-test
  (testing
   "non-Cancelled errors in :error handlers are suppressed via .addSuppressed"
    (let [i-a         {:name  "A"
                       :enter identity
                       :error (fn [_ctx] (throw (ex-info "secondary-A" {})))}
          i-b         {:name  "B"
                       :enter identity
                       :error (fn [_ctx] (throw (ex-info "secondary-B" {})))}
          original-ex (ex-info "original" {})
          task        (pipeline/run-pipeline {}
                                             [i-a i-b]
                                             (fn [_ctx] (m/sp (throw original-ex))))]
      (try (vt/run-task task)
           (is false "should have thrown")
           (catch ExceptionInfo e
             (is (= "original" (.getMessage e)))
             (let [suppressed (vec (.getSuppressed e))]
               (is (= 2 (count suppressed)))
               (is (= "secondary-B"
                      (.getMessage ^Throwable (first suppressed))))
               (is (= "secondary-A"
                      (.getMessage ^Throwable (second suppressed))))))))))

(deftest async-stages-test
  (testing "stages returning m/sp tasks work correctly"
    (let [async-interceptor
          {:name  ::async
           :enter (fn [ctx]
                    (m/sp (m/? (m/sleep 10)) (assoc ctx :async-entered true)))
           :leave (fn [ctx]
                    (m/sp (m/? (m/sleep 5)) (assoc ctx :async-left true)))}
          task                                                                 (pipeline/run-pipeline {} [async-interceptor] simple-execute-fn)
          result                                                               (vt/run-task task)]
      (is (true? (:async-entered result)))
      (is (true? (:async-left result)))
      (is (true? (:executed result))))))

(deftest sync-stages-test
  (testing "stages returning plain maps work correctly"
    (let [sync-interceptor {:name  ::sync
                            :enter (fn [ctx] (assoc ctx :sync-entered true))
                            :leave (fn [ctx] (assoc ctx :sync-left true))}
          task             (pipeline/run-pipeline {} [sync-interceptor] simple-execute-fn)
          result           (vt/run-task task)]
      (is (true? (:sync-entered result)))
      (is (true? (:sync-left result)))
      (is (true? (:executed result))))))

(deftest missing-handlers-test
  (testing "interceptors with missing :enter/:leave/:error default to identity"
    (let [enter-only {:name ::enter-only, :enter (fn [ctx] (assoc ctx :a true))}
          leave-only {:name ::leave-only, :leave (fn [ctx] (assoc ctx :b true))}
          empty-i    {:name ::empty}
          task       (pipeline/run-pipeline {:initial true}
                                            [enter-only leave-only empty-i]
                                            simple-execute-fn)
          result     (vt/run-task task)]
      (is (true? (:initial result)))
      (is (true? (:a result)))
      (is (true? (:b result)))
      (is (true? (:executed result))))))

(deftest empty-interceptor-chain-test
  (testing "pipeline with no interceptors just calls execute-fn"
    (let [task   (pipeline/run-pipeline {:data :initial} [] simple-execute-fn)
          result (vt/run-task task)]
      (is (= :initial (:data result)))
      (is (true? (:executed result))))))

(deftest interceptor-depth-guard-test
  (testing "pipeline rejects interceptor chains longer than 64 entries"
    (let [interceptors (vec (repeat 65 {:name ::noop}))
          task         (pipeline/run-pipeline {} interceptors simple-execute-fn)]
      (try (vt/run-task task)
           (is false "should have thrown")
           (catch ExceptionInfo e
             (is (= {:clj-r2dbc/error   :clj-r2dbc/limit-exceeded
                     :clj-r2dbc/context :pipeline
                     :key               :interceptors
                     :constraint        :max-interceptor-depth
                     :limit             64
                     :value             65}
                    (ex-data e))))))))

(deftest context-threading-test
  (testing "modified context flows through enter -> execute -> leave"
    (let [i-a     {:name  ::a
                   :enter (fn [ctx] (assoc ctx :step1 :entered-a))
                   :leave (fn [ctx] (assoc ctx :step4 :left-a))}
          i-b     {:name  ::b
                   :enter (fn [ctx] (assoc ctx :step2 :entered-b))
                   :leave (fn [ctx] (assoc ctx :step3 :left-b))}
          exec-fn (fn [ctx]
                    (m/sp
                     (assoc ctx :exec-saw-steps [(:step1 ctx) (:step2 ctx)])))
          task    (pipeline/run-pipeline {:origin true} [i-a i-b] exec-fn)
          result  (vt/run-task task)]
      (is (true? (:origin result)))
      (is (= :entered-a (:step1 result)))
      (is (= :entered-b (:step2 result)))
      (is (= [:entered-a :entered-b] (:exec-saw-steps result)))
      (is (= :left-b (:step3 result)))
      (is (= :left-a (:step4 result))))))

(deftest fused-sync-cancellation-test
  (testing "cancellation propagates through fused ::sync interceptors"
    (let [log          (atom [])
          sync-i       (fn [n]
                         {:name           n
                          ::pipeline/sync true
                          :enter          (fn [ctx] (swap! log conj [:enter n]) ctx)
                          :leave          (fn [ctx] (swap! log conj [:leave n]) ctx)
                          :error          (fn [ctx] (swap! log conj [:error n]) ctx)})
          interceptors [(sync-i "A") (sync-i "B") (sync-i "C") (sync-i "D")]
          task         (pipeline/run-pipeline {}
                                              interceptors
                                              (fn [_ctx] (m/sp (throw (Cancelled.)))))]
      (is (thrown? Cancelled (vt/run-task task)))
      (is (= [[:enter "A"] [:enter "B"] [:enter "C"] [:enter "D"] [:error "D"]
              [:error "C"] [:error "B"] [:error "A"]]
             @log)))))

(deftest overridable-max-interceptor-depth-test
  (testing ":max-interceptor-depth 128 allows chain of 100 interceptors"
    (let [interceptors (vec (repeat 100 {:name ::noop}))
          task         (pipeline/run-pipeline {}
                                              interceptors
                                              simple-execute-fn
                                              {:max-interceptor-depth 128})
          result       (vt/run-task task)]
      (is (true? (:executed result)))))
  (testing "default (no key) still rejects 65 interceptors"
    (let [interceptors (vec (repeat 65 {:name ::noop}))
          task         (pipeline/run-pipeline {} interceptors simple-execute-fn)]
      (try (vt/run-task task)
           (is false "should have thrown")
           (catch ExceptionInfo e
             (is (= :clj-r2dbc/limit-exceeded (:clj-r2dbc/error (ex-data e))))
             (is (= 64 (:limit (ex-data e))))))))
  (testing ":max-interceptor-depth 0 throws :invalid-argument"
    (let [task (pipeline/run-pipeline {}
                                      []
                                      simple-execute-fn
                                      {:max-interceptor-depth 0})]
      (try (vt/run-task task)
           (is false "should have thrown")
           (catch ExceptionInfo e
             (is (= :invalid-argument (:clj-r2dbc/error-type (ex-data e))))
             (is (= :max-interceptor-depth (:key (ex-data e))))))))
  (testing ":max-interceptor-depth -1 throws :invalid-argument"
    (let [task (pipeline/run-pipeline {}
                                      []
                                      simple-execute-fn
                                      {:max-interceptor-depth -1})]
      (try (vt/run-task task)
           (is false "should have thrown")
           (catch ExceptionInfo e
             (is (= :invalid-argument (:clj-r2dbc/error-type (ex-data e))))))))
  (testing ":max-interceptor-depth \"foo\" throws :invalid-argument"
    (let [task (pipeline/run-pipeline {}
                                      []
                                      simple-execute-fn
                                      {:max-interceptor-depth "foo"})]
      (try (vt/run-task task)
           (is false "should have thrown")
           (catch ExceptionInfo e
             (is (= :invalid-argument (:clj-r2dbc/error-type (ex-data e)))))))))

(deftest fused-sync-leave-failure-unwind-order-test
  (testing "fused sync leave failure preserves unfused LIFO :error order"
    (let [log          (atom [])
          mk           (fn [n]
                         {:name                                                     n
                          ::pipeline/sync                                           true
                          :enter                                                    (fn [ctx] (swap! log conj [:enter n]) ctx)
                          :leave
                          (fn [ctx]
                            (swap! log conj [:leave n])
                            (if (= n "C") (throw (ex-info "leave-C-fail" {})) ctx))
                          :error                                                    (fn [ctx] (swap! log conj [:error n]) ctx)})
          interceptors [(mk "A") (mk "B") (mk "C") (mk "D")]
          task         (pipeline/run-pipeline {} interceptors simple-execute-fn)]
      (is (thrown? ExceptionInfo (vt/run-task task)))
      (is (= [[:enter "A"] [:enter "B"] [:enter "C"] [:enter "D"] [:leave "D"]
              [:leave "C"] [:error "B"] [:error "A"]]
             @log)))))
