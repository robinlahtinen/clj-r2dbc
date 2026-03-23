;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.bench.bind-params-bench
  "Benchmarks for bind-params! monomorphic and polymorphic fast paths."
  (:require
   [clj-r2dbc.bench.util :as bench-util]
   [clj-r2dbc.impl.sql.params :as params]
   [criterium.bench :as bench]
   [criterium.measured :as measured])
  (:import (io.r2dbc.spi Statement)))

(set! *warn-on-reflection* true)

(defn- noop-stmt
  "Return a Statement that records nothing (pure overhead measurement)."
  ^Statement []
  (reify
    Statement
    (execute [_] (throw (UnsupportedOperationException.)))
    (^Statement bind [this ^int _i ^Object _v] this)
    (^Statement bind [this ^String _name ^Object _v] this)
    (^Statement bindNull [this ^int _i ^Class _t] this)
    (^Statement bindNull [this ^String _name ^Class _t] this)
    (^Statement add [this] this)
    (^Statement fetchSize [this ^int _n] this)
    (^Statement returnGeneratedValues
      [this ^"[Ljava.lang.String;" _cols]
      this)))

(def ^:private mono-params
  "100 String params - monomorphic fast path."
  (vec (repeat 100 "hello")))

(def ^:private poly-params
  "100 mixed params - String/Long/nil."
  (vec (take 100 (cycle ["hello" 42 nil]))))

(defn mono-type-bench
  "Run the 100 String params benchmark (monomorphic fast path)."
  []
  (println "  mono-type ...")
  (let [stmt (noop-stmt)
        m    (measured/callable (fn [] (params/bind-params! stmt mono-params)))]
    (bench/bench-measured bench-util/default-bench-plan m)
    (bench-util/extract-result)))

(defn poly-type-bench
  "Run the 100 mixed String/Long/nil params benchmark (polymorphic fast path)."
  []
  (println "  poly-type ...")
  (let [stmt (noop-stmt)
        m    (measured/callable (fn [] (params/bind-params! stmt poly-params)))]
    (bench/bench-measured bench-util/default-bench-plan m)
    (bench-util/extract-result)))

(defn run-all [] {:mono-type (mono-type-bench), :poly-type (poly-type-bench)})
