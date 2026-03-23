;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.sql.result
  "R2DBC Result processing for clj-r2dbc.

  Provides:
    map-result - processes a Result, dispatching on segment type, collects to vector.

  Segment dispatch:
    Result$RowSegment  - passes Row to row-fn, emits the result.
    Result$UpdateCount - emits {:clj-r2dbc/update-count n}.
    Result$Message     - discarded silently (advisory messages).
    unknown segment    - discarded silently.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Result Result$Message Result$RowSegment Result$UpdateCount)
   (java.util ArrayList)
   (java.util.concurrent CompletableFuture)
   (missionary Cancelled)
   (org.reactivestreams Publisher Subscriber Subscription)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- just-publisher
  "Return a plain Publisher that emits a single non-nil value then completes.

  Args:
    v - the value to emit.

  Uses plain org.reactivestreams.Publisher (not Reactor Mono) to prevent
  Reactor scalar-fusion optimizations that deliver onNext synchronously
  inside request(), which can trigger re-entrant callback hazards."
  ^Publisher [v]
  (reify
    Publisher
    (subscribe [_ s]
      (.onSubscribe s
                    (reify
                      Subscription
                      (request [_ _] (.onNext s v) (.onComplete s))
                      (cancel [_]))))))

(defn- empty-publisher
  "Return a plain Publisher that completes immediately without emitting values.

  Uses plain org.reactivestreams.Publisher (not Reactor Flux) to prevent
  scalar-fusion optimizations."
  ^Publisher []
  (reify
    Publisher
    (subscribe [_ s]
      (.onSubscribe s
                    (reify
                      Subscription
                      (request [_ _] (.onComplete s))
                      (cancel [_]))))))

(defn- segment->publisher
  "Map a Result segment to a Publisher based on its type.

  Row segments are passed through row-fn. Update counts become a map.
  Message and unknown segments are discarded."
  [seg row-fn]
  (cond (instance? Result$RowSegment seg)
        (just-publisher (row-fn (.row ^Result$RowSegment seg)))
        (instance? Result$UpdateCount seg) (just-publisher
                                            {:clj-r2dbc/update-count
                                             (.value ^Result$UpdateCount seg)})
        (instance? Result$Message seg) (empty-publisher)
        :else (empty-publisher)))

(defn- subscribe-collecting!
  "Subscribe to pub, collecting all emitted values into acc and signalling fut.

  Returns nil. Extracts the void Publisher.subscribe call from map-result's m/sp
  body so the coroutine does not create a void-typed place for it."
  [^Publisher pub ^ArrayList acc ^CompletableFuture fut sub-ref]
  (.subscribe pub
              (reify
                Subscriber
                (onSubscribe [_ s]
                  (vreset! sub-ref s)
                  (.request ^Subscription s Long/MAX_VALUE))
                (onNext [_ v] (.add acc v))
                (onError [_ t] (.completeExceptionally fut t))
                (onComplete [_] (.complete fut (vec acc)))))
  nil)

(defn map-result
  "Process an R2DBC Result, dispatching on segment type.

  Args:
    result - the R2DBC Result to process.
    row-fn - 1-arity (Row -> value) function applied to each row segment.

  Returns a Missionary task resolving to a persistent vector of all emitted values:
    Result$RowSegment  - (row-fn row), appended to result vector.
    Result$UpdateCount - {:clj-r2dbc/update-count n}, appended to result vector.
    Result$Message     - discarded (advisory messages do not appear in result).
    unknown segment    - discarded.

  Example:
    (m/? (map-result result row-fn))"
  [^Result result row-fn]
  (let [^ArrayList acc         (ArrayList.)
        ^CompletableFuture fut (CompletableFuture.)
        sub-ref                (volatile! nil)
        ^Publisher pub         (.flatMap result #(segment->publisher % row-fn))]
    (subscribe-collecting! pub acc fut sub-ref)
    (m/sp (try (m/? (m/via m/blk (.get ^CompletableFuture fut)))
               (catch Cancelled c
                 (when-let [^Subscription s @sub-ref]
                   (.cancel s)
                   nil)
                 (throw c))))))

(comment
  (require '[clj-r2dbc.impl.sql.row :as row])
  (def result nil)
  (def cache nil)
  (m/? (map-result result (fn [row] {:id (.get row "id" Long)})))
  (m/? (map-result result (fn [row] (row/row->map row cache)))))
