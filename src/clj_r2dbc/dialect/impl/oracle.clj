;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.impl.oracle
  "Internal Oracle-specific LOB management utilities for clj-r2dbc.

  Provides:
    single-publisher        - creates a single-item Reactive Streams Publisher.
    oversized-bytes?        - returns true when a byte[] exceeds a size threshold.
    oversized-string?       - returns true when a String exceeds a size threshold.
    rewrite-param           - rewrites an oversized param to a Blob/Clob handle.
    rewrite-oversized-lobs  - applies rewrite-param to all params in a sequence.
    discard-lob!            - returns a task that discards a Blob or Clob handle.
    cleanup-lobs            - returns a task that discards all LOB handles in a context.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.util :as util]
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Blob Clob)
   (java.nio ByteBuffer)
   (org.reactivestreams Publisher Subscription)))

(set! *warn-on-reflection* true)

(defn single-publisher
  "Return a Reactive Streams Publisher that emits exactly one item then completes.

  Args:
    item - the single value to emit."
  [item]
  (reify
    Publisher
    (subscribe [_ s]
      (.onSubscribe s
                    (reify
                      Subscription
                      (request [_ _] (.onNext s item) (.onComplete s))
                      (cancel [_]))))))

(defn oversized-bytes?
  "Return true when v is a byte[] with length greater than threshold.

  Args:
    v         - value to check.
    threshold - maximum byte length (exclusive)."
  [v ^long threshold]
  (and (bytes? v) (> (alength ^bytes v) threshold)))

(defn oversized-string?
  "Return true when v is a String with character count greater than threshold.

  Args:
    v         - value to check.
    threshold - maximum character count (exclusive)."
  [v ^long threshold]
  (and (string? v) (> (count ^String v) threshold)))

(defn rewrite-param
  "Rewrite an oversized param value to a Blob or Clob handle.

  Args:
    v         - the parameter value to inspect.
    threshold - byte/character threshold above which rewriting occurs.

  Returns a map {:param rewritten-value, :handle lob-or-nil}. When neither
  size threshold is exceeded, :param is the original value and :handle is nil."
  [v ^long threshold]
  (cond (oversized-bytes? v threshold)
        (let [lob (Blob/from (single-publisher (ByteBuffer/wrap ^bytes v)))]
          {:param lob, :handle lob})
        (oversized-string? v threshold) (let [lob (Clob/from (single-publisher
                                                              ^String v))]
                                          {:param lob, :handle lob})
        :else {:param v, :handle nil}))

(defn rewrite-oversized-lobs
  "Rewrite all oversized params in the sequence, collecting Blob/Clob handles.

  Args:
    params    - sequential collection of parameter values.
    threshold - byte/character threshold passed to rewrite-param.

  Returns a map {:params rewritten-seq, :lob-handles handles-vec}."
  [params ^long threshold]
  (loop [remaining params
         rewritten (transient [])
         handles   (transient [])]
    (if-let [[p & more] (seq remaining)]
      (let [{:keys [param handle]} (rewrite-param p threshold)]
        (recur more
               (conj! rewritten param)
               (cond-> handles (some? handle) (conj! handle))))
      {:params (persistent! rewritten), :lob-handles (persistent! handles)})))

(defn discard-lob!
  "Return a Missionary task that discards a Blob or Clob handle.

  Calls .discard on the handle and drains the resulting Publisher<Void>.
  No-ops on nil and any other non-LOB value.

  Args:
    lob - Blob, Clob, nil, or any other value."
  [lob]
  (m/sp (cond (instance? Blob lob) (m/? (util/void->task (.discard ^Blob lob)))
              (instance? Clob lob) (m/? (util/void->task (.discard ^Clob lob)))
              :else nil)))

(defn cleanup-lobs
  "Return a Missionary task that discards all LOB handles stored in context.

  Discards all values under :clj-r2dbc.dialect.oracle/lob-handles and
  returns the context map with that key removed.

  Args:
    ctx - interceptor context map."
  [ctx]
  (m/sp (doseq [lob (:clj-r2dbc.dialect.oracle/lob-handles ctx)]
          (m/? (discard-lob! lob)))
        (dissoc ctx :clj-r2dbc.dialect.oracle/lob-handles)))

(comment
  (def threshold 32768)
  (oversized-bytes? (byte-array 40000) threshold)
  (oversized-string? (apply str (repeat 40000 "x")) threshold))
