;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.dialect.oracle
  "Optional Oracle-specific interceptor stages.

  Addresses Oracle's LOB size limits: String and byte[] params exceeding a
  threshold are automatically rewritten as Clob/Blob handles at :enter and
  discarded at :leave and :error. Add oracle-lob-interceptor (or
  oracle-lob-interceptor-with for custom thresholds) to :interceptors when
  binding large text or binary parameters."
  (:require
   [clj-r2dbc.dialect.impl.oracle :as impl-oracle]))

(set! *warn-on-reflection* true)

(defn ^{:added "0.1"} oracle-lob-interceptor-with
  "Return an Oracle LOB interceptor with custom options.

  Args:
    opts                 - options map.
      :lob-threshold-bytes - byte size above which String/byte[] params are
                             rewritten as Clob/Blob; default 32768.

  Returns an interceptor map with :name, :enter, :leave, and :error keys."
  [{:keys [lob-threshold-bytes], :or {lob-threshold-bytes 32768}}]
  (let [threshold (long lob-threshold-bytes)]
    (when-not (pos? threshold)
      (throw (ex-info "Invalid :lob-threshold-bytes; must be positive"
                      {:clj-r2dbc/error-type :invalid-argument
                       :key                  :lob-threshold-bytes
                       :value                lob-threshold-bytes})))
    {:name                                                                    ::lob-handler
     :enter
     (fn [ctx]
       (let [{:keys [params lob-handles]} (impl-oracle/rewrite-oversized-lobs
                                           (:clj-r2dbc/params ctx)
                                           threshold)]
         (assoc ctx :clj-r2dbc/params params ::lob-handles lob-handles)))
     :leave                                                                   impl-oracle/cleanup-lobs
     :error                                                                   impl-oracle/cleanup-lobs}))

(def ^{:added "0.1"} oracle-lob-interceptor
  "Interceptor that rewrites oversized String/byte[] params into Oracle-friendly LOB handles.

  Uses a default threshold of 32768 bytes. See oracle-lob-interceptor-with for
  custom thresholds. Handles are discarded on both success and error paths."
  (oracle-lob-interceptor-with {}))

(comment
  (def interceptors [oracle-lob-interceptor]))
