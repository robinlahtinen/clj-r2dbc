;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.debug-log
  "Extreme debug logging for concurrent race condition investigation.

  Provides thread-safe stderr logging with nanosecond-precision timestamps
  and thread names. Activated only when -Dclj.r2dbc.debug=true system property
  is set; when disabled, dlog is a no-op with negligible JIT overhead."
  (:import
   (java.lang System)))

(set! *warn-on-reflection* true)

(def ^:private ^Object print-lock (Object.))

(def ^:private debug-enabled
  (Boolean/parseBoolean (System/getProperty "clj.r2dbc.debug" "false")))

(defn dlog
  "Thread-safe debug log to stderr. Active only when -Dclj.r2dbc.debug=true.
  Parts are concatenated with str. Prefix: [nanoTime|thread-name]."
  [& parts]
  (when debug-enabled
    (locking print-lock
      (.println System/err
                (str "[" (System/nanoTime) "|" (.getName (Thread/currentThread)) "] "
                     (apply str parts))))))
