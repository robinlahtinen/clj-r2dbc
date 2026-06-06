;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.debug-log
  "Property-guarded trace logging for concurrent race-condition investigation.

  Provides thread-safe stderr logging tagged with a global monotonic sequence
  number and the current thread name. The sequence number gives an unambiguous
  total order across threads, so the *last* events before a lost-wakeup
  quiescence pinpoint the dropped notifier/terminator.

  Activated only when -Dclj-r2dbc.trace=true is set; when disabled, `dlog` is a
  no-op guarded by a single volatile read, with negligible JIT overhead.

  To minimise the observer effect on the race under investigation, `dlog` does
  NOT take a coarse application lock: it builds the whole line as one string and
  emits it with a single `PrintStream.println` (internally atomic), so the only
  added cross-thread synchronisation is the lock-free `AtomicLong` sequence. The
  sequence number — not wall-clock ordering of interleaved writes — is the
  authoritative event order.

  This namespace is an implementation detail; do not use from application code."
  (:import
   (java.util.concurrent.atomic AtomicLong)))

(set! *warn-on-reflection* true)

(def ^:private ^AtomicLong seq-counter (AtomicLong. 0))

(def ^:private trace-enabled
  (Boolean/parseBoolean (System/getProperty "clj-r2dbc.trace" "false")))

(defn enabled?
  "Return true when trace logging is active (-Dclj-r2dbc.trace=true)."
  []
  trace-enabled)

(defn dlog
  "Trace log to stderr. Active only when -Dclj-r2dbc.trace=true. Parts are
  concatenated with str. Prefix: [seq|thread-name], where seq is a global
  monotonic counter giving the authoritative total order across threads."
  [& parts]
  (when trace-enabled
    (let [n (.getAndIncrement seq-counter)]
      (.println System/err
                (str "[" n "|" (.getName (Thread/currentThread)) "] "
                     (apply str parts))))))
