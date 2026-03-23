;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.datafy
  "Internal navigation helpers and datafiable-row attachment for clj-r2dbc.

  Provides metadata key defs and attach-datafiable-meta for marking rows
  navigable, and navigate-row/navigate-blocking for REPL nav delegation.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clojure.core.protocols :as core-p]
   [missionary.core :as m])
  (:import
   (clojure.lang IObj)))

(set! *warn-on-reflection* true)

(def marker-key ::datafiable-row)

(def connectable-key ::connectable)

(def opts-key ::opts)

(def nav-fn-key ::nav-fn)

(def ^:private default-nav-timeout-ms 5000)

(defn- to-task [result] (if (fn? result) result (m/sp result)))

(defn attach-datafiable-meta
  "Attach Datafiable metadata to a realized row value.

  Stores clj-r2dbc navigation context in row metadata for optional
  clj-r2dbc.datafy integration. Non-IObj values are returned unchanged.
  Intended for REPL navigation only; not for high-throughput queries.

  Args:
    row         - realized row value; must implement IObj to receive metadata.
    connectable - database connectable passed to the :nav-fn.
    opts        - execution options map; :nav-fn is extracted from this map."
  [row connectable opts]
  (if (instance? IObj row)
    (vary-meta row
               assoc
               marker-key
               true
               connectable-key
               connectable
               opts-key
               opts
               nav-fn-key
               (:nav-fn opts)
               `core-p/datafy
               identity)
    row))

(defn navigate-row
  "Return a Missionary task for row navigation.

  Delegates to the :nav-fn stored in row metadata by attach-datafiable-meta.
  Returns a task yielding nil when no nav-fn is present or the row is not
  marked as datafiable.

  Args:
    row  - row map with optional :datafy metadata.
    k    - key navigated from the row.
    v    - value at k.
    opts - options map merged with the row's stored opts."
  [row k v opts]
  (m/sp (let [md      (meta row)
              marked? (true? (marker-key md))
              nav-fn  (nav-fn-key md)]
          (if (and marked? (fn? nav-fn))
            (m/? (to-task (nav-fn {:connectable (connectable-key md)
                                   :row         row
                                   :k           k
                                   :v           v
                                   :opts        (merge (opts-key md {}) opts)})))
            nil))))

(defn navigate-blocking
  "Navigate a row synchronously for REPL tooling.

  Args:
    row  - row map with optional :datafy metadata.
    k    - key navigated from the row.
    v    - value at k.
    opts - options map; :nav-timeout-ms overrides the default 5000 ms.

  Returns the navigation result or nil.

  Throws (synchronously):
    ex-info :clj-r2dbc/timeout when :nav-timeout-ms elapses."
  [row k v opts]
  (let [timeout-ms  (long (:nav-timeout-ms opts default-nav-timeout-ms))
        timeout-val ::timeout
        result      (m/?
                     (m/timeout (navigate-row row k v opts) timeout-ms timeout-val))]
    (if (= timeout-val result)
      (throw (ex-info "Navigation timed out"
                      {:clj-r2dbc/timeout :nav
                       :nav-timeout-ms    timeout-ms
                       :key               k
                       :value             v}))
      result)))

(comment
  (def row
    (attach-datafiable-meta {:id 1}
                            :db
                            {:nav-fn (fn [{:keys [k v]}]
                                       (case k
                                         :id {:entity/id v}
                                         nil))}))
  (meta row)
  (m/? (navigate-row row :id 1 {}))
  (navigate-blocking row :id 1 {})
  (m/? (navigate-row {:id 1} :id 1 {})))
