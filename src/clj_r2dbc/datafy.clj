;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.datafy
  "REPL navigation support for clj-r2dbc rows.

  Loading this namespace extends Navigable for all IPersistentMap instances.
  Navigation is gated by marker metadata set by
  clj-r2dbc.impl.datafy/attach-datafiable-meta, so only rows returned with
  :datafy true in opts are navigable.

  Provides:
    nav-task - returns a Missionary task for async row navigation.
    nav      - blocking navigation helper for REPL tooling."
  (:require
   [clj-r2dbc.impl.datafy :as datafy-impl]
   [clojure.core.protocols :as core-p])
  (:import
   (clojure.lang IPersistentMap)))

(set! *warn-on-reflection* true)

(defn ^{:added "0.1"} nav-task
  "Return a Missionary task for row navigation.

  Delegates to the :nav-fn stored in the row's metadata by
  clj-r2dbc.impl.datafy/attach-datafiable-meta. Returns a task yielding nil
  when no :nav-fn is present.

  Args:
    row  - navigable row map with :datafy metadata.
    k    - key navigated from the row.
    v    - value at k.
    opts - (optional) options map; defaults to {}.
             :nav-timeout-ms - timeout in milliseconds (default 5000).
           Accepts a map, keyword arguments, or both.

  Returns a Missionary task resolving to the navigation result or nil.

  Example:
    (m/? (nav-task row :id 1))
    (m/? (nav-task row :id 1 {:nav-timeout-ms 2000}))"
  ([row k v & {:as opts}] (datafy-impl/navigate-row row k v (or opts {}))))

(defn ^{:added "0.1"} nav
  "Navigate a row synchronously for REPL tooling.

  Args:
    row  - navigable row map with :datafy metadata.
    k    - key navigated from the row.
    v    - value at k.
    opts - (optional) options map; defaults to {}.
             :nav-timeout-ms - timeout in milliseconds (default 5000).
           Accepts a map, keyword arguments, or both.

  Returns the navigation result or nil.

  Throws (synchronously):
    ex-info :clj-r2dbc/timeout when :nav-timeout-ms elapses.

  Example:
    (nav row :id 1)
    (nav row :id 1 {:nav-timeout-ms 2000})"
  ([row k v & {:as opts}] (datafy-impl/navigate-blocking row k v (or opts {}))))

(extend-protocol core-p/Navigable
  IPersistentMap
  (nav [coll k v]
    (when (true? (datafy-impl/marker-key (meta coll)))
      (clj-r2dbc.datafy/nav coll k v))))

(comment
  (require '[missionary.core :as m])
  (def row
    (datafy-impl/attach-datafiable-meta {:id 1, :name "example.user"}
                                        :db
                                        {:nav-fn (fn [{:keys [k v]}]
                                                   (case k
                                                     :id {:entity/id v}
                                                     nil))}))
  (m/? (nav-task row :id 1))
  (nav row :id 1 {:nav-timeout-ms 1000}))
