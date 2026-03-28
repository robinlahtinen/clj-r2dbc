;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.connection
  "Connection lifecycle internals for clj-r2dbc.

  Manages connection acquisition and teardown using a RAII pattern:
  Connection.close() is guaranteed on success, error, and Missionary
  cancellation. When the caller supplies an existing Connection, RAII
  ownership is skipped.

  Provides:
    ConnectableWithOpts        - record pairing a raw connectable with default opts.
    create-connection-factory* - URL delegation to ConnectionFactories.get.
    acquire-connection         - subscribes to ConnectionFactory.create(), returns task.
    with-connection*           - scoped connection sharing with RAII cleanup.
    with-options*              - wraps a connectable with default opts.
    connection-metadata*       - wraps ConnectionMetadata into a plain Clojure map.

  Protocols extended here:
    proto/Connectable - ConnectionFactory, Connection, ConnectableWithOpts.
    proto/Describable - Connection.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.protocols :as proto]
   [clj-r2dbc.impl.util :as util]
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Connection ConnectionFactories ConnectionFactory ConnectionMetadata)
   (java.util.concurrent CompletableFuture)
   (missionary Cancelled)
   (org.reactivestreams Publisher Subscriber Subscription)))

(set! *warn-on-reflection* true)

(defprotocol Wrapped
  (-unwrap [this]
    "Return [raw-db default-opts-map]."))

(defrecord
 ^{:doc
   "An opaque wrapper pairing a raw connectable with a map of default options.

   Fields:
     connectable - raw ConnectionFactory or Connection.
     options     - map of default execution options merged at call time.

   Created by with-options* and unwrapped by resolve-connectable at call time."}
 ConnectableWithOpts
 [connectable options]
  Wrapped
  (-unwrap [_] [connectable options]))

(defn resolve-connectable
  "Unwrap a connectable into [raw-db merged-opts].

  For ConnectableWithOpts, merges default options with per-call-opts (nil
  values in per-call-opts don't clobber defaults). For raw
  ConnectionFactory/Connection, returns [db per-call-opts] unchanged.

  Args:
    db            - ConnectionFactory, Connection, or ConnectableWithOpts.
    per-call-opts - options map from the caller."
  [db per-call-opts]
  (if (instance? ConnectableWithOpts db)
    (let [[raw-db default-opts] (-unwrap db)]
      [raw-db
       (merge default-opts (into {} (remove (comp nil? val)) per-call-opts))])
    [db per-call-opts]))

(defn with-options*
  "Wrap a connectable with default options.

  Supports nesting: inner defaults override outer defaults.

  Args:
    db   - ConnectionFactory, Connection, or ConnectableWithOpts.
    opts - map of default options to attach."
  [db opts]
  (if (instance? ConnectableWithOpts db)
    (let [[raw-db existing-opts] (-unwrap db)]
      (->ConnectableWithOpts raw-db (merge existing-opts opts)))
    (->ConnectableWithOpts db opts)))

(defn- publisher->task
  "Bridge a Publisher<T> into a Missionary task resolving to the first emitted
  value, or nil when the Publisher completes empty.

  Subscribes to the Publisher, requests one item, and completes a
  CompletableFuture on onNext/onError/onComplete. Uses m/via on the blocking
  thread pool to park until the future resolves.

  On Missionary cancellation, cancels the upstream Subscription so the
  Publisher can release any resources it holds."
  [^Publisher pub]
  (m/sp
   (let [sub-ref (volatile! nil)
         fut     (CompletableFuture.)
         _       (doto pub
                   (.subscribe (reify
                                 Subscriber
                                 (onSubscribe [_ s]
                                   (vreset! sub-ref s)
                                   (.request ^Subscription s 1))
                                 (onNext [_ v] (.complete fut v))
                                 (onError [_ t] (.completeExceptionally fut t))
                                 (onComplete [_] (.complete fut nil)))))]
     (try (m/? (m/via m/blk (.get ^CompletableFuture fut)))
          (catch Cancelled c
            (when-let [^Subscription s @sub-ref]
              (.cancel s)
              nil)
            (throw c))))))

(defn create-connection-factory*
  "Create a ConnectionFactory from a configuration map.

  The :url value is forwarded verbatim to ConnectionFactories.get(String).
  Composite URLs (r2dbc:pool:..., r2dbc:proxy:...) are accepted.

  Args:
    opts - map containing :url - non-blank R2DBC URL string.

  Returns a ConnectionFactory instance.

  Example:
    (create-connection-factory* {:url \"r2dbc:h2:mem:///testdb\"})"
  ^ConnectionFactory [{:keys [url]}]
  (ConnectionFactories/get ^String url))

(defn acquire-connection
  "Acquire an R2DBC connection as a Missionary task.

  When db is a ConnectionFactory, subscribes to ConnectionFactory.create()
  and returns a task resolving to the Connection. The caller is responsible
  for closing the connection (see with-connection* for RAII semantics).

  When db is already a Connection, returns a task resolving to it directly;
  lifecycle is owned by the enclosing scope.

  ConnectableWithOpts is unwrapped before the instance check.

  Args:
    db - ConnectionFactory, Connection, or ConnectableWithOpts.

  Returns a Missionary task resolving to a Connection.

  Example:
    (m/? (acquire-connection factory))  ;;=> Connection
    (m/? (acquire-connection conn))     ;;=> conn (passed through)"
  [db]
  (let [raw-db (if (instance? ConnectableWithOpts db) (first (-unwrap db)) db)]
    (if (instance? Connection raw-db)
      (m/sp raw-db)
      (publisher->task (.create ^ConnectionFactory raw-db)))))

(defn with-connection*
  "RAII-managed connection sharing scope.

  Acquires a connection from db (ConnectionFactory or ConnectionPool) and
  passes it to body-fn. The connection is guaranteed to be closed on success,
  error, and Missionary cancellation - all three paths call Connection.close()
  exactly once.

  When db is already a Connection, passes it directly without RAII wrapping;
  lifecycle is owned by the caller and Connection.close() is NOT called.

  ConnectableWithOpts is unwrapped before the instance check.

  Args:
    db      - ConnectionFactory, Connection, or ConnectableWithOpts.
    body-fn - 1-arity fn [Connection] -> Missionary task.

  Returns a Missionary task resolving to the body-fn result.

  Example:
    (with-connection* pool
      (fn [conn]
        (m/sp (m/? (execute conn \"SELECT 1\")))))"
  [db body-fn]
  (let [raw-db (if (instance? ConnectableWithOpts db) (first (-unwrap db)) db)]
    (if (instance? Connection raw-db)
      (body-fn raw-db)
      (m/sp (let [^Connection conn (m/? (acquire-connection raw-db))]
              (try (m/? (body-fn conn))
                   (finally (m/? (util/void->task (.close conn))))))))))

(defn connection-metadata*
  "Wrap Connection.getMetadata() into a plain Clojure map.

  Both keys come from the R2DBC SPI ConnectionMetadata interface:
    ConnectionMetadata.getDatabaseProductName()
    ConnectionMetadata.getDatabaseVersion()

  Args:
    conn - active R2DBC Connection.

  Returns {:db/product-name string, :db/version string}.

  Example:
    (connection-metadata* conn)
    ;;=> {:db/product-name \"H2\", :db/version \"2.4.240\"}"
  [^Connection conn]
  (let [^ConnectionMetadata md (.getMetadata conn)]
    {:db/product-name (.getDatabaseProductName md)
     :db/version      (.getDatabaseVersion md)}))

(extend-protocol proto/Connectable
  ConnectionFactory
  (-with-connection [db body-fn] (with-connection* db body-fn))
  (-with-options [db opts] (with-options* db opts))
  Connection
  (-with-connection [db body-fn] (with-connection* db body-fn))
  (-with-options [db opts] (with-options* db opts))
  ConnectableWithOpts
  (-with-connection [db body-fn] (with-connection* db body-fn))
  (-with-options [db opts] (with-options* db opts)))

(extend-protocol proto/Describable
  Connection
  (-connection-metadata [conn] (connection-metadata* conn)))

(comment
  (def cf (create-connection-factory* {:url "r2dbc:h2:mem:///testdb"}))
  (m/? (acquire-connection cf))
  (m/? (with-connection* cf (fn [conn] (m/sp (connection-metadata* conn))))))
