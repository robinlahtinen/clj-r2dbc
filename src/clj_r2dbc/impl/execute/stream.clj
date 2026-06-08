;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.execute.stream
  "Streaming row execution for clj-r2dbc.

  Provides:
    stream*  - Missionary flow emitting one immutable value per row (built by
               :builder-fn, defaulting to clj-r2dbc.row/kebab-maps), or one
               vector per :chunk-size batch (when :chunk-size supplied).

  Every emission is an immutable value produced by the builder inside
  result-row-pub's .map while the Row's ByteBuf is live, so it is safe to read
  within the reduce step or retain and read later. There is one row
  representation knob - :builder - and no shared mutable state.

  Backpressure architecture:
    stream* delegates to lifecycle/streaming-plan-flow, which flattens R2DBC's
    push-based Publisher<Result> into a Publisher of rows below the bridge (Reactor
    concatMap) and exposes it as a single demand-driven Missionary discrete flow
    via m/subscribe. Reactive demand is one row at a time; database round-trip
    batching is controlled independently by Statement.fetchSize (applied in
    stmt/prepare!).

    Backpressure chain:
      consumer m/reduce -> m/subscribe(rows Publisher) -> concatMap
        -> Subscription.request(1) -> R2DBC driver -> database cursor

    Chunking (:chunk-size) batches rows below the bridge (Reactor buffer), so the
    consumer performs O(batches) transfers. No eager collection - rows stream
    from database to consumer.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.connection :as conn]
   [clj-r2dbc.impl.connection.lifecycle :as lifecycle]
   [clj-r2dbc.impl.protocols :as proto]
   [clj-r2dbc.row :as pub-row]
   [missionary.core :as m])
  (:import
   (clj_r2dbc.impl.connection ConnectableWithOpts)
   (io.r2dbc.spi Connection ConnectionFactory Row)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn stream*
  "Return a Missionary flow that emits one immutable value per row returned by sql.

  The row value is produced by :builder-fn (fn [Row RowMetadata] -> value),
  applied per row inside result-row-pub's BiFunction while the Row's ByteBuf is
  live. When :builder-fn is absent it defaults to clj-r2dbc.row/kebab-maps, so a
  no-builder call emits standard kebab-case row maps. Every emission is an
  immutable value, safe to read within the reduce step or retain and read later.

  With :chunk-size - requires :builder-fn; emits one vector of up to chunk-size
  built values per emission (the final vector may be shorter). Batching is a
  partition-all transducer over the per-row flow, so the consumer performs
  O(batches) transfers.

  Args:
    db     - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql    - SQL string.
    params - sequential bind parameters.
    opts   - options map:
               :builder-fn - 2-arity (Row, RowMetadata) -> value; defaults to
                             clj-r2dbc.row/kebab-maps when absent.
               :chunk-size - rows per emitted vector; requires :builder-fn.
               :fetch-size - rows per database round-trip batch (default 128),
                             applied to Statement.fetchSize. Independent of
                             reactive demand.
               :returning  - calls Statement.returnGeneratedValues.

  Connection lifecycle:
    When db is a ConnectionFactory, stream* acquires a connection on subscription
    and closes it on completion, error, or cancellation (via Reactor usingWhen in
    streaming-plan-flow). When db is already a Connection, lifecycle is owned by
    the caller; Connection.close() is NOT called.

  Backpressure:
    Row delivery is demand-driven through Missionary's m/subscribe bridge (one
    row per reactive request). Database fetch batching is governed by
    Statement.fetchSize, independently of reactive demand. No eager collection -
    true end-to-end streaming from database to consumer.

  Zero-copy:
    The builder materializes its value inside result-row-pub's BiFunction.apply()
    while the underlying ByteBuf is guaranteed live, so the emitted immutable
    value owns its data and is safe to hold across deref boundaries."
  [db sql params opts]
  (let [[db opts]  (conn/resolve-connectable db opts)
        chunk-size (:chunk-size opts)
        builder-fn (:builder-fn opts)]
    (cond
      chunk-size
      (do (when-not builder-fn
            (throw (ex-info "stream* with :chunk-size requires :builder-fn"
                            {:clj-r2dbc/error   :clj-r2dbc/missing-key
                             :clj-r2dbc/context :stream
                             :key               :builder-fn})))
          ;; :chunk-size in opts selects chunked-pub (Reactor buffer) below the
          ;; bridge in streaming-plan-flow; statement fetch batching is aligned.
          (lifecycle/streaming-plan-flow
           db
           sql
           params
           (assoc opts :fetch-size (long chunk-size))
           (fn chunk-builder-xf [^Row row]
             (builder-fn row (.getMetadata row)))))

      :else
      ;; One row representation: the builder (defaulting to kebab-maps) is applied
      ;; per row inside result-row-pub's .map while the ByteBuf is live, emitting a
      ;; distinct immutable value. Because every emission is its own value, the
      ;; m/subscribe bridge requesting row N+1 before the consumer reads row N
      ;; (Sub.transfer; see lifecycle) can never corrupt an already-emitted value.
      (let [bf (or builder-fn pub-row/kebab-maps)]
        (lifecycle/streaming-plan-flow
         db
         sql
         params
         opts
         (fn row-builder-xf [^Row row]
           (bf row (.getMetadata row))))))))

(extend-protocol proto/Streamable
  ConnectionFactory
  (-stream [db sql params opts] (stream* db sql params opts))
  Connection
  (-stream [db sql params opts] (stream* db sql params opts))
  ConnectableWithOpts
  (-stream [db sql params opts] (stream* db sql params opts)))

(defn- with-default-builder
  "Ensure opts carries a :builder-fn, defaulting to clj-r2dbc.row/kebab-maps."
  [opts]
  (assoc opts :builder-fn (:builder-fn opts pub-row/kebab-maps)))

(defn stream-dispatch*
  "Dispatch stream execution from validated opts.

  Installs the default builder (clj-r2dbc.row/kebab-maps) when no :builder-fn is
  supplied, then delegates to stream*. There is one row-representation knob -
  :builder - so both the per-row and :chunk-size paths flow through the same
  default-builder resolution.

  Args:
    db   - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql  - non-blank SQL string.
    opts - validated options map from stream-opts.

  Returns a Missionary discrete flow emitting one value per row."
  [db sql opts]
  (let [params (:params opts [])
        opts'  (dissoc opts :params)]
    (proto/-stream db sql params (with-default-builder opts'))))

(comment
  (def factory (conn/create-connection-factory* {:url "r2dbc:h2:mem:///db"}))
  ;; Default builder (kebab-maps) - no :builder-fn needed.
  (m/? (m/reduce conj [] (stream-dispatch* factory "SELECT id FROM t" {})))
  ;; Explicit builder.
  (m/? (m/reduce conj
                 []
                 (stream-dispatch* factory
                                   "SELECT id FROM t"
                                   {:builder-fn clj-r2dbc.row/vectors})))
  (m/? (m/reduce (fn [acc chunk] (+ acc (count chunk)))
                 0
                 (stream-dispatch* factory
                                   "SELECT id FROM t"
                                   {:builder-fn clj-r2dbc.row/kebab-maps
                                    :chunk-size 64}))))
