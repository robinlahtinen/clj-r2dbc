;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.execute.stream
  "Streaming row execution for clj-r2dbc.

  Provides:
    stream*  - Missionary flow emitting one value per row as a RowCursor or
               built value (when :builder-fn supplied), or one vector per
               :chunk-size batch (when :chunk-size supplied). Renamed from plan*.

  Note: in flyweight mode (no :builder-fn) stream* emits a distinct immutable
  RowCursor per row, fully materialised inside result-row-pub's .map while the
  Row's ByteBuf is live. Each emission is its own value, so it is safe to read
  within the reduce step or retain and read later. Flyweight differs from the
  :builder-fn path only in the SHAPE of the emitted value (a RowCursor exposing
  cursor-row/cursor-cache vs. an immutable value). Read cursor data with
  impl/sql/row's row->map applied to (cursor-row c) / (cursor-cache c).

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
    consumer performs O(batches) transfers; flyweight mode maps each row onto a
    fresh immutable RowCursor. No eager collection - rows stream from database to
    consumer.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.connection :as conn]
   [clj-r2dbc.impl.connection.lifecycle :as lifecycle]
   [clj-r2dbc.impl.protocols :as proto]
   [clj-r2dbc.impl.sql.cursor :as cursor]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.row :as pub-row]
   [missionary.core :as m])
  (:import
   (clj_r2dbc.impl.connection ConnectableWithOpts)
   (clj_r2dbc.impl.sql.row RowMetadataCache)
   (io.r2dbc.spi Connection ConnectionFactory Row)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn stream*
  "Return a Missionary flow that emits one value per row returned by sql.
  Renamed from plan*; semantics are identical.

  Without :builder-fn - emits a distinct immutable RowCursor (flyweight) per row,
  each materialised inside result-row-pub's .map while the Row's ByteBuf is live.
  Safe to read within the reduce step or retain and read later. Read cursor data
  with cursor-row and cursor-cache (pass to impl/sql/row's row->map).

  With :builder-fn (fn [Row RowMetadata] -> value) - applied per row inside
  result-row-pub's BiFunction, emitting an immutable value safe for retention.
  Use clj-r2dbc.row/kebab-maps for standard kebab-case row maps.

  With :chunk-size - requires :builder-fn; emits one vector of up to chunk-size
  built values per emission (the final vector may be shorter). Batching is a
  partition-all transducer over the per-row flow, so the consumer performs
  O(batches) transfers.

  Args:
    db     - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql    - SQL string.
    params - sequential bind parameters.
    opts   - options map:
               :builder-fn - 2-arity (Row, RowMetadata) -> value; skips RowCursor entirely.
               :chunk-size - rows per emitted vector; requires :builder-fn.
               :qualifier  - column keyword mode used when no :builder-fn is supplied.
               :fetch-size - rows per database round-trip batch (default 128),
                             applied to Statement.fetchSize. Flyweight mode no
                             longer clamps this: values are materialised per row
                             in .map (ByteBuf live), exactly like :builder-fn, so
                             driver Row recycling after onNext is harmless.
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
    Both modes materialize values inside result-row-pub's BiFunction.apply()
    while the underlying ByteBuf is guaranteed live. With :builder-fn the builder
    produces an immutable value; in flyweight mode (no :builder-fn) the row's
    columns are read into a fresh Object[] wrapped in a new RowCursor. Either way
    the emitted value owns its data and is safe to hold across deref boundaries."
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

      builder-fn
      (lifecycle/streaming-plan-flow
       db
       sql
       params
       opts
       (fn row-builder-xf [^Row row]
         (builder-fn row (.getMetadata row))))

      :else
      (let [meta (volatile! nil)]
        ;; cursor-step is the row-xf, applied inside result-row-pub's .map while
        ;; the Row's ByteBuf is live: it materialises the row's column values into
        ;; a FRESH Object[] and emits a new immutable RowCursor per row. The
        ;; per-result metadata (cache, name-index, RowMetadata) is built once and
        ;; shared (immutable). Because every emission is its own value, the
        ;; m/subscribe bridge requesting row N+1 before the consumer reads row N
        ;; (Sub.transfer; see lifecycle) cannot corrupt an already-emitted cursor -
        ;; the same per-row-materialisation property that makes :builder race-free.
        (lifecycle/streaming-plan-flow
         db
         sql
         params
         opts
         (fn cursor-step [^Row row]
           (let [m                          (or @meta
                                                (let [q   (:qualifier opts :unqualified-kebab)
                                                      rmd (.getMetadata row)
                                                      qfn (fn qualify-fn [^String col _]
                                                            (row/qualify-column col nil q))]
                                                  (vreset! meta
                                                           {:cache     (row/build-metadata-cache rmd qfn)
                                                            :name->idx (row/build-name-index rmd)
                                                            :rmd       rmd})))
                 ^RowMetadataCache cache    (:cache m)
                 n                          (.col-count cache)
                 ^"[Ljava.lang.Object;" tys (.java-types cache)
                 vs                         (object-array n)]
             (dotimes [i n]
               (aset vs i (.get row (int i) ^Class (aget tys i))))
             (cursor/->cursor vs cache (:name->idx m) (:rmd m)))))))))

(extend-protocol proto/Streamable
  ConnectionFactory
  (-stream [db sql params opts] (stream* db sql params opts))
  Connection
  (-stream [db sql params opts] (stream* db sql params opts))
  ConnectableWithOpts
  (-stream [db sql params opts] (stream* db sql params opts)))

(defn- immutable-stream-opts
  [opts]
  (assoc opts :builder-fn (:builder-fn opts pub-row/kebab-maps)))

(defn stream-dispatch*
  "Dispatch stream execution from validated opts.

  Resolves the builder mode (:builder-fn present vs. absent) and chunking
  (:chunk-size present vs. absent), then delegates to stream*.

  Args:
    db   - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql  - non-blank SQL string.
    opts - validated options map from stream-opts.

  Returns a Missionary discrete flow emitting one value per row."
  [db sql opts]
  (let [params (:params opts [])
        opts'  (dissoc opts :params)]
    (cond (:chunk-size opts') (proto/-stream db
                                             sql
                                             params
                                             (-> opts'
                                                 immutable-stream-opts
                                                 (dissoc :stream-mode)))
          (= :flyweight (:stream-mode opts'))
          (proto/-stream db sql params (dissoc opts' :stream-mode))
          :else (proto/-stream db
                               sql
                               params
                               (-> opts'
                                   immutable-stream-opts
                                   (dissoc :stream-mode))))))

(comment
  (def factory (conn/create-connection-factory* {:url "r2dbc:h2:mem:///db"}))
  (m/? (m/reduce conj
                 []
                 (stream-dispatch* factory
                                   "SELECT id FROM t"
                                   {:builder-fn clj-r2dbc.row/kebab-maps})))
  (m/?
   (m/reduce
    (fn [acc crs]
      (conj acc
            (row/row->map (cursor/cursor-row crs) (cursor/cursor-cache crs))))
    []
    (stream-dispatch* factory "SELECT id FROM t" {:stream-mode :flyweight})))
  (m/? (m/reduce (fn [acc chunk] (+ acc (count chunk)))
                 0
                 (stream-dispatch* factory
                                   "SELECT id FROM t"
                                   {:builder-fn clj-r2dbc.row/kebab-maps
                                    :chunk-size 64}))))
