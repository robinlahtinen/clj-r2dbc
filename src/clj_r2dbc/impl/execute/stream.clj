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
   [clj-r2dbc.impl.datafy :as datafy-impl]
   [clj-r2dbc.impl.protocols :as proto]
   [clj-r2dbc.impl.sql.reduce :as reduce]
   [clj-r2dbc.row :as pub-row]
   [missionary.core :as m])
  (:import
   (clj_r2dbc.impl.connection ConnectableWithOpts)
   (io.r2dbc.spi Connection ConnectionFactory Row)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn stream*
  "Return a Missionary flow that emits one immutable value per row returned by sql.

  The row value is produced by a row builder, applied per row inside
  result-row-pub's BiFunction while the Row's ByteBuf is live. The builder is
  selected from :builder-fn (if supplied), or from :qualifier (if supplied, via
  reduce/make-row-fn). When neither is supplied, defaults to
  clj-r2dbc.row/kebab-maps (unqualified kebab-case maps). Every emission is an
  immutable value, safe to read within the reduce step or retain and read later.

  With :datafy true, each row has Datafiable metadata attached for REPL navigation.

  With :chunk-size - emits one vector of up to chunk-size built values per
  emission (the final vector may be shorter). :chunk-size only changes the
  emission unit, not the row representation: it uses the same builder as the
  per-row path. Batching happens below the bridge (Reactor buffer), so the
  consumer performs O(batches) transfers.

  Args:
    db     - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql    - SQL string.
    params - sequential bind parameters.
    opts   - options map:
               :builder-fn - 2-arity (Row, RowMetadata) -> value; takes
                             priority over :qualifier if both supplied.
               :qualifier  - mode keyword (:unqualified, :unqualified-kebab,
                             :qualified-kebab); passed to reduce/make-row-fn;
                             ignored if :builder-fn supplied.
               :datafy     - boolean; when true, attaches Datafiable metadata
                             for REPL navigation (default false).
               :chunk-size - rows per emitted vector; uses the same builder as
                             the per-row path.
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
        base-fn    (reduce/make-row-fn opts)
        ;; make-row-fn returns a 1-arity (Row -> value) that honors both
        ;; :builder-fn (if supplied) and :qualifier (if supplied). If neither are
        ;; present, it defaults to :unqualified-kebab (same as pub-row/kebab-maps).
        ;; The base-fn is applied per row inside result-row-pub's .map while the
        ;; ByteBuf is live. If :datafy is true, wrap the result with
        ;; attach-datafiable-meta for REPL navigation support. :chunk-size and
        ;; :builder/:qualifier are orthogonal: chunking only changes the emission
        ;; unit (a Reactor buffer below the bridge groups built values into
        ;; vectors), not the row representation.
        row-xf     (if (:datafy opts)
                     (fn row-builder-xf [^Row row]
                       (datafy-impl/attach-datafiable-meta (base-fn row) db opts))
                     base-fn)]
    (if chunk-size
      (lifecycle/streaming-plan-flow
       db sql params (assoc opts :fetch-size (long chunk-size)) row-xf)
      (lifecycle/streaming-plan-flow
       db sql params opts row-xf))))

(extend-protocol proto/Streamable
  ConnectionFactory
  (-stream [db sql params opts] (stream* db sql params opts))
  Connection
  (-stream [db sql params opts] (stream* db sql params opts))
  ConnectableWithOpts
  (-stream [db sql params opts] (stream* db sql params opts)))

(defn stream-dispatch*
  "Dispatch stream execution from validated opts.

  Delegates to stream*, which honors :builder-fn and :qualifier for row
  representation, :datafy for Datafiable metadata attachment, and :chunk-size
  for batching. Default row representation (when neither :builder-fn nor
  :qualifier supplied) is unqualified kebab-case maps (same as
  clj-r2dbc.row/kebab-maps).

  Args:
    db   - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql  - non-blank SQL string.
    opts - validated options map from stream-opts.

  Returns a Missionary discrete flow emitting one value per row."
  [db sql opts]
  (let [params (:params opts [])
        opts'  (dissoc opts :params)]
    (proto/-stream db sql params opts')))

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
