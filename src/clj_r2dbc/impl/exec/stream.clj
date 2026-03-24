;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.exec.stream
  "Streaming row execution for clj-r2dbc.

  Provides:
    stream*  - Missionary flow emitting one value per row as a RowCursor or
               built value (when :builder-fn supplied), or one ArrayList per
               fetch-size batch (when :chunk-size supplied). Renamed from plan*.

  Note: RowCursor is a shared mutable object. The same instance is mutated for
  every row emitted by stream*. Retaining a reference to the cursor beyond the
  current m/?> boundary silently returns data from a later row - no exception
  is thrown. Callers that need to keep row data must either:
    (a) supply :builder-fn (recommended) to materialize an immutable value, or
    (b) call (cursor-row c) and (cursor-cache c) immediately within the same
        reduce step and pass them to impl/sql/row's row->map before the next
        row arrives.

  Backpressure architecture:
    stream* uses r2dbc-row-flow, a custom Missionary discrete flow that bridges
    R2DBC's push-based Publisher<Row> into demand-driven per-row emission.
    Rows are buffered in an ArrayDeque of capacity fetch-size. When the buffer
    empties, Subscription.request(fetch-size) is issued to pull the next batch.

    Backpressure chain:
      consumer m/reduce -> m/?> row-flow -> r2dbc-row-flow.deref()
        -> Subscription.request(fetch-size) -> R2DBC driver -> database cursor

    No m/observe (push-based, no backpressure).
    No m/subscribe (Reactor scalar-fusion reentrancy hazard).
    No eager collection - rows stream one-by-one from database to consumer.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.conn.core :as conn]
   [clj-r2dbc.impl.conn.lifecycle :as lifecycle]
   [clj-r2dbc.impl.conn.publisher :as pub]
   [clj-r2dbc.impl.protocols :as proto]
   [clj-r2dbc.impl.sql.cursor :as cursor]
   [clj-r2dbc.impl.sql.row :as row]
   [clj-r2dbc.row :as pub-row]
   [missionary.core :as m])
  (:import
   (clj_r2dbc.impl.conn.core ConnectableWithOpts)
   (clojure.lang IDeref IFn)
   (io.r2dbc.spi Connection ConnectionFactory Row)
   (java.util ArrayDeque ArrayList Collection)
   (java.util.concurrent CompletableFuture)
   (java.util.concurrent.atomic AtomicBoolean)
   (missionary Cancelled)
   (org.reactivestreams Publisher Subscriber Subscription)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- r2dbc-row-flow
  "Return a Missionary discrete flow that bridges an R2DBC Publisher<Row>
  into demand-driven row emission with fetch-size batching.

  Subscribes asynchronously (via ForkJoinPool.commonPool) to prevent
  notifier from being called during the flow constructor. This is required
  because Ambiguous.suspend assigns c.iterator = flow.invoke(...) only after
  flow.invoke returns. Synchronous publishers (e.g., Reactor FluxFlatMap with
  scalar fusion) would call notifier from within the constructor, causing
  Ambiguous.backtrack to deref c.iterator = NOP -> ClassCastException.

  Issues .request(fetch-size) when the internal buffer empties. Each
  transfer (deref) returns one transformed row. row-xf is applied in
  onNext before buffering to capture immutable values while Row is valid.

  Error draining: if onError arrives while rows are buffered, buffered rows
  are emitted first; the error is thrown after the last buffered row.

  Missionary discrete flow protocol:
    invoke() - cancel: calls Subscription.cancel(); does NOT call terminator.
    deref()  - transfer: returns next value; calls notifier (more items) or
               terminator (last item, error, or cancelled) as a side-effect.

  Lock-ordering: all notifier/terminator/.request calls happen outside
  the locking block to avoid AB-BA deadlock with Reduce.ready().

  Args:
    row-pub    - Publisher<Row> from an R2DBC Statement.execute() result.
    fetch-size - number of rows to request per batch.
    row-xf     - 1-arity fn applied to each Row in onNext before buffering."
  [^Publisher row-pub ^long fetch-size row-xf]
  (fn [notifier terminator]
    (let [state                                                               (Object.)
          ^ArrayDeque buf                                                     (ArrayDeque. (int fetch-size))
          sub-ref                                                             (volatile! nil)
          error-ref                                                           (volatile! nil)
          done-ref                                                            (volatile! false)
          cancel-ref                                                          (volatile! false)
          ^AtomicBoolean term-ref                                             (AtomicBoolean. false)
          outstanding                                                         (long-array 1)
          signal-terminator!
          (fn signal-term []
            (when (.compareAndSet term-ref false true) (terminator)))
          subscriber
          (reify
            Subscriber
            (onSubscribe [_ s]
              (let [cancelled? (locking state
                                 (if @cancel-ref
                                   true
                                   (do (vreset! sub-ref s)
                                       (aset outstanding 0 fetch-size)
                                       false)))]
                (if cancelled?
                  (.cancel ^Subscription s)
                  (pub/request-async! ^Subscription s fetch-size))))
            (onNext [_ row]
              (let [notify? (locking state
                              (if @cancel-ref
                                false
                                (let [was-empty (.isEmpty buf)]
                                  (.addLast buf (row-xf row))
                                  (aset outstanding
                                        0
                                        (unchecked-dec (aget outstanding 0)))
                                  was-empty)))]
                (when notify? (notifier))))
            (onError [_ t]
              (locking state (vreset! error-ref t) (vreset! done-ref true))
              (notifier))
            (onComplete [_]
              (let [empty?
                    (locking state (vreset! done-ref true) (.isEmpty buf))]
                (if empty? (signal-terminator!) (notifier)))))]
      (CompletableFuture/runAsync (fn []
                                    (.subscribe ^Publisher row-pub subscriber)))
      (reify
        IFn
        (invoke [_]
          (let [^Subscription sub (locking state
                                    (vreset! cancel-ref true)
                                    (.clear buf)
                                    @sub-ref)]
            (when sub (.cancel sub))
            (signal-terminator!))
          nil)
        IDeref
        (deref [_]
          (let [action (locking state
                         (cond @cancel-ref [:term nil]
                               (not (.isEmpty buf))
                               (let [v (.pollFirst buf)]
                                 (cond (and (.isEmpty buf) @done-ref)
                                       (if @error-ref [:last-err v] [:last v])
                                       (and (.isEmpty buf)
                                            (zero? (aget outstanding 0)))
                                       [:req v]
                                       (not (.isEmpty buf)) [:more v]
                                       :else [:wait v]))
                               @error-ref [:err @error-ref]
                               @done-ref [:term nil]
                               :else [:wait-empty nil]))]
            (case (nth action 0)
              :term (do (signal-terminator!)
                        (throw (Cancelled. "Row flow terminated.")))
              :err (do (signal-terminator!) (throw ^Throwable (nth action 1)))
              :last (do (signal-terminator!) (nth action 1))
              :last-err (do (notifier) (nth action 1))
              :more (do (notifier) (nth action 1))
              :wait (nth action 1)
              :req (let [v                 (nth action 1)
                         ^Subscription sub (locking state
                                             (aset outstanding 0 fetch-size)
                                             @sub-ref)]
                     (when sub (pub/request-async! sub fetch-size))
                     v)
              :wait-empty
              (throw
               (IllegalStateException.
                "r2dbc-row-flow: deref called with empty buffer and no terminal state")))))))))

(defn- r2dbc-chunk-flow
  "Return a Missionary discrete flow that bridges an R2DBC Publisher<Row>
  into demand-driven batch (chunk) emission.

  Structurally identical to r2dbc-row-flow with one key difference in deref():
  instead of polling one row, the entire buffered batch is drained into a
  java.util.ArrayList and returned as one chunk value.

  This reduces Missionary step overhead from O(rows) to O(batches). At fetch-size
  32 with identity row-xf, Missionary deref() calls drop from 10,000 to 313 (32x
  step reduction). Wall-clock ratio is bounded by S_sched/T_lock ≈ 7.2x; T12-001
  asserts >=6.0 at fetch-size 32 (identity row-xf isolates scheduling overhead).

  Each emitted ArrayList is non-empty (empty batches are never emitted).
  row-xf is applied in onNext before buffering; values in the chunk are
  fully captured while Row is valid - safe to retain across deref boundaries.

  The :more branch from r2dbc-row-flow is eliminated: since the entire buffer
  is drained per deref(), the buffer is always empty after each transfer."
  [^Publisher row-pub ^long fetch-size row-xf]
  (fn [notifier terminator]
    (let [state                                                               (Object.)
          ^ArrayDeque buf                                                     (ArrayDeque. (int fetch-size))
          sub-ref                                                             (volatile! nil)
          error-ref                                                           (volatile! nil)
          done-ref                                                            (volatile! false)
          cancel-ref                                                          (volatile! false)
          ^AtomicBoolean term-ref                                             (AtomicBoolean. false)
          outstanding                                                         (long-array 1)
          signal-terminator!
          (fn signal-term []
            (when (.compareAndSet term-ref false true) (terminator)))
          subscriber
          (reify
            Subscriber
            (onSubscribe [_ s]
              (let [cancelled? (locking state
                                 (if @cancel-ref
                                   true
                                   (do (vreset! sub-ref s)
                                       (aset outstanding 0 fetch-size)
                                       false)))]
                (if cancelled?
                  (.cancel ^Subscription s)
                  (pub/request-async! ^Subscription s fetch-size))))
            (onNext [_ row]
              (let [notify? (locking state
                              (if @cancel-ref
                                false
                                (let [new-outstanding (unchecked-dec
                                                       (aget outstanding 0))]
                                  (.addLast buf (row-xf row))
                                  (aset outstanding 0 new-outstanding)
                                  (zero? new-outstanding))))]
                (when notify? (notifier))))
            (onError [_ t]
              (locking state (vreset! error-ref t) (vreset! done-ref true))
              (notifier))
            (onComplete [_]
              (let [empty?
                    (locking state (vreset! done-ref true) (.isEmpty buf))]
                (if empty? (signal-terminator!) (notifier)))))]
      (CompletableFuture/runAsync (fn []
                                    (.subscribe ^Publisher row-pub subscriber)))
      (reify
        IFn
        (invoke [_]
          (let [^Subscription sub (locking state
                                    (vreset! cancel-ref true)
                                    (.clear buf)
                                    @sub-ref)]
            (when sub (.cancel sub))
            (signal-terminator!))
          nil)
        IDeref
        (deref [_]
          (let [action (locking state
                         (cond @cancel-ref [:term nil]
                               (not (.isEmpty buf))
                               (let [^ArrayList al (ArrayList. ^Collection
                                                    buf)]
                                 (.clear buf)
                                 (if @done-ref
                                   (if @error-ref [:last-err al] [:last al])
                                   [:req al]))
                               @error-ref [:err @error-ref]
                               @done-ref [:term nil]
                               :else [:wait-empty nil]))]
            (case (nth action 0)
              :term (do (signal-terminator!)
                        (throw (Cancelled. "Row chunk flow terminated.")))
              :err (do (signal-terminator!) (throw ^Throwable (nth action 1)))
              :last (do (signal-terminator!) (nth action 1))
              :last-err (do (notifier) (nth action 1))
              :req (let [v                 (nth action 1)
                         ^Subscription sub (locking state
                                             (aset outstanding 0 fetch-size)
                                             @sub-ref)]
                     (when sub (pub/request-async! sub fetch-size))
                     v)
              :wait-empty
              (throw
               (IllegalStateException.
                "r2dbc-chunk-flow: deref called with empty buffer and no terminal state")))))))))

(defn stream*
  "Return a Missionary flow that emits one value per row returned by sql.
  Renamed from plan*; semantics are identical.

  Without :builder-fn - emits a shared mutable RowCursor (flyweight). The same
  instance is updated in-place for each row. Do NOT retain references across
  m/?> boundaries; silent data corruption otherwise. Materialize data
  immediately using cursor-row and cursor-cache within the same reduce step.

  With :builder-fn (fn [Row RowMetadata] -> value) - applied per row inside the
  fetch loop, emitting an immutable value safe for retention. Use
  clj-r2dbc.row/kebab-maps for standard kebab-case row maps.

  Args:
    db     - ConnectionFactory, Connection, or ConnectableWithOpts.
    sql    - SQL string.
    params - sequential bind parameters.
    opts   - options map:
               :builder-fn - 2-arity (Row, RowMetadata) -> value; skips RowCursor entirely.
               :qualifier  - column keyword mode used when no :builder-fn is supplied.
               :fetch-size - rows per demand-driven batch (default 128).
               :returning  - calls Statement.returnGeneratedValues.

  Connection lifecycle:
    When db is a ConnectionFactory, stream* acquires a connection at flow start
    and closes it via try/finally on completion, error, or cancellation.
    When db is already a Connection, lifecycle is owned by the caller;
    Connection.close() is NOT called.

  Backpressure:
    Both paths use r2dbc-row-flow, a custom Missionary discrete flow that
    bridges Publisher<Row> with demand-driven fetch-size batching. Each
    deref() returns one transformed row. When the internal buffer empties,
    Subscription.request(fetch-size) pulls the next batch from the driver.
    No eager collection - true end-to-end streaming from database to consumer.

  Zero-copy:
    Row data is captured in onNext while Row is valid. Values stored in the
    internal buffer are fully captured before the driver recycles the Row
    object - safe to hold across deref boundaries."
  [db sql params opts]
  (let [[db opts]  (conn/resolve-connectable db opts)
        chunk-size (:chunk-size opts)
        builder-fn (:builder-fn opts)]
    (cond chunk-size
          (do (when-not builder-fn
                (throw (ex-info "stream* with :chunk-size requires :builder-fn"
                                {:clj-r2dbc/error   :clj-r2dbc/missing-key
                                 :clj-r2dbc/context :stream
                                 :key               :builder-fn})))
              (lifecycle/streaming-plan-flow
               db
               sql
               params
               (assoc opts :fetch-size (long chunk-size))
               (fn chunk-builder-xf [^Row row]
                 (builder-fn row (.getMetadata row)))
               r2dbc-chunk-flow))
          builder-fn (lifecycle/streaming-plan-flow
                      db
                      sql
                      params
                      opts
                      (fn row-builder-xf [^Row row]
                        (builder-fn row (.getMetadata row)))
                      r2dbc-row-flow)
          :else (let [crs (cursor/->RowCursor (ArrayDeque. 2) nil 0)]
                  (m/eduction
                   (map (fn cursor-step [^Row row]
                          (when (nil? (cursor/cursor-cache crs))
                            (let [q   (:qualifier opts :unqualified-kebab)
                                  rmd (.getMetadata ^Row row)
                                  qfn (fn qualify-fn [^String col _]
                                        (row/qualify-column col nil q))]
                              (cursor/-set-cache!
                               crs
                               (row/build-metadata-cache rmd qfn))))
                          (cursor/-set-row! crs row)
                          crs))
                   (lifecycle/streaming-plan-flow db
                                                  sql
                                                  params
                                                  opts
                                                  identity
                                                  r2dbc-row-flow))))))

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
  (m/? (m/reduce (fn [acc ^java.util.ArrayList chunk] (+ acc (.size chunk)))
                 0
                 (stream-dispatch* factory
                                   "SELECT id FROM t"
                                   {:builder-fn clj-r2dbc.row/kebab-maps
                                    :chunk-size 64}))))
