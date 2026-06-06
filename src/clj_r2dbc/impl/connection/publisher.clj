;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.connection.publisher
  "Reactive Streams publisher bridges for clj-r2dbc.

  org.reactivestreams interop for clj-r2dbc. Row delivery to Missionary flows is
  handled by Missionary's reactive-streams adapter (m/subscribe, see
  connection/lifecycle); this namespace provides the Publisher constructors and
  the blocking/async bridges used around it.

  Provides:
    result-row-pub      - Publisher<Row> for all rows in one R2DBC Result.
    async-subscribe-pub - Publisher wrapper deferring .subscribe to m/blk.
    buffer-pub          - Publisher wrapper batching items into size-N vectors.
    add-demand          - saturating addition for demand accounting.
    first-pub-blocking  - blocking first-value extraction from a Publisher.
    await-void-pub!     - blocking drain-and-discard for void Publishers.
    await-future        - blocking CompletableFuture unwrap.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Result)
   (java.util ArrayList)
   (java.util.concurrent CompletableFuture CompletionException ExecutionException)
   (java.util.concurrent.atomic AtomicBoolean)
   (java.util.function BiConsumer BiFunction)
   (org.reactivestreams Publisher Subscriber Subscription)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn add-demand
  "Add delta to current demand with saturation at Long/MAX_VALUE.

  Used by buffer-pub to accumulate batch demand without overflow.

  Args:
    current - long, existing demand.
    delta   - long, demand increment.

  Returns a long in [current, Long/MAX_VALUE]."
  ^long [^long current ^long delta]
  (let [sum (+ current delta)] (if (neg? sum) Long/MAX_VALUE sum)))

(defn await-future
  "Block the calling thread until fut completes and returns its result.

  Used inside Reactive Streams callbacks where parking is not available.
  Propagates CancellationException and ExecutionException unwrapped.

  Args:
    fut - CompletableFuture<T>.

  Returns the future's result value."
  [^CompletableFuture fut]
  (try (.get fut) (catch ExecutionException ee (throw (or (.getCause ee) ee)))))

(defn result-row-pub
  "Return a Publisher<T> for all rows in result, each transformed by row-xf.

  row-xf is applied inside BiFunction.apply() - before Result.map's finally
  block calls message.release() on the underlying ByteBuf. This guarantees the
  Row's backing ByteBuf is live when row-xf (e.g., builder-fn) reads column
  values via Row.get(), regardless of whether the downstream Flux buffers the
  row internally (demand == 0) before delivering it.

  Applying row-xf here rather than in the downstream subscriber's onNext is the
  only safe point: once sink.next() queues a row into Reactor's internal buffer
  (because demand is zero at that instant) and the enclosing handler returns,
  message.release() fires and the ByteBuf is returned to the Netty pool. Any
  subsequent Row.get() call on the buffered row then reads freed memory,
  causing IllegalReferenceCountException that escapes the subscriber's onNext
  without calling notifier - leaving the Missionary reduce waiting forever.

  Args:
    result - R2DBC Result to map over.
    row-xf - 1-arity fn [Row] -> T applied to each row inside BiFunction.apply()."
  ^Publisher [^Result result row-xf]
  (^[BiFunction] Result/.map result (fn [row _metadata] (row-xf row))))

(defn async-subscribe-pub
  "Wrap pub so that .subscribe is dispatched on the Missionary blocking
  executor (m/blk) rather than the caller's thread.

  This interposes an async boundary on subscription so a synchronous publisher
  (e.g. Reactor scalar fusion, or H2 in-memory emitting during subscribe)
  cannot invoke a downstream Subscriber callback re-entrantly while a Missionary
  flow is still being constructed. Keeps driver-side subscribe work off the
  ForkJoinPool consumer threads.

  If pub's .subscribe throws synchronously on the m/blk thread, the error is
  forwarded to the Subscriber's onError; otherwise it would be swallowed by the
  discarded CompletableFuture, leaving a downstream m/subscribe parked forever.

  Args:
    pub - Publisher to subscribe to asynchronously.

  Returns a Publisher that defers pub's subscription to m/blk."
  ^Publisher [^Publisher pub]
  (reify
    Publisher
    (subscribe [_ sub]
      (.whenComplete
       (CompletableFuture/runAsync
        ^Runnable (fn [] (.subscribe pub ^Subscriber sub))
        m/blk)
       ^BiConsumer
       (reify BiConsumer
         (accept [_ _ ex]
           (when ex
             (.onError ^Subscriber sub
                       (if (instance? CompletionException ex)
                         (or (.getCause ^Throwable ex) ex)
                         ex))))))
      nil)))

(defn buffer-pub
  "Wrap upstream so emitted items are batched into vectors of up to chunk-size.

  Returns a Publisher<clojure.lang.IPersistentVector> that, on each downstream
  request, pulls chunk-size items from upstream and emits them as one vector;
  the final vector may be shorter. This moves batching to the Reactive Streams
  layer so a downstream m/subscribe performs O(batches) transfers rather than
  O(rows), restoring the per-batch scheduling win without a hand-rolled flow.

  Demand-driven and RS-compliant for a conformant upstream (delivers at most the
  requested count): each downstream request(d) collects d batches, requesting
  chunk-size upstream per batch. onComplete flushes any partial final batch.
  Errors and cancellation propagate to/from upstream. Terminal signals fire once.

  Args:
    upstream   - Publisher<T> to batch.
    chunk-size - positive number of items per emitted vector."
  ^Publisher [^Publisher upstream ^long chunk-size]
  (reify
    Publisher
    (subscribe [_ downstream]
      (let [state                                                                   (Object.)
            ^ArrayList buf                                                          (ArrayList. (int chunk-size))
            up-sub                                                                  (volatile! nil)
            demand                                                                  (long-array 1)
            collecting                                                              (volatile! false)
            done                                                                    (volatile! false)
            cancelled                                                               (volatile! false)
            ^AtomicBoolean term                                                     (AtomicBoolean. false)
            start-collect!
            (fn []
              (let [^Subscription s
                    (locking state
                      (when (and (not @collecting) (not @done) (not @cancelled)
                                 (pos? (aget demand 0)) @up-sub)
                        (vreset! collecting true)
                        @up-sub))]
                (when s (.request s chunk-size))))
            up-subscriber
            (reify
              Subscriber
              (onSubscribe [_ s]
                (let [cancel? (locking state
                                (if @cancelled
                                  true
                                  (do (vreset! up-sub s) false)))]
                  (if cancel? (.cancel ^Subscription s) (start-collect!))))
              (onNext [_ x]
                (let [batch (locking state
                              (when-not (or @cancelled @done)
                                (.add buf x)
                                (when (>= (.size buf) chunk-size)
                                  (let [v (vec buf)]
                                    (.clear buf)
                                    (aset demand 0 (unchecked-dec (aget demand 0)))
                                    (vreset! collecting false)
                                    v))))]
                  (when batch
                    (.onNext ^Subscriber downstream batch)
                    (start-collect!))))
              (onError [_ t]
                (locking state (vreset! done true))
                (when (.compareAndSet term false true)
                  (.onError ^Subscriber downstream t)))
              (onComplete [_]
                (let [tail (locking state
                             (vreset! done true)
                             (when (pos? (.size buf))
                               (let [v (vec buf)] (.clear buf) v)))]
                  (when (.compareAndSet term false true)
                    (when tail (.onNext ^Subscriber downstream tail))
                    (.onComplete ^Subscriber downstream)))))]
        (.onSubscribe
         ^Subscriber downstream
         (reify
           Subscription
           (request [_ d]
             (when (pos? (long d))
               (locking state
                 (aset demand 0 (add-demand (aget demand 0) (long d))))
               (start-collect!)))
           (cancel [_]
             (let [^Subscription s (locking state
                                     (vreset! cancelled true)
                                     @up-sub)]
               (when s (.cancel s))))))
        (.subscribe upstream up-subscriber)))))

(defn first-pub-blocking
  "Subscribe to pub, request one item, and return it synchronously.

  Blocks the calling thread. Production code paths prefer
  conn/acquire-connection (a Missionary task that parks on m/blk rather
  than the FJP, avoiding pool starvation under concurrent stream starts).
  Kept here for the REPL examples and any future callers that already
  hold a non-FJP thread.

  Args:
    pub     - Publisher<T> to subscribe to.
    sub-ref - volatile that receives the Subscription for cancellation."
  [^Publisher pub sub-ref]
  (let [fut (CompletableFuture.)]
    (.subscribe
     pub
     (reify
       Subscriber
       (onSubscribe [_ s] (vreset! sub-ref s) (.request ^Subscription s 1))
       (onNext [_ v] (.complete fut v))
       (onError [_ t] (.completeExceptionally fut t))
       (onComplete [_] (.complete fut nil))))
    (await-future fut)))

(defn await-void-pub!
  "Subscribe to a Publisher<Void> and block until it completes.

  Used for fire-and-forget publishers such as Statement.execute() void
  results. Production code paths prefer util/void->task (returns a
  Missionary task; awaited with m/? inside m/sp or m/ap, so the park
  happens via the Missionary scheduler rather than the calling thread).
  Kept here for REPL examples.

  Args:
    pub - Publisher<Void> to drain."
  [^Publisher pub]
  (let [fut (CompletableFuture.)]
    (.subscribe pub
                (reify
                  Subscriber
                  (onSubscribe [_ s] (.request ^Subscription s Long/MAX_VALUE))
                  (onNext [_ _])
                  (onError [_ t] (.completeExceptionally fut t))
                  (onComplete [_] (.complete fut nil))))
    (await-future fut)
    nil))

(comment
  (let [f (java.util.concurrent.CompletableFuture.)]
    (.complete f :ok)
    (await-future f))
  (try (let [f (java.util.concurrent.CompletableFuture.)]
         (.completeExceptionally f (ex-info "boom" {:x 1}))
         (await-future f))
       (catch Exception e {:message (ex-message e), :data (ex-data e)}))
  (await-void-pub! (reify
                     org.reactivestreams.Publisher
                     (subscribe [_ s]
                       (.onSubscribe s
                                     (reify
                                       org.reactivestreams.Subscription
                                       (request [_ _])
                                       (cancel [_])))
                       (.onComplete s))))
  (try (await-void-pub! (reify
                          org.reactivestreams.Publisher
                          (subscribe [_ s]
                            (.onSubscribe s
                                          (reify
                                            org.reactivestreams.Subscription
                                            (request [_ _])
                                            (cancel [_])))
                            (.onError s (ex-info "pub error" {})))))
       (catch Exception e (ex-message e)))
  (first-pub-blocking (reify
                        org.reactivestreams.Publisher
                        (subscribe [_ s]
                          (.onSubscribe s
                                        (reify
                                          org.reactivestreams.Subscription
                                          (request [_ _]
                                            (.onNext s :first)
                                            (.onNext s :second)
                                            (.onComplete s))
                                          (cancel [_])))))
                      (volatile! nil))
  (first-pub-blocking (reify
                        org.reactivestreams.Publisher
                        (subscribe [_ s]
                          (.onSubscribe s
                                        (reify
                                          org.reactivestreams.Subscription
                                          (request [_ _] (.onComplete s))
                                          (cancel [_])))))
                      (volatile! nil)))
