;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.connection.publisher
  "Reactive Streams publisher bridges for clj-r2dbc.

  Pure org.reactivestreams interop - zero Missionary dependency in this namespace.

  Three utility categories:
    Fetch-size resolution: resolve-fetch-size extracts a safe positive long
    from caller opts.
    Demand arithmetic: add-demand performs saturating addition to accumulate
    backpressure demand without overflow.
    Future bridging: await-future, first-pub-blocking, and await-void-pub!
    block the calling thread to synchronize with Reactive Streams callbacks
    where Missionary parking is unavailable.

  Provides:
    result-row-pub     - Publisher<Row> for all rows in one R2DBC Result.
    result-rows-pub    - Publisher<Row> streaming rows from multiple Results sequentially.
    first-pub-blocking - blocking first-value extraction from a Publisher.
    await-void-pub!    - blocking drain-and-discard for void Publishers.
    await-future       - blocking CompletableFuture unwrap.
    request-async!     - async Subscription.request call via ForkJoinPool.
    resolve-fetch-size - safe fetch-size extraction from opts.
    add-demand         - saturating addition for demand accounting.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.debug-log :as dbg])
  (:import
   (io.r2dbc.spi Result)
   (java.util.concurrent CompletableFuture ExecutionException)
   (java.util.concurrent.atomic AtomicBoolean)
   (java.util.function BiFunction)
   (org.reactivestreams Publisher Subscriber Subscription)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn resolve-fetch-size
  "Resolve the effective fetch size from opts.

  Returns the :fetch-size value when present and valid; otherwise returns
  the default of 128. Applies the value as a primitive long.

  Args:
    opts - map, may contain :fetch-size.

  Returns a positive long."
  ^long [opts]
  (let [raw (long (:fetch-size opts 128))] (if (pos? raw) raw 128)))

(defn add-demand
  "Add delta to current demand with saturation at Long/MAX_VALUE.

  Used to accumulate backpressure demand counts without overflow.

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

(defn result-rows-pub
  "Return a Publisher<T> that streams transformed rows from result-pub sequentially.

  At most one Result is requested at a time. Downstream demand is forwarded to
  the active Result.map publisher, so row demand remains bounded by the row-flow
  fetch-size rather than collapsing to Long/MAX_VALUE at the Result boundary.

  row-xf is forwarded to result-row-pub and applied inside BiFunction.apply()
  while the Row's ByteBuf is guaranteed to be live. See result-row-pub for the
  detailed safety argument.

  Args:
    result-pub     - Publisher<Result> to consume.
    result-sub-ref - volatile holding the outer Subscription for cancellation.
    row-xf         - 1-arity fn [Row] -> T applied to each row."
  ^Publisher [^Publisher result-pub result-sub-ref row-xf]
  (reify
    Publisher
    (subscribe [_ downstream]
      (let [state                                                             (Object.)
            pub-id                                                            (str "PUB" (Integer/toHexString (System/identityHashCode state)))
            requested-ref                                                     (volatile! (long 0))
            inner-sub-ref                                                     (volatile! nil)
            outer-done-ref                                                    (volatile! false)
            outer-pending-ref                                                 (volatile! false)
            inner-live-ref                                                    (volatile! false)
            cancelled-ref                                                     (volatile! false)
            ^AtomicBoolean terminal-ref                                       (AtomicBoolean. false)
            complete!                                                         (fn []
                                                                                (let [won? (.compareAndSet terminal-ref false true)]
                                                                                  (dbg/dlog pub-id " complete! won?=" won?)
                                                                                  (when won? (.onComplete downstream))))
            fail!                                                             (fn [t]
                                                                                (let [won? (.compareAndSet terminal-ref false true)]
                                                                                  (dbg/dlog pub-id " fail! won?=" won? " type=" (.getSimpleName (class t)))
                                                                                  (when won? (.onError downstream t))))
            request-next-result!
            (fn []
              (let [^Subscription outer-sub
                    (locking state
                      (let [ok (and (not @cancelled-ref)
                                    (not @outer-done-ref)
                                    (not @outer-pending-ref)
                                    (not @inner-live-ref)
                                    (nil? @inner-sub-ref)
                                    (pos? ^long @requested-ref)
                                    @result-sub-ref)]
                        (when ok
                          (vreset! outer-pending-ref true)
                          @result-sub-ref)))]
                (when outer-sub (.request outer-sub 1))))
            row-subscriber
            (reify
              Subscriber
              (onSubscribe [_ s]
                (let [[cancel? requested] (locking state
                                            (if @cancelled-ref
                                              [true (long 0)]
                                              (do (vreset! inner-sub-ref s)
                                                  [false
                                                   ^long @requested-ref])))]
                  (if cancel?
                    (.cancel ^Subscription s)
                    (when (pos? ^long requested)
                      (.request ^Subscription s ^long requested)))))
              (onNext [_ row]
                (let [emit? (locking state
                              (when-not @cancelled-ref
                                (vreset! requested-ref
                                         (long (max 0
                                                    (unchecked-dec
                                                     ^long @requested-ref))))
                                true))]
                  (when emit? (.onNext downstream row))))
              (onError [_ t] (fail! t))
              (onComplete [_]
                (let [complete-now? (locking state
                                      (vreset! inner-sub-ref nil)
                                      (vreset! inner-live-ref false)
                                      (and @outer-done-ref
                                           (not @cancelled-ref)))]
                  (if complete-now? (complete!) (request-next-result!)))))]
        (.onSubscribe
         downstream
         (reify
           Subscription
           (request [_ n]
             (when (pos? (long n))
               (let [[^Subscription inner-sub should-complete?]
                     (locking state
                       (if @cancelled-ref
                         [nil false]
                         (do (vreset! requested-ref
                                      (add-demand ^long @requested-ref
                                                  (long n)))
                             [@inner-sub-ref
                              (and @outer-done-ref
                                   (not @inner-live-ref)
                                   (nil? @inner-sub-ref))])))]
                 (cond inner-sub (.request inner-sub (long n))
                       should-complete? (complete!)
                       :else (request-next-result!)))))
           (cancel [_]
             (dbg/dlog pub-id " downstream cancel already?=" @cancelled-ref)
             (let [[^Subscription outer-sub ^Subscription inner-sub]
                   (locking state
                     (when-not @cancelled-ref (vreset! cancelled-ref true))
                     [@result-sub-ref @inner-sub-ref])]
               (when outer-sub (.cancel outer-sub))
               (when inner-sub (.cancel inner-sub))))))
        (.subscribe
         result-pub
         (reify
           Subscriber
           (onSubscribe [_ s]
             (let [cancel?
                   (locking state (vreset! result-sub-ref s) @cancelled-ref)]
               (if cancel? (.cancel ^Subscription s) (request-next-result!))))
           (onNext [_ result]
             (let [cancel? (locking state
                             (vreset! outer-pending-ref false)
                             (if @cancelled-ref
                               true
                               (do (vreset! inner-live-ref true) false)))]
               (when-not cancel?
                 (.subscribe ^Publisher (result-row-pub ^Result result row-xf)
                             ^Subscriber row-subscriber))))
           (onError [_ t]
             (locking state (vreset! outer-pending-ref false))
             (fail! t))
           (onComplete [_]
             (let [complete-now? (locking state
                                   (vreset! outer-pending-ref false)
                                   (vreset! outer-done-ref true)
                                   (and (not @inner-live-ref)
                                        (nil? @inner-sub-ref)
                                        (not @cancelled-ref)))]
               (when complete-now? (complete!))))))))))

(defn first-pub-blocking
  "Subscribe to pub, request one item, and return it synchronously.

  Blocks the calling thread. Intended for driver-level one-shot publishers
  (e.g., connection metadata) where Missionary scheduling is unavailable.

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

  Used for fire-and-forget publishers such as Statement.execute() void results.

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

(defn request-async!
  "Call Subscription.request(n) on sub asynchronously.

  Schedules the request on a ForkJoinPool background thread to avoid
  re-entrant driver callbacks.

  Args:
    sub - Subscription to request from.
    n   - number of items to request."
  [^Subscription sub ^long n]
  (CompletableFuture/runAsync (fn [] (.request sub n))))

(comment
  (resolve-fetch-size {})
  (resolve-fetch-size {:fetch-size 64})
  (resolve-fetch-size {:fetch-size 0})
  (add-demand 10 5)
  (add-demand Long/MAX_VALUE 1)
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
                      (volatile! nil))
  (let [latch (java.util.concurrent.CountDownLatch. 1)
        sub   (reify
                org.reactivestreams.Subscription
                (request [_ _] (.countDown latch))
                (cancel [_]))
        fut   (request-async! sub 1)]
    (.await latch 2 java.util.concurrent.TimeUnit/SECONDS)
    (.isDone fut)))
