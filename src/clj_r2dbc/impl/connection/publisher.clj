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
    async-subscribe-pub - subscribeOn(m/blk) decoupling wrapper for a Publisher.
    first-pub-blocking  - blocking first-value extraction from a Publisher.
    await-void-pub!     - blocking drain-and-discard for void Publishers.
    await-future        - blocking CompletableFuture unwrap.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi Result)
   (java.util.concurrent CompletableFuture ExecutionException)
   (java.util.function BiFunction)
   (org.reactivestreams Publisher Subscriber Subscription)
   (reactor.core.publisher Flux)
   (reactor.core.scheduler Scheduler Schedulers)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

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

(def ^:private ^Scheduler blk-scheduler
  "Reactor Scheduler backed by Missionary's blocking executor (m/blk), so the
  decoupling boundary reuses the same unbounded pool as the rest of clj-r2dbc
  rather than spawning a second one. Wrapping an existing Executor; disposing
  this Scheduler does not shut m/blk down."
  (Schedulers/fromExecutor m/blk))

(defn async-subscribe-pub
  "Wrap pub with a Reactor subscribeOn(m/blk) boundary so subscription AND all
  emissions are dispatched on the Missionary blocking executor rather than the
  consumer's thread.

  Why subscribeOn, and why it is required for correctness (not just thread
  placement): a *synchronous* Publisher (H2 in-memory: H2Statement.execute builds
  results from Flux.fromIterable/Mono.fromSupplier with no Scheduler) emits onNext
  on the consumer's own request thread. Missionary's m/subscribe drops the element
  just before onComplete in that case (its Sub bridge is request(1)-driven and
  built for asynchronous publishers; verified: (m/reduce conj [] (m/subscribe
  (Flux/range 1 3))) => [1 3]). subscribeOn moves the subscription and the whole
  request->onNext cascade onto a blk thread, restoring the asynchrony m/subscribe
  expects, so no element is dropped.

  IMPORTANT - applied ONLY to synchronous drivers. An async driver (Netty-based
  Postgres/MariaDB) emits onNext on its own event-loop thread; wrapping it here
  adds a second requester thread that races that event loop and REORDERS rows
  (observed: one row displaced ~13 positions over a 20k stream). lifecycle
  interposes this wrapper only when synchronous-db? (a cached re-entrancy probe,
  see synchronous-emission?) reports a synchronous driver; asynchronous drivers
  are consumed bare.

  subscribeOn is deliberately chosen over publishOn: publishOn inserts a prefetch
  queue (default 256) that defeats the request(1)/fetchSize-1 backpressure this
  design maintains. subscribeOn relays each request(1) verbatim and buffers
  nothing (verified: max upstream request stays 1).

  Args:
    pub - Publisher to subscribe to / emit from on m/blk.

  Returns a Publisher decoupled from the consumer's thread via subscribeOn."
  ^Publisher [^Publisher pub]
  (.subscribeOn (Flux/from pub) blk-scheduler))

(defn synchronous-emission?
  "Probe whether pub emits its first item on the thread that subscribed.

  Returns true when the first onNext arrives on the SAME thread that called
  onSubscribe/request - the signature of a synchronous publisher that runs on
  the caller's thread (e.g. R2DBC H2, whose H2Statement.execute builds results
  from Flux.fromIterable / Mono.fromSupplier with no Scheduler). Returns false
  when the first item arrives on a different thread (an asynchronous, Netty-backed
  driver such as PostgreSQL/MariaDB), or when the publisher completes empty or
  errors before emitting.

  Why this matters: a synchronous publisher delivers onNext on the consumer's
  request thread, and missionary's m/subscribe drops the element just before
  onComplete in that case (its Sub bridge is request(1)-driven and built for
  asynchronous publishers; verified: (m/reduce conj [] (m/subscribe (Flux/range
  1 3))) => [1 3]). Callers use this probe to decide whether to interpose
  async-subscribe-pub (which moves emission onto m/blk, restoring the asynchrony
  m/subscribe expects). Asynchronous drivers must NOT get that wrapper - the
  extra requester thread races their event loop and reorders rows.

  Blocking and one-shot: subscribes, requests one item, blocks the calling
  thread until the first signal, then cancels. Intended to be run once per
  driver and cached (see lifecycle/synchronous-db?).

  Args:
    pub - Publisher to probe (a representative query such as SELECT 1)."
  [^Publisher pub]
  (let [fut        (CompletableFuture.)
        sub-ref    (volatile! nil)
        sub-thread (volatile! nil)]
    (.subscribe pub
                (reify
                  Subscriber
                  (onSubscribe [_ s]
                    (vreset! sub-ref s)
                    (vreset! sub-thread (Thread/currentThread))
                    (.request ^Subscription s 1))
                  (onNext [_ _]
                    (.complete fut (identical? @sub-thread (Thread/currentThread)))
                    (.cancel ^Subscription @sub-ref))
                  (onError [_ _] (.complete fut false))
                  (onComplete [_] (.complete fut false))))
    (.get ^CompletableFuture fut)))

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
