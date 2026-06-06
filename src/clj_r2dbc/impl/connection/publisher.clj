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
  placement): a *synchronous* Publisher (H2 in-memory, Reactor scalar fusion)
  emits onNext re-entrantly from inside the consumer's request(n) call. When that
  consumer is Missionary's m/subscribe being forked by m/?> (Ambiguous), the
  re-entrant signal races the fork: it either throws ClassCastException mid-fork
  (notifier fires before the iterator is assigned) or, under concurrent load,
  drops a wakeup and parks the flow forever. subscribeOn moves the subscription
  and the whole request->onNext cascade onto a blk thread, so the cascade no
  longer re-enters the consumer's request frame. An async driver (Netty-based
  Postgres/MariaDB) never emits during request and is unaffected; this boundary
  exists for the synchronous case but is applied uniformly.

  subscribeOn is deliberately chosen over publishOn: publishOn inserts a prefetch
  queue (default 256) that both defeats the request(1)/fetchSize-1 backpressure
  this design maintains and buffers Rows across the thread boundary - unsafe in
  flyweight mode where the emitted value is a live-ByteBuf-backed Row.
  subscribeOn relays each request(1) verbatim and buffers nothing (verified: max
  upstream request stays 1).

  Args:
    pub - Publisher to subscribe to / emit from on m/blk.

  Returns a Publisher decoupled from the consumer's thread via subscribeOn."
  ^Publisher [^Publisher pub]
  (.subscribeOn (Flux/from pub) blk-scheduler))

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
