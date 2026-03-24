;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.sql.cursor
  "RowCursor flyweight and generation guard for clj-r2dbc streaming.

  Provides:
    Cursor          - protocol for type-safe access to mutable RowCursor fields.
    GenerationGuard - Row wrapper built by cursor-row; guards against stale reads
                      after the cursor advances to the next row.
    RowCursor       - shared mutable flyweight constructed once per stream* call;
                      reused for every row emitted.

  Note: RowCursor is a shared mutable object. The same instance is mutated for
  every row emitted by stream*. Retaining a reference to the cursor beyond the
  current m/?> boundary silently returns data from a later row - no exception is
  thrown. Callers that need to keep row data must either:
    (a) supply :builder-fn (recommended) to materialize an immutable value, or
    (b) call cursor-row and cursor-cache immediately within the same reduce step
        and pass them to impl/sql/row's row->map before the next row arrives.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.sql.row])
  (:import
   (clj_r2dbc.impl.sql.row RowMetadataCache)
   (io.r2dbc.spi Row)
   (java.util ArrayDeque)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defprotocol Cursor
  "Protocol for type-safe access to RowCursor's mutable fields.
  Used by callers that consume the flyweight cursor directly (e.g., tests,
  custom reduction functions)."
  (cursor-row [this]
    "Return the current Row (valid only within the current step).")
  (cursor-cache [this]
    "Return the current RowMetadataCache.")
  (cursor-generation [this]
    "Return the monotonic cursor generation.")
  (-set-row! [this row]
    "Internal: enqueue row for the next consumer step.")
  (-set-cache! [this cache]
    "Internal: update the RowMetadataCache in place."))

(defn- stale-row-ex
  []
  (IllegalStateException. "RowCursor advanced; stale row reference."))

(deftype
 ^{:doc
   "A Row wrapper that guards against stale reads.

   Fields:
     row        - the underlying Row delegate.
     generation - the cursor generation at the time this guard was created.
     cursor     - the RowCursor whose generation is checked on each access.

   Delegates all Row.get calls to the underlying row only if the cursor
   generation matches; throws IllegalStateException otherwise. Used in
   flyweight streaming mode to detect out-of-step row access."}
 GenerationGuard
 [^Row row ^long generation cursor]
  Row
  (^Object get
    [_ ^int index]
    (if (= generation (cursor-generation cursor))
      (.get row index)
      (throw (stale-row-ex))))
  (^Object get
    [_ ^String name]
    (if (= generation (cursor-generation cursor))
      (.get row name)
      (throw (stale-row-ex))))
  (^Object get
    [_ ^int index ^Class type]
    (if (= generation (cursor-generation cursor))
      (.get row index type)
      (throw (stale-row-ex))))
  (^Object get
    [_ ^String name ^Class type]
    (if (= generation (cursor-generation cursor))
      (.get row name type)
      (throw (stale-row-ex))))
  (getMetadata [_]
    (if (= generation (cursor-generation cursor))
      (.getMetadata row)
      (throw (stale-row-ex)))))

(deftype
 ^{:doc
   "Shared mutable cursor passed as the per-step value in flyweight streaming.

   Fields:
     _row-queue  - queue of pending rows (ArrayDeque).
     _cache      - the current RowMetadataCache (unsynchronized-mutable).
     _generation - monotonic counter incremented on each row advance (volatile).

   Holds a queue of pending rows, the current RowMetadataCache, and a
   monotonic generation counter. Each m/?> deref advances to the next row;
   all access outside the current step is guarded by GenerationGuard."}
 RowCursor
 [^ArrayDeque _row-queue ^:unsynchronized-mutable ^RowMetadataCache _cache
  ^:volatile-mutable ^long _generation]
  Cursor
  (cursor-row [this]
    (let [^Row r (.pollFirst _row-queue)]
      (when r (->GenerationGuard r _generation this))))
  (cursor-cache [_] _cache)
  (cursor-generation [_] _generation)
  (-set-row! [this r]
    (set! _generation (unchecked-inc _generation))
    (.addLast _row-queue r)
    this)
  (-set-cache! [this c] (set! _cache c) this))

(comment
  (def crs (->RowCursor (java.util.ArrayDeque. 2) nil 0))
  (cursor-generation crs)
  (-set-row! crs ::fake-row)
  (cursor-generation crs)
  (def guarded (cursor-row crs))
  (cursor-row crs)
  (-set-row! crs ::next-row)
  (cursor-generation crs)
  (-set-cache! crs ::some-cache)
  (cursor-cache crs))
