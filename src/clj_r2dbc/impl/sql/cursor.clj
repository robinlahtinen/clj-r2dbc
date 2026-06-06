;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.sql.cursor
  "RowCursor flyweight and generation guard for clj-r2dbc streaming.

  Provides:
    Cursor          - protocol for type-safe access to mutable RowCursor fields.
    GenerationGuard - Row view returned by cursor-row; reads the cursor's
                      materialised column values and guards against stale reads
                      after the cursor advances to the next row.
    RowCursor       - shared mutable flyweight constructed once per stream* call;
                      reused for every row emitted.

  Zero-copy without dangling ByteBufs: column values are read into the cursor's
  reused Object[] storage by capture! INSIDE result-row-pub's Result.map - while
  the Row's backing ByteBuf is live - rather than stashing a raw Row to read in
  the consumer step (which reads freed memory once the row publisher advances and
  releases the ByteBuf; see impl/connection/lifecycle). cursor-row then reads the
  already-materialised values, so it is safe regardless of when the consumer
  reads relative to the publisher's release.

  Note: RowCursor is a shared mutable object. The same instance is mutated for
  every row emitted by stream*. Retaining a cursor-row beyond the current step
  reads the next row's values - the GenerationGuard throws IllegalStateException
  in that case. Callers that need to keep row data must either:
    (a) supply :builder-fn (recommended) to materialize an immutable value, or
    (b) call cursor-row and cursor-cache immediately within the same reduce step
        and pass them to impl/sql/row's row->map before the next row arrives.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.sql.row])
  (:import
   (clj_r2dbc.impl.sql.row RowMetadataCache)
   (io.r2dbc.spi Row RowMetadata)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defprotocol Cursor
  "Protocol for type-safe access to RowCursor's mutable fields.
  Used by callers that consume the flyweight cursor directly (e.g., tests,
  custom reduction functions)."
  (cursor-row [this]
    "Return a Row view of the current materialised values (valid only within the
    current step).")
  (cursor-cache [this]
    "Return the current RowMetadataCache.")
  (cursor-generation [this]
    "Return the monotonic cursor generation.")
  (-values [this]
    "Internal: the cursor's reused column-value array.")
  (-name->idx [this]
    "Internal: the column-name -> index map.")
  (-row-metadata [this]
    "Internal: the current RowMetadata.")
  (-capture! [this row]
    "Internal: read row's columns into the reused value array, advancing the
    generation. Must be called while the Row's ByteBuf is live (in Result.map).")
  (-set-meta! [this cache name->idx rmd]
    "Internal: install the per-result metadata (built once on the first row)."))

(defn- stale-row-ex
  []
  (IllegalStateException. "RowCursor advanced; stale row reference."))

(deftype
 ^{:doc
   "A Row view over the cursor's materialised values that guards against stale
   reads.

   Fields:
     generation - the cursor generation at the time this guard was created.
     cursor     - the RowCursor whose generation and values are read.

   Reads the cursor's reused value array by index (or by name via the cursor's
   name->index map) only if the cursor generation still matches; throws
   IllegalStateException otherwise. Used in flyweight streaming mode to detect
   out-of-step row access."}
 GenerationGuard
 [^long generation cursor]
  Row
  (^Object get
    [_ ^int index]
    (if (= generation (cursor-generation cursor))
      (aget ^objects (-values cursor) index)
      (throw (stale-row-ex))))
  (^Object get
    [_ ^String name]
    (if (= generation (cursor-generation cursor))
      (aget ^objects (-values cursor) (int (get (-name->idx cursor) name)))
      (throw (stale-row-ex))))
  (^Object get
    [_ ^int index ^Class _type]
    (if (= generation (cursor-generation cursor))
      (aget ^objects (-values cursor) index)
      (throw (stale-row-ex))))
  (^Object get
    [_ ^String name ^Class _type]
    (if (= generation (cursor-generation cursor))
      (aget ^objects (-values cursor) (int (get (-name->idx cursor) name)))
      (throw (stale-row-ex))))
  (getMetadata [_]
    (if (= generation (cursor-generation cursor))
      (-row-metadata cursor)
      (throw (stale-row-ex)))))

(deftype
 ^{:doc
   "Shared mutable cursor passed as the per-step value in flyweight streaming.

   Fields:
     _values     - reused Object[] of the current row's materialised column
                   values (unsynchronized-mutable; grown to col-count on first
                   capture).
     _cache      - the current RowMetadataCache (unsynchronized-mutable).
     _name->idx  - column-name -> index map (unsynchronized-mutable).
     _rmd        - the current RowMetadata (unsynchronized-mutable).
     _generation - monotonic counter incremented on each capture (volatile).

   capture! materialises the row's columns into _values while the ByteBuf is
   live; all access outside the current step is guarded by GenerationGuard."}
 RowCursor
 [^:unsynchronized-mutable ^objects _values
  ^:unsynchronized-mutable ^RowMetadataCache _cache
  ^:unsynchronized-mutable _name->idx
  ^:unsynchronized-mutable ^RowMetadata _rmd
  ^:volatile-mutable ^long _generation]
  Cursor
  (cursor-row [this] (->GenerationGuard _generation this))
  (cursor-cache [_] _cache)
  (cursor-generation [_] _generation)
  (-values [_] _values)
  (-name->idx [_] _name->idx)
  (-row-metadata [_] _rmd)
  (-capture! [this row]
    (let [^RowMetadataCache c        _cache
          n                          (.col-count c)
          ^"[Ljava.lang.Object;" tys (.java-types c)
          ^"[Ljava.lang.Object;" vs  (if (and _values (= (alength ^objects _values) n))
                                       _values
                                       (object-array n))]
      (dotimes [i n]
        (aset vs i (.get ^Row row (int i) ^Class (aget tys i))))
      (set! _values vs)
      (set! _generation (unchecked-inc _generation))
      this))
  (-set-meta! [this cache name->idx rmd]
    (set! _cache cache)
    (set! _name->idx name->idx)
    (set! _rmd rmd)
    this))

(defn ->cursor
  "Construct an empty RowCursor for one stream* invocation."
  []
  (->RowCursor nil nil nil nil 0))

(comment
  (def crs (->cursor))
  (cursor-generation crs))
