;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.sql.cursor
  "RowCursor flyweight for clj-r2dbc streaming.

  Provides:
    Cursor    - protocol for type-safe access to a RowCursor's fields.
    RowView   - immutable Row view over one row's materialised column values.
    RowCursor - per-row immutable holder emitted in flyweight streaming mode
                (when no :builder is supplied).

  Zero dangling ByteBufs, zero shared mutable state: each row's column values are
  read into a fresh Object[] by stream*'s cursor-step INSIDE result-row-pub's
  Result.map - while the Row's backing ByteBuf is live - and wrapped in a new
  RowCursor per row. The per-result metadata (RowMetadataCache, name->index map,
  RowMetadata) is immutable and shared safely across the cursors of one stream.

  Because every emitted RowCursor is a distinct immutable value, it is safe to
  read at any time - within the reduce step or retained and read later - and the
  flyweight differs from the default :builder mode only in the SHAPE of the
  emitted value (a RowCursor exposing cursor-row/cursor-cache vs. an immutable
  map). This is the same per-row-materialisation property that makes :builder
  mode race-free: the producer materialises before emitting, so a reactive bridge
  that requests the next row before the consumer reads the current one (Missionary
  m/subscribe; see impl/connection/lifecycle) can never clobber an emitted value.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clj-r2dbc.impl.sql.row])
  (:import
   (clj_r2dbc.impl.sql.row RowMetadataCache)
   (io.r2dbc.spi Row RowMetadata)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defprotocol Cursor
  "Protocol for type-safe access to a RowCursor's fields.
  Used by callers that consume the flyweight cursor directly (e.g., tests,
  custom reduction functions)."
  (cursor-row [this]
    "Return an immutable Row view over this cursor's materialised column values.")
  (cursor-cache [this]
    "Return this cursor's RowMetadataCache."))

(deftype
 ^{:doc
   "Immutable Row view over one row's materialised column values.

   Fields:
     values    - Object[] of this row's column values, indexed by column position.
     name->idx - column-name -> index map (shared, immutable).
     rmd       - the RowMetadata for this result (shared, immutable).

   Reads are pure array/map lookups; the view holds its own value array, so it is
   valid for the lifetime of the cursor that produced it - there is no staleness."}
 RowView
 [^objects values name->idx ^RowMetadata rmd]
  Row
  (^Object get [_ ^int index] (aget values index))
  (^Object get [_ ^String name] (aget values (int (get name->idx name))))
  (^Object get [_ ^int index ^Class _type] (aget values index))
  (^Object get [_ ^String name ^Class _type] (aget values (int (get name->idx name))))
  (getMetadata [_] rmd))

(deftype
 ^{:doc
   "Per-row immutable holder emitted as the per-step value in flyweight streaming.

   Fields:
     values    - Object[] of this row's materialised column values.
     cache     - the RowMetadataCache (shared across the stream's cursors).
     name->idx - column-name -> index map (shared, immutable).
     rmd       - the RowMetadata (shared, immutable).

   A distinct instance is constructed per row inside result-row-pub's Result.map;
   cursor-row wraps the values in a RowView, cursor-cache returns the cache for
   use with impl/sql/row's row->map. Safe to read or retain at any time."}
 RowCursor
 [^objects values ^RowMetadataCache cache name->idx ^RowMetadata rmd]
  Cursor
  (cursor-row [_] (->RowView values name->idx rmd))
  (cursor-cache [_] cache))

(defn ->cursor
  "Construct a RowCursor for one materialised row.

  Args:
    values    - Object[] of the row's column values (read while the ByteBuf was live).
    cache     - RowMetadataCache for the result.
    name->idx - column-name -> index map.
    rmd       - RowMetadata for the result."
  [values cache name->idx rmd]
  (->RowCursor values cache name->idx rmd))

(comment
  (def crs (->cursor (object-array [1 "Alice"]) nil {"id" 0, "name" 1} nil))
  (.get ^Row (cursor-row crs) (int 0)))
