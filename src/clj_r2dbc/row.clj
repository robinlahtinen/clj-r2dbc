(ns clj-r2dbc.row
  "Public row and parameter namespace for clj-r2dbc.

  Separates the write path (binding params) from the read path (building row
  maps). Builders are passed as :builder to execute, stream, and related
  functions. Extend Parameter to bind custom types onto R2DBC statements.

  Parameter binding:
    Parameter              - protocol; extend to bind custom types onto a Statement.

  Row builders (pass as :builder to execute/stream):
    kebab-maps             - unqualified kebab-case keyword maps (default).
    raw-maps               - unqualified keyword maps, no case conversion.
    vectors                - plain vectors in column order.
    ->qualified-kebab-maps - factory; returns a qualified kebab-case builder.
    ->qualified-maps       - factory; returns a qualified raw-name builder."
  (:require
   [camel-snake-kebab.core :as csk]
   [clj-r2dbc.impl.coerce :as impl-coerce]
   [clj-r2dbc.impl.sql.row :as impl-row])
  (:import
   (io.r2dbc.spi Statement)
   (java.nio ByteBuffer)))

(set! *warn-on-reflection* true)

(defprotocol Parameter
  "Write-path extensibility: bind a value onto an R2DBC Statement at index.

  Extend this protocol to bind custom types that R2DBC drivers don't handle
  natively."
  (-bind [this stmt index]
    "Bind this value onto stmt at zero-based index.

    Args:
      this  - the value to bind.
      stmt  - R2DBC Statement receiving the binding.
      index - zero-based integer column index.

    Returns stmt."))

(def ^{:added "0.1"} kebab-maps
  "Build unqualified kebab-case keyword maps.
  Column names are converted to kebab-case via camel-snake-kebab.
  This is the default builder for all clj-r2dbc operations."
  (impl-row/map-builder-fn (fn [^String col _cm]
                             (keyword (csk/->kebab-case-string col)))))

(def ^{:added "0.1"} raw-maps
  "Build unqualified keyword maps using raw column names.
  No case conversion."
  (impl-row/map-builder-fn (fn [^String col _cm] (keyword col))))

(def ^{:added "0.1"} vectors
  "Build vectors in column order.
  Each row is a vector of column values in result-set order."
  (impl-row/make-vector-fn))

(defn ^{:added "0.1"} ->qualified-kebab-maps
  "Return a builder function that produces qualified kebab-case keyword maps.

  The `->` prefix signals this is a factory: it takes opts and returns a
  2-arity builder function [row row-metadata] -> map. Call it once at
  setup time; pass the result as :builder.

  Accepts a map, keyword arguments, or both:
    (->qualified-kebab-maps {:qualifier :users})
    (->qualified-kebab-maps :qualifier :users)

  Required keys:
    :qualifier   String, keyword, or symbol used as the namespace.
                 May also be a function of [column-name column-metadata] -> namespace-string."
  [& {:as opts}]
  (let [q (impl-row/ensure-qualifier (or opts {}))]
    (impl-row/map-builder-fn
     (fn [^String col cm]
       (impl-row/qualify-with-explicit q col cm csk/->kebab-case-string)))))

(defn ^{:added "0.1"} ->qualified-maps
  "Return a builder function that produces qualified keyword maps using raw column names.

  Accepts a map, keyword arguments, or both:
    (->qualified-maps {:qualifier :users})
    (->qualified-maps :qualifier :users)

  Required keys:
    :qualifier   String, keyword, or symbol used as the namespace."
  [& {:as opts}]
  (let [q (impl-row/ensure-qualifier (or opts {}))]
    (impl-row/map-builder-fn
     (fn [^String col cm] (impl-row/qualify-with-explicit q col cm identity)))))

(extend-protocol Parameter
  nil
  (-bind [_ ^Statement stmt ^long index]
    (.bindNull stmt (int index) Object)
    stmt)
  io.r2dbc.spi.Parameter
  (-bind [this ^Statement stmt ^long index]
    (let [^io.r2dbc.spi.Parameter p this
          value                     (.getValue p)
          t                         (.getType p)]
      (if (nil? value)
        (.bindNull stmt (int index) (impl-coerce/parameter-type->class t))
        (.bind stmt (int index) this))
      stmt))
  Object
  (-bind [this ^Statement stmt ^long index]
    (if (instance? impl-coerce/byte-array-class this)
      (.bind stmt (int index) (ByteBuffer/wrap ^bytes this))
      (.bind stmt (int index) this))
    stmt))

(comment
  (def qualified-builder (->qualified-kebab-maps {:qualifier :my.ns}))
  (def qualified-builder (->qualified-kebab-maps :qualifier :my.ns))
  (->qualified-maps {:qualifier "my.table"})
  (->qualified-maps :qualifier "my.table"))
