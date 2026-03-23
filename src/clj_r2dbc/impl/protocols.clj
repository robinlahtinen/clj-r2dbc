;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.protocols
  "Protocol definitions for facade dispatch.

  These protocols form the dispatch layer separating the public API
  (clj-r2dbc) from driver implementations. JVM interface dispatch
  replaces delay/requiring-resolve indirection with zero overhead.

  Protocols and their dispatch types:
    Executable  - extended to ConnectionFactory, Connection, ConnectableWithOpts.
                  Covers execute, execute-one, execute-each, execute-batch.
    Streamable  - extended to ConnectionFactory, Connection, ConnectableWithOpts.
                  Covers stream.
    Connectable - extended to ConnectionFactory, Connection, ConnectableWithOpts.
                  Covers with-connection, with-options.
    Describable - extended to Connection only.
                  Covers connection-metadata.

  This namespace contains only protocol definitions - no logic, no imports,
  no side effects.")

(set! *warn-on-reflection* true)

(defprotocol Executable
  "Dispatch protocol for SQL execution.

  Extended by ConnectionFactory, Connection, and ConnectableWithOpts."
  (-execute [db sql params opts]
    "Execute sql against db and return the full labeled result map.")
  (-execute-one [db sql params opts]
    "Execute sql against db and return the first row or nil.")
  (-execute-each [db sql param-sets opts]
    "Execute sql once per param-set in param-sets and return a per-binding-set result vector.")
  (-execute-batch [db statements opts]
    "Execute multiple SQL statements in a single round trip and return a batch result map."))

(defprotocol Streamable
  "Dispatch protocol for streaming row execution.

  Extended by ConnectionFactory, Connection, and ConnectableWithOpts."
  (-stream [db sql params opts]
    "Return a Missionary discrete flow emitting one row value per item."))

(defprotocol Connectable
  "Dispatch protocol for connection acquisition and option attachment.

  Extended by ConnectionFactory, Connection, and ConnectableWithOpts."
  (-with-connection [db body-fn]
    "Acquire a connection, call body-fn with it, and guarantee connection close.")
  (-with-options [db opts]
    "Return a new connectable wrapping db with opts merged as defaults."))

(defprotocol Describable
  "Dispatch protocol for connection metadata retrieval.

  Extended by Connection."
  (-connection-metadata [db]
    "Return a Clojure map with :db/product-name and :db/version keys."))

(comment
  (require '[clj-r2dbc :as r2] '[clj-r2dbc.impl.protocols :as proto])
  (def db (r2/connect "r2dbc:h2:mem:///protocols-repl;DB_CLOSE_DELAY=-1"))
  (def wrapped (r2/with-options db {:fetch-size 64}))
  (satisfies? proto/Executable db)
  (satisfies? proto/Executable wrapped)
  (satisfies? proto/Streamable db)
  (satisfies? proto/Streamable wrapped)
  (satisfies? proto/Connectable db)
  (satisfies? proto/Connectable wrapped)
  (satisfies? proto/Describable db)
  (require '[missionary.core :as m])
  (m/? (r2/with-conn [conn db] (m/sp (proto/-connection-metadata conn))))
  (m/? (proto/-execute db "SELECT 1 AS n" [] {}))
  (m/? (proto/-execute-one db "SELECT 1 AS n" [] {}))
  (m/? (proto/-execute-each db "SELECT $1 AS n" [[1] [2] [3]] {}))
  (m/? (proto/-execute-batch db
                             ["CREATE TABLE IF NOT EXISTS proto_t (id INT)"
                              "INSERT INTO proto_t VALUES (1)"]
                             {}))
  (m/? (m/reduce conj [] (proto/-stream db "SELECT 1 AS n" [] {})))
  (proto/-with-options db {:fetch-size 128})
  (m/? (proto/-with-connection db
                               (fn [conn]
                                 (m/sp (proto/-connection-metadata conn))))))
