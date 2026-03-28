;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc
  "Facilities for Reactive Relational Database Connectivity (R2DBC) communication.

  A functional wrapper around R2DBC, designed for use with the Missionary reactive
  programming library. It manages Resource acquisition is initialization (RAII)
  for database connections and translates result sets into Clojure data structures.

  The basic building blocks are:
    connect          - Returns a ConnectionFactory from an R2DBC URL.
    with-options     - Attaches default options to a connectable.
    info             - Returns database connection metadata.
    execute          - Executes a SQL statement and returns a labeled result map.
    first-row        - Executes a SQL statement and returns the first row or nil.
    stream           - Executes a SQL statement and returns a discrete flow of rows.
    execute-each     - Executes a prepared statement with multiple binding sets.
    batch            - Executes a sequence of SQL statements in a single round trip.
    with-connection  - Acquires a connection and executes a task.
    with-transaction - Acquires a connection, begins a transaction, and executes a task.
    with-conn        - Acquires a connection and binds it for a task (macro).
    with-tx          - Acquires a transaction connection and binds it for a task (macro)."
  (:require
   [clj-r2dbc.impl.connection :as conn]
   [clj-r2dbc.impl.execute]
   [clj-r2dbc.impl.execute.stream :as stream-impl]
   [clj-r2dbc.impl.protocols :as proto]
   [clj-r2dbc.impl.validate :as v]
   [clj-r2dbc.middleware :as middleware]
   [missionary.core :as m]))

(set! *warn-on-reflection* true)

(defn ^{:added "0.1"} connect
  "Return a ConnectionFactory from an R2DBC URL string - synchronous, not a task.

  Args:
    url  - Non-blank R2DBC URL string. Forwarded verbatim to
           io.r2dbc.spi.ConnectionFactories/get. All composite URL schemes
           are accepted without modification:
             \"r2dbc:h2:mem:///name\"
             \"r2dbc:postgresql://user:pass@host/db\"
             \"r2dbc:pool:postgresql://user:pass@host/db\"
             \"r2dbc:proxy:postgresql://user:pass@host/db\".
    opts - (optional) Reserved options map; defaults to {}. Present for
           forward-compatibility; no keys are currently consumed.
           Accepts a map, keyword arguments, or both.

  Returns a ConnectionFactory instance synchronously.

  Throws (synchronously):
    ex-info :clj-r2dbc/missing-key   when url is nil.
    ex-info :clj-r2dbc/invalid-type  when url is not a string.
    ex-info :clj-r2dbc/invalid-value when url is blank.

  Example:
    (def db (connect \"r2dbc:h2:mem:///r2dbc-test;DB_CLOSE_DELAY=-1\"))
    (def pooled (connect \"r2dbc:pool:postgresql://user:pass@localhost/mydb\"))"
  ([url & {:as opts}]
   (v/require-non-blank-string! url "url" :clj-r2dbc/connect)
   (conn/create-connection-factory* (assoc (or opts {}) :url url))))

(defn ^{:added "0.1"} with-options
  "Attach default options to a connectable and return a new wrapper - synchronous, not a task.

  Accepts a ConnectionFactory, Connection, or an already-wrapped connectable.
  Nesting is supported: wrapping a wrapped connectable merges options (outer
  defaults first, inner defaults win on conflict).

  Merge semantics at call time: per-call opts passed to execute, stream, etc.
  win over defaults set here. A nil value in per-call opts doesn't clobber a
  default - nil values are stripped before merging.

  Doesn't modify the underlying object; always returns a new wrapper record.

  Args:
    db   - ConnectionFactory, Connection, or wrapped connectable. Must not be nil.
    opts - Default options to attach. Accepts a map, keyword arguments, or both
          . Defaults to {} when omitted.

  Returns a wrapped connectable that carries the merged default options.

  Throws (synchronously):
    ex-info :clj-r2dbc/missing-key  when db is nil.

  Example:
    (def db-256 (-> (connect \"r2dbc:h2:mem:///db\")
                    (with-options {:fetch-size 256})))
    ;; All calls through db-256 default to :fetch-size 256 unless overridden.

    ;; Equivalent keyword-arg form:
    (def db-256 (with-options (connect \"r2dbc:h2:mem:///db\") :fetch-size 256))"
  [db & {:as opts}]
  (v/require-non-nil! db "db" :clj-r2dbc/with-options)
  (proto/-with-options db (or opts {})))

(defn ^{:added "0.1"} info
  "Return database connection metadata as a Missionary task.

  When db is a ConnectionFactory or pool: acquires a connection, reads
  metadata, and releases the connection via RAII. When db is an existing Connection:
  uses it directly; the caller owns the connection lifecycle.

  Args:
    db - ConnectionFactory, Connection, or wrapped connectable. Must not be nil.

  Returns a task resolving to:
    {:db/product-name String   - from ConnectionMetadata.getDatabaseProductName().
     :db/version      String}  - from ConnectionMetadata.getDatabaseVersion().

  Throws (synchronously):
    ex-info :clj-r2dbc/missing-key when db is nil.

  Throws (inside task, on driver error):
    ex-info wrapping R2dbcException - carries :sql-state, :error-code,
                                       :clj-r2dbc/error-category.

  Example:
    (m/? (info db))
    ;=> {:db/product-name \"H2\" :db/version \"2.4.240\"}"
  [db]
  (v/require-non-nil! db "db" :clj-r2dbc/info)
  (proto/-with-connection db
                          (fn [conn] (m/sp (proto/-connection-metadata conn)))))

(defn ^{:added "0.1"} execute
  "Execute one SQL statement and return a Missionary task resolving to a labeled result map.

  Acquires a connection from db if needed (RAII), executes sql with the given
  params, and releases the connection. When db is an existing Connection, the
  connection is used directly and its lifecycle is owned by the caller.

  Args:
    db   - ConnectionFactory, Connection, or wrapped connectable (from with-options).
    sql  - Non-blank SQL string.
    opts - (optional) Execution options; defaults to {}. Accepts a map, keyword
           arguments, or both.

  Options:
    :params       - sequential, default []. Positional bind parameters. nil,
                    byte arrays, and io.r2dbc.spi.Parameter are dispatched via
                    clj-r2dbc.row/Parameter.
    :fetch-size   - integer in [1, 32768], default driver-determined. Rows per
                    demand batch.
    :builder      - (fn [Row RowMetadata] -> value), default
                    clj-r2dbc.row/kebab-maps. Called once per row. Use
                    row/raw-maps, row/vectors, or a custom 2-arity fn.
    :returning    - boolean or vector of column-name strings. Calls
                    Statement.returnGeneratedValues(). Use true to request all
                    generated values; use a vector to name specific columns.
    :middleware   - sequential of middleware fns (clj-r2dbc.middleware). Mutually
                    exclusive with :interceptors. Applied right-to-left.
    :interceptors - sequential of interceptor maps (clj-r2dbc.interceptor).
                    Mutually exclusive with :middleware. Bounded to 64 stages;
                    override with :max-interceptor-depth (positive integer).

  Returns a task resolving to a labeled result map. Keys present depend on SQL type:
    SELECT, WITH, VALUES, SHOW, DESCRIBE, or EXPLAIN:
      {:clj-r2dbc/rows [{:col val ...} ...]}    - vector, may be empty.
    DML (INSERT, UPDATE, or DELETE):
      {:clj-r2dbc/update-count N}               - N is a long.
    RETURNING or mixed result:
      {:clj-r2dbc/rows [...] :clj-r2dbc/update-count N}.
  Use the rows and update-count accessor fns to extract values with safe defaults.

  Throws (synchronously, before task is created):
    ex-info :clj-r2dbc/missing-key    when db or sql is nil.
    ex-info :clj-r2dbc/invalid-type   when sql is not a string, or :builder is not
                                       a function.
    ex-info :clj-r2dbc/invalid-value  when sql is blank, or :fetch-size is out of
                                       [1, 32768].
    ex-info :clj-r2dbc/unsupported    when both :middleware and :interceptors are
                                       present, or when :builder-fn is passed directly.

  Throws (inside task, on driver error):
    ex-info wrapping R2dbcException - carries :sql-state, :error-code,
                                       :clj-r2dbc/error-category, :sql, :params.

  Example:
    (m/? (execute db \"SELECT id, name FROM users WHERE id = $1\" {:params [42]}))
    ;=> {:clj-r2dbc/rows [{:id 42 :name \"Alice\"}]}

    ;; Equivalent keyword-arg form:
    (m/? (execute db \"SELECT id, name FROM users WHERE id = $1\" :params [42]))

    (m/? (execute db \"INSERT INTO users (name) VALUES ($1)\" {:params [\"Carol\"]}))
    ;=> {:clj-r2dbc/update-count 1}"
  ([db sql & {:as opts}]
   (v/require-non-nil! db "db" :clj-r2dbc/execute)
   (v/require-non-blank-string! sql "sql" :clj-r2dbc/execute)
   (let [opts'     (v/execute-opts (or opts {}) :clj-r2dbc/execute)
         params    (:params opts' [])
         call-opts (dissoc opts' :params)]
     (proto/-execute db sql params call-opts))))

(defn ^{:added "0.1"} first-row
  "Execute one SQL statement and return a Missionary task resolving to the first row or nil.

  Executes sql exactly as execute does, then returns the first element of
  :clj-r2dbc/rows, or nil when the result set is empty. Returns nil (not an
  empty map, not an exception) when zero rows are returned. Suitable for
  existence checks and single-row lookups.

  Args:
    db   - ConnectionFactory, Connection, or wrapped connectable (from with-options).
    sql  - Non-blank SQL string.
    opts - (optional) Execution options; defaults to {}. Accepts a map, keyword
           arguments, or both. Same options as execute: :params,
           :fetch-size, :builder, :returning, :middleware, :interceptors,
           :max-interceptor-depth.

  Returns a task resolving to the first row map, or nil when no rows were returned.

  Throws (synchronously, before task is created):
    ex-info :clj-r2dbc/missing-key    when db or sql is nil.
    ex-info :clj-r2dbc/invalid-type   when sql is not a string, or :builder is not
                                       a function.
    ex-info :clj-r2dbc/invalid-value  when sql is blank, or :fetch-size is out of
                                       [1, 32768].
    ex-info :clj-r2dbc/unsupported    when both :middleware and :interceptors are
                                       present.

  Throws (inside task, on driver error):
    ex-info wrapping R2dbcException - carries :sql-state, :error-code,
                                       :clj-r2dbc/error-category, :sql, :params.

  Example:
    (m/? (first-row db \"SELECT id, name FROM users WHERE id = $1\" {:params [42]}))
    ;=> {:id 42 :name \"Alice\"}

    ;; Equivalent keyword-arg form:
    (m/? (first-row db \"SELECT id, name FROM users WHERE id = $1\" :params [42]))

    (m/? (first-row db \"SELECT 1 WHERE false\"))
    ;=> nil"
  ([db sql & {:as opts}]
   (v/require-non-nil! db "db" :clj-r2dbc/execute)
   (v/require-non-blank-string! sql "sql" :clj-r2dbc/execute)
   (let [opts'     (v/execute-opts (or opts {}) :clj-r2dbc/execute)
         params    (:params opts' [])
         call-opts (dissoc opts' :params)]
     (proto/-execute-one db sql params call-opts))))

(defn ^{:added "0.1"} stream
  "Execute one SQL statement and return a Missionary discrete flow emitting one value per row.

  Unlike execute, which collects all rows into a vector, stream is
  demand-driven: rows are pulled from the database in fetch-size batches and
  emitted one at a time to the consumer. Connection lifecycle follows RAII:
  acquired at flow invocation, released on completion, error, or cancellation.
  When db is an existing Connection, the caller owns the lifecycle.

  Flyweight warning: When :stream-mode is :flyweight, the flow emits a single
  shared RowCursor instance that is mutated in-place for every row. Retaining a
  reference to the cursor past the current m/?> boundary silently returns data
  from a later row - no exception is thrown. Always materialize cursor data
  within the same reduce step. The default :stream-mode :immutable avoids this
  hazard entirely.

  Args:
    db   - ConnectionFactory, Connection, or wrapped connectable (from with-options).
    sql  - Non-blank SQL string.
    opts - (optional) Stream options; defaults to {}. Accepts a map, keyword
           arguments, or both.

  Options:
    :params       - sequential, default []. Positional bind parameters.
    :fetch-size   - integer in [1, 32768], default driver-determined. Controls
                    Subscription.request(N) batch size.
    :builder      - (fn [Row RowMetadata] -> value), default
                    clj-r2dbc.row/kebab-maps. Applied inside the fetch loop
                    while Row is valid. Values are safe to retain. Required
                    when using :chunk-size.
    :stream-mode  - :immutable (default) or :flyweight.
                    :immutable - applies :builder per row; emits immutable values.
                    :flyweight - emits a shared mutable RowCursor; see warning above.
    :chunk-size   - integer in [1, 32768]. Changes emission unit from individual
                    rows to java.util.ArrayList chunks of up to chunk-size
                    elements. Requires :builder. Reduces Missionary scheduler
                    overhead from O(rows) to O(batches).
    :returning    - boolean or vector of column names. Calls
                    Statement.returnGeneratedValues().
    :middleware   - sequential of middleware fns. Mutually exclusive with
                    :interceptors.
    :interceptors - sequential of interceptor maps. Mutually exclusive with
                    :middleware. Bounded to 64 stages (override:
                    :max-interceptor-depth).

  Returns a Missionary discrete flow. Consume with m/reduce or any flow
  combinator. This is not a task - it can't be awaited with m/? directly.

  Throws (synchronously):
    ex-info :clj-r2dbc/missing-key    when :chunk-size is present but :builder is
                                       absent.
    ex-info :clj-r2dbc/invalid-type   when sql is not a string, or :builder is not
                                       a function.
    ex-info :clj-r2dbc/invalid-value  when sql is blank, :chunk-size or :fetch-size
                                       is out of [1, 32768], or :stream-mode is not
                                       :immutable or :flyweight.
    ex-info :clj-r2dbc/unsupported    when both :middleware and :interceptors are
                                       present.

  Example:
    ;; Collect all rows (default :immutable mode)
    (m/? (m/reduce conj [] (stream db \"SELECT id, name FROM users\")))
    ;=> [{:id 1 :name \"Alice\"} {:id 2 :name \"Bob\"}]

    ;; Keyword-arg form:
    (m/? (m/reduce conj [] (stream db \"SELECT id FROM users\" :fetch-size 128)))

    ;; Chunk mode - emit ArrayList chunks; :builder required
    (require '[clj-r2dbc.row :as row])
    (m/? (m/reduce #(+ %1 (.size ^java.util.ArrayList %2))
                   0
                   (stream db \"SELECT 1\"
                           {:builder row/kebab-maps :chunk-size 64})))"
  ([db sql & {:as opts}]
   (v/require-non-nil! db "db" :clj-r2dbc/stream)
   (v/require-non-blank-string! sql "sql" :clj-r2dbc/stream)
   (stream-impl/stream-dispatch* db sql (v/stream-opts (or opts {})))))

(defn ^{:added "0.1"} execute-each
  "Execute one prepared statement with multiple binding sets and return a Missionary task.

  Uses Statement.add() to bind all param sets to a single prepared statement,
  then executes it once. Each inner sequential in params becomes one binding
  set. Suitable for bulk inserts and parameterized batch operations where each
  row uses the same SQL template.

  Doesn't support :middleware or :interceptors. Use execute in a loop for
  interceptor-wrapped per-row execution.

  Args:
    db     - ConnectionFactory, Connection, or wrapped connectable.
    sql    - Non-blank SQL string.
    params - Sequential of sequentials. Each inner sequential is one set of
             positional bind parameters.
    opts   - (optional) Options; defaults to {}. Accepts a map, keyword arguments,
             or both.

  Options:
    :fetch-size     - integer in [1, 32768], default driver-determined.
    :builder        - (fn [Row RowMetadata] -> value), default
                      clj-r2dbc.row/kebab-maps.
    :max-batch-size - positive integer, default 10000. Maximum number of
                      binding sets allowed. Throws :clj-r2dbc/limit-exceeded
                      when count(params) exceeds this limit. Override with
                      :max-batch-size (positive integer).

  Returns a task resolving to:
    {:clj-r2dbc/results [...]}  - vector of per-binding-set result maps.
    Each element has the same shape as an execute result map:
      {:clj-r2dbc/rows [...]}           for SELECT-like SQL.
      {:clj-r2dbc/update-count N}       for DML.
      {:clj-r2dbc/rows [...] :clj-r2dbc/update-count N}  for RETURNING.
  Use the results accessor fn to extract with a safe default.

  Throws (synchronously):
    ex-info :clj-r2dbc/missing-key    when db or sql is nil.
    ex-info :clj-r2dbc/invalid-type   when sql is not a string, or params is not
                                       sequential.
    ex-info :clj-r2dbc/invalid-value  when sql is blank, or :fetch-size is out of
                                       [1, 32768].
    ex-info :clj-r2dbc/limit-exceeded when count(params) exceeds :max-batch-size;
                                       carries :key :param-sets, :limit N,
                                       :value <count>.

  Throws (inside task, on driver error):
    ex-info wrapping R2dbcException - carries :sql-state, :error-code,
                                       :clj-r2dbc/error-category.

  Example:
    (m/? (execute-each db
                       \"INSERT INTO t (id, name) VALUES ($1, $2)\"
                       [[1 \"Alice\"] [2 \"Bob\"] [3 \"Carol\"]]))
    ;=> {:clj-r2dbc/results [{:clj-r2dbc/update-count 1}
    ;;                        {:clj-r2dbc/update-count 1}
    ;;                        {:clj-r2dbc/update-count 1}]}

    ;; Keyword-arg form:
    (m/? (execute-each db \"INSERT INTO t (id) VALUES ($1)\" [[1] [2]] :max-batch-size 5000))"
  ([db sql params & {:as opts}]
   (v/require-non-nil! db "db" :clj-r2dbc/execute-each)
   (v/require-non-blank-string! sql "sql" :clj-r2dbc/execute-each)
   (v/require-sequential-val! params "params" :clj-r2dbc/execute-each)
   (let [opts' (v/execute-each-opts (or opts {}))]
     (proto/-execute-each db sql params (dissoc opts' :params)))))

(defn ^{:added "0.1"} batch
  "Execute a sequence of SQL statements via Connection.createBatch() and return a Missionary task.

  Each string in statements is added to a single Batch object, which is
  executed in one round trip. Row segments in batch results are silently
  discarded - only update counts are returned. Suitable for DDL sequences
  and unconditional DML scripts.

  Doesn't support per-statement parameters. For parameterized bulk operations
  use execute-each.

  Args:
    db         - ConnectionFactory, Connection, or wrapped connectable.
    statements - Sequential of non-blank SQL strings. Each string is added to
                 the batch in order.
    opts       - (optional) Reserved options map; defaults to {}. Accepts a map,
                 keyword arguments, or both.

  Returns a task resolving to:
    {:clj-r2dbc/update-counts [N1 N2 ...]}  - one long per statement in order.
  Use the update-counts accessor fn to extract with a safe default of [].

  Throws (synchronously):
    ex-info :clj-r2dbc/missing-key   when db is nil.
    ex-info :clj-r2dbc/invalid-type  when statements is not sequential.
    ex-info :clj-r2dbc/unsupported   when both :middleware and :interceptors are
                                      present in opts.

  Throws (inside task, on driver error):
    ex-info wrapping R2dbcException - carries :sql-state, :error-code,
                                       :clj-r2dbc/error-category.

  Example:
    (m/? (batch db [\"CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY)\"
                    \"INSERT INTO t (id) VALUES (1)\"
                    \"INSERT INTO t (id) VALUES (2)\"]))
    ;=> {:clj-r2dbc/update-counts [0 1 1]}"
  ([db statements & {:as opts}]
   (v/require-non-nil! db "db" :clj-r2dbc/batch)
   (v/require-sequential-val! statements "statements" :clj-r2dbc/batch)
   (proto/-execute-batch db statements (v/batch-opts (or opts {})))))

(defn ^{:added "0.1"} with-connection
  "Acquire a connection, call f with {:connection conn}, and return a Missionary task.

  When db is a ConnectionFactory or pool: acquires a connection, passes it to
  f inside {:connection conn}, and closes the connection on success, error, or
  Missionary cancellation (RAII). When db is an existing Connection: passes it
  directly; the library doesn't close the connection - the caller owns the
  lifecycle.

  f must accept one map argument and return a Missionary Task. This is the
  function-based equivalent of the with-conn macro; prefer with-conn at the
  call site and with-connection for higher-order usage.

  Args:
    db   - ConnectionFactory, Connection, or wrapped connectable.
    f    - (fn [{:keys [connection]}] -> Task). Receives a map containing the
           live Connection under :connection.
    opts - (optional) Options merged via with-options before acquisition; defaults
           to {}. Accepts a map, keyword arguments, or both.

  Returns a task resolving to the return value of (f {:connection conn}).

  Throws (synchronously):
    ex-info :clj-r2dbc/missing-key   when db or f is nil.
    ex-info :clj-r2dbc/invalid-type  when f is not a function.

  Throws (inside task, on driver error):
    ex-info wrapping R2dbcException - carries :sql-state, :error-code,
                                       :clj-r2dbc/error-category.

  Example:
    (m/? (with-connection db
           (fn [{:keys [connection]}]
             (m/sp
               (m/? (execute connection \"SELECT 1 AS n\"))))))
    ;=> {:clj-r2dbc/rows [{:n 1}]}"
  ([db f & {:as opts}]
   (v/require-non-nil! db "db" :clj-r2dbc/with-connection)
   (v/require-fn-val! f "f" :clj-r2dbc/with-connection)
   (let [opts (or opts {})]
     (proto/-with-connection (if (seq opts) (with-options db opts) db)
                             (fn [conn] (f {:connection conn}))))))

(defn ^{:added "0.1"} with-transaction
  "Acquire a connection, begin a transaction, call f with {:connection conn}, and return a
  Missionary task resolving to the result of f.

  On success: commits the transaction, then closes the connection.
  On any Throwable thrown from f: rolls back (suppressing secondary rollback
  exceptions onto the original throwable via .addSuppressed), closes the
  connection, and re-throws the original.
  missionary.Cancelled: re-thrown immediately without suppression.

  Connection ownership: when db is a ConnectionFactory or pool, the library
  acquires and closes the connection (RAII). When db is an existing Connection,
  it is used directly and its lifecycle is owned by the caller.

  Join semantics: if the context already carries :clj-r2dbc/in-transaction? true
  (e.g. a nested call via with-tx on the same Connection), the transaction
  phase is skipped and f is called directly. No subtransactions are created.

  Args:
    db   - ConnectionFactory, Connection, or wrapped connectable (from with-options).
    f    - (fn [{:keys [connection]}] -> Task). Receives the live Connection.
    opts - (optional) Transaction options; defaults to {}. Accepts a map, keyword
           arguments, or both.

  Transaction Options:
    :isolation            - keyword, default driver-default.
                            :read-uncommitted | :read-committed
                            :repeatable-read  | :serializable
    :read-only?           - boolean, default false.
    :name                 - string, default nil. Driver-specific transaction or
                            savepoint name.
    :lock-wait-timeout-ms - positive integer, default nil. Converted to
                            java.time.Duration.
  When opts is empty, uses the no-arg Connection.beginTransaction() overload.

  Returns a task resolving to the return value of (f {:connection conn}).

  Throws (synchronously):
    ex-info :clj-r2dbc/missing-key   when db or f is nil.
    ex-info :clj-r2dbc/invalid-type  when f is not a function.

  Throws (inside task, on driver error):
    ex-info wrapping R2dbcException - carries :sql-state, :error-code,
                                       :clj-r2dbc/error-category.

  Example:
    (m/? (with-transaction db
           (fn [{:keys [connection]}]
             (m/sp
               (m/? (execute connection \"INSERT INTO t (v) VALUES ($1)\" {:params [1]}))
               (m/? (execute connection \"UPDATE s SET n = n + 1\"))))
           {:isolation :read-committed}))

    ;; Equivalent keyword-arg form:
    (m/? (with-transaction db
           (fn [{:keys [connection]}]
             (execute connection \"INSERT INTO t (v) VALUES ($1)\" :params [1]))
           :isolation :read-committed))"
  ([db f & {:as opts}]
   (v/require-non-nil! db "db" :clj-r2dbc/with-transaction)
   (v/require-fn-val! f "f" :clj-r2dbc/with-transaction)
   (proto/-with-connection db
                           (fn [conn]
                             (((middleware/with-transaction (or opts {}))
                               (fn [_ctx] (f {:connection conn})))
                              {:r2dbc/connection conn})))))

(defmacro ^{:added "0.1"} with-conn
  "Macro. Acquire a connection, bind it to conn, and run body as a Missionary task.
  Expands to with-connection.

  Internal symbols are generated via gensym (db__, opts__, ctx__) - no variable capture
  regardless of the name chosen for conn or any surrounding locals.

  Binding vector:
    [conn db]       - conn bound to Connection; default options
    [conn db opts]  - conn bound to Connection; opts merged via with-options
                      before connection acquisition

  conn is bound to the raw Connection object, not the context map. All RAII
  semantics from with-connection apply: the connection is closed on success,
  error, or Missionary cancellation when db is a ConnectionFactory or pool.
  When db is an existing Connection, the caller owns its lifecycle.

  Args (binding vector):
    conn - Symbol. Bound to the raw Connection object within body.
    db   - Expression evaluating to ConnectionFactory, Connection, or wrapped
           connectable.
    opts - (optional) Options map: any key accepted by execute, stream, etc.

  Returns a Missionary task resolving to the value of the last body form.

  Example:
    (m/? (with-conn [conn db]
           (m/sp
             (m/? (execute conn \"SELECT 1 AS n\"))
             (m/? (execute conn \"SELECT 2 AS n\")))))
    ;=> {:clj-r2dbc/rows [{:n 2}]}"
  [[conn db & [opts]] & body]
  (let [db-sym   (gensym "db__")
        opts-sym (gensym "opts__")
        ctx-sym  (gensym "ctx__")]
    (if opts
      `(let [~db-sym   ~db
             ~opts-sym ~opts]
         (with-connection ~db-sym
           (fn [~ctx-sym]
             (let [~conn (:connection ~ctx-sym)] ~@body))
           ~opts-sym))
      `(let [~db-sym ~db]
         (with-connection ~db-sym
           (fn [~ctx-sym]
             (let [~conn (:connection ~ctx-sym)] ~@body)))))))

(defmacro ^{:added "0.1"} with-tx
  "Macro. Acquire a connection in a transaction, bind it to conn, and run body as a
  Missionary task. Expands to with-transaction.

  Internal symbols are generated via gensym (db__, opts__, ctx__) - no variable capture
  regardless of the name chosen for conn or any surrounding locals.

  Binding vector:
    [conn db]       - conn bound to Connection; default transaction options
    [conn db opts]  - conn bound to Connection; opts map passed to
                      with-transaction for transaction configuration

  Transaction semantics, connection ownership, join semantics, commit/rollback
  behavior, and error handling are identical to with-transaction. See
  with-transaction.

  Args (binding vector):
    conn - Symbol. Bound to the raw Connection object within body.
    db   - Expression evaluating to ConnectionFactory, Connection, or wrapped
           connectable.
    opts - (optional) Transaction options map: :isolation, :read-only?, :name,
           :lock-wait-timeout-ms.

  Returns a Missionary task resolving to the value of the last body form.

  Example:
    (m/? (with-tx [conn db {:isolation :serializable}]
           (m/sp
             (m/? (execute conn \"SELECT val FROM t WHERE id = $1 FOR UPDATE\"
                            {:params [1]}))
             (m/? (execute conn \"UPDATE t SET val = val + 1 WHERE id = $1\"
                            {:params [1]})))))

    ;; Nested with-tx joins the outer transaction (no subtransaction created)
    (m/? (with-tx [outer db]
           (with-tx [inner outer]
             (execute inner \"INSERT INTO audit (msg) VALUES ($1)\"
                      {:params [\"done\"]}))))"
  [[conn db & [opts]] & body]
  (let [db-sym   (gensym "db__")
        opts-sym (gensym "opts__")
        ctx-sym  (gensym "ctx__")]
    (if opts
      `(let [~db-sym   ~db
             ~opts-sym ~opts]
         (with-transaction ~db-sym
           (fn [~ctx-sym]
             (let [~conn (:connection ~ctx-sym)] ~@body))
           ~opts-sym))
      `(let [~db-sym ~db]
         (with-transaction ~db-sym
           (fn [~ctx-sym]
             (let [~conn (:connection ~ctx-sym)] ~@body)))))))

(comment
  (def db (connect "r2dbc:h2:mem:///r2dbc-test;DB_CLOSE_DELAY=-1"))
  (m/? (info db))
  (m/? (execute db "SELECT 1 AS id"))
  (m/? (first-row db "SELECT 1 AS id"))
  (m/?
   (execute-each db "INSERT INTO t(id,name) VALUES ($1,$2)" [[1 "a"] [2 "b"]]))
  (m/? (batch db
              ["CREATE TABLE IF NOT EXISTS t(id int primary key)"
               "INSERT INTO t(id) VALUES (1)"]))
  (m/? (m/reduce conj [] (stream db "SELECT 1 AS id"))))
