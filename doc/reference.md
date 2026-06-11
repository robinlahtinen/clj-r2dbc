# Reference

This document specifies the data shapes, options, bounds, and error contracts of
clj-r2dbc. It is a precise technical reference, not a tutorial. For a guided
introduction, read the [user guide](user-guide.md) first; for the internal
architecture, see [design](design.md).

## Calling conventions

Every public function that takes options accepts three equivalent forms:

| Form | Example |
|---|---|
| Plain map | `(execute db sql {:params [1] :fetch-size 64})` |
| Keyword arguments | `(execute db sql :params [1] :fetch-size 64)` |
| Mixed | `(execute db sql :params [1] {:fetch-size 64})` |

This applies to every function with an `opts` parameter: `connect`,
`with-options`, `execute`, `first-row`, `stream`, `execute-each`, `batch`,
`with-connection`, `with-transaction`, `->qualified-kebab-maps`,
`->qualified-maps`, `transaction-interceptor`, `middleware/with-transaction`,
`nav-task`, and `nav`. Existing code that passes plain maps needs no changes.

## Result data shapes

The task-based functions resolve to labeled result maps. The keys present depend
on the SQL type and the function called.

| Key | Returned by | Value | Notes |
|---|---|---|---|
| `:clj-r2dbc/rows` | `execute`, `first-row` (internally) | vector of row values | Empty vector when a SELECT returns no rows. |
| `:clj-r2dbc/update-count` | `execute` (DML) | `long` | Absent on pure SELECT results. |
| `:clj-r2dbc/results` | `execute-each` | vector of result maps | One labeled result map per binding set. |
| `:clj-r2dbc/update-counts` | `batch` | vector of `long` | One count per statement, in order. |

```clojure
;; SELECT
(m/? (execute db "SELECT id, full_name FROM people ORDER BY id"))
;; => #:clj-r2dbc{:rows [{:id 1, :full-name "Alice"} {:id 2, :full-name "Bob"}]}

;; DML
(m/? (execute db "UPDATE people SET full_name = 'X' WHERE id = 1"))
;; => #:clj-r2dbc{:update-count 1}

;; first-row resolves to the first row value, or nil on an empty result
(m/? (first-row db "SELECT id, full_name FROM people ORDER BY id"))
;; => {:id 1, :full-name "Alice"}

;; execute-each: one result map per binding set
(m/? (execute-each db "INSERT INTO people (id, full_name) VALUES ($1, $2)"
                   [[3 "Cy"] [4 "Di"]]))
;; => #:clj-r2dbc{:results [#:clj-r2dbc{:update-count 1} #:clj-r2dbc{:update-count 1}]}

;; batch: one update count per statement
(m/? (batch db ["UPDATE people SET full_name = 'X' WHERE id = 1"
                "DELETE FROM people WHERE id = 4"]))
;; => #:clj-r2dbc{:update-counts [1 1]}
```

`stream` does not return a labeled map. It returns a Missionary discrete flow
that emits one row value per item (or one vector per item under `:chunk-size`);
consume it with `m/reduce` or any flow combinator.

`info` resolves to a connection-metadata map:

```clojure
(m/? (info db))
;; => #:db{:product-name "H2", :version "2.4.240"}
```

## Row values and the `:builder` option

A row value is whatever `:builder` produces. `:builder` is a 2-arity function
`(fn [Row RowMetadata] -> value)`, applied once per row while the row's backing
buffer is live. The default is `clj-r2dbc.row/kebab-maps`.

`clj-r2dbc.row` provides the built-in builders:

| Builder | Row value | Example column `USER_ID` |
|---|---|---|
| `kebab-maps` (default) | unqualified kebab-case keyword map | `{:user-id 7}` |
| `raw-maps` | unqualified keyword map, names verbatim | `{:USER_ID 7}` |
| `vectors` | values in column order | `[7]` |
| `(->qualified-kebab-maps {:qualifier q})` | namespaced kebab-case keyword map | `{:q/user-id 7}` |
| `(->qualified-maps {:qualifier q})` | namespaced keyword map, names verbatim | `{:q/USER_ID 7}` |

`->qualified-kebab-maps` and `->qualified-maps` are factories; both require a
non-nil `:qualifier` (a string, keyword, symbol, or a `(fn [col-name col-meta])`).
A missing `:qualifier` throws `ex-info` with `:clj-r2dbc/error-type
:invalid-argument` (see [Error contracts](#error-contracts)).

`:builder` works uniformly on `execute`, `first-row`, and `stream`.

### `:qualifier` (all paths)

`execute`, `first-row`, and `stream` all honor a `:qualifier` keyword-mode shortcut applied
to the default builder:

| `:qualifier` | Column `USER_ID` |
|---|---|
| `:unqualified` | `{:USER_ID 7}` |
| `:unqualified-kebab` (default) | `{:user-id 7}` |
| `:qualified-kebab` | `{:table/user-id 7}` when the driver exposes the table name; otherwise unqualified kebab-case |

`:qualified-kebab` namespaces a column only when the driver reports a table name for it.

## Execution options

| Option | Functions | Type / range | Default |
|---|---|---|---|
| `:params` | all query fns | sequential of bind values | `[]` |
| `:fetch-size` | all query fns | integer in `[1, 32768]` | driver-determined |
| `:builder` | `execute`, `first-row`, `stream` | `(fn [Row RowMetadata] -> value)` | `kebab-maps` |
| `:qualifier` | `execute`, `first-row`, `stream` | `:unqualified` \| `:unqualified-kebab` \| `:qualified-kebab` | `:unqualified-kebab` |
| `:chunk-size` | `stream` only | integer in `[1, 32768]` | unset (per-row emission) |
| `:returning` | `execute`, `first-row`, `stream` | boolean or vector of column-name strings | unset |
| `:datafy` | `execute`, `first-row`, `stream` | boolean | `false` |
| `:middleware` | all query fns | sequential of middleware fns | unset |
| `:interceptors` | all query fns | sequential of interceptor maps | unset |
| `:max-interceptor-depth` | all query fns | positive integer | `64` |
| `:max-batch-size` | `execute-each` | positive integer | `10000` |

`:middleware` and `:interceptors` are mutually exclusive; supplying both throws.
`:returning` calls `Statement.returnGeneratedValues()` (pass `true` for all
generated values, or a vector of column names). `:datafy true` attaches
Datafiable metadata for REPL navigation (see `clj-r2dbc.datafy`).

## Parameter binding contract

- `clj-r2dbc.row/Parameter` is the write-path extension point: one method,
  `(-bind [this stmt index])`.
- Every bound value is delegated through that protocol-aware binding path,
  including `nil`, strings, boxed numbers, `io.r2dbc.spi.Parameter`, and user
  extensions. Common JVM scalars take a fast path before the protocol fallback.

## Bounds

| Option | Range | On out-of-range |
|---|---|---|
| `:fetch-size` | integer in `[1, 32768]` | `:clj-r2dbc/invalid-value` |
| `:chunk-size` | integer in `[1, 32768]` | `:clj-r2dbc/invalid-value` |
| `:max-batch-size` | positive integer (no upper bound) | `:clj-r2dbc/invalid-value` if non-positive; `:clj-r2dbc/limit-exceeded` when the count exceeds it |
| `:max-interceptor-depth` | positive integer (no upper bound) | `:clj-r2dbc/invalid-value` if non-positive; `:clj-r2dbc/limit-exceeded` when the count exceeds it |

`:max-batch-size` and `:max-interceptor-depth` raise the default ceilings; the
user takes responsibility for the new limit. Set them per call or via
`with-options`:

```clojure
;; Per call
(execute db sql {:params params :interceptors chain :max-interceptor-depth 128})
(execute-each db sql param-sets {:max-batch-size 50000})

;; Via with-options (applies to all calls)
(def db (with-options factory {:max-batch-size 50000}))
```

## Error contracts

clj-r2dbc reports errors as `ex-info`. There are two distinct sources.

### Boundary validation (synchronous, before any task runs)

Validation failures carry a category under `:clj-r2dbc/error` and the operation
under `:clj-r2dbc/context`, plus operation-specific keys.

| `:clj-r2dbc/error` | Meaning |
|---|---|
| `:clj-r2dbc/missing-key` | a required value (`db`, `sql`) was nil |
| `:clj-r2dbc/invalid-type` | a value had the wrong type (e.g. non-string SQL, non-fn `:builder`) |
| `:clj-r2dbc/invalid-value` | a value was out of range or blank (e.g. `:fetch-size 0`) |
| `:clj-r2dbc/unsupported` | conflicting options (e.g. both `:middleware` and `:interceptors`) |
| `:clj-r2dbc/limit-exceeded` | a bound was exceeded (`:max-batch-size`, `:max-interceptor-depth`) |
| `:clj-r2dbc/missing-dependency` | a required library (e.g. `io.r2dbc/r2dbc-postgresql`) is not on the classpath |

The `:clj-r2dbc/context` key names the operation that failed:

| `:clj-r2dbc/context` | Operation |
|---|---|
| `:clj-r2dbc/execute` | `execute` query |
| `:clj-r2dbc/stream` | `stream` query |
| `:clj-r2dbc/execute-each` | `execute-each` batch query |
| `:clj-r2dbc/pipeline` | interceptor pipeline |
| `:clj-r2dbc/row-builder` | row-builder factory (`:->qualified-kebab-maps`, etc.) |
| `:clj-r2dbc/transaction` | `with-transaction` configuration |
| `:clj-r2dbc/oracle` | Oracle dialect interceptor |
| `:clj-r2dbc/postgresql` | PostgreSQL dialect interceptor |

```clojure
;; out-of-range :fetch-size
(try (m/? (execute db "SELECT 1" {:fetch-size 0}))
     (catch Exception e (ex-data e)))
;; => {:clj-r2dbc/error :clj-r2dbc/invalid-value
;;     :clj-r2dbc/context :clj-r2dbc/execute
;;     :key :fetch-size, :value 0, :min 1, :max 32768}

;; binding-set count over the limit
(try (m/? (execute-each db "SELECT $1" [[1] [2] [3]] {:max-batch-size 2}))
     (catch Exception e (ex-data e)))
;; => {:clj-r2dbc/error :clj-r2dbc/limit-exceeded
;;     :clj-r2dbc/context :clj-r2dbc/execute-each
;;     :key :param-sets, :constraint :max-batch-size, :limit 2, :value 3}
```

### Driver errors (inside the task, on `R2dbcException`)

Driver exceptions are translated to `ex-info` with a stable category and the
statement that caused them. The original `R2dbcException` is the cause.

```clojure
(try (m/? (execute db "NOT VALID SQL"))
     (catch Exception e (ex-data e)))
;; => {:clj-r2dbc/error-category :bad-grammar
;;     :sql-state "42000", :error-code 42000
;;     :sql "NOT VALID SQL", :params []}
```

| `R2dbcException` subclass | `:clj-r2dbc/error-category` |
|---|---|
| `R2dbcBadGrammarException` | `:bad-grammar` |
| `R2dbcDataIntegrityViolationException` | `:integrity` |
| `R2dbcNonTransientResourceException` | `:non-transient-resource` |
| `R2dbcPermissionDeniedException` | `:permission` |
| `R2dbcRollbackException` | `:rollback` |
| `R2dbcTimeoutException` | `:timeout` |
| `R2dbcTransientResourceException` | `:transient-resource` |
| `R2dbcNonTransientException` | `:non-transient` |
| `R2dbcTransientException` | `:transient` |
| (any other) | `:unknown` |

`missionary.Cancelled` is never caught or translated; it always propagates to the
Missionary scheduler.

## Connection-level defaults with `with-options`

`with-options` wraps a connectable (`ConnectionFactory`, `Connection`, or an
existing wrapper) with a default opts map:

```clojure
(def db (-> (connect "r2dbc:h2:mem:///test")
            (with-options {:fetch-size 256
                           :max-batch-size 50000})))
```

- **Merge semantics:** per-call opts win; nil values in per-call opts do not
  clobber defaults.
- **Nesting:** `with-options` is nestable — inner defaults override outer
  defaults.
- **Open system:** any key placed in defaults flows through to all entry points.
  Future option keys work without changes to `with-options`.

## Dialect interceptors

Dialect interceptors are optional enhancements for drivers with known
type-mapping quirks. Wire them via `:interceptors`.

| Namespace | Var | Kind | Purpose |
|---|---|---|---|
| `clj-r2dbc.dialect.h2` | `h2-array-interceptor` | interceptor map | H2 `ARRAY` columns → vectors; `byte[]` preserved |
| `clj-r2dbc.dialect.postgresql` | `pg-array-interceptor` | interceptor map | PostgreSQL arrays → vectors |
| `clj-r2dbc.dialect.postgresql` | `pg-json-interceptor` | factory `(fn [{:keys [json->clj]}])` | PostgreSQL JSON/JSONB → Clojure via the supplied `:json->clj` |
| `clj-r2dbc.dialect.mysql` | `mysql-type-interceptor` | interceptor map | MySQL `Float`/`Double` → `BigDecimal` |
| `clj-r2dbc.dialect.mariadb` | `mariadb-type-interceptor` | interceptor map | MariaDB `Float`/`Double` → `BigDecimal` |
| `clj-r2dbc.dialect.oracle` | `oracle-lob-interceptor` | interceptor map | Oracle LOB params at the 32768-byte default threshold |
| `clj-r2dbc.dialect.oracle` | `oracle-lob-interceptor-with` | factory `(fn [{:keys [lob-threshold-bytes]}])` | Oracle LOB params at a custom threshold |

`pg-json-interceptor` and `oracle-lob-interceptor-with` are factories — call them
to obtain an interceptor map:

```clojure
(require '[clj-r2dbc.dialect.postgresql :as pg]
         '[clj-r2dbc.dialect.oracle :as oracle])

(pg/pg-json-interceptor {:json->clj #(json/read-str % :key-fn keyword)})
(oracle/oracle-lob-interceptor-with {:lob-threshold-bytes 65536})
```

## Driver compatibility

clj-r2dbc works with any R2DBC driver through the standard SPI. Dialect
interceptors are optional.

| Driver | Dialect namespace | Notes |
|---|---|---|
| r2dbc-h2 | `clj-r2dbc.dialect.h2` | `ARRAY` column coercion |
| r2dbc-postgresql | `clj-r2dbc.dialect.postgresql` | array and JSON coercion |
| r2dbc-mariadb | `clj-r2dbc.dialect.mariadb` | `Float`/`Double` precision |
| r2dbc-mysql | `clj-r2dbc.dialect.mysql` | `Float`/`Double` precision |
| oracle-r2dbc | `clj-r2dbc.dialect.oracle` | LOB parameter handling |
| r2dbc-mssql | (none needed) | all types map to standard Java types |
| jasync-sql | (reuse pg/mysql) | alternative PostgreSQL/MySQL driver |
| cloud-spanner-r2dbc | (none needed) | standard SPI for basic types |
| clickhouse-r2dbc | (none needed) | standard SPI for basic types |
| r2dbc-pool | (none needed) | connection pooling layer |
| r2dbc-proxy | (none needed) | debugging proxy layer |

## Real-database proof hooks

- `clojure -M:integration` runs optional PostgreSQL and MariaDB cancellation and
  early-termination tests when `CLJ_R2DBC_TEST_PG_URL` and/or
  `CLJ_R2DBC_TEST_MARIADB_URL` are set.
- `clojure -M:tck` includes matching optional real-driver proofs so the TCK
  layer validates more than the in-memory H2 contract.

## AOT compilation and uberjars

clj-r2dbc is AOT-safe. Consumer applications can AOT-compile their code (which
transitively compiles clj-r2dbc) and package the result into uberjars without
special configuration.

### What clj-r2dbc ships

A standard thin JAR containing only `.clj` source files and a `pom.xml`. Zero
`.class` files, verified in CI. clj-r2dbc is never distributed as an
AOT-compiled artifact or uberjar — the consumer's build handles compilation.

### tools.build

A main namespace needs `(:gen-class)` to produce a runnable uberjar. Isolate
`gen-class` in a dedicated namespace (e.g. `your.app.main`) to limit the AOT
surface.

```clojure
;; build.clj
(b/compile-clj {:basis      (b/create-basis {:aliases [:build]})
                :src-dirs   ["src"]
                :class-dir  "target/classes"
                :ns-compile ['your.app.main]
                :sort       :topo})
(b/uber {:class-dir "target/classes"
         :uber-file "target/your-app.jar"
         :basis     (b/create-basis {:aliases [:build]})
         :main      'your.app.main})
```

### Leiningen

```clojure
;; project.clj
:aot  ['your.app.main]
:main 'your.app.main
```

Or to AOT everything: `:aot :all`.

### Compiler options

- `-Dclojure.compiler.direct-linking=true` — replaces var deref with static
  invocation for faster startup and smaller classes.
- `-Dclojure.compiler.elide-meta=[:doc :file :line :added]` — removes metadata
  from compiled classes for smaller artifacts.

See [Clojure AOT Compilation](https://clojure.org/reference/compilation) for the
full reference.

### Known patterns

`impl/coerce.clj` calls `(Class/forName "[B")` at top level to cache the JVM
byte-array class. `[B` is a JVM intrinsic — always available, zero side effects.
This runs during compilation but is deterministic and safe.

### What to avoid

- Do not pre-AOT clj-r2dbc separately and then include the compiled classes
  alongside source. This risks classloader mismatches. Let the build tool's
  transitive compilation handle it.
- Do not add top-level side effects (e.g. `(def db (connect "..."))`) in
  namespaces that require clj-r2dbc. These run during AOT compilation and crash
  the build.
- Ensure every namespace the code uses is explicitly declared in its `ns` form.
  Incidental transitive loads may work in a REPL but fail under strict AOT
  compilation.
