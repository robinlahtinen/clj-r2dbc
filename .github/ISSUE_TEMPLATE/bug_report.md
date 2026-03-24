---
name: Bug report
about: Report a bug in clj-r2dbc
title: "[BUG] "
labels: bug
assignees: ""
---

## Describe the bug

Describe the bug clearly and concisely.

## To reproduce

```clojure
(require '[clj-r2dbc :as r2dbc]
         '[missionary.core :as m])

(def db (r2dbc/connect "r2dbc:h2:mem:///testdb"))

;; m/? runs the task and blocks until it completes (like deref for tasks).
(m/? (r2dbc/execute db "SELECT ..." {:params [...]}))
```

## Expected behavior

Describe what you expected to happen.

## Actual behavior

Describe what actually happened, including any error messages or stack traces.

<details>
<summary>Stack trace (if applicable)</summary>

```
Paste full stack trace here
```

</details>

## Environment

Check your `deps.edn` for version numbers.

| Component            | Version                                  |
|----------------------|------------------------------------------|
| **clj-r2dbc**        | e.g., 0.1.0                              |
| **R2DBC driver**     | e.g., r2dbc-postgresql 1.1.1.RELEASE     |
| **Missionary**       | e.g., b.47                               |
| **Clojure**          | e.g., 1.12.4                             |
| **Java**             | e.g., OpenJDK 21.0.2                     |
| **Operating system** | e.g., Ubuntu 24.04, Windows 11, macOS 14 |

## Error data (if applicable)

If an exception occurred, paste the `ex-data` map here. In a Clojure REPL, `*e` is automatically bound to the last exception. Run `(ex-data *e)` to extract its error data map.

<details>
<summary>ex-data from the exception</summary>

```clojure
(ex-data *e)
;; => {:clj-r2dbc/error-category :integrity
;;     :sql "INSERT INTO ..."
;;     :params [...]
;;     :sql-state "23505"
;;     :error-code 0}
```

</details>

## Additional context

Add any other context about the problem here.
