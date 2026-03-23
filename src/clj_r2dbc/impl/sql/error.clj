;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.sql.error
  "R2DBC exception translation utilities for clj-r2dbc.

  Provides:
    r2dbc-ex->ex-info - converts R2dbcException to Clojure ex-info with standard keys.
    wrap-error        - wraps a Missionary task, translating R2dbcException on failure.

  All R2DBC driver exceptions are mapped to stable :clj-r2dbc/error-category keywords.
  Missionary Cancelled is never swallowed.

  This namespace is an implementation detail; do not use from application code."
  (:require
   [missionary.core :as m])
  (:import
   (io.r2dbc.spi R2dbcBadGrammarException R2dbcDataIntegrityViolationException R2dbcException R2dbcNonTransientException R2dbcNonTransientResourceException R2dbcPermissionDeniedException R2dbcRollbackException R2dbcTimeoutException R2dbcTransientException R2dbcTransientResourceException)
   (missionary Cancelled)))

(set! *warn-on-reflection* true)

(defn- r2dbc-category
  "Map an R2dbcException to a stable category keyword.

  Most-specific subclasses are matched before their parents to ensure
  correct categorization.

  Args:
    ex - the R2dbcException to categorize.

  Returns a keyword: :bad-grammar, :integrity, :non-transient-resource,
  :permission, :rollback, :timeout, :transient-resource, :non-transient,
  :transient, or :unknown."
  [^R2dbcException ex]
  (condp instance? ex
    R2dbcBadGrammarException :bad-grammar
    R2dbcDataIntegrityViolationException :integrity
    R2dbcNonTransientResourceException :non-transient-resource
    R2dbcPermissionDeniedException :permission
    R2dbcRollbackException :rollback
    R2dbcTimeoutException :timeout
    R2dbcTransientResourceException :transient-resource
    R2dbcNonTransientException :non-transient
    R2dbcTransientException :transient
    :unknown))

(defn r2dbc-ex->ex-info
  "Convert an R2dbcException to a Clojure ex-info with standard keys.

  Args:
    ex - the R2dbcException to convert.

  Returns an ExceptionInfo with:
    :clj-r2dbc/error-category - keyword category (see r2dbc-category).
    :sql-state                - SQL state string from the exception.
    :error-code               - driver-specific error code integer.

  The original R2dbcException is set as the cause.

  Example:
    (r2dbc-ex->ex-info (R2dbcBadGrammarException. \"msg\" \"42000\" 1234 nil))"
  [^R2dbcException ex]
  (ex-info (.getMessage ex)
           {:clj-r2dbc/error-category (r2dbc-category ex)
            :sql-state                (.getSqlState ex)
            :error-code               (.getErrorCode ex)}
           ex))

(defn wrap-error
  "Wrap a Missionary task, translating any R2dbcException to an ex-info on failure.

  Missionary Cancelled is re-thrown immediately and is never translated.
  Any other exception passes through unchanged.

  Args:
    task - the Missionary task to wrap.

  Returns a task that resolves to the same value on success, or rejects with
  an ex-info (from r2dbc-ex->ex-info) when an R2dbcException is thrown.

  Example:
    (m/? (wrap-error (m/sp (throw (R2dbcTimeoutException. \"timed out\" \"HYT00\" 0 nil)))))"
  [task]
  (m/sp (try (m/? task)
             (catch Cancelled c (throw c))
             (catch R2dbcException ex (throw (r2dbc-ex->ex-info ex))))))

(comment
  (r2dbc-ex->ex-info (R2dbcBadGrammarException. "bad sql" "42000" 1234 nil))
  (m/? (wrap-error (m/sp (throw (R2dbcTimeoutException. "timed out" "HYT00"
                                                        0 nil))))))
