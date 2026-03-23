;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.impl.validate
  "Boundary validation for clj-r2dbc public entry points.

  All functions are pure: no side effects, no Missionary scheduling, and no
  dependency on concrete R2DBC runtime values. Each function throws ex-info on
  failure using the stable public error contract:

    {:clj-r2dbc/error   <category-keyword>
     :clj-r2dbc/context <operation-keyword>
     ...operation-specific keys...}

  This namespace is an implementation detail; do not use from application code."
  (:require
   [clojure.string :as str]))

(set! *warn-on-reflection* true)

(defn check-mode!
  "Throw ex-info when both :middleware and :interceptors are supplied.

  Exactly one execution mode may be active per call.

  Args:
    opts    - options map to check.
    context - (optional) keyword used as :clj-r2dbc/context; defaults to
              :clj-r2dbc/execute.

  Throws (synchronously):
    ex-info :clj-r2dbc/unsupported when both :middleware and :interceptors are present."
  ([opts] (check-mode! opts :clj-r2dbc/execute))
  ([opts context]
   (when (and (contains? opts :middleware) (contains? opts :interceptors))
     (throw (ex-info "Cannot supply both :middleware and :interceptors"
                     {:clj-r2dbc/error   :clj-r2dbc/unsupported
                      :clj-r2dbc/context context
                      :keys              [:middleware :interceptors]})))))

(defn require-map!
  "Throw ex-info when v is not a map.

  Args:
    v       - value to check.
    name    - parameter name used in the error message.
    context - keyword used as :clj-r2dbc/context in thrown ex-info.

  Throws (synchronously):
    ex-info :clj-r2dbc/invalid-type when v is not a map."
  [v name context]
  (when-not (map? v)
    (throw (ex-info (str name " must be a map")
                    {:clj-r2dbc/error   :clj-r2dbc/invalid-type
                     :clj-r2dbc/context context
                     :name              name
                     :value             (type v)}))))

(defn require-non-nil!
  "Throw ex-info when v is nil.

  Args:
    v       - value to check.
    name    - parameter name used in the error message.
    context - keyword used as :clj-r2dbc/context in thrown ex-info.

  Throws (synchronously):
    ex-info :clj-r2dbc/missing-key when v is nil."
  [v name context]
  (when (nil? v)
    (throw (ex-info (str name " must not be nil")
                    {:clj-r2dbc/error   :clj-r2dbc/missing-key
                     :clj-r2dbc/context context
                     :name              name}))))

(defn require-non-blank-string!
  "Throw ex-info when v is not a non-blank string.

  Args:
    v       - value to check.
    name    - parameter name used in the error message.
    context - keyword used as :clj-r2dbc/context in thrown ex-info.

  Throws (synchronously):
    ex-info :clj-r2dbc/missing-key   when v is nil.
    ex-info :clj-r2dbc/invalid-type  when v is not a string.
    ex-info :clj-r2dbc/invalid-value when v is blank."
  [v name context]
  (require-non-nil! v name context)
  (cond (not (string? v)) (throw (ex-info (str name " must be a string")
                                          {:clj-r2dbc/error
                                           :clj-r2dbc/invalid-type
                                           :clj-r2dbc/context      context
                                           :name                   name
                                           :value                  v}))
        (str/blank? v) (throw (ex-info (str name " must not be blank")
                                       {:clj-r2dbc/error
                                        :clj-r2dbc/invalid-value
                                        :clj-r2dbc/context       context
                                        :name                    name
                                        :value                   v}))))

(defn require-fn-val!
  "Throw ex-info when v is not a function.

  Args:
    v       - value to check.
    name    - parameter name used in the error message.
    context - keyword used as :clj-r2dbc/context in thrown ex-info.

  Throws (synchronously):
    ex-info :clj-r2dbc/invalid-type when v is not a fn."
  [v name context]
  (when-not (fn? v)
    (throw (ex-info (str name " must be a function")
                    {:clj-r2dbc/error   :clj-r2dbc/invalid-type
                     :clj-r2dbc/context context
                     :name              name
                     :value             (type v)}))))

(defn require-sequential-val!
  "Throw ex-info when v is not sequential.

  Args:
    v       - value to check.
    name    - parameter name used in the error message.
    context - keyword used as :clj-r2dbc/context in thrown ex-info.

  Throws (synchronously):
    ex-info :clj-r2dbc/invalid-type when v is not sequential."
  [v name context]
  (when-not (sequential? v)
    (throw (ex-info (str name " must be sequential")
                    {:clj-r2dbc/error   :clj-r2dbc/invalid-type
                     :clj-r2dbc/context context
                     :name              name
                     :value             (type v)}))))

(defn reject-key!
  "Throw ex-info when opts contains k.

  Used to reject removed or renamed public keys.

  Args:
    opts    - options map to check.
    k       - key to reject.
    context - keyword used as :clj-r2dbc/context in thrown ex-info.
    message - error message string.

  Throws (synchronously):
    ex-info :clj-r2dbc/unsupported when k is present in opts."
  [opts k context message]
  (when (contains? opts k)
    (throw (ex-info message
                    {:clj-r2dbc/error   :clj-r2dbc/unsupported
                     :clj-r2dbc/context context
                     :key               k}))))

(defn require-int-range!
  "Throw ex-info when key k is present in opts but not an integer within [min max].

  Args:
    opts      - options map to check.
    k         - key whose value to validate.
    min-value - minimum allowed integer value (inclusive).
    max-value - maximum allowed integer value (inclusive).
    context   - keyword used as :clj-r2dbc/context in thrown ex-info.

  Throws (synchronously):
    ex-info :clj-r2dbc/invalid-type  when the value is not an integer.
    ex-info :clj-r2dbc/invalid-value when the value is outside [min max]."
  [opts k min-value max-value context]
  (when-some [v (get opts k)]
    (cond (not (integer? v)) (throw (ex-info (str k " must be an integer")
                                             {:clj-r2dbc/error
                                              :clj-r2dbc/invalid-type
                                              :clj-r2dbc/context      context
                                              :key                    k
                                              :value                  v}))
          (or (< (long v) (long min-value)) (> (long v) (long max-value)))
          (throw (ex-info
                  (str k " must be between " min-value " and " max-value)
                  {:clj-r2dbc/error   :clj-r2dbc/invalid-value
                   :clj-r2dbc/context context
                   :key               k
                   :value             v
                   :min               min-value
                   :max               max-value})))))

(defn require-keyword-in!
  "Throw ex-info when key k is present in opts but not one of allowed values.

  Args:
    opts    - options map to check.
    k       - key whose value to validate.
    allowed - set of allowed keyword values.
    context - keyword used as :clj-r2dbc/context in thrown ex-info.

  Throws (synchronously):
    ex-info :clj-r2dbc/invalid-value when the value is not in allowed."
  [opts k allowed context]
  (when-some [v (get opts k)]
    (when-not (contains? allowed v)
      (throw (ex-info (str k " has unsupported value")
                      {:clj-r2dbc/error   :clj-r2dbc/invalid-value
                       :clj-r2dbc/context context
                       :key               k
                       :value             v
                       :allowed           (sort allowed)})))))

(defn require-string!
  "Throw ex-info if k is absent from opts or its value is not a non-blank string."
  [opts k context]
  (let [v (get opts k ::absent)]
    (cond (= v ::absent) (throw (ex-info (str "Required key " k " is missing")
                                         {:clj-r2dbc/error
                                          :clj-r2dbc/missing-key
                                          :clj-r2dbc/context     context
                                          :key                   k}))
          (not (string? v)) (throw (ex-info (str k " must be a string")
                                            {:clj-r2dbc/error
                                             :clj-r2dbc/invalid-type
                                             :clj-r2dbc/context      context
                                             :key                    k
                                             :value                  v}))
          (str/blank? v) (throw (ex-info (str k " must not be blank")
                                         {:clj-r2dbc/error
                                          :clj-r2dbc/invalid-value
                                          :clj-r2dbc/context       context
                                          :key                     k
                                          :value                   v})))))

(defn require-fn!
  "Throw ex-info if k is present in opts but its value is not a function."
  [opts k context]
  (when-let [v (get opts k)]
    (when-not (fn? v)
      (throw (ex-info (str k " must be a function")
                      {:clj-r2dbc/error   :clj-r2dbc/invalid-type
                       :clj-r2dbc/context context
                       :key               k
                       :value             (type v)})))))

(defn require-sequential!
  "Throw ex-info if k is present in opts but its value is not sequential."
  [opts k context]
  (when-let [v (get opts k)]
    (when-not (sequential? v)
      (throw (ex-info (str k " must be sequential")
                      {:clj-r2dbc/error   :clj-r2dbc/invalid-type
                       :clj-r2dbc/context context
                       :key               k
                       :value             (type v)})))))

(defn- normalize-builder-opts
  [opts context]
  (reject-key! opts
               :builder-fn
               context
               "The :builder-fn option has been renamed to :builder.")
  (require-fn! opts :builder context)
  (cond-> (dissoc opts :builder)
    (:builder opts) (assoc :builder-fn (:builder opts))))

(defn validate-common-query-opts!
  "Validate opts keys common to execute, first-row, stream, and execute-each.

  Checks :params, :fetch-size, :builder, :middleware, and :interceptors.
  Throws ex-info for any invalid value.

  Args:
    opts    - map of caller-supplied options.
    context - keyword used as :clj-r2dbc/context in thrown ex-info.

  Returns the normalized opts map (with :builder renamed to :builder-fn)."
  [opts context]
  (require-map! opts "opts" context)
  (check-mode! opts context)
  (require-sequential! opts :params context)
  (require-sequential! opts :middleware context)
  (require-sequential! opts :interceptors context)
  (require-int-range! opts :fetch-size 1 32768 context)
  (normalize-builder-opts opts context))

(defn execute-opts
  "Validate and normalize opts for clj-r2dbc/execute.

  Delegates to validate-common-query-opts! and checks execute-specific keys.

  Args:
    opts    - raw options map from the caller.
    context - keyword used as :clj-r2dbc/context in thrown ex-info.

  Returns the validated opts map unchanged."
  [opts context]
  (validate-common-query-opts! opts context))

(defn stream-opts
  "Validate and normalize opts for clj-r2dbc/stream.

  Checks stream-specific keys in addition to common query opts.

  Args:
    opts - raw options map from the caller.

  Returns the validated opts map unchanged."
  [opts]
  (let [context :clj-r2dbc/stream
        opts'   (validate-common-query-opts! opts context)]
    (require-int-range! opts' :chunk-size 1 32768 context)
    (require-keyword-in! opts' :stream-mode #{:immutable :flyweight} context)
    opts'))

(defn execute-each-opts
  "Validate and normalize opts for clj-r2dbc/execute-each.

  Checks execute-each-specific keys in addition to common query opts.

  Args:
    opts - raw options map from the caller.

  Returns the validated opts map unchanged."
  [opts]
  (let [context :clj-r2dbc/execute-each
        opts'   (validate-common-query-opts! opts context)]
    (require-int-range! opts' :max-batch-size 1 Integer/MAX_VALUE context)
    opts'))

(defn batch-opts
  "Validate and normalize opts for clj-r2dbc/batch.

  Args:
    opts - raw options map from the caller.

  Returns the validated opts map unchanged."
  [opts]
  (let [context :clj-r2dbc/batch]
    (require-map! opts "opts" context)
    (check-mode! opts context)
    opts))

(comment
  (check-mode! {:middleware [identity]})
  (check-mode! {:middleware [identity], :interceptors []})
  (require-string! {:sql ""} :sql :execute))
