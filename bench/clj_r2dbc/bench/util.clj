;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.bench.util
  "Shared utilities for extracting and serializing Criterium bench results.

  Provides extract-result, which captures the last bench run's data map and
  returns a serialization-safe summary with per-invocation mean/variance."
  (:require
   [criterium.bench :as bench]
   [criterium.util.helpers :as helpers]))

(set! *warn-on-reflection* true)

(def default-bench-plan
  "Standard bench-plan for all bench-measured functions.
  criterium/options->bench-plan returns a plain config map; evaluated once at load time."
  (bench/options->bench-plan :viewer :none :limit-time-s 10))

(defn extract-result
  "Extract a serialization-safe result map from the last bench run.

  Returns a map with:
    :mean-ns     - mean elapsed time per invocation in nanoseconds (transformed)
    :variance-ns - variance of elapsed time per invocation in nanoseconds (transformed),
                   or 0.0 for single-sample (:one-shot) results
    :batch-size  - number of invocations per sample
    :num-samples - number of samples collected

  Handles both multi-sample results (:with-jit-warmup, :stats present) and
  single-sample results (:one-shot, no :stats). For :one-shot, the raw elapsed
  time is read directly from [:samples :metric->values [:elapsed-time]]; the
  transform is always identity and batch-size is always 1, so no correction is
  needed."
  []
  (let [{:keys [data]} (bench/last-bench)]
    (if (contains? data :stats)
      {:mean-ns     (helpers/stats-value data :stats :elapsed-time :mean)
       :variance-ns (helpers/stats-value data :stats :elapsed-time :variance)
       :batch-size  (get-in data [:samples :batch-size])
       :num-samples (get-in data [:samples :num-samples])}
      {:mean-ns     (first (get-in data [:samples :metric->values [:elapsed-time]]))
       :variance-ns 0.0
       :batch-size  (get-in data [:samples :batch-size])
       :num-samples (get-in data [:samples :num-samples])})))
