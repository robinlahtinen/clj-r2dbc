(ns clj-r2dbc.test-runner.tck
  (:require
   [cognitect.test-runner :as test-runner]))

(defn -main
  [& args]
  (apply test-runner/-main (concat ["-n" "clj-r2dbc.tck.r2dbc-tck-test"] args)))
