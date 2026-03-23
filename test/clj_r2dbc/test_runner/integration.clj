(ns clj-r2dbc.test-runner.integration
  (:require
   [cognitect.test-runner :as test-runner]))

(defn -main
  [& args]
  (apply test-runner/-main
         (concat ["-n" "clj-r2dbc.integration.h2-test" "-n"
                  "clj-r2dbc.integration.mariadb-test" "-n"
                  "clj-r2dbc.integration.postgresql-test"]
                 args)))
