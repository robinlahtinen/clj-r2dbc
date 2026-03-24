;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns build
  "Build, test, lint, and release tasks for the clj-r2dbc library.

  Run with clojure -T:build <task-name>. Pass :release true for release
  versions. The default is a SNAPSHOT build."
  (:refer-clojure :exclude [test])
  (:require
   [clojure.java.io :as io]
   [clojure.tools.build.api :as b]
   [deps-deploy.deps-deploy :as dd])
  (:import
   (java.util.zip ZipFile)))

(def lib 'com.github.robinlahtinen/clj-r2dbc)
(def version "0.1.0")

(defn- run-command
  "Run a command line process and throw if it fails."
  [{:keys [command-args error-message]}]
  (let [{:keys [exit]} (b/process {:command-args command-args})]
    (when-not (zero? exit) (throw (ex-info error-message {})))))

(defn- snapshot-version
  "Append -SNAPSHOT suffix for development builds."
  []
  (str version "-SNAPSHOT"))

(defn- get-version
  "Return release version or snapshot version based on opts."
  [opts]
  (if (:release opts) version (snapshot-version)))

(def class-dir "target/classes")

(defn lint
  "Run clj-kondo over source, test, bench, and build code."
  [opts]
  (run-command {:command-args  ["clojure" "-M:lint"]
                :error-message "Lint failed"})
  opts)

(defn format-check
  "Verify cljfmt formatting across source, test, bench, and build code."
  [opts]
  (run-command {:command-args  ["clojure" "-T:format" "check"]
                :error-message "Format check failed"})
  opts)

(defn test
  "Run unit and property tests. Excludes integration and TCK scopes."
  [opts]
  (let [basis          (b/create-basis {:aliases [:test]})
        cmds           (b/java-command {:basis     basis
                                        :main      'clojure.main
                                        :main-args ["-m" "cognitect.test-runner" "-e"
                                                    ":integration" "-e" ":tck"]})
        {:keys [exit]} (b/process cmds)]
    (when-not (zero? exit) (throw (ex-info "Tests failed" {}))))
  opts)

(defn integration-test
  "Run the integration test scope only."
  [opts]
  (run-command {:command-args  ["clojure" "-M:integration"]
                :error-message "Integration tests failed"})
  opts)

(defn tck-test
  "Run the TCK test scope only."
  [opts]
  (run-command {:command-args  ["clojure" "-M:tck"]
                :error-message "TCK tests failed"})
  opts)

(defn aot-check
  "AOT-compile all source namespaces in a subprocess. Verify that compilation
  succeeds without error. Does not ship compiled artifacts; output is
  discarded. This is a safety gate, not a build step."
  [opts]
  (b/delete {:path "target/aot-check"})
  (b/compile-clj {:basis     (b/create-basis {})
                  :src-dirs  ["src"]
                  :class-dir "target/aot-check"
                  :sort      :topo})
  (println "AOT check passed: all namespaces compiled successfully.")
  (b/delete {:path "target/aot-check"})
  opts)

(defn uberjar-check
  "Simulate a downstream consumer AOT-compiling against clj-r2dbc.
  Write a minimal consumer namespace, compile it with clj-r2dbc on the
  classpath, and verify that compilation succeeds. Output is discarded."
  [opts]
  (let [consumer-dir "target/uberjar-check/consumer-src"
        class-dir    "target/uberjar-check/classes"]
    (b/delete {:path "target/uberjar-check"})
    (io/make-parents (str consumer-dir "/test/aot/consumer.clj"))
    (spit (str consumer-dir "/test/aot/consumer.clj")
          (str "(ns test.aot.consumer\n"
               "  (:require [clj-r2dbc :as r2dbc]\n"
               "            [clj-r2dbc.row :as row]\n"
               "            [clj-r2dbc.interceptor :as i]\n"
               "            [clj-r2dbc.middleware :as mw]))\n"
               "(defn consumer-fn []\n"
               "  [r2dbc/connect r2dbc/execute r2dbc/stream\n"
               "   r2dbc/with-connection r2dbc/with-transaction\n"
               "   row/kebab-maps i/logging-interceptor mw/with-logging])\n"))
    (b/compile-clj {:basis      (b/create-basis {:extra {:paths [consumer-dir]}})
                    :class-dir  class-dir
                    :ns-compile ['test.aot.consumer]})
    (println "Uberjar check passed: consumer AOT compilation succeeded.")
    (b/delete {:path "target/uberjar-check"}))
  opts)

(defn test-all
  "Run all correctness checks: unit tests, integration tests, TCK compliance,
  AOT compilation safety, and uberjar consumer compatibility.

  Integration and TCK tests depend on CLJ_R2DBC_TEST_PG_URL and
  CLJ_R2DBC_TEST_MARIADB_URL being present in the environment; this task does
  not inject credentials on your behalf."
  [opts]
  (test opts)
  (integration-test opts)
  (tck-test opts)
  (aot-check opts)
  (uberjar-check opts)
  opts)

(defn bench
  "Run the benchmark suite.

  Uses Clojure exec to invoke the :exec-fn declared in the :bench alias,
  which ensures :jvm-opts (Criterium blackhole, heap sizing) are respected."
  [opts]
  (run-command {:command-args  (cond-> ["clojure" "-X:bench"]
                                 (:only opts) (into [":only" (pr-str (:only opts))]))
                :error-message "Benchmarks failed"})
  opts)

(defn docs
  "Generate Codox documentation using the project classpath.
  Pass the version derived from opts so docs always reflect the actual build version."
  [opts]
  (run-command {:command-args  ["clojure" "-X:docs" ":version" (pr-str (get-version opts))]
                :error-message "Docs generation failed"})
  opts)

(defn verify
  "Run the full repository verification matrix for local development.

  Order is intentional: format check first, then lint, then all correctness
  checks (test-all), then benchmarks, then docs.
  Integration and TCK tests within test-all depend on CLJ_R2DBC_TEST_PG_URL and
  CLJ_R2DBC_TEST_MARIADB_URL being present in the environment; this task does
  not inject credentials on your behalf."
  [opts]
  (format-check opts)
  (lint opts)
  (test-all opts)
  (bench opts)
  (docs opts)
  opts)

(defn- pom-template
  [version]
  [[:description
    "A modern low-level Clojure wrapper for non-blocking, R2DBC-based access to databases."]
   [:url "https://github.com/robinlahtinen/clj-r2dbc"] [:inceptionYear "2026"]
   [:licenses
    [:license [:name "MIT"] [:url "https://opensource.org/licenses/MIT"]]]
   [:developers
    [:developer [:id "robinlahtinen"] [:name "Robin Lahtinen"]
     [:url "https://github.com/robinlahtinen"]]]
   [:scm [:url "https://github.com/robinlahtinen/clj-r2dbc"]
    [:connection "scm:git:https://github.com/robinlahtinen/clj-r2dbc.git"]
    [:developerConnection
     "scm:git:ssh:git@github.com:robinlahtinen/clj-r2dbc.git"]
    [:tag (str "v" version)]]
   [:issueManagement [:system "GitHub issues"]
    [:url "https://github.com/robinlahtinen/clj-r2dbc/issues"]]])

(defn- verify-no-class-files!
  "Assert that the JAR at jar-path contains zero .class files.
  Prevents accidental inclusion of AOT-compiled artifacts in the thin JAR."
  [jar-path]
  (with-open [zf (ZipFile. (str jar-path))]
    (let [class-entries (->> (enumeration-seq (.entries zf))
                             (filter #(.endsWith (.getName %) ".class"))
                             (mapv #(.getName %)))]
      (when (seq class-entries)
        (throw (ex-info
                "JAR contains .class files; source-only JAR invariant violated"
                {:class-files class-entries, :jar-file (str jar-path)}))))))

(defn- jar-opts
  [opts]
  (let [v (get-version opts)]
    (assoc opts
           :lib lib
           :version v
           :jar-file (format "target/%s-%s.jar" lib v)
           :basis (b/create-basis {})
           :class-dir class-dir
           :target "target"
           :src-dirs ["src"]
           :pom-data (pom-template v))))

(defn build-jar
  "Write POM, copy source, build JAR, and verify the source-only invariant.
  Does not run tests. Use test-all for correctness checks."
  [opts]
  (b/delete {:path "target"})
  (let [opts (jar-opts opts)]
    (println "\nVersion:" (:version opts))
    (println "\nWriting pom.xml...")
    (b/write-pom opts)
    (println "\nCopying source...")
    (b/copy-dir {:src-dirs ["src"] :target-dir class-dir})
    (println "\nBuilding JAR..." (:jar-file opts))
    (b/jar opts)
    (println "\nVerifying source-only JAR...")
    (verify-no-class-files! (:jar-file opts)))
  opts)

(defn ci
  "Build the JAR for deployment. Does not run tests; use test-all for that."
  [opts]
  (build-jar opts))

(defn install
  "Install the JAR locally. Run build-jar first."
  [opts]
  (let [opts (jar-opts opts)]
    (b/install opts))
  opts)

(defn deploy
  "Deploy the JAR to Clojars. Run build-jar first."
  [opts]
  (let [{:keys [jar-file] :as opts} (jar-opts opts)]
    (dd/deploy {:installer :remote
                :artifact  (b/resolve-path jar-file)
                :pom-file  (b/pom-path (select-keys opts [:lib :class-dir]))}))
  opts)
