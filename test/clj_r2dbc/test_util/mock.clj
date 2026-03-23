;;  Copyright (c) Robin Lahtinen and contributors. All rights reserved.
;;  Licensed under the MIT License. See LICENSE in the project root for license information.

(ns clj-r2dbc.test-util.mock
  "Thin construction helpers around r2dbc-spi-test mock classes.
  Provides factory functions for pre-configured mock objects used
  in unit tests of impl.* namespaces. No logic. Only boilerplate reduction."
  (:import
   (io.r2dbc.spi Batch Connection ConnectionFactory ConnectionFactoryMetadata ConnectionMetadata IsolationLevel Nullability Result Result$Segment Row Statement TransactionDefinition ValidationDepth)
   (io.r2dbc.spi.test MockBatch MockBatch$Builder MockBlob MockClob MockColumnMetadata MockColumnMetadata$Builder MockConnection MockConnection$Builder MockConnectionFactory MockOutParameters MockOutParameters$Builder MockResult MockResult$Builder MockRow MockRow$Builder MockRowMetadata MockRowMetadata$Builder MockStatement MockStatement$Builder MockType)
   (java.nio ByteBuffer)
   (java.time Duration)
   (java.util.concurrent CompletableFuture)
   (org.reactivestreams Publisher Subscriber Subscription)))

(set! *warn-on-reflection* true)

(defn mock-type
  "Create a MockType from a Java class and SQL type name string.

  (mock-type String \"VARCHAR\")
  (mock-type Integer \"INTEGER\")"
  ^MockType [^Class java-class ^String sql-name]
  (MockType. java-class sql-name))

(defn mock-column-metadata
  "Create a MockColumnMetadata from a spec map.

  Required keys:
    :name       - column name (String)

  Optional keys:
    :java-type   - Java class (auto-infers Type when set)
    :type        - io.r2dbc.spi.Type instance (overrides auto-inferred type)
    :nullability - io.r2dbc.spi.Nullability enum (default: Nullability/UNKNOWN)
    :precision   - Integer
    :scale       - Integer

  (mock-column-metadata {:name \"id\" :java-type Integer})
  (mock-column-metadata {:name \"name\" :java-type String :nullability Nullability/NULLABLE})"
  ^MockColumnMetadata
  [{:keys [name java-type type nullability precision scale]}]
  (let [^MockColumnMetadata$Builder b (MockColumnMetadata/builder)]
    (.name b ^String name)
    (when java-type (.javaType b ^Class java-type))
    (when type (.type b type))
    (.nullability b (or nullability Nullability/UNKNOWN))
    (when precision (.precision b (Integer/valueOf (int precision))))
    (when scale (.scale b (Integer/valueOf (int scale))))
    (.build b)))

(defn mock-row-metadata
  "Create a MockRowMetadata from a sequence of column specs.

  Each spec is either a MockColumnMetadata instance or a map passed
  to mock-column-metadata.

  (mock-row-metadata [{:name \"id\" :java-type Integer}
                      {:name \"name\" :java-type String}])"
  ^MockRowMetadata [column-specs]
  (let [^MockRowMetadata$Builder b (MockRowMetadata/builder)]
    (doseq [spec column-specs]
      (let [^MockColumnMetadata cm (if (instance? MockColumnMetadata spec)
                                     spec
                                     (mock-column-metadata spec))]
        (.columnMetadata b cm)))
    (.build b)))

(defn mock-row
  "Create a MockRow from a map of column-name to value pairs.

  Each entry is registered under both the column name (String) and
  column index (Integer) so that row.get(name, type) and row.get(index, type)
  both work. The Java type for each column is determined by the value's class,
  or Object for nil values.

  For deterministic index mapping, use small literal maps (<= 8 entries,
  which are array-maps preserving insertion order) or (array-map ...).

  Options map (optional second arg):
    :metadata - a RowMetadata instance; auto-generated from column data if omitted
    :types    - map of column-name (String) -> Java Class for explicit type control

  (mock-row {\"id\" 1 \"name\" \"Alice\"})
  (mock-row {\"id\" 1 \"name\" \"Alice\"} {:types {\"id\" Long \"name\" String}})"
  (^MockRow [columns] (mock-row columns {}))
  (^MockRow [columns {:keys [metadata types]}]
   (let [col-entries        (seq columns)
         resolve-type       (fn [col-name value]
                              (or (get types col-name)
                                  (if (some? value) (class value) Object)))
         ^MockRow$Builder b (MockRow/builder)]
     (doseq [[idx [col-name value]] (map-indexed vector col-entries)]
       (let [t (resolve-type col-name value)]
         (.identified b col-name t value)
         (.identified b (int idx) t value)
         (.identified b (long idx) t value)))
     (let [rm (or metadata
                  (mock-row-metadata (mapv (fn [[col-name value]]
                                             {:name      col-name
                                              :java-type (resolve-type col-name
                                                                       value)})
                                           col-entries)))]
       (.metadata b rm))
     (.build b))))

(defn mock-result
  "Create a MockResult. Flexible creation from different segment types.

  Keyword options:
    :rows           - sequence of MockRow instances (creates RowSegments)
    :update-count   - long value (creates UpdateCount segment)
    :segments       - sequence of Result.Segment instances (raw segments)
    :out-parameters - OutParameters instance

  Multiple keys can be combined. With no args, returns MockResult/empty.

  (mock-result)
  (mock-result {:rows [row1 row2]})
  (mock-result {:update-count 3})
  (mock-result {:rows [row1] :update-count 1})"
  (^MockResult [] (MockResult/empty))
  (^MockResult [{:keys [rows update-count segments out-parameters]}]
   (let [^MockResult$Builder b (MockResult/builder)]
     (when (seq rows) (.row b (into-array Row rows)))
     (when update-count (.rowsUpdated b (long update-count)))
     (when (seq segments) (.segment b (into-array Result$Segment segments)))
     (when out-parameters (.outParameters b out-parameters))
     (.build b))))

(defn mock-statement
  "Create a MockStatement that returns the given result(s) when executed.

  Accepts a single MockResult or a sequential collection of MockResult instances.

  (mock-statement (mock-result {:update-count 1}))
  (mock-statement [(mock-result {:rows [r1]}) (mock-result {:rows [r2]})])"
  ^MockStatement [results]
  (let [rs                       (if (sequential? results) results [results])
        ^MockStatement$Builder b (MockStatement/builder)]
    (doseq [^Result r rs] (.result b r))
    (.build b)))

(defn mock-batch
  "Create a MockBatch that returns the given result(s) when executed.

  Accepts a single MockResult or a sequential collection of MockResult instances.

  (mock-batch (mock-result {:update-count 5}))"
  ^MockBatch [results]
  (let [rs                   (if (sequential? results) results [results])
        ^MockBatch$Builder b (MockBatch/builder)]
    (doseq [^Result r rs] (.result b r))
    (.build b)))

(defn mock-connection
  "Create a MockConnection with optional pre-configured statement and batch.

  Options:
    :statement - MockStatement instance (returned by createStatement)
    :batch     - MockBatch instance (returned by createBatch)
    :valid     - boolean for validate() result (default: true)

  Calling createStatement() on a connection with no :statement set
  will throw AssertionError (r2dbc-spi-test behavior).

  (mock-connection)
  (mock-connection {:statement stmt})
  (mock-connection {:statement stmt :batch batch :valid true})"
  (^MockConnection [] (mock-connection {}))
  (^MockConnection [{:keys [statement batch valid], :or {valid true}}]
   (let [^MockConnection$Builder b (MockConnection/builder)]
     (when statement (.statement b ^MockStatement statement))
     (when batch (.batch b ^MockBatch batch))
     (.valid b (boolean valid))
     (.build b))))

(defn mock-connection-factory
  "Create a MockConnectionFactory that yields the given connection from create().

  Calling create() on a factory with no connection will throw
  AssertionError (r2dbc-spi-test behavior).

  (mock-connection-factory conn)"
  ^MockConnectionFactory [^Connection conn]
  (-> (MockConnectionFactory/builder)
      (.connection conn)
      (.build)))

(defn mock-blob
  "Create a MockBlob from a byte array.

  (mock-blob (.getBytes \"hello\" \"UTF-8\"))"
  ^MockBlob [^bytes data]
  (-> (MockBlob/builder)
      (.item (into-array ByteBuffer [(ByteBuffer/wrap data)]))
      (.build)))

(defn mock-clob
  "Create a MockClob from a string.

  (mock-clob \"hello world\")"
  ^MockClob [^String text]
  (-> (MockClob/builder)
      (.item (into-array CharSequence [text]))
      (.build)))

(defn mock-out-parameters
  "Create MockOutParameters from a types map and a values map.

  types   - map of identifier (String or Integer) -> Java Class
  values  - map of identifier (String or Integer) -> value

  (mock-out-parameters {\"status\" String} {\"status\" \"OK\"})"
  ^MockOutParameters [types values]
  (let [^MockOutParameters$Builder b (MockOutParameters/builder)]
    (doseq [[id v] values]
      (let [t (or (get types id) Object)] (.identified b id t v)))
    (.build b)))

(defn tracking-publisher
  "Create a Publisher plus atoms that record demand and cancellation.

  Options:
    :items - sequence of values to emit in order
    :error - Throwable to signal after all items are emitted

  Returns:
    {:publisher   Publisher
     :requests    atom of request sizes in call order
     :cancelled?  atom boolean
     :completed?  atom boolean}

  The publisher is demand-driven: each request only emits up to the requested
  number of remaining items. Terminal signals occur exactly once."
  [{:keys [items error], :or {items []}}]
  (let [state                                                                 (Object.)
        items                                                                 (vec items)
        requests                                                              (atom [])
        cancelled?                                                            (atom false)
        completed?                                                            (atom false)
        index-ref                                                             (volatile! 0)
        requested                                                             (volatile! (long 0))
        done-ref                                                              (volatile! false)
        draining-ref                                                          (volatile! false)
        next-signal!                                                          (fn []
                                                                                (locking state
                                                                                  (cond @done-ref nil
                                                                                        @cancelled? (do (vreset! done-ref true) nil)
                                                                                        (and (pos? ^long @requested)
                                                                                             (< ^long @index-ref (count items)))
                                                                                        (let [idx ^long @index-ref]
                                                                                          (vreset! index-ref (unchecked-inc idx))
                                                                                          (vreset! requested
                                                                                                   (unchecked-dec ^long @requested))
                                                                                          [:next (nth items idx)])
                                                                                        (= ^long @index-ref (count items))
                                                                                        (do (vreset! done-ref true)
                                                                                            (if error [:error error] [:complete nil]))
                                                                                        :else nil)))
        drain!                                                                (fn [^Subscriber subscriber]
                                                                                (loop []
                                                                                  (when-let [[signal value] (next-signal!)]
                                                                                    (let [continue?
                                                                                          (try (case signal
                                                                                                 :next (do (.onNext subscriber value) true)
                                                                                                 :error (do (reset! completed? true)
                                                                                                            (.onError subscriber
                                                                                                                      ^Throwable value)
                                                                                                            false)
                                                                                                 :complete (do (reset! completed? true)
                                                                                                               (.onComplete subscriber)
                                                                                                               false))
                                                                                               (catch Throwable t
                                                                                                 (locking state (vreset! done-ref true))
                                                                                                 (reset! completed? true)
                                                                                                 (.onError subscriber t)
                                                                                                 false))]
                                                                                      (when continue? (recur))))))
        schedule-drain!
        (fn [^Subscriber subscriber]
          (let [start?
                (locking state
                  (when-not @draining-ref (vreset! draining-ref true) true))]
            (when start?
              (CompletableFuture/runAsync
               (fn []
                 (loop []
                   (drain! subscriber)
                   (let [continue?
                         (locking state
                           (if (and (not @done-ref)
                                    (not @cancelled?)
                                    (or (pos? ^long @requested)
                                        (= ^long @index-ref (count items))))
                             true
                             (do (vreset! draining-ref false) false)))]
                     (when continue? (recur)))))))))]
    {:publisher  (reify
                   Publisher
                   (^void subscribe
                     [_ ^Subscriber subscriber]
                     (.onSubscribe subscriber
                                   (reify
                                     Subscription
                                     (request [_ n]
                                       (when (pos? (long n))
                                         (swap! requests conj (long n))
                                         (locking state
                                           (vreset! requested
                                                    (+ ^long @requested
                                                       (long n))))
                                         (schedule-drain! subscriber)))
                                     (cancel [_] (reset! cancelled? true))))))
     :requests   requests
     :cancelled? cancelled?
     :completed? completed?}))

(defn tracking-connection-factory
  "Create a ConnectionFactory that tracks how many times Connection.close() is called.

  The returned factory wraps a MockConnection (pre-configured with :statement if
  supplied). Each call to create() yields a Connection proxy that delegates all
  methods to the underlying MockConnection but increments close-count when close()
  is called.

  Returns:
    {:factory    ConnectionFactory
     :close-count atom<int> - incremented once per Connection.close() call}

  Options:
    :statement - MockStatement returned by createStatement (optional)"
  [{:keys [statement]}]
  (let [close-count    (atom 0)
        base-conn      (mock-connection (if statement {:statement statement} {}))
        tracking-conn  (reify
                         Connection
                         (^Statement createStatement
                           [_ ^String sql]
                           (.createStatement ^Connection base-conn sql))
                         (^Publisher close
                           [_]
                           (swap! close-count inc)
                           (.close ^Connection base-conn))
                         (^Publisher beginTransaction
                           [_]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher beginTransaction
                           [_ ^TransactionDefinition _def]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher commitTransaction
                           [_]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Batch createBatch
                           [_]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher createSavepoint
                           [_ ^String _name]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^boolean isAutoCommit
                           [_]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^ConnectionMetadata getMetadata
                           [_]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^IsolationLevel getTransactionIsolationLevel
                           [_]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher releaseSavepoint
                           [_ ^String _name]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher rollbackTransaction
                           [_]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher rollbackTransactionToSavepoint
                           [_ ^String _name]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher setAutoCommit
                           [_ ^boolean _ac]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher setLockWaitTimeout
                           [_ ^Duration _t]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher setStatementTimeout
                           [_ ^Duration _t]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher setTransactionIsolationLevel
                           [_ ^IsolationLevel _level]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented")))
                         (^Publisher validate
                           [_ ^ValidationDepth _depth]
                           (throw (UnsupportedOperationException.
                                   "tracking-conn: not implemented"))))
        conn-publisher (reify
                         Publisher
                         (^void subscribe
                           [_ ^Subscriber subscriber]
                           (.onSubscribe subscriber
                                         (reify
                                           Subscription
                                           (request [_ n]
                                             (when (pos? (long n))
                                               (.onNext subscriber
                                                        tracking-conn)
                                               (.onComplete subscriber)))
                                           (cancel [_])))))
        factory        (reify
                         ConnectionFactory
                         (^Publisher create [_] conn-publisher)
                         (^ConnectionFactoryMetadata getMetadata
                           [_]
                           (reify
                             ConnectionFactoryMetadata
                             (getName [_] "tracking-mock"))))]
    {:factory factory, :close-count close-count}))
