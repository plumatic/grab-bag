(ns store.sql
  (:use plumbing.core)
  (:require
   [clojure.java.jdbc.deprecated :as jdbc]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [honeysql.core :as honeysql]
   [honeysql.helpers :as helpers]
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.error :as err]
   [plumbing.resource :as resource]
   [store.bucket :as bucket])
  (:import
   [com.mchange.v2.c3p0 ComboPooledDataSource]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private
(def +h2-subprotocols+
  #{"h2:file" "h2:mem"})

(defn clob-to-string
  "Convert a JdbcClob objects to String"
  [^org.h2.jdbc.JdbcClob clob]
  (slurp (.getCharacterStream clob)))

(defn convert-clobs
  "Walk o converting clobs to strings (h2 stores :text as clob)"
  [o]
  (walk/postwalk
   (fn [x]
     (if (instance? org.h2.jdbc.JdbcClob x)
       (clob-to-string x)
       x))
   o))

(defn insert-helper
  "Produces sql and args ready for do-prepared to insert the specified map into db"
  [table-name insert-map]
  (-> (helpers/insert-into (keyword table-name))
      (helpers/values [insert-map])
      (honeysql/format :quoting :mysql)))

(defn create-date-format-alias []
  (jdbc/do-commands
   "CREATE ALIAS IF NOT EXISTS date_format AS $$
            String date_format(String date, String pattern) throws java.text.ParseException
            {
                // partial implementation that might need to be extended if
                //  we start using additional date format strings.
                String fixedPattern = pattern.replaceAll(\"%Y\", \"y\")
                                             .replaceAll(\"%m\", \"MM\")
                                             .replaceAll(\"%d\", \"dd\")
                                             .replaceAll(\"%H\", \"kk\");
                if (fixedPattern.contains(\"%\")) {
                   throw new RuntimeException(\"Unsupported date_format for h2, augment implementation: \"
                       + pattern);
                }
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(fixedPattern);
                java.util.Date d =
                   new java.text.SimpleDateFormat(\"yyyy-MM-dd HH:mm:ss.S\").parse(date);
                return sdf.format(d);
            } $$;"))

(defn create-unix-timestamp-alias []
  (jdbc/do-commands
   "CREATE ALIAS IF NOT EXISTS unix_timestamp AS $$
            long unix_timestamp(java.sql.Timestamp d)
            {
               return d.getTime() / 1000;
            } $$;"
   "CREATE ALIAS IF NOT EXISTS from_unixtime AS $$
            java.sql.Timestamp unix_timestamp(long d)
            {
               return new java.sql.Timestamp(d*1000);
            } $$;"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(s/defn sql-date :- java.sql.Date
  "Coerces java.util.Date to java.sql.Date"
  [d :- java.util.Date]
  (if (instance? java.sql.Date d) d (java.sql.Date. (.getTime d))))

(defn all-tables
  "seq of table names"
  []
  (set
   (for [table (jdbc/resultset-seq
                (-> (jdbc/connection)
                    (.getMetaData)
                    (.getTables nil nil nil (into-array ["TABLE" "VIEW"]))))]
     (:table_name table))))

(defn create-table-if-missing!
  [table-name & column-specs]
  (when-not ((all-tables) table-name)
    (apply jdbc/create-table table-name column-specs)))

(defn drop-table-if-exists! [table-name]
  (when ((all-tables) table-name)
    (jdbc/drop-table table-name)))


(def +h2-classname+
  "org.h2.Driver")

(def h2-test-connection-spec
  {:connection-uri "jdbc:h2:mem:~/test;DATABASE_TO_UPPER=FALSE;MODE=MySQL"
   :classname +h2-classname+
   :subprotocol "h2:mem"
   :user "sa"
   :password ""})

(defrecord DBConnectionPool [^ComboPooledDataSource datasource]
  java.io.Closeable
  (close [this]
    (.close datasource)))

(defnk connection-pool
  "Create a connection pool for the given database spec. Copied from korma.db"
  [subprotocol {table-spec nil} :as spec]
  (let [pool (DBConnectionPool.
              (letk [[classname subprotocol user password
                      {excess-timeout (* 30 60)} {idle-timeout (* 3 60 60)}] spec]
                (doto (ComboPooledDataSource.)
                  (.setDriverClass classname)
                  (.setJdbcUrl (or (:connection-uri spec)
                                   (letk [[host port db] spec]
                                     (format "jdbc:%s://%s:%s/%s" subprotocol host port db))))
                  (.setUser user)
                  (.setPassword password)
                  (.setMaxIdleTimeExcessConnections excess-timeout)
                  (.setMaxIdleTime idle-timeout))))]
    (jdbc/with-connection pool
      (doseq [[table-name table-def] table-spec]
        (apply create-table-if-missing! table-name table-def)))

    (err/with-retries 3 100 #(throw %) ;; attempt to work around travis flakiness.
      (when (+h2-subprotocols+ subprotocol)
        (jdbc/with-connection pool
          (create-date-format-alias)
          (create-unix-timestamp-alias))))
    pool))

(defprotocol SQLBucket
  (db-connection [this])
  (table-name [this])
  (primary-key-seq [this])
  (primary-key-clause [this]))

(defnk default->mem
  "A default ->mem function that converts clob fields to Strings for h2 db's"
  [{datasource nil} :as connection-spec]
  (if (and (instance? ComboPooledDataSource datasource)
           (= +h2-classname+ (.getDriverClass ^ComboPooledDataSource datasource)))
    convert-clobs
    identity))

(defmethod bucket/bucket :sql [opts]
  "A key-value abstraction of a SQL table. primary-key may be either a column
   id or a sequence of column ids. The keys of the bucket are each either a
   single value or a vector containing the values of each column in the specified
   primary key, in the same order. It need not correspond to the actual PRIMARY
   KEY of the table. Currently only works for a flat map of strings and numbers."
  (letk [[connection-spec table {primary-key :id}
          {->mem identity}
          {mem-> identity}] opts]
    (s/validate
     (s/conditional
      vector? [s/Keyword]
      :else s/Keyword)
     primary-key)
    (let [->mem (comp ->mem (default->mem connection-spec))
          multi-key? (vector? primary-key)
          ensure-seq (fn [thing]
                       (if (vector? thing) thing [thing]))
          primary-key (ensure-seq primary-key)
          row->primary-key (fn [row]
                             (let [key (map #(safe-get row %) primary-key)]
                               (if multi-key?
                                 key
                                 ;; caller doesn't expect keys to be wrapped in a sequence
                                 (first key))))
          pkey-clause (str/join " AND " (map #(str "`" (name %) "` = ?") primary-key))
          ;; if connection-spec is a connection, just reuse it instead of making a new one
          reuse-connection? (= (keys connection-spec) [:datasource])
          connection (if reuse-connection?
                       connection-spec
                       (connection-pool connection-spec))]
      (bucket/wrapper-policy
       (reify
         store.bucket.IReadBucket
         (get [this k]
           (jdbc/with-connection connection
             (jdbc/with-query-results res
               (vec (cons (str "SELECT *"
                               " FROM " table
                               " WHERE " pkey-clause)
                          (ensure-seq k)))
               (when-first [x res]
                 (->mem x)))))
         (batch-get [this ks] (bucket/default-batch-get this ks))
         (exists? [this k]
           (boolean (bucket/get this k)))
         (keys [this]
           (map first (bucket/seq this)))
         (vals [this]
           (map second (bucket/seq this)))
         (seq [this]
           (err/with-retries
             3 1000 #(throw %)
             (jdbc/with-connection connection
               (jdbc/with-query-results res
                 [(str "SELECT * FROM " table)]
                 (mapv (juxt row->primary-key ->mem) res)))))
         (count [this]
           (jdbc/with-connection connection
             (jdbc/with-query-results res
               [(str "SELECT COUNT(*) c FROM " table)]
               (:c (first res)))))

         store.bucket.IWriteBucket
         (put [this k v]
           (bucket/batch-put this [[k v]]))

         (batch-put [this kvs]
           (when (seq kvs)
             (doseq [p (->> kvs
                            (map (fn [item]
                                   (if (vector? item)
                                     (let [[k v] item]
                                       (let [v (mem-> v)
                                             k (ensure-seq k)]
                                         (assert (map? v))
                                         (when (some #(contains? v %) primary-key)
                                           (assert (= (map #(% v) primary-key) k)))
                                         (apply merge v (zipmap primary-key k))))
                                     (let [v (mem-> item)]
                                       (assert (every? #(contains? v %) primary-key))
                                       v))))
                            (partition-all 100))]
               (jdbc/with-connection connection
                 (apply jdbc/do-prepared
                        (str "REPLACE INTO " table
                             " (`" (str/join "`,`" (map name (keys (first p)))) "`)"
                             " VALUES (" (str/join "," (repeat (count (first p)) "?")) ")")
                        (mapv (comp vec vals) p))))))

         (delete [this k]
           (jdbc/with-connection connection
             (jdbc/do-prepared
              (str "DELETE FROM " table
                   " WHERE " pkey-clause)
              (vec (ensure-seq k)))))

         (sync [this])
         (close [this] (resource/close connection))

         (update [this k f]
           (bucket/default-update this k f))

         SQLBucket
         (db-connection [this] connection)
         (table-name [this] table)
         (primary-key-seq [this] primary-key)
         (primary-key-clause [this] pkey-clause))
       opts))))

(defn lisp->sql [kw]
  (-> kw name (str/replace #"-" "_") keyword))

(defn sql->lisp [kw]
  (-> kw name (str/replace #"_" "-") keyword))

;; Generic SQL Utilities - moved from user_store/schema.clj

(defn fk-constraint-id [spec]
  ;; SQL won't let you have negative number fk_-42
  (str "fk_" (Math/abs (long (hash spec)))))

(defn add-fk-stmt [[table key ftable fkey mods :as spec]]
  (format "ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) %s"
          (fk-constraint-id spec)
          (name key) (name ftable) (name fkey) mods))

(defn alter-table-cmd [table cmds]
  (str "ALTER TABLE " (name table) " "
       (str/join " , " cmds) ";"))

(defn create-foreign-key-constraints! [foreign-key-specs]
  (jdbc/transaction
   (doseq [fk-spec-sql foreign-key-specs]
     (let [fk-spec (map #(if (keyword? %) (lisp->sql %) %) fk-spec-sql)]
       (jdbc/do-commands (alter-table-cmd (first fk-spec) [(add-fk-stmt fk-spec)]))))))

(defn select
  "Queries an SQL table with a map of constraints
  from column name to value.  Returns the matching
  entries in the table."
  [connection table-name constraint-map]
  (jdbc/with-connection connection
    (jdbc/with-query-results res
      (honeysql/format
       (honeysql/build :select :*
                       :from (keyword table-name)
                       :where (cons :and (for [[k v] constraint-map] [:= k v])))
       :quoting :mysql)
      (mapv (fn [result]
              (for-map [[k v] result]
                (sql->lisp k)
                (convert-clobs v)))
            res))))

(defn insert!
  "Inserts an entry into a table, generating new keys when applicable
   and respecting uniqueness constraints. Returns inserted value with
   populated generated keys."
  [bucket insert-map]
  (let [db-connection (db-connection bucket)
        table-name (table-name bucket)]
    (jdbc/with-connection db-connection
      (let [[sql & args] (insert-helper table-name insert-map)]
        (jdbc/do-prepared sql args)))
    (pci/safe-singleton (select db-connection table-name insert-map))))

(s/defn increment-field!
  "Increment `inc-field` for the specified entry by given amount, other fields are left
   unchanged. Acts like `insert!` if entry does not already exist."
  [bucket
   inc-field :- s/Keyword
   entry :- {s/Keyword s/Any}]
  (let [db-connection (db-connection bucket)
        table-name (table-name bucket)
        inc-field-name (name inc-field)]
    (jdbc/with-connection db-connection
      (let [[sql & args] (insert-helper table-name entry)]
        (-> sql
            (str " ON DUPLICATE KEY UPDATE `" inc-field-name "`=`" inc-field-name "` + ? ;")
            (jdbc/do-prepared (-> args vec (conj (safe-get entry inc-field)))))))))
