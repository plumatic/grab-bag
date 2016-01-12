(ns store.bdb
  (:require
   [clojure.java.io :as java-io]
   [clojure.java.shell :as shell]
   [plumbing.error :as err]
   [plumbing.logging :as log]
   [plumbing.observer :as observer]
   [plumbing.serialize :as serialize]
   [store.bucket :as bucket])
  (:import
   [com.sleepycat.je CacheMode CursorConfig Database
    DatabaseConfig DatabaseEntry
    DiskOrderedCursorConfig Environment
    EnvironmentConfig ForwardCursor LockMode
    OperationStatus]
   [com.sleepycat.je.util DbBackup]
   [java.util.concurrent LinkedBlockingQueue]))


;;http://download.oracle.com/docs/cd/E17277_02/html/GettingStartedGuide

(defn- megs [x] (* x 1000000))
(defn- seconds [x] (* x 1000))

(def cache-modes {:default CacheMode/DEFAULT
                  :evict-bin CacheMode/EVICT_BIN
                  :evict-ln CacheMode/EVICT_LN
                  :keep-hot CacheMode/KEEP_HOT
                  :make-cold CacheMode/MAKE_COLD
                  :unchanged CacheMode/UNCHANGED})

(defn _cursor-next
  "returns a fn which acts as a cursor over db. each call
   returns a [key value] pair. closes cursor when all entries exhausted"
  [^ForwardCursor cursor keys-only key-deserialize deserialize]
  (let [k (DatabaseEntry.)
        v (if keys-only
            (doto (DatabaseEntry.)
              (.setPartial 0 0 true))
            (DatabaseEntry.))]
    (if (not (= (.getNext cursor k v LockMode/READ_UNCOMMITTED)
                OperationStatus/SUCCESS))
      ;; return nil
      (do (.close cursor)
          nil)
      (if keys-only
        (key-deserialize (.getData k))
        [(key-deserialize (.getData k))
         (deserialize (.getData v))]))))

(defn seque3 [observer n seq-fn]
  (let [q (LinkedBlockingQueue. (int n))
        NIL (Object.)   ;nil sentinel since LBQ doesn't support nils
        done? (atom false)]
    (observer/watch-fn!
     observer :queue-size :log
     (fn [^LinkedBlockingQueue q done?]
       (if @done? observer/+stop-watching+ {:count (long 1) :size (long (.size q))}))
     q done?)
    (future
      (try
        (loop [s (seq  (seq-fn))]
          (if s
            (do (let [x (first s)] (.put q (if (nil? x) NIL x)))
                (recur (next s)))
            (.put q q))) ; q itself is eos sentinel
        (catch Exception e
          (.put q q)
          (throw e))))

    ((fn drain []
       (lazy-seq
        (let [x (.take q)]
          (if (identical? x q)    ;q itself is eos sentinel
            (do (reset! done? true) nil)
            (cons (if (identical? x NIL) nil x) (drain)))))))))

(defprotocol PCloser
  (justClosed [this]))

(deftype Closer [^ForwardCursor cursor ^{:unsynchronized-mutable true} closed?]
  Object
  (finalize [this]
    (when-not closed?
      (log/warn (str "leaked BDB cursor " cursor this))
      (.close cursor)))

  PCloser
  (justClosed [this] (set! closed? true)))

(defn cursor-seq [^Database db key-deserialize deserialize observer & {:keys [keys-only]
                                                                       :or {keys-only false}}]
  (seque3 observer 64
          #(let [cursor (.openCursor db (DiskOrderedCursorConfig.))
                 dummy (Closer. cursor false)]
             ((fn make-seq [dummy]
                (lazy-seq
                 (if-let [x (_cursor-next cursor keys-only key-deserialize deserialize)]
                   (cons x (make-seq dummy))
                   (do (justClosed dummy) nil))))
              dummy))))

(defn optimize-db
  "Walk over the database and rewrite each record, so that subsequent traversals
   will be in sequential order on disk"
  [^Database db]
  (let [cursor (.openCursor db nil (doto (CursorConfig.) (.setReadUncommitted true)))
        k (DatabaseEntry.)
        v (DatabaseEntry.)]
    (while (= (.getNext cursor k v LockMode/READ_UNCOMMITTED)
              OperationStatus/SUCCESS)
      (.putCurrent cursor v))
    (.close cursor)))

(defn ^DatabaseConfig bdb-conf [read-only-db deferred-write cache-mode]
  (doto (DatabaseConfig.)
    (.setReadOnly read-only-db)
    (.setAllowCreate (not read-only-db))
    (.setDeferredWrite deferred-write)
    (.setCacheMode (cache-modes cache-mode))))

(defn- ^Environment bdb-env
  "Parameters:
   :path - bdb environment path
   :read-only - set bdb environment to be read-only
   :checkpoint-kb - how many kb to write before checkpointing
   :checkpoint-mins - how many mins to wait before checkpointing
   :clean-util-thresh - % to trigger log file cleaning (higher means cleaner)
   :locking - toggle locking, if turned off then the cleaner is also
   :cache-percent - percent of heap to use for BDB cache"
  [{:keys [path
           read-only
           checkpoint-kb  ;;corresponds to new
           checkpoint-mins ;;corresponda to new
           num-cleaner-threads
           locking
           lock-timeout
           cache-size ;;new
           clean-util-thresh
           checkpoint-high-priority?
           checkpoint-bytes-interval
           max-open-files
           min-file-utilization
           checkpoint-wakeup-interval
           log-file-max]
    :or {read-only false
         path "/var/bdb/"
         checkpoint-kb 0
         num-cleaner-threads 3
         checkpoint-mins 0
         clean-util-thresh 75
         min-file-utilization 5
         checkpoint-high-priority? true
         checkpoint-bytes-interval (megs 5)
         checkpoint-wakeup-interval (seconds 30000) ;;in microseconds
         locking true
         lock-timeout 500 ;;new
         cache-size 512 ;;new -- based on % of heap give it about 2/3 of heap up to like 10 ... cap at 10 GB above
         max-open-files 512
         log-file-max (megs 64)}}]
  (shell/sh "mkdir" "-p" path)
  (let [env-config (doto (EnvironmentConfig.)
                     (.setReadOnly read-only)
                     (.setAllowCreate (not read-only))
                     (.setConfigParam (EnvironmentConfig/CLEANER_MIN_UTILIZATION)
                                      (str clean-util-thresh))
                     (.setConfigParam (EnvironmentConfig/CLEANER_MIN_FILE_UTILIZATION)
                                      (str min-file-utilization))
                     (.setConfigParam (EnvironmentConfig/CLEANER_THREADS)
                                      (str num-cleaner-threads))
                     (.setConfigParam (EnvironmentConfig/CHECKPOINTER_HIGH_PRIORITY)
                                      (str checkpoint-high-priority?))
                     (.setConfigParam (EnvironmentConfig/CHECKPOINTER_WAKEUP_INTERVAL)
                                      (str checkpoint-wakeup-interval)) ;;new
                     (.setConfigParam (EnvironmentConfig/CHECKPOINTER_BYTES_INTERVAL)
                                      (str checkpoint-bytes-interval)) ;;new
                     (.setConfigParam (EnvironmentConfig/LOG_FILE_CACHE_SIZE)
                                      (str max-open-files))
                     (.setConfigParam (EnvironmentConfig/LOG_FILE_MAX)  ;;new
                                      (str log-file-max))
                     (.setLocking locking)
                     (.setCacheSize (megs cache-size)))]
    (java-io/make-parents (java-io/file path "touch"))
    (Environment. (java-io/file path) env-config)))

(defprotocol PCursorIterator
  (cursor-next [this])
  (cursor-close [this])
  (cursor-remove [this]))

(defprotocol PCursorIterable
  (cursor-iterator [this]))


(defmethod bucket/bucket :bdb
  [{:keys [^String name path cache read-only-env cache-mode
           read-only deferred-write merge
           observer serialize deserialize serialize-method
           key-serialize-method]
    :or {cache-mode :evict-ln
         read-only false
         deferred-write false
         serialize-method serialize/+default+}
    :as args}]
  (err/assert-keys [:name :path] args)
  (let [serialize (or serialize (partial serialize/serialize serialize-method))
        deserialize (or deserialize serialize/deserialize)
        key-serialize (if key-serialize-method
                        (partial serialize/serialize key-serialize-method)
                        serialize)
        key-deserialize (if key-serialize-method
                          serialize/deserialize
                          deserialize)
        observer (observer/sub-observer observer name)
        db-conf (bdb-conf read-only deferred-write cache-mode)

        ;;never open environemtns readonly, see: http://forums.oracle.com/forums/thread.jspa?threadID=2239407&tstart=0
        env (bdb-env (if read-only-env
                       (do (assert read-only)
                           (assoc args :read-only true))
                       (dissoc args :read-only)))
        db (.openDatabase env nil name db-conf)]
    (->
     (reify bucket/IReadBucket
       (bucket/get [this k]
         (let [entry-key (DatabaseEntry. (key-serialize k))
               entry-val (DatabaseEntry.)]
           (when (= (.get db nil entry-key entry-val LockMode/DEFAULT)
                    OperationStatus/SUCCESS)
             (deserialize (.getData entry-val)))))
       (bucket/batch-get [this ks]
         (bucket/default-batch-get this ks))
       ;;This method does an optimized, internal traversal, does not impact the working set in the cache, but may not be accurate in the face of concurrent modifications in the database
       ;;see:  http://www.oracle.com/technetparallel/database/berkeleydb/je-faq-096044.html#31
       (bucket/count [this]
         (.count db))
       (bucket/keys [this]
         (cursor-seq db key-deserialize deserialize observer
                     :keys-only true))

       (bucket/vals [this]
         (map second (bucket/seq this)))

       (bucket/seq [this]
         (cursor-seq db key-deserialize deserialize observer))

       (bucket/exists? [this k]
         (let [entry-key (DatabaseEntry. (key-serialize k))
               entry-val (DatabaseEntry.)]
           (.setPartial entry-val (int 0) (int 0) true)
           (= (.get db nil entry-key entry-val LockMode/DEFAULT)
              OperationStatus/SUCCESS)))

       bucket/IOptimizeBucket
       (bucket/optimize [this] (optimize-db db))

       bucket/IMergeBucket
       (bucket/merge [this k v]
         (bucket/default-merge this merge k v))
       (bucket/batch-merge [this kvs]
         (bucket/default-batch-merge this merge kvs))

       bucket/IWriteBucket
       (bucket/put [this k v]
         (let [entry-key (DatabaseEntry. (key-serialize k))
               entry-val (DatabaseEntry. (serialize v))]
           (str (.put db nil entry-key entry-val))))
       (bucket/batch-put [this kvs]
         (bucket/default-batch-put this kvs))
       (bucket/delete [this k]
         (str (.delete db nil (DatabaseEntry. (key-serialize k)))))
       (bucket/update [this k f]
         (bucket/default-update this k f))
       (bucket/sync [this]
         (when (-> db .getConfig .getDeferredWrite)
           (.sync db)))
       (bucket/close [this]
         (do
           (.close db)
           (.close env)))

       PCursorIterable
       (cursor-iterator [this]
         (let [cursor (.openCursor db nil (doto (CursorConfig.) (.setReadUncommitted true)))]
           (reify PCursorIterator
             (cursor-next [this] (_cursor-next cursor true key-deserialize deserialize))
             (cursor-remove [this] (str (.delete cursor)))
             (cursor-close [this] (.close cursor))))))
     (bucket/wrapper-policy args))))
