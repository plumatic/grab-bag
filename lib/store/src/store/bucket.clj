(ns store.bucket
  (:refer-clojure :exclude [get get-in put keys seq count sync update merge vals empty? update-in])
  (:use [plumbing.core :exclude [update]])
  (:require
   [clojure.core :as clojure]
   [clojure.java.io :as java-io]
   [ring.util.codec :as codec]
   [plumbing.chm :as chm]
   [plumbing.error :as err]
   [plumbing.graph :as graph]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [plumbing.serialize :as serialize])
  (:import
   [java.io ByteArrayOutputStream File]
   [java.util.concurrent ConcurrentHashMap ConcurrentMap]
   [org.apache.commons.io IOUtils]))

(set! *warn-on-reflection* true)

(defprotocol IReadBucket
  (get [this k] "fetch value for key")
  (batch-get [this ks] "return seq of [k v] pairs")
  (exists? [this k] "does key exist in bucket")
  (keys [this] "seq of existing keys")
  (vals [this] "seq of existing vals")
  (seq [this] "seq of [k v] elems")
  (count [this] "the number of kv pairs in this bucket (boxed Long)"))

(defn empty? [b] (-> b count zero?))

(defn pseq
  "Parallel bucket/seq in n-threads.  Not lazy."
  [b n-threads]
  (parallel/map-work n-threads (fn [k] [k (get b k)]) (keys b)))

(defprotocol IWriteBucket
  (put [this k v]
    "write value for key. return value can be anything")
  (batch-put [this kvs] "put a seq of [k v] pairs")
  (delete [this k] "remove key-value pair")
  (update [this k f])
  (sync [this])
  (close [this]))

(defmacro get!
  "Get the value in the bucket under key k. If the key is not present,
   set the value to the result of default-expr and return it."
  [m k default-expr]
  `(let [m# ~m k# ~k]
     (or (get m# k#)
         (let [nv# ~default-expr]
           (put m# k# nv#)
           nv#))))

(defn update-in [b [k & ks] f]
  (if-not (clojure.core/seq ks)
    (update b k f)
    (update b k #(clojure.core/update-in % (vec ks) f))))

(defn get-in [b [k & ks]]
  (clojure.core/get-in (get b k) ks))

(extend-type store.bucket.IWriteBucket
  resource/PCloseable
  (close [this] (close this)))

(defprotocol IMergeBucket
  (merge [this k v] "merge v into current value")
  (batch-merge [this kvs] "merge key valye pairs kvs into current values"))

(defprotocol IConditionalWriteBucket
  (put-cond [this k v old-v] "Put v under k and return true iff old value is old-v, else return false."))

(defprotocol IOptimizeBucket
  (optimize [this] "optimize for in order reads from disk on :keys and :seq requests"))

;;; Default Bucket Operations
(defn default-batch-put [b kvs]
  (doseq [[k v] kvs] (put b k v)))

(defn default-batch-get [b ks]
  (for [k ks] [k (get b k)]))

;;TODO: put on the protocol, implementations can be much more efficient deleting with cursor.
(defn clear [b]
  (doseq [k (keys b)]
    (delete b k)))

(defn default-update [b k f]
  (->>  k
        (get b)
        f
        (put b k)))

(defn default-seq [b]
  (for [k (keys b)]
    [k (get b k)]))

(defn default-merge [b merge-fn k v]
  (assert merge-fn)
  (update b k (fn [v-to] (merge-fn k v-to v))))

(defn default-batch-merge [b merge-fn kvs]
  (assert merge-fn)
  (doseq [[k v] kvs]
    (update b k (fn [v-to] (merge-fn k v-to v)))))

;; make this a deftype for purposes of extend-type
(deftype HashmapBucket [^ConcurrentMap h merge-fn]
  IReadBucket
  (keys [this]
    (clojure.core/seq (.keySet h)))
  (vals [this]
    (clojure.core/seq (.values h)))
  (get [this k]
    (.get h k))
  (batch-get [this ks] (default-batch-get this ks))
  (seq [this]
    (clojure.core/seq
     (for [^java.util.Map$Entry e
           (.entrySet h)]
       [(.getKey e) (.getValue e)])))
  (exists? [this k] (.containsKey h k))
  (count [this] (long (.size h)))

  IMergeBucket
  (merge [this k v]
    (default-merge this merge-fn k v))
  (batch-merge [this kvs]
    (default-batch-merge this merge-fn kvs))

  IWriteBucket
  (put [this k v]
    (.put h k v))
  (batch-put [this kvs] (default-batch-put this kvs))
  (delete [this k]
    (.remove h k))
  (update [this k f] (chm/update! h k f))
  (sync [this] nil)
  (close [this] nil)

  IConditionalWriteBucket
  (put-cond [this k v old-v]
    (chm/try-replace! h k v old-v)))

(defn hashmap-bucket [^ConcurrentMap h & [merge-fn]]
  (->HashmapBucket h merge-fn))

(defn lru-hashmap-bucket [cache-size & [entry-weight-fn]]
  (hashmap-bucket (chm/lru-chm cache-size entry-weight-fn)))

(defmulti bucket #(or (:type %) :mem))

(defn write-blocks! [b writes block-size]
  (doseq [blocks (partition-all block-size writes)
          :when blocks
          [op vs] (group-by first blocks)]
    (when op
      (case op
        :put    (batch-put b (map second vs))
        :update (batch-merge b (map second vs))))))

(defn drain-seq [b]
  (->> (keys b)
       (map (fn [k] (let [[v op] (delete b k)]
                      [op [k v]])))))

(defn with-flush
  "Takes a bucket with a merge fn and wraps with an in-memory cache that can be flushed with sync.
   Read operations read from the underlying store, and will not reflect unflushed writes."
  ([b merge-fn & {:keys [block-size]
                  :or {block-size 100}}]
     (let [mem-bucket (bucket {:type :mem})]
       (reify
         IReadBucket
         (get [this k] (get b k))
         (exists? [this k] (exists? b k))
         (keys [this] (keys b))
         (count [this] (long (count b)))
         (batch-get [this ks] (batch-get b ks))
         (seq [this] (seq b))

         IMergeBucket
         (merge [this k v]
           (update this k #(merge-fn k % v)))
         (batch-merge [this kvs]
           (default-batch-merge this merge-fn kvs))

         IWriteBucket
         (update [this k f]
           (update
            mem-bucket k
            (fn [old-tuple]
              (let [[val op] (or old-tuple [nil :update])]
                [(f val) op]))))
         (delete [this k]
           (delete mem-bucket k)
           (delete b k))
         (put [this k v]
           (put mem-bucket k [v :put]))
         (batch-put [this kvs] (default-batch-put this kvs))
         (sync [this]
           (write-blocks! b (drain-seq mem-bucket) block-size)
           (sync b))
         (close [this]
           (sync this)
           (close b))))))

;; TODO: observe for hitrate, etc?
;; TODO: fails if vals not present?
(defn mem-cache
  "full read memory cache in front of bucket b.
   If no-write-through? is true, then only store writes in the mem bucket but not backing.
   Otherwise, write-through to underlying bucket."
  [b & [mem no-write-through?]]
  (let [mem (or mem (bucket {:type :mem}))]
    (reify
      IReadBucket
      (get [this k]
        (or (get mem k)
            (do (let [v (get b k)]
                  (when-not (nil? v) (put mem k v))
                  v))))
      (exists? [this k] (get this k))
      (batch-get [this ks] (default-batch-get this ks))

      IWriteBucket
      (put [this k v]
        (assert (not (nil? v)) "Cannot send put nil in this bucket")
        (put mem k v)
        (when-not no-write-through?
          (put b k v)))
      (batch-put [this kvs] (default-batch-put this kvs))
      (close [this] (close b))
      (sync [this] (sync b))
      (delete [this k] (doseq [bb [b mem]]
                         (delete bb k))))))

(defn bounded-write-through-cache
  [b cache-size & [entry-weight-fn]]
  (mem-cache b (lru-hashmap-bucket cache-size entry-weight-fn)))

(defn full-mem-cache
  "memory cache of the entire bucket, with write-through.  Reads full contents
   into memory on creation, and never reads again (unless you call sync)."
  [backing]
  (let [mem (bucket {:type :mem})]
    (doto (reify
            IReadBucket
            (get [this k] (get mem k))
            (exists? [this k] (exists? mem k))
            (keys [this] (keys mem))
            (vals [this] (vals mem))
            (count [this] (long (count mem)))
            (batch-get [this ks] (batch-get mem ks))
            (seq [this] (seq mem))

            IWriteBucket
            (put [this k v]
              (doseq [b [backing mem]] (put b k v)))
            (batch-put [this kvs]
              (doseq [b [backing mem]] (batch-put b kvs)))
            (update [this k f]
              (default-update this k f))
            (close [this] (close backing))
            (sync [this] (clear mem) (batch-put mem (seq backing)))
            (delete [this k] (doseq [b [backing mem]] (delete b k))))
      (sync))))

(defn wrapper-policy [b {:keys [merge bounded-cache-size entry-weight-fn full-cache] :as args}]
  (cond
   bounded-cache-size (bounded-write-through-cache b bounded-cache-size entry-weight-fn)
   full-cache (full-mem-cache b)
   merge (with-flush b merge)
   :else b))

(defn serialization-fns [serialize-method]
  (case serialize-method
    nil [(fn [x] (let [baos (ByteArrayOutputStream.)]
                   (serialize/serialize-impl serialize/+clojure+ baos x)
                   (.toByteArray baos)))
         (partial serialize/deserialize-impl serialize/+clojure+)]
    :raw [(let [byte-class (class (byte-array 0))]
            #(do (when-not (instance? byte-class %)
                   (throw (RuntimeException. "Non-byte-array!"))) %))
          #(IOUtils/toByteArray ^java.io.InputStream %)]
    [(partial serialize/serialize serialize-method)
     serialize/deserialize-stream]))

(set! *warn-on-reflection* false)
(defmethod bucket :fs [{:keys [name path serialize-method merge] :as args}]
  (err/assert-keys [:name :path] args)
  (let [[serialize deserialize] (serialization-fns serialize-method)
        dir-path (str (java-io/file path name))
        f (if (string? dir-path)
            (java-io/file dir-path)
            dir-path)]
    (.mkdirs f)
    (->
     (reify
       IReadBucket
       (get [this k]
         (let [f (File. f ^String (codec/url-encode k))]
           (when (.exists f) (-> f java-io/input-stream deserialize))))
       (batch-get [this ks] (default-batch-get this ks))
       (seq [this] (default-seq this))
       (vals [this] (map second (seq this)))
       (exists? [this k]
         (let [f (File. f ^String (codec/url-encode k))]
           (.exists f)))
       (keys [this]
         (unchunk
          (for [^File c (.listFiles f)
                :when (and (.isFile c) (not (.isHidden c)))]
            (codec/url-decode (.getName c)))))
       (count [this] (long (clojure.core/count (keys this))))

       IMergeBucket
       (merge [this k v]
         (default-merge this merge k v))
       (batch-merge [this kvs]
         (default-batch-merge this merge kvs))

       IWriteBucket
       (put [this k v]
         (let [f (File. f ^String (codec/url-encode k))]
           (-> v serialize (java-io/copy f))))
       (batch-put [this kvs] (default-batch-put this kvs))
       (delete [this k]
         (let [f (File. f ^String (codec/url-encode  k))]
           (.delete f)))
       (update [this k f]
         (default-update this k f))
       (sync [this] nil)
       (close [this] nil))
     (wrapper-policy args))))
(set! *warn-on-reflection* true)

(defmethod bucket :mem [{:keys [merge]}]
  (hashmap-bucket (ConcurrentHashMap.) merge))

(defmethod bucket :mem-lru [{:keys [cache-size entry-weight-fn] :as m}]
  (assert (integer? cache-size))
  (lru-hashmap-bucket cache-size entry-weight-fn))

;;; Extend Read buckets to clojure maps

(def ^:private read-bucket-map-impls
  {:get (fn [this k] (this k))
   :seq (fn [this] (clojure/seq this))
   :batch-get (fn [this ks] (default-batch-get this ks))
   :keys (fn [this] (clojure/keys this))
   :exists? (fn [this k] (find this k))
   :count (fn [this] (clojure/count this))
   :vals (fn [this] (clojure/vals this))})

(doseq [c [clojure.lang.PersistentHashMap
           clojure.lang.PersistentArrayMap
           clojure.lang.PersistentStructMap]]
  (extend c IReadBucket read-bucket-map-impls))

(defnk bucket-resource [{env :stage} ec2-keys & bucket-args]
  (if (= env :test)
    (bucket
     (assoc bucket-args
       :type (if (= (:type bucket-args) :sql)
               :sql
               :mem)))
    (bucket (clojure.core/merge ec2-keys bucket-args))))

(def env-bucket (graph/instance bucket-resource [name env]
                  {:name (str name "-" (clojure.core/name env))}))

(defnk bucket-key [ec2-keys key {env :stage} & bucket-args]
  (if (= env :test)
    nil
    (let [b (bucket (clojure.core/merge ec2-keys bucket-args))
          v (get b key)]
      (close b)
      v)))

(def env-bucket-key (graph/instance bucket-key [name env]
                      {:name (str name "-" (clojure.core/name env))}))

(defn ->mem-bucket [data]
  (doto (bucket {})
    (batch-put data)))

(defn ->map
  "Convert a bucket into a map"
  [b]
  (into {} (seq b)))

(set! *warn-on-reflection* false)
