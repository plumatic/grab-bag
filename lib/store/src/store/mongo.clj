(ns store.mongo
  (:refer-clojure :exclude [update ])
  (:use [plumbing.core :exclude [update]])
  (:require
   [monger.collection :as mc]
   [monger.core :as monger]
   [monger.query :as query]
   [store.bucket :as bucket])
  (:import
   [com.mongodb WriteConcern]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Protocols (meant to decouple from mongo / bucket)

(defprotocol LogDataStore
  (drop-collection [this name])
  (ensure-index [_ name ks unique? index-name-override]
    "if index-name-override is not-nil, will use it as the index name")
  (bucket-collection [this name] "Treat a collection as a bucket; value should be doc and key will be _id.")
  (capped-collection [this name args] "Args has :max-mb, optional :max-count")
  (aggregate-collection [this name args] "Args has :agg-keys, optional :daily?"))

(defprotocol QueryableCollection
  (query [_ q]
    "Query is a map, with optional keys:
      :query, a map of required fiels and values
      :sort, a map from fields to +/- 1
      :fields, a list of fields to return.
      :skip and :limit,
      :batch-size"))

(defprotocol AppendableCollection
  "a collection to which you can append, possibly dropping earlier elements"
  (append [this m]))

(defprotocol AggregatedCollection
  "given set of keys provided by base-m, which together make up a primary key,
   update the corresponding document by setting keys in sets to the provided values
   and incrementing keys in incs by the provided values.  Example:

   from an empty database,
   (update this {:date \"20120101\" :user 100} {:last-login \"20111212\" :click-count 1} {})
   yields a new document
   {:date \"20120101\" :user 100 :last-login \"20111212\" :click-count 1}
   and following by
   (update this {:date \"20120101\" :user 100} {:last-login \"20111215\"} {:click-count 2 :view-count 4})
   updates this document to be
   {:date \"20120101\" :user 100 :last-login \"20111215\" :click-count 3 :view-count 4}"
  (update [this base-m sets incs] [this base-m raw-ops])
  (insert [this base-m] "Insert if does not yet exist."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Implementation


(def +mongo-gen-key+ ::kittens-on-our-abstraction)

(defn mongo-query [db coll-name q]
  (assert (every? #{:query :sort :fields :skip :limit :batch-size} (keys q)))
  (monger/with-db db
    (query/with-collection coll-name
      (merge q))))

(defn- pack [k v]
  (assert (or (not v) (map? v)))
  (assert (not (contains? v :_id)))
  (if (= k +mongo-gen-key+)
    v
    (assoc v :_id k)))

(defn- unpack-local [m]
  [(:_id m) (-> m
                (dissoc :_id)
                )])

(defn mongo-bucket [db coll-name write-concern unpack]
  (reify
    store.bucket.IReadBucket
    (get [this k]
      (dissoc (monger/with-db db (mc/find-map-by-id coll-name k)) :_id))

    (batch-get [this ks]
      (bucket/default-batch-get this ks))

    (exists? [this k]
      (monger/with-db db (mc/find-map-by-id coll-name k []0)))

    (seq [this]
      (->> (mc/find-maps coll-name {} [])
           (monger/with-db db)
           (map unpack)))
    (keys [this]
      (->> (mc/find-maps coll-name {} [])
           (monger/with-db db)
           (map (comp first unpack))))
    (vals [this]
      (map second (bucket/seq this)))

    QueryableCollection
    (query [this q] (mongo-query db coll-name q))

    store.bucket.IWriteBucket
    (put [this k v] ;; gargle
      (binding [monger/*mongodb-write-concern* write-concern]
        (monger/with-db db
          (mc/save coll-name (pack k v)))))

    (batch-put [this kvs]
      (bucket/default-batch-put this kvs))

    (delete [this k]
      (monger/with-db db
        (mc/remove coll-name {:_id k}))) ;; Monger doesn't take write concern...

    (sync [this])

    (close [this])

    (update [this k f] ;; FCS -- wrong concurrency semantics
      (monger/with-db db
        (let [v (bucket/get this k)
              vp (f v)]
          (bucket/put this k vp)
          vp)))

    store.bucket.IMergeBucket
    (merge [this k v]
      (monger/with-db db
        (mc/update coll-name {:_id k} {"$set" v} :upsert true :write-concern write-concern)))
    (batch-merge [this kvs]
      (bucket/default-batch-merge this kvs))))

(def +wc-normal+ WriteConcern/NORMAL)
(def +wc-safe+ WriteConcern/SAFE)


(defmethod bucket/bucket :mongo
  [{:keys [connection host port db name write-concern unpack]
    :or {write-concern +wc-safe+
         unpack unpack-local}}]
  (assert (or connection (and host port)))
  (assert db)
  (let [connection (or connection (monger/connect {:host host :port port}))]
    (mongo-bucket
     (monger/get-db connection db)
     name
     write-concern unpack)))

;; TODO: just auto-munging keys for now, do something smarter
(defn munge-key [x]
  (let [s (if (keyword? x) (name x) (str x))
        s (if (.startsWith s "$") (str "_" s) s)]
    (.replace s "." "__")))

(defn munge-map [m]
  (map-keys munge-key m))

(defrecord MongoLogDataStore [db write-concern munge-keys?]
  LogDataStore
  (drop-collection [_ name]
    (monger/with-db db (mc/drop name)))
  (ensure-index [_ name ks unique? index-name-override]
    (monger/with-db db
      (mc/ensure-index
       name
       (->> (for [k ks]
              (let [[k dir] (if (vector? k) k [k 1])]
                (assert (#{-1 1} dir))
                [(if munge-keys? (munge-key k) k)
                 dir]))
            (apply concat)
            (apply array-map))
       (assoc-when
        {:unique (boolean unique?)}
        :name index-name-override))))
  (bucket-collection [_ name]
    (mongo-bucket db name write-concern unpack-local))
  (capped-collection [_ name args]
    (letk [[max-mb {max-count nil} & more] args]
      (assert (empty? more))
      (monger/with-db db
        (when-not (mc/exists? name)
          (println "Creating collection" name)
          (mc/create
           name
           (assoc
               (if max-count {:max max-count} {})
             :capped true
             :size (* max-mb 1024 1024)))))
      (reify AppendableCollection
        (append [this m] ;; gargle
          (binding [monger/*mongodb-write-concern* write-concern]
            (monger/with-db db
              (mc/save name (if munge-keys? (munge-map m) m)))))
        QueryableCollection
        (query [this q] (mongo-query db name q)))))
  (aggregate-collection [_ name args]
    (letk [[agg-keys {daily? false} {index-name-override nil}] args]
      (let [munged-keys (if munge-keys? (map munge-key agg-keys) agg-keys)]
        (when daily? (println "Warning: ignoring daily option for now in mongo."))
        (monger/with-db db
          (when-not (mc/exists? name)
            (println "Creating collection" name)
            (mc/create name {}))
          (mc/ensure-index
           name
           (apply array-map (interleave munged-keys (repeat 1)))
           (assoc-when
            {:unique true}
            :name index-name-override))) ;;TODO:unique
        (reify AggregatedCollection
          (update [this base-m raw-ops]
            (monger/with-db db
              (mc/update name (apply array-map (interleave munged-keys (map base-m agg-keys)))
                         raw-ops :upsert true :write-concern write-concern)))
          (update [this base-m sets incs]
            (update
             this base-m
             (merge
              {}
              (when sets
                (assert (map? sets))
                {"$set" (if munge-keys? (munge-map sets) sets)})
              (when incs
                (assert (map? incs))
                {"$inc" (if munge-keys? (munge-map incs) incs)}))))
          (insert [this base-m] "Insert if does not yet exist. Should fail if exists due to unique index."
            (monger/with-db db
              (mc/insert name base-m write-concern)))
          QueryableCollection
          (query [this q] (mongo-query db name q)))))))

(defn health-check [db]
  (monger/with-db db
    (mc/exists? "grabbag")))

(defn mongo-log-data-store [address db & [write-concern no-munge-keys?]]
  (MongoLogDataStore.
   (doto (monger/get-db (monger/connect address) db)
     health-check)
   (or write-concern +wc-safe+)
   (not no-munge-keys?)))

(defn test-log-data-store []
  (let [buckets (atom {})]
    (reify LogDataStore
      (drop-collection [_ name]
        (swap! buckets dissoc name))

      (ensure-index [_ name ks unique? index-name-override])

      (bucket-collection [_ name]
        (safe-get (swap! buckets #(merge {name (bucket/bucket {})} %)) name))

      (capped-collection [this name args]
        (assert (:max-mb args))
        (let [b (bucket-collection this name)
              id (atom 0)
              max-count (or (:max-count args) Integer/MAX_VALUE)]
          (reify AppendableCollection
            (append [this m]
              (let [i (swap! id inc)]
                (when (> i max-count) (bucket/delete b (- i max-count)))
                (bucket/put b i m))))))

      (aggregate-collection [this name args]
        (let [b (bucket-collection this name)
              agg-keys (:agg-keys args)]
          (assert (seq agg-keys))
          (reify AggregatedCollection
            (update [this base-m raw-ops]
              (assert (every? #{"$set" "$inc" "$addToSet" "$push"} (keys raw-ops)))
              (bucket/update
               b (map base-m agg-keys)
               (fn [m]
                 (-> (apply assoc m (interleave agg-keys (map base-m agg-keys)))
                     (merge (get raw-ops "$set"))
                     (#(merge-with + % (get raw-ops "$inc")))
                     (#(merge-with into % (map-vals (fn [x] #{x}) (get raw-ops "$addToSet"))))
                     (#(merge-with conj % (get raw-ops "$push")))))))
            (update [this base-m sets incs]
              (update this base-m {"$set" sets "$inc" incs}))
            (insert [this base-m]
              (let [k (map base-m agg-keys)]
                (when-not (bucket/exists? b k)
                  (bucket/put b k base-m))))
            clojure.lang.Seqable
            (seq [this] (bucket/seq b))))))))
