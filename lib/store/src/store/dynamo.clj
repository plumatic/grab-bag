(ns store.dynamo
  (:use plumbing.core)
  (:require
   [plumbing.logging :as log]
   [plumbing.serialize :as serialize]
   [aws.dynamo :as dynamo]
   [store.bucket :as bucket])
  (:import
   [com.amazonaws.services.dynamodbv2.model AttributeValue
    Condition
    ConditionalCheckFailedException
    DeleteItemRequest
    ExpectedAttributeValue
    GetItemRequest
    GetItemResult
    LimitExceededException
    ProvisionedThroughputExceededException
    PutItemRequest
    PutItemResult
    QueryRequest
    QueryResult
    ScanRequest
    ScanResult]
   [store.bucket HashmapBucket]))


(set! *warn-on-reflection* true)

(defn ensure-seq [x] (if (instance? Iterable x) x [x]))

(defn to-key [key-spec k]
  (let [k (ensure-seq k)]
    (assert (= (count key-spec) (count k)))
    (zipmap (map name key-spec) (map dynamo/to-attribute k))))

(defn to-datum [key-spec [k m]]
  (assert (every? keyword? (keys m)))
  (assert (not-any? #(contains? m %) key-spec))
  (let [k (ensure-seq k)]
    (assert (= (count k) (count key-spec)))
    (for-map [[k v] (concat m (map vector key-spec k))]
      (name k) (dynamo/to-attribute v))))

(defn from-datum [key-spec m]
  (let [m (for-map [[k v] m] (keyword k) (dynamo/from-attribute v))]
    [(vec (map m key-spec))
     (apply dissoc m key-spec)]))

;;; Actual item ops

(defprotocol DynamoBucket
  (consistent-get [this k])
  (used-read-capacity [this])
  (used-write-capacity [this])
  (scan [this start-key attributes page-size])
  (query-page [this hash-value start-key backward? range-condition attributes limit])
  (query [this hash-value start-key backward? range-condition attributes page-size])
  (simple-query [this hash-value start backward? limit]))


(defn dynamo-method [table-name f max-attempts]
  (loop [attempt 1]
    (let [sleep-exponentially
          (fn [e]
            (log/warnf e "Dynamo limit exceeded for table %s. Exception type %s" table-name (class e))
            (Thread/sleep (rand-int (long (Math/pow 5 attempt))))
            ::retry)

          abort
          (fn [e]
            (log/errorf e "Unable to write to dynamo table %s after %d tries" table-name attempt)
            (throw e))

          handle-exception
          (fn [e] (if (>= attempt max-attempts) (abort e) (sleep-exponentially e)))

          value (try (f)
                     (catch LimitExceededException e
                       (handle-exception e))
                     (catch ProvisionedThroughputExceededException e
                       (handle-exception e)))]

      (if (= ::retry value)
        (recur (inc attempt))
        value))))

(defn atomic-update
  "Updates value at k by applying f to current value and using
   bucket/put-cond to ensure atomicity. Note that f may be called
   multiple times, and thus should be free of side-effects.
   Returns new value that was written to bucket."
  [b k f]
  (loop []
    (let [v (bucket/get b k)
          new-v (f v)]
      (if (bucket/put-cond b k new-v v)
        new-v
        (recur)))))

(defmethod bucket/bucket :dynamo
  [{:keys [merge ^String name serialize-method inconsistent-read? binary?] :as args}]
  (let [client (dynamo/dynamo-client args)
        td     (dynamo/describe-table {:client client :name name})
        serialize (case serialize-method
                    nil identity
                    :clj (fn [x] {:data (pr-str x)})
                    :raw (fn [x] {:data x})
                    (if binary?
                      (fn [x] {:data (serialize/serialize serialize-method x)})
                      (fn [x] {:data (serialize/utf8-serialize serialize-method x)})))
        deserialize (case serialize-method
                      nil identity
                      :clj (fn [x] (when x (read-string (:data x))))
                      :raw (fn [x] (when x (:data x)))
                      (fn [x]
                        (when-let [v (:data x)]
                          (if (string? v)
                            (serialize/utf8-deserialize v)
                            (serialize/deserialize v)))))
        deserialize (fn [v]
                      (let [ds (deserialize v)]
                        (if (instance? clojure.lang.IMeta ds)
                          (with-meta ds {:raw v})
                          ds)))
        key-spec (dynamo/table-key-spec td)
        used-read (atom 0.0)
        used-write (atom 0.0)
        get-fn (fn [k incons?]
                 (let [res ^GetItemResult
                       (dynamo-method name
                                      #(.getItem client (doto (GetItemRequest. name (to-key key-spec k))
                                                          (.setConsistentRead (not incons?))
                                                          (.setReturnConsumedCapacity dynamo/+total+)))
                                      6)]
                   (swap! used-read + (dynamo/capacity-units (.getConsumedCapacity res)))
                   (when-let [item (.getItem res)]
                     (deserialize (second (from-datum key-spec item))))))]
    ;; (println (bean td))
    (->
     (reify bucket/IReadBucket
       (bucket/keys [this]
         (map first (scan this nil (map clojure.core/name key-spec) nil)))
       (bucket/get [this k] (get-fn k inconsistent-read?))
       (bucket/exists? [this k] (if (bucket/get this k) true false))
       (bucket/seq [this] (scan this nil nil nil))
       (bucket/batch-get [this ks] (throw (UnsupportedOperationException.)))
       (bucket/count [this] (throw (UnsupportedOperationException.)))

       bucket/IMergeBucket
       (bucket/merge [this k v] (bucket/default-merge this merge k v))

       bucket/IWriteBucket
       (bucket/put [this k v]
         (let [res ^PutItemResult (dynamo-method name
                                                 #(.putItem client (doto (PutItemRequest. name (to-datum key-spec [k (serialize v)]))
                                                                     (.setReturnConsumedCapacity dynamo/+total+)))
                                                 6)]
           (swap! used-write + (dynamo/capacity-units (.getConsumedCapacity res)))
           v))
       (bucket/delete [this k] (dynamo-method name #(.deleteItem client (DeleteItemRequest. name (to-key key-spec k))) 6))
       (bucket/update [this k f] (atomic-update this k f))
       (bucket/close [this] (dynamo/shutdown-client client))
       (bucket/sync [this] nil)

       bucket/IConditionalWriteBucket
       (put-cond [this k v req-old-v]
         (try ;; TODO: handle remove and partial attributes properly.
           (let [req (doto (PutItemRequest. name (to-datum key-spec [k (serialize v)]))
                       (.setExpected (if req-old-v
                                       (map-vals (fn [att-val] (ExpectedAttributeValue. ^AttributeValue att-val))
                                                 (to-datum key-spec
                                                           [k (or (when (instance? clojure.lang.IMeta req-old-v)
                                                                    (:raw (meta req-old-v)))
                                                                  (serialize req-old-v))]))
                                       (for-map [k key-spec]
                                         (clojure.core/name k) (ExpectedAttributeValue. false))))
                       (.setReturnConsumedCapacity dynamo/+total+))
                 res ^PutItemResult (dynamo-method name #(.putItem client req) 6)]
             (swap! used-write + (dynamo/capacity-units (.getConsumedCapacity res)))
             true)
           (catch ConditionalCheckFailedException e nil)))

       DynamoBucket
       (consistent-get [this k] (get-fn k false))
       (used-read-capacity [this] @used-read)
       (used-write-capacity [this] @used-read)
       (scan [this start-key attributes page-size]
         (let [req (ScanRequest. name)]
           (.setReturnConsumedCapacity req dynamo/+total+)
           (when start-key (.setExclusiveStartKey req start-key))
           (when attributes (.setAttributesToGet req attributes))
           (when page-size (.setLimit req (Integer. (int page-size))))
           (let [res ^ScanResult (dynamo-method name #(.scan client req) 6)]
             (swap! used-read + (dynamo/capacity-units (.getConsumedCapacity res)))
             (lazy-cat
              (map #(let [d (from-datum key-spec %)]
                      (if attributes
                        d
                        (update-in d [1] deserialize)))
                   (.getItems res))
              (when-let [lk (.getLastEvaluatedKey res)]
                (lazy-seq (scan this lk attributes page-size)))))))
       (query-page [this hash-value start-key backward? range-condition attributes limit]
         (let [req (QueryRequest. name)]
           (doto req
             (.addKeyConditionsEntry (clojure.core/name (first key-spec))
                                     (doto (Condition.)
                                       (.setAttributeValueList [(dynamo/to-attribute hash-value)])
                                       (.setComparisonOperator "EQ")))
             (.setReturnConsumedCapacity dynamo/+total+)
             (.setScanIndexForward (if backward? false true))
             (.setConsistentRead (not inconsistent-read?)))
           (when start-key (.setExclusiveStartKey req start-key))
           (when attributes (.setAttributesToGet req attributes))
           (when limit (.setLimit req (Integer. (int limit))))
           (when range-condition
             (assert (= 2 (count key-spec)) "Table does not have range key")
             (.addKeyConditionsEntry req
                                     (clojure.core/name (second key-spec))
                                     range-condition))
           (let [res ^QueryResult (dynamo-method name #(.query client req) 6)]
             (swap! used-read + (dynamo/capacity-units (.getConsumedCapacity res)))
             [(map #(let [d (from-datum key-spec %)]
                      (if attributes
                        d
                        (update-in d [1] deserialize)))
                   (.getItems res))
              (.getLastEvaluatedKey res)])))
       (query [this hash-value start-key backward? range-condition attributes page-size]
         (let [[docs last-k] (query-page this hash-value start-key backward? range-condition attributes page-size)]
           (lazy-cat docs
                     (when last-k
                       (lazy-seq (query this hash-value last-k backward? range-condition attributes page-size))))))

       (simple-query [this hash-value start backward? limit]
         (query-page this hash-value nil backward?
                     (when start
                       (doto (Condition.)
                         (.setComparisonOperator (if backward? "LT" "GT"))
                         (.setAttributeValueList [(dynamo/to-attribute start)])))
                     nil limit)))
     (bucket/wrapper-policy args))))


(defn- paginate [limit s]
  (let [[result more] (split-at limit s)]
    [result
     (when (seq more) (-> result last first second))]))

(defn- simple-query-seq [b hash-value start-key backward? page-limit]
  (let [[vs last-k] (simple-query b hash-value start-key backward? page-limit)]
    (lazy-cat vs
              (when last-k
                (lazy-seq (simple-query-seq b hash-value last-k backward? page-limit))))))

(extend-type HashmapBucket
  DynamoBucket
  ;; partial impl only meant to get us through resource/slave in :test
  (simple-query [this hash-value start backward? limit]
    (->> this
         bucket/seq
         (filter (fn-> ffirst (= hash-value)))
         (sort-by (fn-> first second))
         (?>> backward? reverse)
         (?>> start
              (drop-while #(>= (* (if backward? -1 1) (compare start (-> % first second))) 0)))
         (paginate limit)))

  (query [this hash-value start-key backward? range-condition attributes page-size]
    (assert (not range-condition)) (assert (not attributes)) (assert (not start-key))
    (simple-query-seq this hash-value nil backward? page-size)))

(set! *warn-on-reflection* false)
