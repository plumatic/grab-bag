(ns store.s3
  (:use plumbing.core)
  (:require
   [clojure.string :as str]
   [schema.core :as s]
   [plumbing.error :as err]
   [plumbing.logging :as log]
   [store.bucket :as bucket]
   [aws.core :as aws])
  (:import
   [com.amazonaws AmazonServiceException AmazonClientException]
   [com.amazonaws.auth AWSCredentialsProvider]
   [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
   [com.amazonaws.services.s3.model Bucket DeleteObjectsRequest
    ListObjectsRequest
    ObjectListing
    ObjectMetadata S3Object
    S3ObjectSummary]
   [java.io ByteArrayInputStream]
   [java.util.concurrent ConcurrentHashMap]
   [store.bucket HashmapBucket]))

(set! *warn-on-reflection* true)

(defn ^AmazonS3 s3-connection
  ([{access-key :key secret-key :secretkey}]
     (s3-connection access-key secret-key))
  ([k sk] (AmazonS3Client. (aws/credentials-provider k sk))))

(defn s3-buckets [^AmazonS3 s3]
  (.listBuckets s3))

(defn s3-bucket-names [s3]
  (map #(.getName ^Bucket %) (s3-buckets s3)))

(s/defn list-objects-request :- ListObjectsRequest
  "List distinct keys in bucketname with a given prefix (if present),
   truncating at the first instance of delimiter (if present),
   starting at the first key strictly lexicographically greater than
   marker (if present).
   http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/ListObjectsRequest.html"
  [bucket-name :- String
   prefix :- (s/maybe String)
   delimiter :- (s/maybe String)
   marker :- (s/maybe String)]
  (let [lor (ListObjectsRequest.)]
    (.setBucketName lor bucket-name)
    (when prefix (.setPrefix lor prefix))
    (when delimiter (.setDelimiter lor delimiter))
    (when marker (.setMarker lor marker))
    lor))

(defn ^ObjectMetadata metadata [^AmazonS3 s3 bucket key]
  (.getObjectMetadata s3 bucket key))

(defn size [s3 bucket key]
  (.getContentLength (metadata s3 bucket key)))

(defmacro with-retries [^String explanation & body]
  `(err/with-retries
     3 500
     #(throw (RuntimeException. ^String ~explanation ^Throwable %))
     ~@body))

(defn ^ObjectListing list-objects-page
  "List a single page of object keys from s3."
  [^AmazonS3 s3
   ^String bucket-name ^String prefix ^String delimiter ^String marker]
  (with-retries
    "Failed 3 times on s3 bucket list keys."
    (.listObjects s3 (list-objects-request bucket-name prefix delimiter marker))))

(defn paginate-object-listing [^AmazonS3 s3 ^ObjectListing ol]
  (concat (.getObjectSummaries ol)
          (when (.isTruncated ol)
            (lazy-seq (paginate-object-listing
                       s3
                       (with-retries
                         "Failed 3 times on s3 bucket list more keys."
                         (.listNextBatchOfObjects s3 ol)))))))

(defn list-objects [^AmazonS3 s3 ^String bucket-name & [^String prefix ^String marker]]
  (paginate-object-listing s3 (list-objects-page s3 bucket-name prefix nil marker)))

(defn get-keys [s3 bucket-name & [prefix marker]]
  (for [^S3ObjectSummary o (list-objects s3 bucket-name prefix marker)]
    (.substring (.getKey o) (count prefix))))

(defn du
  "List size of bucket by iterating through all the keys (the only way to do so
   programatically), grouping by group-fn (by default, up to first /)"
  ([s3 bucket-name prefix]
     (du s3 bucket-name prefix #(first (.split ^String % "/"))))
  ([s3 bucket-name prefix group-fn]
     (reduce
      (fn [m ^S3ObjectSummary o]
        (update m (group-fn (.substring (.getKey o) (count prefix))) (fnil + 0) (.getSize o)))
      {}
      (list-objects s3 bucket-name prefix))))

(defn get-key-prefixes [^AmazonS3 s3 ^String bucket-name ^String prefix ^String delimiter]
  ((fn lazy [marker]
     (let [res (list-objects-page s3 bucket-name prefix delimiter marker)]
       (concat (map #(.substring ^String % (count prefix)) (.getCommonPrefixes res))
               (when (.isTruncated res)
                 (lazy-seq (lazy (.getNextMarker res)))))))
   nil))


(defn create-bucket [^AmazonS3 s3 ^String bucket-name]
  (.createBucket s3 bucket-name))

(defn delete-bucket [^AmazonS3 s3 ^String bucket-name]
  (.deleteBucket s3 bucket-name))


(defn delete-objects [^AmazonS3 s3 bucket-name ks]
  (doseq [part (seq (partition-all 1000 ks))]
    (.deleteObjects s3 (-> (DeleteObjectsRequest. bucket-name)
                           (.withKeys ^"[Ljava.lang.String;" (into-array part))
                           (.withQuiet true)))))

(defn trim!
  "Delete all objects but the lastest keep-latest with this prefix."
  [s3 bucket-name key-prefix keep-latest]
  (let [ks (sort (map (partial str key-prefix) (get-keys s3 bucket-name key-prefix)))
        [kill retain] ((juxt drop-last take-last) keep-latest ks)]
    (log/infof "Trimming %s batches in range %s; keeping %s in range %s"
               (count kill) ((juxt first last) kill)
               (count retain) ((juxt first last) retain))
    (delete-objects s3 bucket-name kill)))

(defn delete-all-objects [^AmazonS3 s3 ^String bucket-name key-prefix]
  (delete-objects s3 bucket-name (map (partial str key-prefix) (get-keys s3 bucket-name key-prefix))))

(defn delete-bucket-recursive [^AmazonS3 s3 ^String bucket-name]
  (delete-all-objects s3 bucket-name "")
  (delete-bucket s3 bucket-name))


(defn delete-object [^AmazonS3 s3 ^String bucket-name ^String key]
  (.deleteObject s3 bucket-name key)
  :success)

(defn put-object
  "Puts the serialized object into the s3 bucket at the given keypath.
   Optionally takes a map of headers, which delegate to the appropriate
   ObjectMetadata setter methods for known headers.
   NOTE: Unfamiliar headers are still set on the request, but may fail to work in the future
   because they use an undocumented API."
  [^AmazonS3 s3 ^String bucket-name ^String key-path ^bytes serialized-object
   & [headers]]
  (let [meta (doto (ObjectMetadata.)
               (.setContentLength (count serialized-object)))]
    (doseq [[^String header value] headers]
      (case (str/lower-case header)
        "cache-control" (.setCacheControl meta value)
        "content-disposition" (.setContentDisposition meta value)
        "content-encoding" (.setContentEncoding meta value)
        "content-type"(.setContentType meta value)
        (.setHeader meta header value)))
    (with-retries
      "Failed 3 times on s3 bucket put."
      (.putObject s3 bucket-name key-path (ByteArrayInputStream. serialized-object) meta))
    serialized-object))

(defprotocol S3Bucket
  "An extension to bucket protocol for hierarchically structured, sorted String keys
   following s3 semantics.  See docs including this one for details:

   http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/ListObjectsRequest.html

   s3 buckets can also be constructed with a 'key prefix', where they operate over only the
   subset of keys in the bucket with this prefix, and where the prefix is implicit
   (not present) when interacting with the bucket.  E.g., if I put a key \"d\" into a bucket
   with key-prefix \"abc/\", a key \"abc/d\" will be written to the s3.  If I then call
   bucket/keys on this sub-bucket, I'll get back just [\"d\"].

   These operations are also (inefficiently) extended onto ordinary memory buckets."
  (bucket-name [this]
    "The name of the s3 bucket")
  (key-prefix [this]
    "The full prefix for this bucket")
  (put-with-headers [this k v headers]
    "Put an object with HTTP headers, which will be served up when objects are retrieved
     from s3 (or s3-backed cloudfront) over http")
  (delete-all [this]
    "Irreversibly batch delete all objects in this bucket.")
  (sub-bucket [this key-prefix]
    "Create a sub-bucket of this bucket, with a key-prefix equal to the concatenation
     of any existing key prefix for this concatenated with the provided key-prefix")
  (keys-with-prefix [this key-prefix] [this key-prefix marker]
    "List all keys with this prefix, in lexicographic order, starting at marker (exclusive)
     if provided.")
  (key-prefixes [this key-prefix delimiter]
    "List all unique matches of <key-prefix>(.*<delimiter>).* on keys in this bucket."))

(defn aws-404? [^Exception e]
  (and (instance? AmazonServiceException e)
       (= 404 (.getStatusCode ^AmazonServiceException e))))

(defn aws-fail-404?
  "The s3 client periodically fails to parse the server's bad missing key exceptions.
   Fail back to catching these 404 which are not proper AmazonServiceExceptions"
  [^Exception e]
  (and (instance? AmazonClientException e)
       (not (instance? AmazonServiceException e))
       (.contains (.getMessage e) "Response Code: 404")))

(defmethod bucket/bucket :s3
  [{:keys [prefix key-prefix merge name serialize-method num-retries create-if-needed]
    :or {num-retries 1 create-if-needed true}
    :as args}]
  (let [s3 (s3-connection args)
        bucket-name (str prefix name)
        [serialize deserialize] (bucket/serialization-fns serialize-method)]
    (when (and create-if-needed (not (.doesBucketExist s3 bucket-name)))
      (try (create-bucket s3 bucket-name) (catch Exception e (log/errorf e "create bucket error %s" bucket-name)
                                                 (throw e))))
    (->
     (reify bucket/IReadBucket
       (bucket/keys [this]
         (get-keys s3 bucket-name key-prefix))
       (bucket/get [this k] ;; TODO: missing ??
         ((fn suck [retries-left]
            (try (when-let [^S3Object o (.getObject s3 bucket-name (str key-prefix k))]
                   (with-open [is (.getObjectContent o)]
                     (when is (deserialize is))))
                 (catch Exception e
                   (cond (aws-404? e) nil
                         (pos? retries-left) (suck (dec retries-left))
                         (aws-fail-404? e)
                         (do (log/warnf e "Unexpected s3 client 404 error")
                             nil)
                         :else
                         (log/with-elaborated-exception
                           {:bucket-name bucket-name :key k :key-prefix key-prefix :serialize-method serialize-method}
                           (throw e))))))
          num-retries))
       (bucket/exists? [this k]
         (try (metadata s3 bucket-name (str key-prefix k))
              (catch Exception e (when-not (aws-404? e) (throw e)))))
       (bucket/seq [this] (bucket/default-seq this))
       (bucket/vals [this] (map #(bucket/get this %) (bucket/keys this)))
       (bucket/batch-get [this ks] (bucket/default-batch-get this ks))
       (bucket/count [this] (long (count (bucket/keys this))))

       bucket/IMergeBucket
       (bucket/merge [this k v] (bucket/default-merge this merge k v))

       bucket/IWriteBucket
       (bucket/put [this k v]
         (put-with-headers this k v nil))
       (bucket/batch-put [this kvs] (bucket/default-batch-put this kvs))
       (bucket/delete [this k]
         (with-retries
           "Failed 3 times on s3 bucket delete."
           (delete-object s3 bucket-name (str key-prefix k))))
       (bucket/update [this k f] (bucket/default-update this k f))
       (bucket/close [this] nil)
       (bucket/sync [this] nil)

       S3Bucket
       (bucket-name [this] bucket-name)
       (key-prefix [this] key-prefix)
       (put-with-headers [this k v headers]
         (put-object s3 bucket-name (str key-prefix k) (serialize v) headers)
         v)
       (delete-all [this]
         (with-retries
           "Failed 3 times on s3 bucket mass delete."
           (delete-all-objects s3 bucket-name key-prefix)))
       (sub-bucket [this new-kp]
         (-> args
             (update-in [:key-prefix] str new-kp)
             (assoc :create-if-needed false)
             bucket/bucket))
       (keys-with-prefix [this key-prefix1]
         (keys-with-prefix this key-prefix1 nil))
       (keys-with-prefix [this key-prefix1 marker]
         (for [^String k (get-keys s3 bucket-name (str key-prefix key-prefix1)
                                   (when marker (str key-prefix marker)))]
           (str key-prefix1 k)))
       (key-prefixes [this key-prefix1 delimiter]
         (get-key-prefixes s3 bucket-name (str key-prefix key-prefix1) delimiter)))
     (bucket/wrapper-policy args))))

(defn s3-bucket-wrapper
  "Takes a bucket and wraps it so that it supports the S3Bucket operations.
   Writing to sub-buckets will write through to the underlying bucket."
  [b & [merge-fn sub-bucket-prefix]]
  (let [sub-bucket-prefix (str sub-bucket-prefix)]
    (reify
      bucket/IReadBucket
      (get [this k]
        (bucket/get b (str sub-bucket-prefix k)))
      (batch-get [this ks] (bucket/default-batch-get this ks))
      (seq [this]
        (bucket/default-seq this))
      (keys [this]
        (sort
         (let [prefix-length (count sub-bucket-prefix)]
           (for [^String k (bucket/keys b)
                 :when (.startsWith k sub-bucket-prefix)]
             (subs k prefix-length)))))
      (vals [this]
        (map #(bucket/get this %) (bucket/keys this)))
      (exists? [this k] (bucket/exists? b (str sub-bucket-prefix k)))
      (count [this] (long (count (bucket/seq this))))

      bucket/IMergeBucket
      (merge [this k v]
        (bucket/default-merge this merge-fn k v))

      bucket/IWriteBucket
      (put [this k v]
        (bucket/put b (str sub-bucket-prefix k) v))
      (batch-put [this kvs] (bucket/default-batch-put this kvs))
      (delete [this k]
        (bucket/delete b (str sub-bucket-prefix k)))
      (update [this k f] (bucket/default-update this k f))
      (sync [this] (bucket/sync b))
      (close [this] (bucket/close b))

      S3Bucket
      (bucket-name [this] (bucket-name b))
      (key-prefix [this] (str (key-prefix b) sub-bucket-prefix))
      (put-with-headers [this k v headers]
        (put-with-headers b (str sub-bucket-prefix k) v headers))
      (delete-all [this] (doseq [k (bucket/keys this)] (bucket/delete this k)))
      (sub-bucket [this key-prefix]
        (s3-bucket-wrapper b merge-fn (str sub-bucket-prefix key-prefix)))
      (keys-with-prefix [this key-prefix]
        (filter (fn [k] (.startsWith ^String k key-prefix)) (bucket/keys this)))
      (keys-with-prefix [this key-prefix marker]
        (drop-while #(<= 0 (compare marker %))
                    (keys-with-prefix this key-prefix)))
      (key-prefixes [this key-prefix delimiter]
        (->> (keys-with-prefix this key-prefix)
             (map (fn-> (subs (count key-prefix))
                        (.split delimiter)
                        first
                        (str delimiter)))
             distinct)))))

(extend-type HashmapBucket
  S3Bucket
  (bucket-name [this] "<!hashmap!>")
  (key-prefix [this] "")
  (put-with-headers [this k v _]
    (bucket/put this k v))
  (sub-bucket [this key-prefix]
    (sub-bucket (s3-bucket-wrapper this) key-prefix))
  (key-prefixes [this key-prefix delimiter]
    (key-prefixes (s3-bucket-wrapper this) key-prefix delimiter))
  (keys-with-prefix
    ([this key-prefix]
       (keys-with-prefix (s3-bucket-wrapper this) key-prefix))
    ([this key-prefix marker]
       (keys-with-prefix (s3-bucket-wrapper this) key-prefix marker))))

(defn s3-url [bucket key]
  (str "s3://" (bucket-name bucket) "/" (key-prefix bucket) key))

(set! *warn-on-reflection* false)
