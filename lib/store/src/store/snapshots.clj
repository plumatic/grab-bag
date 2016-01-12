(ns store.snapshots
  (:require
   [store.bucket :as bucket]
   [store.s3 :as s3]))

;; TOOD: remove this stuff -- canonical impl moved to service.bootstrap
(defn snapshot-store [{:keys [key secretkey]} service-name]
  (bucket/bucket
   {:type :s3
    :key key :secretkey secretkey
    :name "grabbag-snapshots" :key-prefix (str service-name "/")}))

(defn fresh-snapshot-store [config]
  (let [b (snapshot-store config (:service config))]
    (if (instance? store.s3.S3Bucket b)
      (s3/delete-all b)
      (doseq [k (bucket/keys b)] (bucket/delete b k)))
    b))

(defn fresh-test-snapshot-store [_]
  (bucket/bucket {:type :mem}))



(defn write-snapshot [ss n data]
  (bucket/put ss n data)
  (bucket/put ss "latest" n))

(defn read-snapshot [ss n]
  (bucket/get ss n))

(defn read-latest-snapshot [ss]
  (when-let [n (bucket/get ss "latest")]
    (read-snapshot ss n)))

(defn snapshot-seq
  "Lazy seq of all snapshots, starting with most recent"
  [ss]
  (when-let [n (bucket/get ss "latest")]
    ((fn snapshots [n]
       (lazy-seq
        (when (>= n 0)
          (cons (read-snapshot ss n)
                (snapshots (dec n))))))
     n)))
