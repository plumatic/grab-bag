(ns store.snapshot-bucket
  (:use plumbing.core)
  (:require
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [plumbing.serialize :as serialize]
   [store.bucket :as bucket])
  (:import
   [store.bucket IConditionalWriteBucket IMergeBucket IReadBucket IWriteBucket]))

(defnk s3-snapshot-bucket
  [ec2-keys snapshot-bucket-name snapshot-key period-secs env
   {prune identity} {mem-> identity} {->mem identity} {serialize-method serialize/+default+}
   {read-only? false}]
  (let [bucket (bucket/bucket {})
        s3b (if (= env :test)
              (bucket/bucket {})
              (bucket/bucket (merge ec2-keys {:type :s3 :name snapshot-bucket-name :serialize-method serialize-method})))
        prune-and-snapshot #(do (prune bucket)
                                (bucket/put s3b snapshot-key
                                            (for [[k v] (bucket/seq bucket)] [k (mem-> v)])))]
    (doseq [[k v] (bucket/get s3b snapshot-key)]
      (bucket/put bucket k (->mem v)))
    (let [sched-work (when-not read-only?
                       (parallel/schedule-work prune-and-snapshot period-secs))]
      (reify
        IReadBucket
        (keys [this] (bucket/keys bucket))
        (vals [this] (bucket/vals bucket))
        (get [this k] (bucket/get bucket k))
        (batch-get [this ks] (bucket/batch-get bucket ks))
        (seq [this] (bucket/seq bucket))
        (exists? [this k] (bucket/exists? bucket k))
        (count [this] (bucket/count bucket))

        IMergeBucket
        (merge [this k v] (bucket/merge bucket k v))
        (batch-merge [this kvs] (bucket/batch-merge bucket kvs))

        IWriteBucket
        (put [this k v] (bucket/put bucket k v))
        (batch-put [this kvs] (bucket/batch-put bucket kvs))
        (delete [this k] (bucket/delete bucket k))
        (update [this k f] (bucket/update bucket k f))
        (sync [this] (bucket/sync bucket))
        (close [this] (do (when-not read-only?
                            (resource/close sched-work)
                            (prune-and-snapshot))
                          (bucket/close bucket)))

        IConditionalWriteBucket
        (put-cond [this k v old-v] (bucket/put-cond bucket k v old-v))))))
