(ns store.cache
  (:use plumbing.core)
  (:require [store.bucket :as bucket]))

;; TODO(jw): eventually move all cache-related stuff in here,
;; plumbing.cache. plumbing.expiring-cache, other cache stuff, figure out how to
;; unify all of it.

;; TODO(jw): this should probably be an adapter of sorts, like serialization, as part
;; of bucket API, hopefully we can just write this once rather than 10 times
;; and it can compose with other things like bucket choice, pruning time choice, ...

(defn expiring-bucket-wrapper
  "Wraps an underlying bucket with behavior to expire keys after at most timeout-ms from
   the put (unaffected by subsequent gets).  keys and count may return a stale view;  stale
   values will be removed lazily on getting the actual value, or on calls to sync.
   Treats a put of nil as a delete."
  [bucket timeout-ms]
  (let [wrap (fn [v] [v (millis)])
        unwrap (fn [[v t]]
                 (when (> (+ t timeout-ms) (millis))
                   v))]
    (reify
      bucket/IReadBucket
      (keys [this] (bucket/keys bucket))
      (vals [this] (map second (bucket/seq this)))
      (get [this k] (when-let [e (bucket/get bucket k)]
                      (let [v (unwrap e)]
                        (if-not (nil? v)
                          v
                          (do (bucket/delete bucket k) nil)))))
      (batch-get [this ks] (bucket/default-batch-get this ks))
      (seq [this] (keep #(when-let [v (bucket/get this %)] [% v]) (bucket/keys bucket)))
      (exists? [this k] (boolean (bucket/get this k)))
      (count [this] (bucket/count bucket))

      bucket/IWriteBucket
      (put [this k v]
        (do (if (nil? v)
              (bucket/delete bucket k)
              (bucket/put bucket k (wrap v)))
            v))
      (batch-put [this kvs] (bucket/default-batch-put this kvs))
      (delete [this k] (bucket/delete bucket k))
      (update [this k f] (bucket/update bucket k
                                        (fn [e]
                                          (let [nv (f (when e (unwrap e)))]
                                            (when-not (nil? nv)
                                              (wrap nv))))))
      (sync [this] (dorun (bucket/seq this)))
      (close [this] (bucket/close bucket)))))

(defn cache
  "Get the value stored under k, or compute, store, and return (f k)."
  [bucket f k]
  (let [v (bucket/get bucket k)]
    (if-not (nil? v)
      v
      (let [v (f k)]
        (bucket/put bucket k v)
        v))))

(defn bucket-memoizer
  "Memoize function f (of one argument), using bucket for storage of cached values."
  [bucket f]
  (fn memoized [k]
    (cache bucket f k)))