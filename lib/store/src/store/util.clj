(ns store.util
  (:use plumbing.core)
  (:require
   [plumbing.logging :as log]
   [plumbing.parallel :as parallel]
   [store.bucket :as bucket]))

(defn put-versioned
  "Put a version of a file to a bucket. Uses the ts as the key, and
   returns the key"
  ;; TODO make a 3 arg version that takes a key and prepends the ts to it?
  [b v]
  (let [k (str (millis))]
    (bucket/put b k v)
    k))

(defn latest-with-key [b]
  (if-let [ks (not-empty (bucket/keys b))]
    (let [latest (apply max-key #(Long/parseLong %) ks)]
      [latest (bucket/get b latest)])
    (log/throw+ {:message (format "No versions have been put in this bucket yet: %s" b)})))

(defn get-latest-version
  "Get the key for the latest verion of a file from a bucket.
   For use with put-version above"
  [b]
  (second (latest-with-key b)))

(defn slurp-batches
  "Slurp batched data from a bucket in parallel."
  ([b read-fn]
     (slurp-batches b read-fn Integer/MAX_VALUE))
  ([b read-fn max-batches]
     (->> (bucket/keys b)
          (take max-batches)
          (parallel/map-work 32 (fn->> (bucket/get b) read-fn))
          aconcat
          vec)))
