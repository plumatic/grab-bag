(ns domain.interests.type.bucket
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [store.bucket :as bucket]
   [domain.interests.core :as interests]
   [domain.interests.manager :as manager]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(defn- key-key [key]
  (str "key://" key))

(defn- index-key [key]
  (str "id://" key))

(defn- entry [id interest]
  {:id id :interest interest})

(defn bad-image-replacement [interest]
  (when (or (map? (:img interest))
            (map? (:highres-img interest)))
    (let [fix #(if-not (map? %)
                 %
                 (let [i (safe-get % :url)]
                   (assert (string? i))
                   i))]
      (-> interest
          (update-in-when [:img] fix)
          (update-in-when [:highres-img] fix)))))

(defn- cached-get
  "Replacement-fn takes an interest and returns a new interest to write in its place, or
   nil to leave the current value alone."
  [cache bucket k replacement-fn]
  (or (bucket/get cache k)
      (let [v (bucket/get bucket k)]
        (when-not (nil? v)
          (let [interest (safe-get v :interest)
                bad-image-replacement (bad-image-replacement interest)
                replacement-interest (or (replacement-fn
                                          (or bad-image-replacement interest))
                                         bad-image-replacement)
                final-v (if replacement-interest
                          (entry (safe-get v :id) replacement-interest)
                          v)]
            (doseq [b (if replacement-interest [cache bucket] [cache])
                    bk [(index-key (safe-get v :id)) (key-key (safe-get interest :key))]]
              (bucket/put b bk final-v))
            final-v)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(defrecord BucketInterestTypeManager
    [cache id-bucket namespace-size bad-interest? key->interest]
  manager/PInterestTypeManager
  (type-index! [this interest]
    (let [interest-key (key-key (safe-get interest :key))]
      (if-let [previous-value (cached-get cache id-bucket interest-key
                                          #(when (or (bad-interest? %)
                                                     (s/check interests/Interest %))
                                             (s/validate interests/Interest interest)))]
        (safe-get previous-value :id)
        (let [id (pci/rand-long namespace-size)
              v (entry id (s/validate interests/Interest interest))
              key-id (index-key id)]
          (or
           (when (bucket/put-cond id-bucket key-id v nil)
             (if (bucket/put-cond id-bucket interest-key v nil)
               (do ;; only add to cache if both puts succeed
                 (bucket/put cache key-id v)
                 (bucket/put cache interest-key v)
                 id)
               (do (bucket/delete id-bucket key-id) nil)))
           (recur interest))))))

  (type-lookup [this index]
    (safe-get (cached-get cache id-bucket (index-key index) (constantly nil)) :interest))

  (type-key-index [this key]
    (or (get (cached-get cache id-bucket (key-key key) (constantly nil)) :id)
        (when key->interest (->> key key->interest manager/sanitize-interest (manager/type-index! this))))))

(defn bucket-interest-type-manager
  "Pass an optional bad-interest? fn to replace bad values on the next type-index!
   Pass optional key->interest fn when you want lookups on unindexed keys to produce an interest
   by calling key->interest on the key, instead of returning ni. Used, e.g., for raw-query interests."
  [id-bucket namespace-size cache-size & [bad-interest? key->interest]]
  (->BucketInterestTypeManager
   (bucket/lru-hashmap-bucket cache-size)
   id-bucket
   namespace-size
   (or bad-interest? (constantly false))
   key->interest))
