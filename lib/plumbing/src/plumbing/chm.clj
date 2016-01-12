(ns plumbing.chm
  (:use plumbing.core)
  (:import
   [com.googlecode.concurrentlinkedhashmap
    ConcurrentLinkedHashMap
    ConcurrentLinkedHashMap$Builder
    EntryWeigher]
   [java.util.concurrent ConcurrentMap ConcurrentHashMap]))

(set! *warn-on-reflection* true)

(defn ^ConcurrentHashMap chm []
  (ConcurrentHashMap.))

;; concurrent linekd hashmap from google http://code.google.com/p/concurrentlinkedhashmap/
;; (https://github.com/ben-manes/concurrentlinkedhashmap on github)
;; can also support entry weights and stuff like that.
(defn ^ConcurrentLinkedHashMap lru-chm
  "Optionally weighted lru, (weight-fn k v) returns int weight, max-weight is long."
  [max-weight & [weight-fn]]
  (let [b (doto (ConcurrentLinkedHashMap$Builder.)
            (.maximumWeightedCapacity max-weight))]
    (when weight-fn
      (.weigher b (reify EntryWeigher
                    (weightOf [this k v] (weight-fn k v)))))
    (.build b)))

(defn try-replace! [^ConcurrentMap h k v old-v]
  (cond (= v old-v) (= (.get h k) v)

        (nil? v)
        (.remove h k old-v)

        (nil? old-v)
        (nil? (.putIfAbsent h k v))

        :else
        (.replace h k old-v v)))

(defn try-update! [^ConcurrentMap h k f]
  (let [v (.get h k)
        new-v (f v)
        replaced? (cond
                   (identical? v new-v) true
                   (nil? new-v) (or (nil? v) (.remove h k v))
                   (nil? v) (nil? (.putIfAbsent h k new-v))
                   :else (.replace h k v new-v))]
    {:success? replaced?
     :old-value v
     :new-value new-v}))

(defn update-pair! [^ConcurrentMap h k f]
  (loop []
    (letk [[success? old-value new-value] (try-update! h k f)]
      (if success?
        [old-value new-value]
        (recur)))))

(defn update! [^ConcurrentMap h k f]
  (second (update-pair! h k f)))

(defn inc! [^ConcurrentMap h k v]
  (update! h k (fn [x] (+ (or x 0.0) v))))

(defn ensure! [^ConcurrentMap h k f]
  (when-not (.containsKey h k)
    (.putIfAbsent h k (f))))


(set! *warn-on-reflection* false)