(ns plumbing.expiring-cache
  (:refer-clojure :exclude [get put count update])
  (:use [plumbing.core :exclude [update]])
  (:require
   [plumbing.chm :as chm]
   [plumbing.error :as err]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource])
  (:import
   [java.util Collections LinkedHashMap Map]))

(set! *warn-on-reflection* true)

;; TODO(jw): move to store, build these on top of buckets
;; (possibly abstracted with write/read policies)

(defprotocol PExpiringCache
  (get [this k] [this k f])
  (delete [this k])
  (put [this k v])
  (count [this])
  (update [this k f])
  (_prune-old-entries [this]))

;; TODO make in-flight safe

(deftype ExpiringCache
    [^Map m ^long timeout-ms ^long prune-ms ^long ^:volatile-mutable last-prune]
  PExpiringCache
  (get [this k]
    (_prune-old-entries this)
    (first (.get m k)))
  (get [this k f]
    (if-let [e (.get m k)]
      (do
        (_prune-old-entries this)
        (first e))
      (put this k (f k))))
  (delete [this k] (first (.remove m k)))
  (put [this k v]
    (_prune-old-entries this)
    (.remove m k)
    (.put m k [v (millis)])
    v)
  (count [this]
    (.size m))
  (update [this k f]
    (let [res (f (get this k))]
      (if (nil? res)
        (delete this k)
        (put this k res))))
  (_prune-old-entries [this]
    (let [now (millis)]
      (when (<= last-prune (- now prune-ms))
        (locking m
          (when (<= last-prune (- now prune-ms))
            (set! last-prune now)
            (let [it (.iterator (.values m))
                  old (- now timeout-ms)]
              (loop []
                (when (.hasNext it)
                  (let [[_ timestamp] (.next it)]
                    (when (<= (long timestamp) old)
                      (.remove it)
                      (recur))))))))))))

(defn create
  "Keeps things up to timeout-ms. Stores data as a Synchronized linked hashmap.
   Lightweight in that it doesn't have it's own thread for pruning.  Instead,
   piggiebacks on the thread that is putting data to also prune (if prune-ms is < now - last-prune-time) before it puts."
  [^long timeout-ms ^long prune-ms]
  (-> (LinkedHashMap.)
      Collections/synchronizedMap
      (ExpiringCache. timeout-ms prune-ms (millis))))

(defn cached-fn [timeout-ms f]
  (let [ec (create timeout-ms 1000)]
    (fn memoized [& args] (get ec args #(apply f %)))))


;; This one is heavier-weight, uses a background thread to prune.
;; But is lock-free, and has a keep-alive option.

(deftype ConcurrentExpiringCache [^Map m keep-alive? on-expire flush-pool]
  PExpiringCache
  (get [this k]
    (let [v (.get m k)]
      (when (and v keep-alive?)
        (chm/update! m k (fn [[val timestamp :as pair]]
                           [(or val (first v)) (millis)])))
      (first v)))
  (get [this k f]
    (or (get this k)
        (put this k (f k))))
  (delete [this k] (first (.remove m k)))
  (put [this k v] (.put m k [v (millis)]) v)
  (count [this] (.size m))
  (_prune-old-entries [this]
    (throw (RuntimeException. "No manual pruning for concurrent exp cache")))
  (update [this k f]
    (first (chm/update! m k (fn [[val]]
                              (when-let [res (f val)]
                                [res (millis)])))))

  resource/PCloseable
  (close [this]
    (doseq [k (keys m)]
      (let [[ov nv] (chm/update-pair! m k (fn [e] nil))]
        (when (and ov (not nv)) (on-expire [k (first ov)]))))
    (parallel/shutdown-now flush-pool)))


(defn prune-old-entries [^Map m ^long timeout-ms on-expire]
  (let [old (- (millis) timeout-ms)]
    (doseq [k (keys m)]
      (let [[ov nv] (chm/update-pair!
                     m k
                     (fn [[_ timestamp :as val]]
                       (when (and timestamp (> timestamp old))
                         val)))]
        (when (and ov (not nv)) (err/?error "Error in concurrent-expiring-cache prune"
                                            (on-expire [k (first ov)])))))))

(defn create-concurrent [^long timeout-ms ^long prune-ms keep-alive? on-expire]
  (let [m (chm/chm)]
    (ConcurrentExpiringCache.
     m
     keep-alive?
     on-expire
     (parallel/schedule-work #(prune-old-entries m timeout-ms on-expire) (/ prune-ms 1000.0)))))

(defnk concurrent-expiring-cache [timeout-ms prune-ms keep-alive? {on-expire (constantly nil)}]
  (create-concurrent (long timeout-ms) (long prune-ms) keep-alive? on-expire))



(set! *warn-on-reflection* false)
