(ns plumbing.queue
  (:refer-clojure :exclude [peek])
  (:import
   [java.util.concurrent
    BlockingQueue
    LinkedBlockingQueue
    PriorityBlockingQueue]))

(set! *warn-on-reflection* true)

(defn poll [^java.util.Queue q] (.poll q))

(defn peek [^java.util.Queue q] (.peek q))

(defn offer [^java.util.Queue q x]
  (.offer q x))

(defn put [^BlockingQueue q x] (.put q x))

(defn ^BlockingQueue local-queue
  "return LinkedBlockingQueue implementation
   of Queue protocol."
  ([]
     (LinkedBlockingQueue.))
  ([^java.util.Collection xs]
     (LinkedBlockingQueue. xs)))

(defn ^BlockingQueue blocking-queue [capicity]
  (LinkedBlockingQueue. (int capicity)))

(defn drain
  "return seq of drained elements from Blocking Queue"
  [^BlockingQueue q & [max-elems]]
  (let [c (java.util.ArrayList.)]
    (if max-elems
      (.drainTo q c (int max-elems))
      (.drainTo q c))
    (seq c)))


(defn priority-queue
  ([]
     (PriorityBlockingQueue.))
  ([^java.util.Collection xs]
     (PriorityBlockingQueue. xs))
  ([init-size ordering]
     (PriorityBlockingQueue. init-size (comparator ordering))))

(defn clear-priority-queue [^PriorityBlockingQueue q]
  (.clear q))

(defn offer-all [q vs]
  (doseq [v vs]
    (offer q v)))
