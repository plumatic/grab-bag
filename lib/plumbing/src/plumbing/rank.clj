(ns plumbing.rank
  (:refer-clojure :exclude [update ])
  (:import
   [java.util Comparator PriorityQueue]))

(set! *warn-on-reflection* true)

(defrecord ScoredItem [item ^double score])

(def scored-comparator
  (reify
    Comparator
    (compare [_ x y]
      (let [d (- (.score ^ScoredItem x) (.score ^ScoredItem y))]
        (cond (pos? d) (int 1)
              (neg? d) (int -1)
              :else (int 0))))
    (equals [x y]
      (= (.score ^ScoredItem x) (.score ^ScoredItem y)))))

(def scored-inverse-comparator
  (reify
    Comparator
    (compare [_ x y]
      (let [d (- (.score ^ScoredItem x) (.score ^ScoredItem y))]
        (cond (pos? d) (int -1)
              (neg? d) (int 1)
              :else (int 0))))
    (equals [x y]
      (= (.score ^ScoredItem x) (.score ^ScoredItem y)))))

(defn drain-queue [^PriorityQueue q]
  (loop [elts nil]
    (if (.isEmpty q)
      elts
      (recur (cons (.item ^ScoredItem (.poll q)) elts)))))

(defn top-k [k score-fn xs]
  (let [k (int k)
        q (PriorityQueue. (inc k) scored-comparator)]
    (doseq [x xs]
      (.offer q (ScoredItem. x (score-fn x)))
      (when (> (.size q) k)
        (.poll q)))
    (drain-queue q)))

;; TODO(jw): this can be up to a few times faster using hiphip:
(comment
  (defn top-k
    "Return the top k elements of xs according to score-fn"
    [k score-fn xs]
    (let [n (count xs)
          scores (dbl/amake [i n] (score-fn (nth xs i)))
          ^ints idxs (dbl/amax-indices scores k)]
      (seq (hiphip.array/amake Object [i k] (nth xs (aget idxs (- n i 1))))))))

(defprotocol Reranker
  (score [this x])
  (update [this x]))

(defn rerank [reranker xs]
  (when (seq xs)
    (let [q (PriorityQueue. (count xs) scored-comparator)]
      (doseq [x xs]
        (.offer q (ScoredItem. x (- (score reranker x)))))
      ((fn lazy-reranker []
         (when-not (.isEmpty q)
           (cons
            (loop []
              (let [f ^ScoredItem (.poll q)
                    cur-score  (score reranker (.item f))
                    next-score (if-let [nxt (.peek q)] (- (.score ^ScoredItem nxt)) Double/NEGATIVE_INFINITY)]
                (if (>= cur-score next-score)
                  (do (update reranker (.item f)))
                  (do (.offer q (ScoredItem. (.item f) (- cur-score)))
                      (recur)))))
            (lazy-seq (lazy-reranker)))))))))

(set! *warn-on-reflection* false)
