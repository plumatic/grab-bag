(ns domain.docs.fitness-stats
  "Mutable type to hold view count and Facebook statistics for a Doc."
  (:use plumbing.core)
  (:require potemkin))

(set! *warn-on-reflection* true)

(potemkin/definterface+ PFitnessStats
  (update-facebook-error! [this])
  (set-facebook-counts! [this counts])
  (set-view-count! [this ^long new-view-count])
  (increment-view-count! [this new-views])
  (merge-in! [this other-stats])
  (write-fitness-stats [this])
  (view-count ^long [this])
  (facebook-updated-date ^long [this])
  (facebook-likes ^long [this])
  (facebook-shares ^long [this])
  (facebook-comments ^long [this])
  (facebook-total-count ^long [this]))

(deftype FitnessStats [^long ^:unsynchronized-mutable facebook-likes
                       ^long ^:unsynchronized-mutable facebook-comments
                       ^long ^:unsynchronized-mutable facebook-shares
                       ^long ^:unsynchronized-mutable facebook-updated-date
                       ^long ^:unsynchronized-mutable view-count]
  PFitnessStats
  (update-facebook-error! [this]
    (set! facebook-updated-date (millis)))
  (set-facebook-counts! [this counts]
    (locking this
      (letk [[like_count comment_count share_count] counts]
        (set! facebook-likes (long (or like_count 0)))
        (set! facebook-comments (long (or comment_count 0)))
        (set! facebook-shares (long (or share_count 0)))
        (set! facebook-updated-date (millis)))))
  (set-view-count! [this new-view-count]
    (locking this
      (set! view-count new-view-count)))
  (increment-view-count! [this new-views]
    (locking this
      (set! view-count (+ view-count (long new-views)))))
  (merge-in! [this other-stats]
    (let [[other-facebook-likes other-facebook-comments other-facebook-shares other-facebook-updated-date
           other-view-count]
          (write-fitness-stats other-stats)]
      (locking this
        (when (> other-facebook-updated-date (long facebook-updated-date))
          (set! facebook-likes (long other-facebook-likes))
          (set! facebook-comments (long other-facebook-comments))
          (set! facebook-shares (long other-facebook-shares))
          (set! facebook-updated-date (long other-facebook-updated-date)))
        (set! view-count (+ view-count (long other-view-count))))))
  (write-fitness-stats [this]
    [facebook-likes facebook-comments facebook-shares facebook-updated-date view-count])

  (facebook-updated-date [this] facebook-updated-date)
  (view-count [this] view-count)
  (facebook-likes [this] facebook-likes)
  (facebook-shares [this] facebook-shares)
  (facebook-comments [this] facebook-comments)
  (facebook-total-count [this] (+ facebook-likes facebook-comments facebook-shares))

  Object
  (equals [this x]
    (and (instance? FitnessStats x)
         (= (write-fitness-stats this) (write-fitness-stats x))))
  (hashCode [this]
    (hash (write-fitness-stats this))))

(defn ^FitnessStats read-fitness-stats [l]
  (when-not (= (count l) 5) (throw (RuntimeException. (str "Bad fitness stats " l))))
  (let [[a b c d e] l]
    (FitnessStats. a b c d e)))

(defn ^FitnessStats clone [^FitnessStats fs]
  (-> fs write-fitness-stats read-fitness-stats))

(defn ^FitnessStats fitness-stats []
  (FitnessStats. 0 0 0 0 0))


(set! *warn-on-reflection* false)
