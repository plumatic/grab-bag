(ns flop.binning
  "Utilities for automatically binning items to reduce distortion"
  (:use plumbing.core)
  (:require
   [hiphip.double :as dbl]
   [flop.stats :as stats]
   [schema.core :as s])
  (:import
   [flop.stats UnivariateStats]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(s/defschema Partition
  "Represents a partitioning of a sequence into bins"
  {:indices (s/named [s/Int] "split indices of the binned sequence")
   :score (s/named s/Num "sum of the distortions within each bin")
   :r2 (s/named s/Num "score normalized by total distortion of the input")})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(defn cum-sum
  [xs]
  (object-array (reductions
                 (fn [^UnivariateStats a ^UnivariateStats b] (stats/merge-stats a b))
                 (UnivariateStats. 0 0 0 0 0)
                 xs)))

(defn distortion ^double
  [^long num-obs ^double sum-xs ^double sum-sq-xs]
  (if (zero? num-obs)
    0.0
    (let [p2 (/ sum-sq-xs num-obs)
          p (/ sum-xs num-obs)]
      (* num-obs (- p2 (* p p))))))

(defn distortion-difference
  "The distortion resulting from subtracting stats (- b a)"
  ^double
  [^UnivariateStats b ^UnivariateStats a]
  (distortion
   (- (.num-obs b) (.num-obs a))
   (- (.sum-xs b) (.sum-xs a))
   (- (.sum-sq-xs b) (.sum-sq-xs a))))

(defn span-distortion
  "returns the cumulative distortion in the [i-inclusive, j-exclusive) span"
  ^double [^objects cum-sums
           ^long i
           ^long j]
  (let [^Stats a (aget cum-sums i)
        ^Stats b (aget cum-sums j)]
    (distortion-difference b a)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(s/defn opt-bins :- Partition
  "Find partitioning of stats into bins that minimizes the sum of the distortions of the bins.

   Implementation is via dynamic programming:
    define bins(n, e) = 'best way to partition vector into n bins ending with position e'
     bins(1, i) = var[0:i]
     bins(n + 1, e) = min_{n<j<e}  var([j:e]) + bins(n, j)"
  [num-bins :- s/Int
   stats :- [UnivariateStats]]
  (assert (< num-bins (count stats))
          {:num-bins num-bins :stats-count (count stats)})
  (let [cum-sums (cum-sum stats)
        N (inc (count stats))
        table (reductions
               (fn [^doubles prev ^long n]
                 (dbl/amake [e N]
                            (dbl/areduce [[start score] prev :range [n e]]
                                         res
                                         Double/POSITIVE_INFINITY
                                         (min res (+ score (span-distortion cum-sums start e))))))
               (dbl/amake [i N] (span-distortion cum-sums 0 i))
               (range 1 num-bins))
        value (aget ^doubles (last table) (dec N))]
    ;; search for solution
    (loop [e (dec N)
           score value
           [^doubles scores & table] (next (reverse table))
           bins []]
      (if (nil? scores)
        {:score value
         :r2 (- 1 (/ value (span-distortion cum-sums 0 (dec N))))
         :indices bins}
        (let [ni (dbl/areduce [[start sc] scores :range [0 e]]
                              i
                              -1
                              (if (= score (+ sc (span-distortion cum-sums start e)))
                                (long start) i))]
          (recur
           (long ni)
           (aget scores ni)
           table
           (cons ni bins)))))))

(set! *warn-on-reflection* false)
