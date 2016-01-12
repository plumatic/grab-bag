(ns flop.stats
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [hiphip.double :as dbl]
   [plumbing.math :as plumbing-math]
   [flop.math :as math])
  (:import
   [java.util Random]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(s/defn sample-mean-variance :- (s/pair double "mean" double "variance")
  "Compute the sample mean and variance"
  [^double sum-xs ^double sum-sq-xs ^double n]
  (let [mean (if (zero? n) Double/NaN (/ sum-xs n))]
    [mean
     (if (<= n 1)
       Double/POSITIVE_INFINITY
       (/ (+ sum-sq-xs
             (* -2 mean sum-xs)
             (* n mean mean))
          (- n 1)))]))

(s/defn mean-variance :- (s/pair double "mean" double "variance")
  "Compute the mean and variance"
  [sum-xs sum-sq-xs n]
  (let [mean (if (zero? n) Double/NaN (/ sum-xs n))]
    [mean
     (if (<= n 0)
       Double/POSITIVE_INFINITY
       (/ (+ sum-sq-xs
             (* -2 mean sum-xs)
             (* n mean mean))
          n))]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Univariate Stats

(defrecord UnivariateStats
    [^double sum-xs ^double sum-sq-xs ^double min-x ^double max-x ^double num-obs])

(defn add-obs
  ([^UnivariateStats uni-stats ^double x]
     (add-obs uni-stats x 1))
  ([^UnivariateStats uni-stats ^double x ^double wt]
     (let [sum-xs (+ (.sum-xs uni-stats) (* x wt))
           sum-sq-xs (+ (.sum-sq-xs uni-stats) (* x x wt))
           min-x (min x (.min-x uni-stats))
           max-x (max x (.max-x uni-stats))
           num-obs (+ (.num-obs uni-stats) wt)]
       (UnivariateStats. sum-xs sum-sq-xs min-x max-x num-obs))))

(defn merge-stats [^UnivariateStats uni-stats1 ^UnivariateStats uni-stats2]
  (let [sum-xs (+ (.sum-xs uni-stats1) (.sum-xs uni-stats2))
        sum-sq-xs (+ (.sum-sq-xs uni-stats1) (.sum-sq-xs uni-stats2))
        min-x (min (.min-x uni-stats1) (.min-x uni-stats2))
        max-x (max (.max-x uni-stats1) (.max-x uni-stats2))
        num-obs (+ (.num-obs uni-stats1) (.num-obs uni-stats2))]
    (UnivariateStats. sum-xs sum-sq-xs min-x max-x num-obs)))

(defn scale-stats [^UnivariateStats uni-stats ^double w]
  (UnivariateStats.
   (* w (.sum-xs uni-stats))
   (* w (.sum-sq-xs uni-stats))
   (.min-x uni-stats)
   (.max-x uni-stats)
   (* w (.num-obs uni-stats))))

(def +empty-uni-stats+ (UnivariateStats. 0.0 0.0 Double/POSITIVE_INFINITY Double/NEGATIVE_INFINITY 0))

(defn uni-stats
  "construct a univariate stats object from a sequence of observations"
  ^UnivariateStats
  [observations]
  (UnivariateStats.
   (sum observations)
   (sum #(* % %) observations)
   (reduce min Double/POSITIVE_INFINITY observations)
   (reduce max Double/NEGATIVE_INFINITY observations)
   (count observations)))

(defn weighted-stats [xw-pairs]
  (reduce
   (fn [stats [x w]]
     (add-obs stats x w))
   +empty-uni-stats+
   xw-pairs))

(defn uni-mean ^double [^UnivariateStats uni-stats]
  (/ (.sum-xs uni-stats) (.num-obs uni-stats)))

(defn uni-sample-mean-var
  [^UnivariateStats uni-stats]
  (sample-mean-variance (.sum-xs uni-stats) (.sum-sq-xs uni-stats) (.num-obs uni-stats)))

(defn uni-sample-mean-std-str [uni-stats]
  (let [[m v] (uni-sample-mean-var uni-stats)]
    (format "%.4g (± %.2g)" (double m) (Math/sqrt (double v)))))

(defn uni-report [uni-stats]
  (let [[mean var] (uni-sample-mean-var uni-stats)]
    {:min (:min-x uni-stats)
     :max (:max-x uni-stats)
     :mean mean
     :var var
     :count (:num-obs uni-stats)}))

(defn new-bernoulli-stats
  "Returns a new UnivariateStats instance for a Bernoulli distribution"
  ([] +empty-uni-stats+)
  ([num-success num-fail]
     (UnivariateStats.
      (double num-success)
      (double num-success)
      (cond
       (pos? num-fail) 0.0
       (pos? num-success) 1.0
       :else Double/POSITIVE_INFINITY)
      (cond
       (pos? num-success) 1.0
       (pos? num-fail) 0.0
       :else Double/NEGATIVE_INFINITY)
      (+ num-success num-fail))))

(let [;; 95% t values from
      index (plumbing-math/fast-discretizer (concat (range 1 30) [30 40 50 60 80 100 120]) true)
      vs [12.71 4.303 3.182 2.776 2.571 2.447 2.365 2.306 2.262 2.228 2.201 2.179 2.160 2.145 2.131 2.120 2.110 2.101 2.093 2.086 2.080 2.074 2.069 2.064 2.060 2.056 2.052 2.048 2.045 2.042 2.021 2.009 2.000 1.990 1.984 1.980 1.960]]
  (defn two-sided-t-95
    "Table-based approximation for the point in a T distribution at which 95%
     of the mass is between -x and +x, useful for constructing confidence intervals
     with small n (number of observations, = DOF + 1)

     https://en.wikipedia.org/wiki/Student%27s_t-distribution"
    ^double [n]
    (when (> n 1)
      (nth vs (index (dec n))))))

(defn uni-sample-mean-error
  "Mean and 95% confidence error."
  [^UnivariateStats uni-stats]
  (let [[mean var] (uni-sample-mean-var uni-stats)
        n (.num-obs uni-stats)]
    [mean
     (if (<= n 2)
       Double/POSITIVE_INFINITY
       (/ (* (two-sided-t-95 (long n))
             (Math/sqrt var))
          (Math/sqrt (double n))))]))

(defn uni-conf-interval-str
  "95% confidence interval string with small-sample correction."
  [uni-stats]
  (let [[m e] (uni-sample-mean-error uni-stats)]
    (format "%.4g (± %.2g)" (double m) (double e))))

(defn uni-conf-interval
  "95% confidence interval values with small-sample correction."
  [^UnivariateStats uni-stats]
  (let [[mean tail] (uni-sample-mean-error uni-stats)]
    (list (- mean tail) (+ mean tail))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; BivariateStats

(defrecord BivariateStats
    [^double sum-xs
     ^double sum-ys
     ^double sum-xys
     ^double sum-sq-xs
     ^double sum-sq-ys
     ^long num-obs])

(defn add-xy-obs [^BivariateStats bi-stats [^double x ^double y]]
  (let [sum-xs (+ (.sum-xs bi-stats) x)
        sum-ys (+ (.sum-ys bi-stats) y)
        sum-xys (+ (.sum-xys bi-stats) (* x y))
        sum-sq-xs (+ (.sum-sq-xs bi-stats) (* x x))
        sum-sq-ys (+ (.sum-sq-ys bi-stats) (* y y))
        num-obs (inc (.num-obs bi-stats))]
    (BivariateStats. sum-xs sum-ys sum-xys sum-sq-xs sum-sq-ys num-obs)))

(defn merge-bi-stats [^BivariateStats bi-stats1 ^BivariateStats bi-stats2]
  (let [sum-xs (+ (.sum-xs bi-stats1) (.sum-xs bi-stats2))
        sum-ys (+ (.sum-ys bi-stats1) (.sum-ys bi-stats2))
        sum-xys (+ (.sum-xys bi-stats1) (.sum-xys bi-stats2))
        sum-sq-xs (+ (.sum-sq-xs bi-stats1) (.sum-sq-xs bi-stats2))
        sum-sq-ys (+ (.sum-sq-ys bi-stats1) (.sum-sq-ys bi-stats2))
        num-obs (+ (.num-obs bi-stats1) (.num-obs bi-stats2))]
    (BivariateStats. sum-xs sum-ys sum-xys sum-sq-xs sum-sq-ys num-obs)))

(def +empty-bi-stats+ (BivariateStats. 0.0 0.0 0.0 0.0 0.0 0))

(s/defn bi-stats
  "Construct a new bivariate stats from a sequence of xy-pairs"
  [xys :- [(s/pair s/Num "x" s/Num "y")]]
  (let [square (fn [z] (* z z))
        sum-xs (sum first xys)
        sum-ys (sum second xys)
        sum-xys (sum #(apply * %) xys)
        sum-sq-xs (sum (comp square first) xys)
        sum-sq-ys (sum (comp square second) xys)
        num-obs (count xys)]
    (BivariateStats. sum-xs sum-ys sum-xys sum-sq-xs sum-sq-ys num-obs)))

(defn bi-report
  "Report statistics about this bi-stats.
   Includes fitting a linear regression: y = beta1 * x + beta0
   and the correlation coefficient, r-xy between x and y.
   http://faculty.washington.edu/dbp/s423/PDFs/02-chapter-ALR-for-printing.pdf (II-14)"
  [^BivariateStats bi-stats]
  (let [n (.num-obs bi-stats)
        [mean-x var-x] (sample-mean-variance
                        (.sum-xs bi-stats) (.sum-sq-xs bi-stats) (.num-obs bi-stats))
        [mean-y var-y] (sample-mean-variance
                        (.sum-ys bi-stats) (.sum-sq-ys bi-stats) (.num-obs bi-stats))

        sxy (- (.sum-xys bi-stats) (* n mean-x mean-y))
        sxx (- (.sum-sq-xs bi-stats) (* n mean-x mean-x))
        syy (- (.sum-sq-ys bi-stats) (* n mean-y mean-y))

        beta1 (/ sxy sxx)
        beta0 (- mean-y (* beta1 mean-x))

        r-xy (/ sxy (Math/sqrt (* sxx syy)))]
    {:mean-x mean-x
     :mean-y mean-y
     :var-x var-x
     :var-y var-y
     :count n
     :beta0 beta0
     :beta1 beta1
     :r-xy r-xy}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Beta Distributions and Bandits

(defn beta-confidence-interval
  "Estimate a two-sided confidence interval of a probability by sampling n times."
  [n-pos n-neg n p-value]
  (let [ns (long (* n (- 1 p-value) 0.5))
        r (Random. 123)
        alpha (double (+ n-pos 0.5))
        beta (double (+ n-neg 0.5))
        xs (doto (dbl/amake [i n] (math/sample-beta r alpha beta)) java.util.Arrays/sort)]
    [(aget xs (dec ns)) (aget xs (- n ns))]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Sliding window stats

(s/defrecord WindowedStats
    [queue
     window-size :- long
     stats :- UnivariateStats])

(defn windowed-stats [window-size]
  (WindowedStats. clojure.lang.PersistentQueue/EMPTY window-size +empty-uni-stats+))

(defn full? [^WindowedStats ws]
  (= (count (.queue ws)) (.window-size ws)))

(defn add-windowed-obs [^WindowedStats ws ^double x]
  (let [q (.queue ws)
        window (.window-size ws)
        ^WindowedStats ws (if (= (count q) window)
                            (WindowedStats.
                             (pop q)
                             window (add-obs (.stats ws) (peek q) -1.0))
                            ws)]
    (WindowedStats.
     (conj (.queue ws) x)
     window
     (add-obs (.stats ws) x))))

(set! *warn-on-reflection* false)
