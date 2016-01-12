(ns flop.array
  (:require
   [hiphip.double :as dbl]
   [flop.math :as fm])
  (:import
   [flop DArray]
   [java.util Random]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

(defn scale-in-place! [^doubles a ^double scale]
  (dbl/afill! [x a] (* x scale)))

(defn scale [^doubles a scale]
  (scale-in-place! (dbl/aclone a) scale))

(defn add-in-place! [^doubles accum ^doubles by scale offset]
  (DArray/addInPlace accum by (double scale) (double offset))
  accum)

;; 50% faster than afill!
(defn multiply-in-place! [^doubles accum ^doubles by]
  (DArray/multiplyInPlace accum by)
  accum)

(defn interpolate [^doubles a ^double ascale ^doubles b ^double bscale]
  (dbl/amap [x a y b] (+ (* ascale x) (* bscale y))))

;;; Utilities.

(defn normalize! [^doubles d]
  (let [t (double (dbl/asum d))]
    (if (= t (double 0.0))
      d
      (scale-in-place! d (/ (double 1.0) t)))))

(defn normalize [^doubles d]
  (normalize! (dbl/aclone d)))

(defn random-direction [^Random r d]
  (let [a (dbl/amake [i d] (fm/sample-gaussian r))]
    (scale-in-place! a (/ 1.0 (Math/sqrt (dbl/asum [x a] (* x x)))))))

(definline sample-discrete [^Random r ^doubles dist]
  `(DArray/sampleDiscrete ~r ~dist))

(definline log-add [^doubles logV]
  `(DArray/logAdd ~logV))

(defn log-normalize-in-place! [^doubles logV]
  (let [log-sum (log-add logV)]
    (dbl/afill! [v logV] (- v log-sum))))

(defn rand-dirichlet [random K alpha]
  (let [K (int K), alpha (double alpha)]
    (normalize! (dbl/amake [_ K] (fm/sample-gamma random alpha 1.0)))))

(defn l1 [^doubles xs ^doubles ys]
  (dbl/asum [x xs y ys] (Math/abs (- x y))))

(defn sup-norm [^doubles xs ^doubles ys]
  (dbl/areduce [x xs y ys] r 0.0 (Math/max r (Math/abs (- x y)))))

(defn approx-equal?-fn [tol]
  (fn [^doubles xs ^doubles ys]
    (let [len (alength xs)]
      (if (or (nil? xs) (nil? ys) (not (= len (alength ys))))
        false
        (loop [i 0]
          (cond
           (= i len) true
           (not (let [x (dbl/aget xs i) y (dbl/aget ys i)]
                  (< (Math/abs (- x y)) tol))) false
                  :default (recur (inc i))))))))

(defn ^long discretize
  "Given a monotonically increasing array of bin separators, returns the min index i into bins
   s.t. val is <= (aget bins i), or (inc (alength bins)) if no such index exists."
  [^doubles bins ^double val]
  (let [m (alength bins)]
    (loop [i 0]
      (if (or (== i m) (<= val (aget bins i)))
        i
        (recur (inc i))))))

(set! *warn-on-reflection* false)
