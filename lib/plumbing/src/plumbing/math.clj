(ns plumbing.math
  (:use plumbing.core)
  (:require
   [plumbing.core-incubator :as pci])
  (:import
   [java.text DecimalFormat DecimalFormatSymbols]))

(set! *warn-on-reflection* true)

(defn ceil [x] (long (Math/ceil x)))

(defn floor [x] (long (Math/floor x)))

(defn approx-= [a b & [tolerance]]
  (< (Math/abs (double (- a b))) (or tolerance 0.001)))

(def ^:const +log-2-base-10+ (Math/log 2))

(defn log2 ^double [^double x] (/ (Math/log x) +log-2-base-10+))

(defn entropy [xs]
  (- (sum
      (fn [x]
        (if (zero? x)
          x
          (* x (log2 x))))
      xs)))

(defn mutual-information
  "Takes a map from pairs to probabilities / counts."
  [xs]
  (let [t (double (sum second xs))
        norm (map-vals #(/ % t) xs)
        [x-marginals y-marginals] (for [ind [0 1]]
                                    (->> norm
                                         (group-by #(nth (key %) ind))
                                         (map-vals #(sum val %))))]
    (sum (fn [[[x y] p]]
           (if (zero? p) 0.0 (* p (log2 (/ p (x-marginals x) (y-marginals y))))))
         norm)))

(defn square [x] (* x x))

(defn mean [xs]
  (/ (reduce + xs) (count xs)))

(defn var [xs]
  (let [m (mean xs)]
    (mean (for [x xs] (square (- x m))))))

(defn median [xs]
  (let [sorted (vec (sort xs))
        mid (quot (count sorted) 2)]
    (nth sorted mid)))

(defn mode [xs]
  (->> xs frequencies vec shuffle (apply max-key second) first))

(defmacro map-reduce [f literal-combine init xs]
  `(let [f# ~f]
     (loop [r# ~init xs# (seq ~xs)]
       (if xs#
         (recur (~literal-combine r# (f# (first xs#))) (next xs#))
         r#))))

(defn- max-dd
  ^double [^double d1 ^double d2]
  (if (> d1 d2) d1 d2))

(defn max-od ^double [^clojure.lang.IFn$OD f xs]
  (map-reduce f max-dd Double/NEGATIVE_INFINITY xs))

(defn sum-od ^double [^clojure.lang.IFn$OD f xs]
  (map-reduce f + 0.0 xs))

(defmacro best-by-fast [f literal-better? xs]
  `(when-let [xs# (seq ~xs)]
     (let [f# ~f]
       (loop [best# (first xs#) best-val# (f# (first xs#)) remaining# (next xs#)]
         (if remaining#
           (let [fst# (first remaining#)
                 val# (f# fst#)]
             (if (~literal-better? val# best-val#)
               (recur fst# val# (next remaining#))
               (recur best# best-val# (next remaining#))))
           best#)))))

(defn max-key-od [^clojure.lang.IFn$OD f xs]
  (best-by-fast f > xs))

(defn fast-discretizer
  "Return a function from x to bin given a set of thresholds, in time
   logarithmic in the number of thresholds. Boundaries are classified
   as belonging to the higher index.  If `group-to-left?` is passed,
   then the threshold is classified as belonging to the lower index."
  ([thresholds] (fast-discretizer thresholds false))
  ([thresholds group-to-left?]
     (let [^java.util.Map m (for-map [[i t] (indexed thresholds)] (double t) i)
           s (java.util.TreeMap. m)
           n (count thresholds)]
       (fn [x]
         (if-let [e (if group-to-left?
                      (.ceilingEntry s (double x))
                      (.higherEntry s (double x)))]
           (.getValue e)
           n)))))

(defn discretize [val thresholds pad-to]
  "Generates a pretty string given thresholds."
  (if-let [t (first (filter #(<= val %) thresholds))]
    (str "<=" (let [ts (str t)] (str (apply str (repeat (- pad-to (count ts)) "0")) ts)))
    (str ">" (last thresholds))))

(defn quantiles [xs qs]
  "Return a map of quantiles of xs, where each of qs is between 0 and 1.
   Currently rounds down and does not do any averaging."
  (let [a (double-array xs)
        n (alength a)]
    (java.util.Arrays/sort a)
    (mapv #(aget a (min (dec n) (int (* (double %) n)))) qs)))

(defn normalize-vals
  "Normalize numeric vals in a map, or return nil when they total to 0."
  [m]
  (let [total (sum (vals m))]
    (when-not (zero? total)
      (map-vals #(/ % total) m))))

(defn histogram
  "Bin and normalize the values. Produce a map with human-readable keys."
  [ns bins pad]
  (->> ns (map #(discretize % bins pad)) frequencies normalize-vals))

(defn order-of-magnitude
  "Discretize based on the log of raw-val, within a range.
   Vals < start have value 0,
        < start * base have value 1,
   and so on up to cap at max-oom."
  ^long [^double raw-val ^double start ^double base ^long max-oom]
  (loop [cutoff start o 0]
    (if (or (>= o max-oom) (< raw-val cutoff))
      o
      (recur (* cutoff base) (unchecked-inc o)))))

(defn safe-div [n d] (double (if (zero? d) 0 (/ n d))))

(defn round
  "Round value to precision decimal places"
  [value precision]
  (let [scale (Math/pow 10 precision)]
    (/ (Math/round (* value scale)) scale)))

(let [max-digits 340
      df (doto (DecimalFormat. "0" (DecimalFormatSymbols/getInstance java.util.Locale/ENGLISH))
           (.setMaximumFractionDigits max-digits))]
  (defn compact-str
    "Encode x as the most compact representation that doesn't lose accuracy."
    [^double x]
    (.format df x)))

(defn coalesce [rows feature-keys merge-stats]
  (->> rows
       (pci/group-map-by #(select-keys % feature-keys) #(apply dissoc % feature-keys))
       (map-vals #(apply merge-stats %))
       (map (fn [[feats stats]] (merge feats stats)))))

(defn project
  "Takes a set of maps representing a combination of features and sufficient statistics,
   setting feature k to v and merging statistics for rows with matching features."
  [rows feature-keys merge-stats k v]
  (coalesce (map #(assoc % k v) rows) feature-keys merge-stats))

(set! *warn-on-reflection* false)
