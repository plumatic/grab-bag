(ns dashboard.utils
  (:use plumbing.core)
  (:require
   [flop.stats :as stats]))

(defn format-conf-interval [num den]
  (if (or (not den) (zero? den) (zero? num))
    ""
    (let [[lower upper] (stats/uni-conf-interval (stats/new-bernoulli-stats num (- den num)))]
      (format "(%.2f%%; %.2f%%)" (* 100 lower) (* 100 upper)))))

(defn format-number-and-percent [n p]
  (str n " (" (format "%.2f" p) "%)"))

(defn format-with-percent [n d]
  (format-number-and-percent n (float (/ (* 100 (int n)) d))))
