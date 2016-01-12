(ns flop.empirical-gradient-test
  (:require
   [hiphip.double :as dbl]
   [flop.empirical-gradient :as empirical-gradient]
   [plumbing.core :refer :all]
   [plumbing.test :refer :all]
   [clojure.test :refer :all]))

(deftest empirical-gradient-test
  (let [f (fn [xs] (sum #(* % %) xs))
        grad-f (fn [xs] (dbl/amap [x xs] (* 2.0 x)))]
    (is-= [[2.0 4.0] [0.0 2.0]]
          (empirical-gradient/empirical-gradient
           {:num-dims 2
            :f (juxt f grad-f)
            :dx 2.0
            :xs (double-array [1.0 0])}))))
