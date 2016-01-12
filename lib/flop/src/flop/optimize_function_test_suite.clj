(ns flop.optimize-function-test-suite
  "A collection of differentiable, multivariate functions for
   verifying correctness of optimizer code."
  (:use plumbing.core)
  (:require
   [hiphip.double :as dbl]))

(defn square [x]
  (* x x))

(defn shifted
  "Returns a version of f shifted by vector a"
  [f a]
  (fn [xs]
    (f (dbl/amap [x xs c a] (- x c)))))

(defn sphere [xs]
  [(sum square xs)
   (dbl/amap [x xs] (* 2.0 x))])

(defn rosenbrock
  "Not convex everywhere, but still useful as a test case"
  [[x y]]
  [(+ (square (- 1 x))
      (* 100 (square (- y (square x)))))
   (double-array
    [(+ (* 400 x (- (square x) y)) (* 2 (- x 1)))
     (* 200 (- y (square x)))])])

(defn booth [[x y]]
  [(+ (square (+ x (* 2 y) -7))
      (square (+ (* 2 x) y -5)))
   (double-array
    [(+ (* 8 y) (* 10 x) -34)
     (+ (* 8 x) (* 10 y) -38)])])

(def +test-suite+
  (map #(update-in % [:min] double-array)
       [{:f sphere :min [0 0]}
        {:f (shifted sphere (double-array [2 -3])) :min [2 -3]}
        {:f rosenbrock :min [1 1]}
        {:f booth :min [1 3]}]))
