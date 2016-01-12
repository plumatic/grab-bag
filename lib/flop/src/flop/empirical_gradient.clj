(ns flop.empirical-gradient
  "An empirical test to verify the consistency of the objective value and gradient

   Computes an empirical approximation to the gradient at point x:
     empirical-grad(x) = (f(x + dx) - f(x)) / dx
   and reports the differences between the empirical gradient and
    the gradient defined in the function (grad f(x))."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [hiphip.double :as dbl]
   [plumbing.logging :as log]
   [flop.optimize :as optimize]))

(s/defschema DimensionDiff
  "Represents the gradient and empirical gradient along a single dimension"
  [(s/one s/Num "gradient") (s/one s/Num "empirical gradient")])

(s/defn report
  "Displays a report of the difference between the gradient and empirical gradient"
  [diffs :- [DimensionDiff]]
  (doseq [diff diffs]
    (let [[dimension [grad emp-grad]] (indexed diff)]
      (log/infof "GRADIENT-TEST: dim %03d . %s vs %s . diff = %s\n"
                 dimension grad emp-grad (- grad emp-grad)))))

(defnk empirical-gradient :- [DimensionDiff]
  "Computes the gradient and empirical gradient of `f` at point `xs`
   by independentally varying each of the `num-dims` dimensions of
   initial point `xs` by infinitesimal amount `dx`."
  [num-dims :- Long
   f :- (s/=> optimize/ValueGradientPair doubles)
   {dx 1e-4}
   {xs (double-array (repeatedly num-dims #(rand)))}]
  (let [empirical-grad (dbl/amake [i num-dims] 0.0)
        [obj computed-grad] (f xs)]
    (doseq [i (range num-dims)]
      (dbl/ainc xs i dx)
      (let [[new-obj _] (f xs)]
        (dbl/aset empirical-grad i (/ (- new-obj obj) dx)))
      (dbl/ainc xs i (- dx)))

    (for [i (range num-dims)]
      [(dbl/aget computed-grad i) (dbl/aget empirical-grad i)])))
