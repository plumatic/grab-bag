(ns classify.linear-test
  (:use plumbing.core plumbing.test clojure.test)
  (:require
   [classify.learn :as learn]
   [classify.linear :as linear]))

(def test-data
  [[0 0 1]
   [1 0 10]
   [1 2 10]
   [2 2 1]])

(deftest l2-evaluator-test
  (is-= {:num-obs 22.0 :sum-xs 0.0 :sum-sq-xs 22.0}
        (select-keys
         (learn/evaluate-all linear/l2-evaluator (linear/->ConstantModel 1) test-data)
         [:num-obs :sum-xs :sum-sq-xs]))
  (is-= {:num-obs 22.0 :sum-xs 22.0 :sum-sq-xs 44.0}
        (select-keys
         (learn/evaluate-all linear/l2-evaluator (linear/->ConstantModel 0) test-data)
         [:num-obs :sum-xs :sum-sq-xs])))

(deftest constant-trainer-test
  (is-approx-= 1.0
               (:mean (linear/+constant-trainer+ test-data))
               1.0e-10))

(deftest bucket-trainer-test
  (is-approx-= {:mean-map {true (/ 20 21) false 2} :overall-mean 1.0}
               (select-keys ((linear/bucket-trainer #(<= % 1.5)) test-data)
                            [:mean-map :overall-mean])
               1.0e-10))

(deftest linear-trainer-test
  (is-approx-= {:bias 0.0 :a 1.0}
               (linear/explain
                ((linear/linear-trainer #(hash-map :a %) {:sigma-sq 1000000})
                 test-data))
               1.0e-6))
