(ns plumbing.rank-test
  (:use clojure.test plumbing.core)
  (:require
   [plumbing.rank :as rank]))


(deftest top-k-test
  (is (= [-10 -10 -9 -9 -8 -8]
         (rank/top-k 6 - (concat (range -10 0) (range -10 0)))))
  (is (= [9]
         (rank/top-k 1 identity (range 10))))
  (is (= (empty?
          (rank/top-k 0 identity (range 10)))))
  (is (= [2 1 0]
         (rank/top-k 10 identity (range 3))))
  (is (= (empty?
          (rank/top-k 10 identity []))))
  (is (= (empty?
          (rank/top-k 0 identity [])))))
