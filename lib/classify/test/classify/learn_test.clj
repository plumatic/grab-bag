(ns classify.learn-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [classify.learn :as learn]))

(deftest cross-validate-partitions-test
  (is (= [[[:d :e :f] [:a :b :c]]
          [[:a :b :c] [:d :e :f]]]
         (learn/cross-validation-partitions 2 [:a :b :c :d :e :f])))
  (is (= '[[(:b :c :d :e) (:a)]
           [(:a :c :d :e) (:b)]
           [(:a :b :d :e) (:c)]
           [(:a :b :c) (:d :e)]]
         (learn/cross-validation-partitions 4 [:a :b :c :d :e]))))

(deftest grouped-cross-validation-partitions-test
  (is (= #{[#{1 3 5} #{2 4 6}]
           [#{2 4 6} #{1 3 5}]}
         (set
          (map
           #(map set %)
           (learn/grouped-cross-validation-partitions
            odd? 2 [1 2 3 4 5 6]))))))

(use-fixtures :once validate-schemas)
