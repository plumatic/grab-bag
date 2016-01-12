(ns plumbing.stats-test
  (:use clojure.test plumbing.test)
  (:require
   [plumbing.stats :as stats])
  (:import
   [java.util Random]))

(deftest sample-test
  (testing "stable"
    (let [r (java.util.Random. 100)
          dist [[0 1.0] [1 1.0] [2 1.0]]
          result (stats/sample r dist)]
      (dotimes [n 100]
        (is-= result (stats/sample (Random. 100) dist)))))

  (testing "interleaves all data"
    (let [r (java.util.Random. 200)
          weights [(/ 1 3) (/ 2 3)]
          colls [[1 3 5] [2 4 6]]]
      (is-= [1 2 3 4 5 6]
            (sort (stats/interleave-by-weight r weights colls))))))

(deftest interleave-by-weight-test
  (testing "interleaving with zero weight"
    (is-= [:a :b :c] (stats/interleave-by-weight (Random.) [1.0 0] [[:a :b :c] [:d :e :f]]))
    (is-= [:d :e :f] (stats/interleave-by-weight (Random.) [0 1.0] [[:a :b :c] [:d :e :f]])))

  (testing "interleaves collections approximately correctly"
    (let [odds (iterate #(+ 2 %) 1)
          evens (iterate #(+ 2 %) 2)
          weights [1 2]
          colls [odds evens]
          sz 300
          ;; evens:odds is 2:1 ~66%
          expected-evens (* sz (/ 2 3))]
      (doseq [n (range 100)]
        (let [actual-evens
              (->> (stats/interleave-by-weight (Random.) weights colls)
                   (take sz)
                   (filter even?)
                   count)]
          (is (< (* 0.8 expected-evens) actual-evens (* 1.2 expected-evens))))))))
