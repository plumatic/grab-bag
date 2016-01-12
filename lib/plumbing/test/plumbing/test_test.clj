(ns plumbing.test-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [schema.core :as s]))

(def OddList [(s/pred odd?)])
(def EvenList [(s/pred even?)])

(s/defn ^EvenList map-inc-odd-list
  [^OddList o]
  (map inc o))

(deftest validate-schemas-test
  (is (= (range 2 1002 2) (map-inc-odd-list (range 1 1001 2)))))

(s/defn map-inc-odd-list-input-only
  [^OddList o]
  (map inc o))

(s/defn map-inc-odd-list-output-only :- EvenList
  [o]
  (map inc o))

(deftest failing-validate-schemas-test
  (is (thrown? Exception (map-inc-odd-list (range 0 1000 2))))
  (is (thrown? Exception (map-inc-odd-list-input-only (range 0 1000 2))))
  (is (thrown? Exception (map-inc-odd-list-output-only (range 0 1000 2)))))

(deftest assert-=-by-test
  (is (thrown? AssertionError (assert-=-by inc 1 3)))
  (is-= nil (assert-=-by odd? 1 3))
  (try (assert-= [1 2] [1 3])
       (catch AssertionError e
         (is-= (str e)
               "java.lang.AssertionError: Assert failed: {:left-side-only [nil 2], :right-side-only [nil 3], :both-sides [1]}\n\n(empty-diff? diff)"))))

(deftest recorder-test
  (let [r (recorder +)]
    (is-= 0 (r))
    (is-= 5 (r 2 3))
    (is-= 17 (apply r [10 5 2]))
    (is-= [[] [2 3] [10 5 2]] @r)
    (is-= [] @r)
    (r) (r 1)
    (is-= [[] [1]] @r)))

(use-fixtures :once validate-schemas)
