(ns plumbing.logging-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [plumbing.logging :as logging]))

(deftest jsonify-test
  (are [x y] (= x (logging/jsonify y))
       "a" "a"
       :a :a
       "1" 1
       ["1" :a] [1 :a]
       {:a "1" "[2]" "3"} {:a 1 [2] 3}))
