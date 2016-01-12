(ns tubes.search-test
  (:use-macros [cljs-test.macros :only [deftest is is=]])
  (:require [cljs-test.core :as test]
            [tubes.search :as search]))

(deftest exhaustive-search


  (let [trans {:a [[:b 1.0] [:c 1.0]]
               :b [[:c 1.0] [:d 1.0]]
               :c [[:d 1.0] [:goal 1.0]]
               :d [[:goal 1.0]]}
        problem (reify search/SearchProblem
                  (successor-states [this s]
                    (assert (not= s :goal))
                    (trans s))
                  (terminal-state? [this s] (= s :goal)))]
    (is= (set (search/exhaustive-search problem :a 1))
         (set
          [[1.0 [:a :b]]
           [1.0 [:a :c]]]))

    (is= (set (search/exhaustive-search problem :a 2))
         (set
          [[2.0 [:a :b :c]]
           [2.0 [:a :b :d]]
           [2.0 [:a :c :d]]
           [2.0 [:a :c :goal]]]))))
