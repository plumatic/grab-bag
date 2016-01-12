(ns tubes.test-metrics-test
  (:use-macros [cljs-test.macros :only [deftest is is= read-json]])
  (:require
   [cljs-test.core :as test]
   [tubes.text-metrics :as text-metrics]))

(def heavy-hang-measurer
  (reify text-metrics/PTextMeasurer
    (text-width [this s]
      (if (= s "ng") 10 (count s)))))

(def square-measurer
  (reify text-metrics/PTextMeasurer
    (text-width [this s]
      (let [l (count s)] (* l l)))))

(deftest approximate-bigram-width
  (let [w (partial text-metrics/approximate-bigram-width heavy-hang-measurer)]
    (is= (w "") 0)
    (is= (w "a") 1)
    (is= (w "ng") 10)
    (is= (w "hang city bang") 30)))

(deftest approximate-break-text 
  (let [b (partial text-metrics/approximate-break-text heavy-hang-measurer)]
    (is= (b "" 10) [0 0])
    (is= (b "a" 10) [1 1])
    (is= (b "ng" 5) [1 1])
    (is= (b "hang city bang" 0) [0 0])
    (is= (b "hang city bang" 5) [3 3])
    (is= (b "hang city bang" 15) [7 15])
    (is= (b "hang city bang" 30) [14 30])
    (is= (b "hang city bang" 100) [14 30])))

(deftest break-text
  (let [b (partial text-metrics/break-text square-measurer)]
    (is= (b "" 10) [0 0])
    (is= (b "a" 10) [1 1])
    (is= (b "ng" 5) [2 4])
    (is= (b "hang city bang" 0) [0 0])
    (is= (b "hang city bang" 5) [2 4])
    (is= (b "hang city bang" 15) [3 9])
    (is= (b "hang city bang" 30) [5 25])
    (is= (b "hang city bang" 100) [10 100])
    (is= (b "hang city bang" 1000) [14 196])))

(deftest break-text-at
  (let [breaker (partial text-metrics/approximate-break-text heavy-hang-measurer)
        b #(text-metrics/break-text-at breaker %1 %2 (set " "))]
    (is= (b "" 10) 0)
    (is= (b "a" 10) 1)
    (is= (b "ng" 5) 1)
    (is= (b "hang city bang" 0) 0)
    (is= (b "hang city bang" 5) 3)
    (is= (b "hang city bang" 12) 4)
    (is= (b "hang city bang" 15) 4)
    (is= (b "hang city bang" 29) 9)
    (is= (b "hang city bang" 30) 14)
    (is= (b "hang city bang" 100) 14)))

(deftest clamp-text
  (let [c #(text-metrics/clamp-text heavy-hang-measurer %1 %2 (set " ") %3 0)]
    (is= (c "" 10 1) [""])
    (is= (c "ab" 1 2) ["a" "b"])
    (is= (c "hang city bang" 9 1) ["hang…"])
    (is= (c "hang city bang" 5 3) ["hang" "city" "bang"])
    (is= (c "A Private Photos App That’s Growing? KeepSafe Crosses 1B Hidden Photos, 3M Active Users" 37 2)
         ["A Private Photos App That’s Growing?"
          "KeepSafe Crosses 1B Hidden Photos,…"])))
