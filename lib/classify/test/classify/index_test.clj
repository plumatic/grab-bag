(ns classify.index-test
  (:use clojure.test classify.index))

(deftest build-pred-index-test
  (let [p-index
        (build-pred-index
         [ [[[:p1 1] [:p2 1]] :l1]
           [[[:p2 1] [:p3 1]] :l2]
           ] {})]
    (is (= #{:p1 :p2 :p3} (set (seq p-index)))))
  (let [p-index
        (build-pred-index
         [ [[[:p1 1] [:p2 1]] :l1]
           [[[:p2 1] [:p3 1]] :l2]
           ]
         {:pred-thresh 1}) ]
    (is (= #{:p2} (set (seq p-index))))))
