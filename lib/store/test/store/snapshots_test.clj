(ns store.snapshots-test
  (:use clojure.test store.snapshots))


(deftest snapshots-test
  (let [ss (fresh-test-snapshot-store nil)]
    (dotimes [i 5] (write-snapshot ss i i))
    (dotimes [i 5] (is (= (read-snapshot ss i) i)))
    (is (= (read-latest-snapshot ss) 4))
    (is (= (snapshot-seq ss) [4 3 2 1 0]))))
