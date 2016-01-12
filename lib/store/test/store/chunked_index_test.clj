(ns store.chunked-index-test
  (:use clojure.test plumbing.core store.chunked-index)
  (:require [store.bucket :as bucket]))

(deftest simple-chunked-index-test
  (let [b (bucket/bucket {})
        idx (long-chunked-index {:bucket b :chunk-size 3})]

    (is (nil? (find-page idx :foo 10)))
    (is (nil? (index-get idx :foo 10)))

    (let [init-data [[:foo 10 100]
                     [:foo 12 102]
                     [:foo 13 103]
                     [:foo 14 104]
                     [:foo 8  98]
                     [:bar 10 200]]]
      (doseq [[a b c] init-data] (index-put! idx a b c))

      (doseq [i [7 9 11 15]]
        (is (= nil (index-get idx :foo i))))
      (doseq [i [9 11]]
        (is (= nil (index-get idx :bar i))))
      (is (= nil (index-get idx :bar 12)))

      (doseq [[a b c] init-data] (is (= c (index-get idx a b))))

      (is (= nil (find-page idx :baz nil)))
      (is (= nil (find-page idx :baz 10)))
      (is (= nil (find-page idx :foo 2)))
      (is (= [[10 200]] (find-page idx :bar 10)))
      (is (= [[10 200]] (find-page idx :bar 20)))
      (is (= [[14 104]] (find-page idx :foo 20)))
      (is (= [[13 103] [12 102] [10 100]] (find-page idx :foo 12)))
      (is (= [[8 98]] (find-page idx :foo 9)))
      (is (= nil (find-page idx :foo 7)))

      (is (= [[14 104] [13 103] [12 102] [10 100] [8 98]] (index-get-all idx :foo)))
      (is (= [[12 102] [10 100] [8 98]] (index-get-all idx :foo 12)))
      (is (= 4 (bucket/count b)))

      (index-put! idx :foo 11 -1)
      (doseq [[a b c] init-data] (is (= c (index-get idx a b))))
      (is (= -1 (index-get idx :foo 11)))

      (is (= [[14 104]] (find-page idx :foo 20)))
      (is (= [[13 103]] (find-page idx :foo 13)))
      (is (= [[12 102] [11 -1] [10 100]] (find-page idx :foo 12)))
      (is (= [[8 98]] (find-page idx :foo 9)))
      (is (= nil (find-page idx :foo 7)))

      (is (= 5 (bucket/count b)))

      (is (not (index-delete! idx :foo 17)))
      (is (not (index-delete! idx :foo 9)))
      (is (= 100 (index-delete! idx :foo 10)))
      (is (= [[8 98]] (find-page idx :foo 10)))
      (is (= [[12 102] [11 -1]] (find-page idx :foo 11)))

      (index-put! idx :foo 12 77)
      (is (= [[12 77] [11 -1]] (find-page idx :foo 11)))
      (index-put! idx :foo 11 37)
      (is (= [[12 77] [11 37]] (find-page idx :foo 11)))
      (is (= 77 (index-delete! idx :foo 12)))
      (is (= 37 (index-delete! idx :foo 11)))
      (is (= [[8 98]] (find-page idx :foo 11))))))
