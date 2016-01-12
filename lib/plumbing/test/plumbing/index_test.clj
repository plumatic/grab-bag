(ns plumbing.index-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [plumbing.io :as io]
   [plumbing.index :as index]))

(deftest static-indexer-test
  (let [my-index (index/static [:a :b :c])]
    (is-= 1 (index/index! my-index :b ))
    (is-= 1 (index/safe-get-index my-index :b))
    (is-= :c (index/item my-index 2))
    (is (thrown? Exception (index/index! my-index :not-a-valid-value)))
    (is (thrown? Exception (index/safe-get-index my-index :not-a-valid-value)))
    (is (thrown? Exception (index/item my-index 123456)))
    (index/reset! my-index)
    (is-= :c (index/item my-index 2))))

(deftest simple-dynamic-indexer-test
  (let [idx (index/dynamic)]
    (is (= 0 (index/index! idx :foo)))
    (is (= 1 (index/index! idx :bar)))
    (is (= 1 (index/safe-get-index idx :bar)))
    (is (= 0 (index/index! idx :foo)))
    (is (= 0 (index/safe-get-index idx :foo)))
    (is (thrown? Throwable (index/safe-get-index idx :baz)))
    (is (= 1 (index/index! idx :bar)))
    (is (= :foo (index/item idx 0)))
    (index/reset! idx)
    (is (= 0 (index/index! idx :bar)))))

(deftest fancier-dynamic-index-test
  (let [index (index/dynamic)]
    (doseq [x [:a :b :a :a :a :c :c :a]]
      (index/index! index x))
    (is (= 3 (count index)))
    (is (.contains index :a))
    (is (.contains index :b))
    (is (.contains index :c))
    (is (= [:a :b :c] (seq index)))
    (is (= 0 (index/index! index :a)))
    (is (= 1 (index/index! index :b)))
    (is (= 2 (index/index! index :c)))
    (is (= :a (index/item index 0)))
    (is (= :b (index/item index 1)))
    (is (= :c (index/item index 2)))
    (is (not (index/locked? index)))
    (is (= 3 (count index)))
    (index/lock! index)
    (is (index/locked? index))
    (is (thrown? Throwable (index/index! index :z)))
    (is (= 0 (index/index! index :a)))
    (index/unlock! index)
    (is (not (index/locked? index)))
    (is (= 3 (index/index! index :z)))
    (is (= 4 (count index)))))

(deftest roundtrip-test
  (let [index (index/dynamic)]
    (index/index! index "hello")
    (index/index! index "world")
    (index/index! index "hello")
    (index/index! index "world")
    (doseq [munge [identity
                   (fn [v]
                     (assoc v 0 :classify.index/index))]]
      (is (= (seq index)
             (seq (->> index
                       io/to-data
                       munge
                       io/from-data)))))))

(use-fixtures :once validate-schemas)
