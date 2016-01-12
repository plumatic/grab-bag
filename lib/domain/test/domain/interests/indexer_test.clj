(ns domain.interests.indexer-test
  (:use clojure.test plumbing.core plumbing.test domain.interests.indexer)
  (:require
   [plumbing.index :as index]))

(deftest indexer-test
  (let [types (take 10 (map #(keyword (str "type" %)) (iterate inc 0)))
        type-index (index/static types)
        encoded (+ (bit-shift-left 8 56) 3)]

    (is-= 3 (key-of encoded))

    (is-= :type8 (type-of type-index encoded))

    (is-= [:type8 3] (value-of type-index encoded))

    (is-= encoded (index-of type-index :type8 3))
    (is (thrown? AssertionError (index-of type-index :type8 (bit-shift-left 12 56))))))

(deftest extract-type-key-test
  (is-= {:type :typeA :key "abc:def"}
        (extract-type-key "typeA:abc:def")))

(use-fixtures :once validate-schemas)
