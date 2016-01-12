(ns store.util-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [store.bucket :as bucket]
   [store.util :as util]))

(deftest put-and-get-lastest-test
  (let [bucket (bucket/bucket {})
        k (with-millis 1
            (util/put-versioned bucket :1))]

    (is-= :1 (util/get-latest-version bucket))

    (with-millis 2
      (util/put-versioned bucket :2))
    (is-= :2 (util/get-latest-version bucket))
    (is-= :1 (bucket/get bucket k))
    (is-= 2 (count (bucket/seq bucket)))))

(deftest slurp-batches-test
  (is-= (repeat 4 3)
        (util/slurp-batches
         (for-map [i [1 3 4]] i [2 2])
         (partial mapv inc)
         2))
  (is-=-by set
           (range 10)
           (util/slurp-batches
            {1 [0 1 2 3] 2 [4] 3 [5 6 7 8 9]}
            identity)))
