(ns store.flushing-log-test
  (:require
   [plumbing.core :refer :all]
   [plumbing.test :refer :all]
   [plumbing.graph :as graph]
   [plumbing.resource :as resource]
   [store.bucket :as bucket]
   [store.flushing-log :refer :all]
   [clojure.test :refer :all]))

(deftest log!-test
  (let [bucket (bucket/bucket {})]
    (with-open [bundle (resource/bundle-run logging-bundle {:log-flush-size 2 :bucket bucket})]
      (assert (= 0 (bucket/count bucket)))

      (testing "first log makes it to bucket"
        (log! bundle 1)
        (is-= 0 (bucket/count bucket)))

      (testing "second log pushes it over the limit, and the log waits in the cache"
        (log! bundle 2)
        (log! bundle 3)

        (testing "one batch has been flushed"
          (is-eventually (= [[1 2]] (bucket/vals bucket))))))
    (testing "after closing, the remaining log is finally written to the bucket"
      (is-=-by set [[1 2] [3]] (bucket/vals bucket)))))
