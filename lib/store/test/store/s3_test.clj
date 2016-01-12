(ns store.s3-test
  (:use clojure.test plumbing.core plumbing.test store.s3)
  (:require
   [store.bucket :as bucket]
   [store.bucket-test :as bucket-test])
  (:import
   [com.amazonaws AmazonServiceException AmazonClientException]))


(deftest aws-404?-test
  (is (aws-404? (doto (AmazonServiceException. "asdf") (.setStatusCode 404))))
  (is (not (aws-404? (doto (AmazonServiceException. "asdf") (.setStatusCode 500))))))

(deftest aws-fail-404?-test
  (is (not (aws-fail-404? (doto (AmazonServiceException. "asdf Response Code: 404") (.setStatusCode 404)))))
  (is (not (aws-fail-404? (AmazonClientException. "asdf 404"))))
  (is (aws-fail-404? (AmazonClientException. "asdf Response Code: 404"))))

(defn test-s3-bucket [b]
  (let [sub-b (sub-bucket b "hello/")]
    (is (nil? (bucket/get b "1jl2k/123")))
    (bucket/put b "cats/world" 789)
    (bucket/put b "hello/world" 123)
    (is-=-by set
             [["cats/world" 789] ["hello/world" 123]]
             (bucket/seq b))
    (is-= [["world" 123]] (bucket/seq sub-b))
    (is-= 123 (bucket/get sub-b "world"))
    (is-=-by set
             ["hello/" "cats/"]
             (key-prefixes b "" "/"))
    (bucket/put sub-b "abc" 456)
    (is-=-by set
             ["world" "abc"]
             (bucket/keys sub-b))
    (is-=-by set
             [123 456]
             (bucket/vals sub-b))
    (is (bucket/exists? sub-b "world"))
    (is (not (bucket/exists? sub-b "hello")))
    (testing "write through"
      (is-=-by set
               [["hello/world" 123] ["hello/abc" 456] ["cats/world" 789]]
               (bucket/seq b)))
    (bucket/delete sub-b "world")
    (is (not (bucket/exists? sub-b "world")))
    (is (not (bucket/exists? b "hello/world")))
    (is-= ["hello/abc"]
          (keys-with-prefix b "hello/abc"))
    (bucket/put b "hello/abcd" 1)
    (is-= ["hello/abc" "hello/abcd"]
          (keys-with-prefix b "hello"))
    (is-= ["abc" "abcd"]
          (bucket/keys sub-b))
    (is-= ["hello/abc" "hello/abcd"]
          (keys-with-prefix b "hello" "hello/ab"))
    (is-= ["abcd"]
          (keys-with-prefix sub-b "" "abc"))
    (is-= ["hello/abcd"]
          (keys-with-prefix b "hello" "hello/abc"))
    (testing "put-with-headers"
      (let [k "key-with-headers"
            v "val-with-headers"]
        (put-with-headers b k v {"content-type" "text/plain"})
        (is-= v (bucket/get b k))))))

(deftest s3-bucket-wrapper-test
  (test-s3-bucket (s3-bucket-wrapper (bucket/bucket {}))))

;; Integration test, to be run manually with ec2-keys
(defn run-s3-test [ec2-keys]
  (let [bucket-name "grabbag-tmp-data"
        bdaddy (bucket/bucket (merge ec2-keys {:type :s3 :name bucket-name :key-prefix "s3-test-"}))
        bkid1 (bucket/bucket (merge ec2-keys {:type :s3 :name bucket-name :key-prefix "s3-test-kid1-"}))
        bkid2 (bucket/bucket (merge ec2-keys {:type :s3 :name bucket-name :key-prefix "s3-test-kid2-"}))]
    (delete-all bdaddy)
    (test-s3-bucket bdaddy)
    (delete-all bdaddy)
    (bucket/put bkid1 "a" "k1a")
    (bucket/put bkid2 "b" "k2b")
    (is (= {"kid1-a" "k1a" "kid2-b" "k2b"} (into {} (bucket/seq bdaddy))))
    (is (= {"a" "k1a"} (into {} (bucket/seq bkid1))))
    (is (= {"b" "k2b"} (into {} (bucket/seq bkid2))))
    (delete-all bkid2)
    (is (= {"kid1-a" "k1a"} (into {} (bucket/seq bdaddy))))
    (bucket-test/generic-bucket-test bkid2)
    (is (= {"kid1-a" "k1a"} (into {} (bucket/seq bdaddy))))))
