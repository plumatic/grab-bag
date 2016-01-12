(ns store.bdb-test
  (:use clojure.test store.bdb)
  (:require
   [clojure.java.io :as java-io]
   [plumbing.io :as io]
   [store.bucket :as bucket]
   [store.bucket-test :as core-test]
   [plumbing.parallel :as parallel]
   ))


(def default-args {:type :bdb
                   :name "bdb_test"
                   :path "/tmp/bdbtest/"})

(defn test-bdb [& [args]]
  (bucket/bucket (merge default-args args)))

(defn new-test-bdb [& [args]]
  (let [args (merge default-args args)
        path (:path args)]
    (assert (> (count path) 5))
    (io/delete-quietly path)
    (java-io/make-parents (java.io.File. (str path "/ping")))
    (java-io/delete-file (java.io.File.  (str path "/ping")) true)
    (test-bdb args)))

(deftest bdb-basics
  (let [db (new-test-bdb)
        _ (bucket/put db :foo [1 2 3])]
    (is (= [1 2 3] (bucket/get db :foo)))
    (is (= [:foo [1 2 3]] (first (doall (bucket/seq db)))))
    (do (bucket/delete db :foo))
    (is (empty? (bucket/seq db)))
    (bucket/close db)))

(deftest bdb-read-only-test
  (let [db (new-test-bdb)]
    (bucket/put db "k" "v")
    (bucket/close db))
  (let [db-read (test-bdb {:read-only true})]
    (is (= "v" (bucket/get db-read "k")))
    (bucket/close db-read)))

(deftest bdb-deferred-write-test
  (let [db (new-test-bdb {:deferred-write true})]
    (bucket/put db "k" "v")
    (bucket/close db))
  (let [db-read (test-bdb {:deferred-write false})]
    (is (= "v" (bucket/get db-read "k")))
    (bucket/close db-read)))

(deftest bdb-bucket-test
  (let [db (new-test-bdb)]
    (core-test/generic-bucket-test db)
    (bucket/close db)))

(deftest bucket-keys-test
  (let [b (new-test-bdb)]
    (bucket/put b "k1" "v1")
    (bucket/put b "k2" "v2")
    (bucket/put b "k4" "v4")
    (is (= '("k1" "k2" "k4")
           (sort (bucket/keys b))))
    (bucket/close b)))

;;bdb can allow multiple vales for the same key, ensure we are operating in overwrite mode.
(deftest bdb-duplicate-puts
  (let [db (new-test-bdb)
        _ (bucket/put db :foo [1 2 3])
        _ (bucket/put db :foo [1])]
    (is (= [1] (bucket/get db :foo)))
    (is (= 1 (bucket/count db)))
    (do (bucket/delete db :foo))
    (is (empty? (bucket/seq db)))
    (bucket/close db)))

(deftest bdb-count
  (let [db (new-test-bdb)]
    (doseq [[k v] (partition-all 2 (range 100))] (bucket/put db k v))
    (is (= 50 (bucket/count db)))
    (bucket/close db)))

(require '[plumbing.serialize :as serialize])

(deftest bdb-cursor-iterator
  (let [db (new-test-bdb {:key-serialize-method serialize/+default-uncompressed+ } )]
    (dotimes [i 100]
      (bucket/put db i (str i)))
    (let [it (cursor-iterator db)]
      (dotimes [i 100]
        (is (= (cursor-next it) i))
        (if (odd? i) (cursor-remove it)))
      (cursor-close it))
    (let [it (cursor-iterator db)]
      (dotimes [i 100]
        (when (even? i)
          (is (= (cursor-next it) i))))
      (cursor-close it))
    (bucket/close db)))


(comment ;; For testing speed of raw BDB IO (serialization, compression,  etc)
  (defn make-test-bdb []
    (let [b (new-test-bdb {:path "/Volumes/data/tmp/"})]
      (time
       (dotimes [i 10000]
         (bucket/bucket-put b (str i) (apply str (repeat 10000 \x)))))
      (bucket/bucket-close b)))

  (defn seq-test-bdb []
    (let [b (test-bdb {:path "/Volumes/data/tmp/"})]
      (time (count (bucket/bucket-seq b)))
      (bucket/bucket-close b))))

(comment ;; For testing speed of seqing bdb
  (defn make-test-bdb []
    (let [b (new-test-bdb)]
      (time
       (dotimes [i 1000000]
         (bucket/bucket-put b i #_ (rand-int 10000000) "foo")))
      (bucket/bucket-close b)))

  (defn funny-seq [b]
    (->> (bucket-keys b)
         (partition-all 100)
         (parallel/map-work 10 (partial bucket-batch-get b))))

  (defn seq-test-bdb []
    (let [b (test-bdb)]
      (dotimes [i 3]
        (println (time (count (bucket/bucket-seq b))))
        (println (time (count (apply concat (funny-seq b))))))
      (bucket/bucket-close b))))
