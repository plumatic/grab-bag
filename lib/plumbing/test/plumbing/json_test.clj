(ns plumbing.json-test
  (:use clojure.test)
  (:require [plumbing.json :as json])
  (:import
   [java.io BufferedReader StringReader]
   [java.util UUID]))


(deftest test-longs-round-trip
  (let [obj {:a (long 13) :b (long -133)}
        serialized (json/generate-string obj)
        deserialized (json/parse-string serialized true)]
    (is (= "{\"a\":13,\"b\":-133}" serialized))
    (is (= obj deserialized))
    (is (= Long (class (:a deserialized))))
    (is (= Long (class (:b deserialized))))))

(deftest test-string-round-trip
  (let [obj {"int" 3 "long" 52001110638799097 "bigint" 9223372036854775808
             "double" 1.23 "boolean" true "nil" nil "string" "string"
             "vec" [1 2 3] "map" {"a" "b"} "list" (list "a" "b")}]
    (is (= obj (json/parse-string (json/generate-string obj))))))

(deftest test-generate-accepts-float
  (is (= "3.14" (json/generate-string (float 3.14)))))

(deftest test-ratio
  (is (= "1.5" (json/generate-string (/ 3 2)))))

(deftest test-key-coercion
  (is (= {"foo" "bar" "1" "bat" "2" "bang" "3" "biz"}
         (json/parse-string
          (json/generate-string
           {:foo "bar" 1 "bat" (long 2) "bang" (bigint 3) "biz"})))))

(deftest test-keywords
  (is (= {:foo "bar" :bat 1}
         (json/parse-string
          (json/generate-string {:foo "bar" :bat 1})
          true))))

(deftest test-set
  (is (= #{1 "a"}
         (into #{} (json/parse-string (json/generate-string #{1 "a"}))))))



(deftest test-unicode-control
  (is (= "\"\\u2028 \\u2028 runMobileCom\""
         (json/generate-string (String.
                                (byte-array
                                 (map byte
                                      [-30 -128 -88 32 -30 -128 -88 32
                                       114 117 110 77 111 98
                                       105 108 101 67 111 109]))
                                "UTF8")))))

(deftest test-uuid-serialization
  (dotimes [_ 10]
    (let [uuid (UUID/randomUUID)]
      (is (= (.toString uuid)) (json/generate-string uuid)))))

(deftest generate-literal-map-test
  (is (= "{\"foo\": new Date(),\"bar\": 1}"
         (json/generate-literal-map
          (array-map
           :foo "new Date()"
           :bar 1)))))
