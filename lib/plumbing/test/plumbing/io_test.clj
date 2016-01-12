(ns plumbing.io-test
  (:use clojure.test plumbing.io plumbing.core)
  (:require
   [schema.core :as s]
   [clojure.java.io :as java-io]))

(deftest mk-test-dir
  (let [p1 "/tmp/foo"]
    (is (not (.exists (java-io/file p1))))
    (with-test-dir [p "/tmp/foo"]
      (spit (java-io/file p "test1") "hi!")
      (is "hi!" (slurp (java-io/file p "test1")))
      (is (.exists (java-io/file p))))
    (is (not (.exists (java-io/file p1))))))

(deftest data-literal-test
  (let [roundtrip? (fn [x] (is (= x (-> x to-data from-data))))]
    (roundtrip? {:a 1 :b 2})
    (roundtrip? [:a :b])
    (roundtrip? 5)
    (roundtrip? "Aria")
    (roundtrip? #{:a 1 3})
    (roundtrip? (list :a [:b]))))

(deftest copy-to-temp-file-test
  (let [data1 "data1"
        ^File f (copy-to-temp-file data1 "test")]
    (is (= data1 (slurp f)))
    (java-io/delete-file f)))

(deftest shell-fn-test
  (is (= 2
         (shell-fn
          {:i1 "Mary had a\nlittle lamb\n"}
          [:o1]
          (fnk [[:in i1] [:out o1]]
            [["/bin/sh" "-c" (str  "wc " i1 " > " o1)]])
          (comp read-string :o1)))))

(s/defrecord Foo
    [x :- String]
  (fnk [x] (odd? (count x)))
  PDataLiteral
  (to-data [this] (schematized-record this)))

(deftest schematized-record-test
  (let [goods [(Foo. "asd")]
        bads [(Foo. 123) (Foo. "asdf")]]
    (doseq [good goods]
      (is (not (s/check Foo good)))
      (is (= good (from-data (to-data good)))))
    (doseq [bad bads]
      (is (s/check Foo bad))
      (is (thrown? Exception (from-data (to-data bad)))))))
