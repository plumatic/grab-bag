(ns plumbing.string-test
  (:use clojure.test plumbing.test)
  (:require
   [plumbing.string :as string]))

(deftest truncate-test
  (is-= "this..." (string/truncate "this is a test" 0))
  (is-= "this is a test" (string/truncate "this is a test" 15)))

(deftest truncate-to-test
  (is-= "th" (string/truncate-to "this could be a test too" 2))
  (is-= "this is a test" (string/truncate-to "this is a test" 15))
  (is-= "" (string/truncate-to "this is a test" 0))
  (is-= "this..." (string/truncate-to "this could be a test too" 7))
  (is-= "this..." (string/truncate-to "this could be a test too" 9))
  (is-= "thi..." (string/truncate-to "this could be a test too" 6)))

(deftest and-list-vec-test
  (is-= ["hello"] (string/and-list-vec ["hello"]))
  (is-= ["hello" " and " "world"] (string/and-list-vec ["hello" "world"]))
  (is-= ["hello" ", " "world" ", and " "dinos"] (string/and-list-vec ["hello" "world" "dinos"])))

(deftest from-tsv-test
  (is-= [{:foo-bar "1" :baz "2"} {:foo-bar "4" :baz "6"}]
        (string/from-tsv ["FooBar\tBaz" "1\t2" "4\t6"])))

(deftest shard-test
  (let [lines ["these are some lines \n"
               "not the same length \n"
               "but close enough \n"
               "these are some lines \n"
               "not the same length \n"
               "but close enough\n"]]
    (is-= (map (partial apply str) (partition 2 lines))
          (string/shard (apply str lines) 3))))
