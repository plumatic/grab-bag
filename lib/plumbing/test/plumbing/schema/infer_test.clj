(ns plumbing.schema.infer-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [schema.core :as s]
   [plumbing.schema.infer :as infer]))

(deftest infer-test
  (is-= (s/enum :a :b :c)
        (infer/infer-schema [:a :b :c]))

  (is-= [(s/enum :a :b :c)]
        (infer/infer-schema [[:a] [:c :b] [:a :c]]))

  (is-= {:a (s/enum :b :c) (s/optional-key :b) {:e (s/enum :f :g)}}
        (infer/infer-schema [{:a :b :b {:e :f}} {:a :c} {:a :b :b {:e :g}}]))

  (is-= (infer/class-ex clojure.lang.Keyword :a)
        (infer/infer-schema [:b :c :d :a :e :f])))
