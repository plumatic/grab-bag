(ns tubes.core-test
  (:use-macros
   [cljs-test.macros :only [deftest is is=]])
  (:require
   [cljs-test.core :as test]
   [tubes.core :as tubes]))

(deftest permutations
  (is= (tubes/permutations nil) '())
  (is= (tubes/permutations []) '())
  (is= (set (tubes/permutations [1 2 3]))
       #{(list 1 2 3)
         (list 3 2 1)
         (list 2 1 3)
         (list 3 1 2)
         (list 2 3 1)
         (list 1 3 2)}))

(deftest cross-product
  (is= (tubes/cross-product nil) (list))
  (is= (tubes/cross-product []) (list))
  (is= (tubes/cross-product [[1 2 3] []]) (list))
  (is= (tubes/cross-product [[:a :b] [1 2]])
       (list (list :a 1) (list :a 2) (list :b 1) (list :b 2))))

(deftest url-seq
  (is= (tubes/url-seq "google.com") '("google.com"))
  (is (not= (tubes/url-seq "foo/bar") '("foo/bar")))
  (is= (tubes/url-seq "www.google.com") '("www.google.com"))
  (is= (tubes/url-seq "http://www.google.com") '("http://www.google.com"))
  (is= (tubes/url-seq "http://www.bbc.co.uk/news/business-23261289")
       '("http://www.bbc.co.uk/news/business-23261289")))


(deftest indexed-keep-first-test
  (is= nil (tubes/indexed-keep-first pos? []))
  (is= nil (tubes/indexed-keep-first pos? [-1]))
  (is= nil (tubes/indexed-keep-first pos? (vec (range -20 -1))))
  (is= [0 true] (tubes/indexed-keep-first pos? [1]))
  (is= [0 true] (tubes/indexed-keep-first pos? [1 2]))
  (is= [1 true] (tubes/indexed-keep-first pos? [-1 2]))
  (is= [3 true] (tubes/indexed-keep-first pos? (vec (range -2 4)))))
