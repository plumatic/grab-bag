(ns plumbing.template-test
  (:use plumbing.core clojure.test plumbing.test)
  (:require [plumbing.template :as template]))

(deftest interpolate-into-string-test
  (is-= "history:ooyala,sumo,grabbag"
        (template/interpolate-into-string
         "history:#{w1},#{w2},#{w3}"
         [[:w1 "ooyala"] [:w2 "sumo"] [:w3 "grabbag"]]))
  (is-= "current:grabbag"
        (template/interpolate-into-string "current:#{work}" [[:work "grabbag"]]))
  (is (thrown? IllegalArgumentException
               (template/interpolate-into-string "current:#{work}" [[:working "foo"]]))))
