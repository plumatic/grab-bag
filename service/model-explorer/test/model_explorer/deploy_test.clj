(ns model-explorer.deploy-test
  (:use plumbing.core plumbing.test)
  (:require [model-explorer.deploy :refer :all]
            [clojure.test :refer :all]
            [web.client :as client]))


(deftest ping-test
  (test-service
   (fnk [[:service server-port]]
     (is-= 200 (:status (client/rest-request {:uri "/ping" :port server-port}))))))
