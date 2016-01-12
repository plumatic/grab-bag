(ns web.middleware-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [web.middleware :as middleware]))

(deftest gzip-middleware-test
  (let [h (constantly {:headers {"Content-Type" "application/json; charset=UTF-8"
                                 "Accept-encoding" "gzip"}
                       :status 200
                       :body (apply str (repeat 500 \a))})
        r ((middleware/gzip-middleware h) {:headers {"Accept-encoding" "gzip"}})]
    (is (= "gzip" (get-in r [:headers "content-encoding"])))))
