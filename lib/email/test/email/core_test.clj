(ns email.core-test
  (:require [clojure.test :refer :all]
            [email.sendgrid :as sendgrid]))

(deftest sanitize-subject-test
  (are [x y] (= x (sendgrid/sanitize-subject y))
       "" ""
       " " "\r"
       " " "\n"
       " " "\r\n"
       "test ..." "test ..."
       "test   a" "test \r\n a"
       "test a" "test\ra"
       "test a" "test\na"))
