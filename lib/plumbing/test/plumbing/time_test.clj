(ns plumbing.time-test
  (:use clojure.test plumbing.time)
  (:import
   [org.joda.time DateTimeZone]))

(deftest test-to-long
  (is (= 893462400000 (to-long (date-time 1998 4 25)))))

(deftest test-from-long
  (is (= (date-time 1998 4 25) (from-long 893462400000))))

(deftest test-to-string
  (is (= (to-string (date-time 1998 4 25))
         "1998-04-25T00:00:00.000Z")))

(deftest test-formatter
  (let [fmt (formatter "yyyyMMdd")]
    (is (= (date-time 2010 3 11)
           (parse fmt "20100311")))))

(deftest test-parse
  (let [fmt (formatters :date)]
    (is (= (date-time 2010 3 11)
           (parse fmt "2010-03-11"))))
  (let [fmt (formatters :basic-date-time)]
    (is (= (date-time 2010 3 11 17 49 20 881)
           (parse fmt "20100311T174920.881Z")))))

(deftest test-unparse
  (let [fmt (formatters :date)]
    (is (= "2010-03-11"
           (unparse fmt (date-time 2010 3 11)))))
  (let [fmt (formatters :basic-date-time)]
    (is (= "20100311T174920.881Z"
           (unparse fmt (date-time 2010 3 11 17 49 20 881))))
    (is (= "20100311T124920.881-0500"
           (unparse (formatter "yyyyMMdd'T'HHmmss.SSSZ"
                               (DateTimeZone/forOffsetHours -5))
                    (date-time 2010 3 11 17 49 20 881))))))
