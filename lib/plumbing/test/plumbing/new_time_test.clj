(ns plumbing.new-time-test
  (:use plumbing.core clojure.test plumbing.test)
  (:require
   [plumbing.new-time :as new-time]
   [plumbing.time :as time]))

(deftest to-millis-test
  (is-= 43200000 (new-time/to-millis 0.5 :days)))

(deftest parse-us-date-string-test
  (is (= 318945600000
         (new-time/parse-us-date-string "2/9/80")
         (new-time/parse-us-date-string "2/9/1980")
         (new-time/parse-us-date-string "02/09/80")
         (new-time/parse-us-date-string "02/09/1980")))
  (is-approx-= -727617600000
               (new-time/parse-us-date-string "12/11/1946")))

(deftest to-start-of-week-test
  ;; start with 2014/4/7 which is a monday.
  ;; randomly pick an instant of time for each distinct day for this week.
  ;; Make sure that it is always rounded down to Monday 00:00:00 UTC.
  (let [dates-in-week
        (map #(time/date-time 2014 4 (+ 7 %) (rand 23) (rand 59)) (range 0 7))]
    (doseq [date dates-in-week]
      (is-= (.getMillis (time/date-time 2014 4 7 0 0 0))
            (new-time/to-start-of-week (.getMillis date))))))

(deftest floor-day-test
  (testing "idempotent floor-day"
    (let [ms (millis)]
      (is-= (new-time/floor-day ms)
            (new-time/floor-day (new-time/floor-day ms)))))

  (let [expected-date (new-time/parse-date "yyyy-MM-dd" "2015-03-17" (new-time/utc-calendar))
        parse-date #(new-time/parse-date "yyyy-MM-dd HH:mm:ss.SS" % (new-time/utc-calendar))]
    (are [date-str] (is (= expected-date (new-time/floor-day (parse-date date-str))))
         "2015-03-17 00:00:00.00"
         "2015-03-17 00:00:00.01"
         "2015-03-17 00:00:01.00"
         "2015-03-17 11:59:59.99"
         "2015-03-17 23:59:59.99")))

(deftest ceil-day-test
  (testing "idempotent ceil-day"
    (doseq [m (take 10 (repeatedly #(+ 10000000000 (* 1000000 (rand-int 1000)))))]
      (is-= (new-time/ceil-day m)
            (new-time/ceil-day (new-time/ceil-day m)))))
  (let [expected-date (new-time/parse-date "yyyy-MM-dd" "2015-03-18" (new-time/utc-calendar))
        parse-date #(new-time/parse-date "yyyy-MM-dd HH:mm:ss.SS" % (new-time/utc-calendar))]
    (are [date-str] (is (= expected-date (new-time/ceil-day (parse-date date-str))))
         "2015-03-18 00:00:00.00"
         "2015-03-17 00:00:00.01"
         "2015-03-17 00:00:01.00"
         "2015-03-17 11:59:59.99"
         "2015-03-17 23:59:59.99")))
