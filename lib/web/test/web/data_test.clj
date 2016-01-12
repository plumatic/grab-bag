(ns web.data-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [web.data :as data]))

(deftest jsonp-str-test
  (is-= "callback(data)"
        (data/jsonp-str "callback" "data"))
  (testing "newlines stripped"
    (is-= "callback(data)"
          (data/jsonp-str "call\nback" "data")))
  (testing "carriage return stripped"
    (is-= "callback(data)"
          (data/jsonp-str "call\rback" "data")))
  (testing "parens allowed"
    (is-= "grabbag.promise(\"user\").fulfill(data)"
          (data/jsonp-str "grabbag.promise(\"user\").fulfill" "data")))
  (is-= "callback(data)"
        (data/jsonp-str "call\nb\r\r\nac\r\rk" "data")))

(deftest mqs-test
  (let [check-key (partial map-keys #(if (string? %) (keyword %) %))
        data
        [{:foo "bar" :a "asdf"}
         {}
         {"foo" "a"}
         {"123" "c"}]]
    (doseq [d data]
      (is (= (-> d
                 data/map->query-string
                 data/query-string->map)
             (check-key d))))))
