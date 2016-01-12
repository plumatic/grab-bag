(ns cljs-request.core-test
  (:use-macros
   [cljs-test.macros :only [deftest is is= are]])
  (:require
   [cljs-test.core :as test]
   [cljs-request.core :as request]))

(deftest map->query-string
  (is= (request/map->query-string {}) "")
  (is= (request/map->query-string {:foo "bar"}) "foo=bar")
  (is= (request/map->query-string {:foo "bar" :lorem "ipsum"}) "foo=bar&lorem=ipsum"))

(deftest query-string->map
  (is= (request/query-string->map "") {})
  (is= (request/query-string->map "foo=bar") {:foo "bar"})
  (is= (request/query-string->map "foo=bar&lorem=ipsum") {:foo "bar" :lorem "ipsum"}))

(defn sub-map? [m sm]
  (= sm (select-keys m (keys sm))))

(deftest sub-map?-test
  (is (sub-map? {:a "b" :c "d"} {:a "b"}))
  (is (sub-map? {:a "b" :c "d"} {:a "b" :c "d"}))
  (is (not (sub-map? {:a "b" :c "d"} {:e "f"}))))

(deftest parse-url
  (is (sub-map? (request/parse-url "/foo") {:absolute? false :path "/foo"}))
  (is (sub-map? (request/parse-url "http://google.com")
                {:absolute? true
                 :host "google.com"
                 :port 80}))
  (is (sub-map? (request/parse-url "https://example.com:1337/news/home?hello=world")
                {:absolute? true
                 :protocol "https"
                 :host "example.com"
                 :port 1337
                 :path "/news/home"
                 :query-params {:hello "world"}})))

(deftest build-url
  (is= "/test" (request/build-url {:path "/test"}))
  (is= "?foo=bar" (request/build-url {:query-params {:foo "bar"}})))
