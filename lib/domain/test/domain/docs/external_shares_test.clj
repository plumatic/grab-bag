(ns domain.docs.external-shares-test
  (:use clojure.test domain.docs.external-shares plumbing.core plumbing.test)
  (:require
   [schema.core :as s]
   [domain.doc-test-utils :as doc-test-utils])
  (:import [domain.docs.external_shares TwitterShare]))

(deftest merge-external-shares-test
  (let [s1 (doc-test-utils/test-external-shares
            {:twitter [{:user-id 1 :id 1} {:user-id 2 :id 2}]
             :facebook []
             :rss [{:feed-url "1" :shared-url "1"} {:feed-url "2" :shared-url "2"}]})
        s2 (doc-test-utils/test-external-shares
            {:twitter [{:user-id 1 :id 11 :action :retweet} {:user-id 4 :id 44}]
             :facebook [{:user-id 1 :id "1"} {:user-id 2 :id "2"}]
             :rss [{:feed-url "22" :shared-url "22"}
                   {:feed-url "2" :shared-url "2"}
                   {:feed-url "1" :shared-url "1" :action :canonical}]})
        new (new-external-shares s1 s2)
        merged (merge-external-shares s1 s2)
        extract-shares-info (fn [shares]
                              (for-map [[k f] {:twitter (juxt :user-id :id :action)
                                               :facebook (juxt :user-id :id)
                                               :rss (juxt :feed-url :shared-url)}]
                                k (map f (safe-get shares k))))]
    (is-= {:twitter [[4 44 :tweet]]
           :facebook [[1 "1"] [2 "2"]]
           :rss [["22" "22"] ["1" "1"]]}
          (extract-shares-info new))

    (is-= {:twitter [[1 1 :tweet] [2 2 :tweet] [4 44 :tweet]]
           :facebook [[1 "1"] [2 "2"]]
           :rss [["1" "1"] ["2" "2"] ["22" "22"] ["1" "1"]]}
          (extract-shares-info merged))))

(deftest twitter-share-missing-data
  (let [no-name {:sharer {:type :twitter :id 4
                          :username "alice"
                          :name nil
                          :image nil
                          :favicon "http://domain.tld/favicon.ico"}
                 :comment {:text "I like bananas."
                           :summary-score 3.2}
                 :date 13700
                 :id 123454321
                 :action :tweet
                 :source-set {:type :type :key "I'm a key"}}
        twitter-share (share-data->TwitterShare no-name)
        with-missing (-> no-name
                         (assoc-in [:sharer :name] "alice")
                         (assoc-in [:sharer :image] +default-twitter-profile-image+))]
    ;; confirm that the twitter-share we created validates
    (is (not (s/check TwitterShare twitter-share)))
    (is-= (-> with-missing
              ;; we explicitly drop favicon
              (assoc :tweet-info nil)
              (assoc-in [:sharer :favicon] nil))
          (TwitterShare->old-share-data twitter-share))
    (is-= (-> with-missing
              (assoc :tweet-info nil)
              (assoc-in [:sharer :favicon] nil))
          (TwitterShare->old-share-data (share-data->TwitterShare with-missing)))))

(deftest round-trip-twitter-share-test
  (let [s (map->TwitterShare
           {:id 1
            :action :tweet
            :user-id 10
            :screen-name "screen-name"
            :name "name"
            :profile-image "profile-image"
            :comment "foo"
            :summary-score 1.2
            :date 123
            :verified? true
            :num-followers 1000
            :reply? true
            :coordinates [100.2 200.3]
            :num-retweets 10
            :num-favorites 20
            :media-type :video})
        rt (-> s TwitterShare->old-share-data share-data->TwitterShare)]
    (is (not (s/check TwitterShare rt)))
    (is-= rt s)))

(use-fixtures :once validate-schemas)
