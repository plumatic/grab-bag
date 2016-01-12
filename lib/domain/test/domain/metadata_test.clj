(ns domain.metadata-test
  (:require
   [schema.core :as s]
   [domain.metadata :refer :all]
   [plumbing.test :refer :all]
   [clojure.test :refer :all]))

(deftest hiccup-schema-validation-test
  (is (nil? (s/check HiccupElement [:a {:k "v"} [:b "c"]])))
  (is (s/check HiccupElement [:a {:k "v"} {:k2 "v2"}])))

(deftest matches-tag?-test
  (is (matches-tag? :meta [:meta {:attr "key"} "x"]))
  (is (not (matches-tag? :meta [:div [:meta {:attr "key"} "x"]]))))

(deftest micro-data?-test
  (is (micro-data? [:div {:itemprop "key"} "x"]))
  (is (micro-data? [:div {:property "key"} "x"]))
  (is (not (micro-data? [:div {:NOT!itemprop "key"} "x"])))
  (is (not (micro-data? [:div {:NOT!property "key"} "x"]))))

(def +test-dom+
  [:html
   [:body
    [:div
     {:itemprop "review"
      :itemscope "itemscope"
      :itemtype "http://schema.org/Review"}
     [:span {:itemprop "name"} "Not a happy camper"] "by"
     [:span {:itemprop "author"} "Ellie"]
     ","]]
   [:head
    [:meta {:content "2011-04-01" :itemprop "datePublished"}]
    [:meta {:content "some-other-date" :name "datePublished"}]
    [:meta {:content "ignored" :bad-name "bad-attr-name"}]
    [:meta {:not-content "not-content" :name "missing-content"}]]
   [:body
    "April 1, 2011"
    [:div
     {:itemprop "reviewRating"
      :itemscope "itemscope"
      :itemtype "http://schema.org/Rating"}]]
   [:head [:meta {:content "1" :itemprop "worstRating"}]]
   [:body
    [:span {:itemprop "ratingValue"} "1"]
    "/"
    [:span {:itemprop "bestRating"} "5"]
    "stars"]])

(deftest filter-dom-test
  (testing "simple meta extraction"
    (is-= [[:meta {:k "value"}]
           [:meta {:k2 "value2"} "text"]]
          (filter-dom
           (partial matches-tag? :meta)
           [:html [:head {:attr "attr-value"}
                   [:span {:why "is there"} "a span in the header?"]
                   [:meta {:k "value"}]
                   [:meta {:k2 "value2"} "text"]]])))

  (testing "nested meta extraction"
    (is-=
     [[:meta
       {:k "value"}
       [:span "text"]
       [:span
        [:div {:extra "bonus"}
         [:meta {:k2 "value2"} "text"]]]]]
     (filter-dom
      (partial matches-tag? :meta)
      [:html [:head {:attr "attr-value"}
              [:meta {:k "value"}
               [:span "text"]
               [:span
                [:div {:extra "bonus"}
                 [:meta {:k2 "value2"} "text"]]]]]])))

  (testing "nested itemprops"
    (is-= [[:div
            {:itemtype "http://schema.org/Review",
             :itemprop "review",
             :itemscope "itemscope"}
            [:span {:itemprop "name"} "Not a happy camper"]
            "by"
            [:span {:itemprop "author"} "Ellie"]
            ","]
           [:meta {:content "2011-04-01" :itemprop "datePublished"}]
           [:div
            {:itemtype "http://schema.org/Rating",
             :itemprop "reviewRating",
             :itemscope "itemscope"}]
           [:meta {:content "1" :itemprop "worstRating"}]
           [:span {:itemprop "ratingValue"} "1"]
           [:span {:itemprop "bestRating"} "5"]]
          (filter-dom micro-data? +test-dom+))))

(deftest extract-meta-test
  (is-= {:meta-tags
         {"datePublished" ["2011-04-01" "some-other-date"]
          "worstRating" ["1"]}
         :micro-data
         [[:div
           {:itemtype "http://schema.org/Review",
            :itemprop "review",
            :itemscope "itemscope"}
           [:span {:itemprop "name"} "Not a happy camper"]
           "by"
           [:span {:itemprop "author"} "Ellie"]
           ","]
          [:div
           {:itemtype "http://schema.org/Rating",
            :itemprop "reviewRating",
            :itemscope "itemscope"}]
          [:span {:itemprop "ratingValue"} "1"]
          [:span {:itemprop "bestRating"} "5"]]}
        (extract-meta +test-dom+)))

(deftest microdata-props-test
  (is-= [{:itemtype "A"} {:rel "B"} {:property "foo"}]
        (microdata-props
         {:meta-tags {}
          :micro-data [[:a {:b :c}
                        [:c [:d {:itemtype "A"}] "test" [:e [:f {:rel "B"}]]]]
                       [:e {:property "foo"}]]})))

(deftest keywords-test
  (is-=-by set
           ["foo" "bar baz (bat)" "chicken" "b" "c"]
           (keywords {:meta-tags {"KeyWoRds" [" foo, bar baz (Bat), " "chicken"]
                                  "abc" ["123"]
                                  "keywords" ["b,c"]}})))
