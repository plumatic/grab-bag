(ns domain.docs.tags-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [schema.core :as s]
   [domain.doc-test-utils :as doc-test-utils]
   [domain.docs.tags :as tags]))

(deftest update-tags-test
  (let [user1 {:type :user
               :date 123
               :user-id 10
               :tag "offensive"
               :info nil}
        user2 (assoc user1
                :user-id 20
                :tag "other"
                :info "this story is just too good")
        admin1 {:type :admin
                :date 234
                :user-id 39
                :tag "wtf"
                :info nil}
        admin2 (assoc admin1
                 :tag "conspiracy")
        admin3 (assoc admin1
                 :user-id 40)
        auto1 {:type :auto
               :date 333
               :tag "wtf"
               :posterior 1.0}
        tags (reduce tags/conj tags/+empty-tags+ [user1 admin1 user2 admin2 auto1 admin3])]
    (is-= {:user [user1 user2]
           :admin [admin1 admin2 admin3]
           :auto [auto1]}
          tags)
    (is-= {:user [user1 user2]
           :admin [admin2]
           :auto [auto1]}
          (-> tags
              (tags/untag :admin "wtf")
              (tags/untag :auto "not present")))
    (is-= {:user [user1 user2]
           :admin []
           :auto []}
          (-> tags
              (tags/untag :admin "wtf")
              (tags/untag :admin "conspiracy")
              (tags/untag :auto "wtf")))
    (testing "user-tags are distinct"
      (is-= tags (tags/conj tags user2))
      (is-= (assoc tags :user [user2 user1])
            (tags/conj tags user1)))))

(deftest tag-label-test
  (is-= "asdf" (tags/label-tag "asdf" true))
  (is-= "~asdf" (tags/label-tag "asdf" false))
  (is-= ["asdf" true] (tags/split-tag-label "asdf"))
  (is-= ["asdf" false] (tags/split-tag-label "~asdf")))

(deftest blocked-test
  (is (tags/blocked-tag? "block"))
  (is (tags/blocked-tag? "block-porn"))
  (is (not (tags/blocked-tag? "bleck")))
  (is (tags/blocked? (doc-test-utils/test-tags {:admin ["block"]})))
  (is (tags/blocked? (doc-test-utils/test-tags {:auto ["block"]})))
  (is (not (tags/blocked? (doc-test-utils/test-tags {:admin ["bleck"]}))))
  (is (not (tags/blocked? (doc-test-utils/test-tags {:user ["offensive"]})))))

(deftest typed-tag-test
  (is-= "admin:foo" (tags/typed-tag :admin "foo"))
  (is-= [:admin "foo"] (tags/split-typed-tag "admin:foo"))
  (is (thrown? Exception (tags/split-typed-tag "foo:bar")))
  (is (s/check tags/TypedTag "foo:bar")))

(deftest attributed-tag-test
  (is (tags/attributed? "alskjdflj=asdf"))
  (is (not (tags/attributed? "alskjdflj;asdf")))
  (is-= [["a" "b"] ["cd" "ef"]] (tags/tag->attributes "a=b;cd=ef"))
  (doseq [t ["a=b" "a12=c3;c12=d091;kj_0=a-b;z=q"]]
    (is-= t (-> t tags/tag->attributes tags/attributes->tag))))

(deftest tag-tree-test
  (let [leaf (fn [t] {:tag t :sub-keys {}})]
    (is-= (tags/tag-tree ["a=b" "a=c" "a=c;f=g" "a=c;f=e" "b=c" "a=b;q=r;t=s"])
          {"a" {"b" {:tag "a=b"
                     :sub-keys {"q" {"r" {:tag "a=b;q=r"
                                          :sub-keys {"t" {"s" (leaf "a=b;q=r;t=s")}}}}}}
                "c" {:tag "a=c"
                     :sub-keys {"f" {"e" (leaf "a=c;f=e")
                                     "g" (leaf "a=c;f=g")}}}}
           "b" {"c" {:tag "b=c"
                     :sub-keys {}}}})))

(deftest mutex?-test
  (is (tags/mutex? "content=news;region=local" "content=news;region=us"))
  (is (tags/mutex? "content=review;region=local" "content=news;region=local"))
  (is (tags/mutex? "content=review" "content=news;opinion=true"))
  (is (not (tags/mutex? "content=news" "content=news;opinion=true")))
  (is (not (tags/mutex? "type=video;region=us" "content=news;region=local")))
  (is (not (tags/mutex? "content=news;region=local" "content=news;opinion=true")))
  (is (not (tags/mutex? "type=video;region=local" "content=news;opinion=true"))))

(deftest subclass?-test
  (is (tags/subclass? "content=news" "content=news;region=us"))
  (is (tags/subclass? "content=news;region=us" "content=news;region=us"))
  (is (not (tags/subclass? "content=news;region=us" "content=news")))
  (is (not (tags/subclass? "content=review" "content=news;region=us")))
  (is (not (tags/subclass? "content=news;region=us" "content=news;region=local")))
  (is (not (tags/subclass? "type=video;region=us" "content=news;region=local"))))

(deftest sibling-mutex?-test
  (is (tags/sibling-mutex? "content=news" "content=fun"))
  (is (tags/sibling-mutex? "content=news;region=local" "content=news;region=usa"))
  (is (not (tags/sibling-mutex? "content=news;region=local" "content=news;region=local")))
  (is (not (tags/sibling-mutex? "content=news;region=local" "content=news")))
  (is (not (tags/sibling-mutex? "content=news;type=local" "content=news;region=usa")))
  (is (not (tags/sibling-mutex? "type=news;region=local" "content=news;region=usa"))))

(deftest superclasses-test
  (is-=-by set
           ["type=article" "type=article;content=review" "type=article;content=review;target=product"]
           (tags/superclasses "type=article;content=review;target=product"))
  (is-= ["type=article"] (tags/superclasses "type=article")))

(use-fixtures :once validate-schemas)
