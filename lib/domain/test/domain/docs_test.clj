(ns domain.docs-test
  (:use clojure.test plumbing.core plumbing.test domain.doc-test-utils
        domain.docs)
  (:require
   [schema.core :as s]
   [plumbing.io :as io]
   [domain.docs.fitness-stats :as fitness-stats]
   [domain.docs.products :as products]
   [domain.docs.views-by-client :as views-by-client]
   [domain.interests.indexer :as indexer]
   [domain.interests.manager :as interests-manager]
   [domain.metadata :as metadata]))

(deftest all-feed-interest-ids-test
  (is-=-by set
           (map #(apply interests-manager/interest-id %) [[:feed 42]])
           (all-feed-interest-ids
            (test-doc {:feed-ids [42]}))))

(deftest all-topic-interest-ids-test
  (is-=-by set
           (map #(apply interests-manager/interest-id %)
                [[:topic 1]
                 [:topic 6]])
           (all-topic-interest-ids
            (test-doc {:topic-predictions [[1 1.0 1.0] [6 1.0 1.0]]}))))

(deftest test-new-roundtrips
  (doseq [[label doc] {"external" (test-external-doc
                                   {:external-shares {:twitter [1 2 3]}
                                    :activity [3 4 {:user-id 5 :action :click :dwell-ms 1000}]
                                    :feed-ids [8 10]
                                    :comments [5]
                                    :tags {:admin ["foo"]}})
                       "post" (test-post
                               {:submitter-id 3
                                :external-shares {:twitter [1 2 3]}
                                :activity [3 4]
                                :comments {5 true 6 {7 true 8 [9 10]}}})}]
    (is-=-doc doc (-> doc write-doc read-doc))
    (is-=-doc doc (-> doc io/to-data io/from-data))))

(deftest dwell-times-test
  (is-= [1000 24] (dwell-times (test-external-doc
                                {:external-shares {:twitter [1 2 3]}
                                 :activity [3 4 {:user-id 5 :action :click :dwell-ms 1000}
                                            {:user-id 5 :action :click :dwell-ms 24}]
                                 :feed-ids [8 10]
                                 :comments [5]
                                 :tags {:admin ["foo"]}}))))

;; Can we read super-old docs, retrofitted from client-docs way back when?
(deftest old-read-test
  (let [d (test-external-doc
           {:external-shares {:twitter [1 2 3]}
            :activity [3 4]
            :comments [5]
            :topic-predictions nil})
        old-data (-> d
                     write-doc
                     (dissoc :feed-ids :topic-predictions :experimental :views-by-client)
                     (assoc :cluster-features nil
                            :topics [["iPad Apps" 1.9773323452423037]
                                     ["Twitter" 1.9686608205344753]]
                            :ner []))
        old-doc (read-doc old-data)]
    (is (not (s/check domain.docs.Doc old-doc)))
    (is-=-doc old-doc
              (-> d
                  (assoc :feed-ids nil
                         :cluster-features {}
                         :topics [["iPad Apps" 1.9773323452423037 42]
                                  ["Twitter" 1.9686608205344753 42]])
                  (assoc-in [:type-info :ner] {:title-entities [] :text-entities []})))

    (testing "fixing old images"
      ;; coercion tested by validation
      (let [[s1 s2 s3] (for [i (range 3)] {:width (int i) :height (int i)})]
        (doseq [[name [old new]]
                {"empty" [nil nil]
                 "no-in-div" [[{:size s1 :url "u1"}
                               {:size s2 :url "u2"}]
                              [{:size s1 :url "u1" :in-div false}
                               {:size s2 :url "u2" :in-div true}]]
                 "with-in-div" [[{:size s1 :url "u1" :in-div true}
                                 {:size s2 :url "u2" :in-div false}]
                                [{:size s1 :url "u1" :in-div true}
                                 {:size s2 :url "u2" :in-div false}]]}]
          (testing name
            (is-= new
                  (:images (read-doc (assoc old-data :images old))))))))

    (testing "fixing old non-image ints"
      ;; schema validation confirms conversion
      (let [cluster-features {(int 1) 1.0 (int 2) 2.0}
            layout {:iphone-retina
                    {:feed-portrait-full
                     {:image-key "a" :extension "b" :image-size [(int 2) (int 3)]}}}
            doc (read-doc (merge old-data
                                 {:cluster-features cluster-features
                                  :layout layout}))]
        (is-= cluster-features (:cluster-features doc))
        (is-= layout (:layout (external-info doc)))
        ))

    (testing "fixing html-des"
      (is-= nil
            (:html-des (external-info (read-doc (assoc (write-doc d) :html-des {:html "<a tag></a>"})))))
      (let [h {:html "<a tag></a>" :chunk-offsets [[1 10] [11 200]]}]
        (is-= h
              (:html-des (external-info (read-doc (assoc (write-doc d) :html-des h)))))))

    (testing "adding tags"
      (is-= {} (:tags old-data))
      (is-= {} (:tags (read-doc (dissoc old-data :tags)))))

    (testing "adding metadata"
      (is (-> old-data (dissoc :metadata) read-doc (contains? :metadata)))
      (let [metadata (metadata/metadata {"meta" ["data"]} [])]
        (is-= metadata (-> old-data (assoc :metadata metadata) read-doc :metadata))))))

(deftest read-old-products-test
  (let [itunes-product {:product {:name "fish"
                                  :type "animal"
                                  :genres []
                                  :price nil
                                  :rating nil}
                        :url "fish.com"
                        :key "01234567"
                        :source :itunes
                        :referral-type :internal
                        :in-div true
                        :highlights ["gills" "feet"]}
        legacy-itunes {:name "fish"
                       :type "animal"
                       :key "01234567"
                       :in-div true
                       :highlights ["gills" "feet"]
                       :url "fish.com"
                       :source :itunes}
        legacy-amazon {:title "Dog"
                       :price "$0.30"
                       :url "dog.com"
                       :item-attributes {}
                       :type :internal}
        empty-products (test-external-doc
                        {:products nil})
        written-empty (write-doc empty-products)
        with-products (test-external-doc
                       {:products [itunes-product]})
        written-products (assoc (write-doc empty-products)
                           :products [legacy-itunes])]
    (testing "Ignore nil :commerce"
      (is-=-doc empty-products
                (read-doc (assoc written-empty :commerce nil)))
      (is-=-doc (assoc-in empty-products [:type-info :products] [itunes-product])
                (read-doc (assoc written-products :commerce nil))))
    (testing "Add non-nil :commerce to :products"
      (is-= [(products/old-itunes->reference legacy-itunes)
             (products/old-amazon->reference legacy-amazon)]
            (-> written-empty
                (assoc
                    :commerce legacy-amazon
                    :products [legacy-itunes])
                read-doc
                (safe-get-in [:type-info :products])))
      (is-= [(products/old-amazon->reference legacy-amazon)]
            (-> written-empty
                (assoc :commerce legacy-amazon)
                read-doc
                (safe-get-in [:type-info :products]))))))

;; can we read docs with new keys, and just ignore them?
(deftest forward-compatible-test
  (doseq [[t d] {"external" (test-external-doc
                             {:external-shares {:twitter [1 2 3]}
                              :activity [3 4]
                              :comments [5]})
                 "post" (test-post
                         {:submitter-id 10
                          :activity [3 4]
                          :comments [5]})}]
    (testing t
      (let [data (write-doc d)
            d2 (read-doc (assoc data :an-extra-key "fooo"))]
        (is (not (s/check domain.docs.Doc d2)))
        (is-=-doc d d2)))))

(deftest type-info-validation
  (is-=-by str
           `{:type-info {:url (~'not (~'instance? String 1))}}
           (s/check domain.docs.Doc
                    (assoc-in (test-external-doc {})
                              [:type-info :url] 1))))

(deftest write-doc-test
  (let [d (test-external-doc {})
        w (write-doc d)]
    (testing "empty metadata is not written"
      (is (contains? d :metadata))
      (is-= (metadata/metadata {} []) (:metadata d))
      (is (not (contains? w :metadata))))))

(deftest read-and-write-validation
  (is (thrown? Exception (write-doc (assoc-in (test-external-doc {})
                                              [:type-info :url] 1))))
  (let [w (write-doc (test-external-doc {}))]
    (is (thrown? Exception (read-doc (assoc w :url 1))))))

(deftest upgrade-feed-and-topic-id-test
  (with-redefs [interests-manager/index!
                (fn [i {:keys [type key]}]
                  (let [type (keyword type)]
                    (indexer/index-of
                     @#'interests-manager/+type-index+ type
                     (safe-get-in
                      {:topic {"foo" 99 "bar" 101}
                       :feed {"ponies.com" 1000}}
                      [type key]))))]
    (is-= {:topic-predictions [99 101]
           :feed-ids [1000]}
          (-> (test-external-doc
               {:topics ["foo" "baz" "bar"]
                :domain {:name "ponies.com"}})
              write-doc
              (dissoc :topic-predictions :feed-ids)
              (read-doc ::interest-manager)
              (select-keys [:topic-predictions :feed-ids])
              (update-in [:topic-predictions] (partial mapv :id))))))

(deftest keep-top-topics-test
  (let [process (fn->> (keep-top-topics second)
                       (map first)
                       (apply str))]
    (doseq [[start result] [[0.651 "mlkjihg"]
                            [0.45 "ml"]]
            :let [topics (map list "abcdefghijklm" (range start 100 0.01))]
            t [topics (reverse topics)]]
      (is-= result (process t)))))

(deftest top-topic-interest-ids-test
  (is-= [10]
        (top-topic-interest-ids
         (test-doc {:topic-predictions
                    [{:id 10 :score 0.8 :confidence 1.0}
                     {:id 5 :score 0.5 :confidence 1.0}]}))))

(deftest clone-writable-fields-test
  (let [d (test-external-doc {})
        od (-> d write-doc read-doc)
        c (clone-writable-fields d)]
    (is (= (dissoc d :ranking-features)) (dissoc c :ranking-features))
    (fitness-stats/increment-view-count! (:fitness-stats c) 1)
    (views-by-client/add-view! (:views-by-client c) :iphone 2)
    (is (= (dissoc d :ranking-features) (dissoc od :ranking-features)))
    (is (not= (dissoc d :ranking-features) (dissoc c :ranking-features)))
    (is (not= (:fitness-stats d) (:fitness-stats c)))
    (is (not= (:views-by-client d) (:views-by-client c)))))

(deftest reconcile-fitness-stats-with-views!-test
  (let [d (test-external-doc {})
        fitness-count 123
        client-count 7]
    (fitness-stats/increment-view-count! (:fitness-stats d) fitness-count)
    (testing "when no client-views, fitness stats remain"
      (is-= fitness-count
            (-> d write-doc read-doc (safe-get :fitness-stats) fitness-stats/view-count)))
    (testing "client-views override fitness-stats when present"
      (let [clients [:iphone :android]]
        (doseq [client clients]
          (dotimes [n client-count]
            (views-by-client/add-view! (:views-by-client d) client n)))
        (is-= (* (count clients) client-count)
              (-> d write-doc read-doc (safe-get :fitness-stats) fitness-stats/view-count))))))

(deftest core-title-test
  (is-= "3sfhomesalespriceyouwontbelievetrustme"
        (core-title
         (test-external-doc
          {:title "3 S.F. home sales: Price$ you won't believe - trust me!"}))))

(deftest core-url-test
  (is-= "httpwwweastbayexpresscomoakland2015bestof"
        (core-url "http://www.eastbayexpress.com/Oakland/2015/BestOf"))
  (is-= "httpwwweastbayexpresscomoakland2015bestof"
        (core-url "http://www.eastbayexpress.com/Oakland/2015/BestOf?category=1166270")))
