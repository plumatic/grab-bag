(ns domain.interests.type.bucket-test
  (:use clojure.test plumbing.core plumbing.test
        domain.interests.type.bucket)
  (:require
   [store.bucket :as bucket]
   [domain.interests.manager :as manager]))

(deftest bad-image-replacement-test
  (is (nil? (bad-image-replacement {:type :a :key "b"})))
  (is (nil? (bad-image-replacement {:type :a :key "b" :img "test"})))
  (is (= {:type :a :key "b" :img "test"}
         (bad-image-replacement {:type :a :key "b" :img {:url "test"}})))
  (is (= {:type :a :key "b" :img "test" :highres-img "a"}
         (bad-image-replacement {:type :a :key "b" :img "test" :highres-img {:url "a"}}))))

(deftest bucket-manager-test
  (let [id-bucket (bucket/bucket {})
        namespace-size 3
        bucket-manager (bucket-interest-type-manager
                        id-bucket namespace-size 4
                        #(not (contains? % :title)))
        interest1 {:type :a :key "a" :title "a title"}
        interest2 {:type :a :key "b" :foo "bar"}
        id1 (manager/type-index! bucket-manager interest1)
        id2 (manager/type-index! bucket-manager interest2)]

    (testing "basic operation"
      (is-= interest1 (manager/type-lookup bucket-manager id1))
      (is-= interest2 (manager/type-lookup bucket-manager id2))
      (is-= id1 (manager/type-key-index bucket-manager (:key interest1)))
      (is-= id2 (manager/type-key-index bucket-manager (:key interest2)))
      (is (number? (manager/type-index! bucket-manager {:type :a :key "c"})))
      (is (thrown? Exception (manager/type-index! bucket-manager {:type :a :key "z" :title {}})))
      (is (= 4 (bucket/count (:cache bucket-manager))))) ;; max size

    (bucket/clear (:cache bucket-manager))
    (testing "Fills cache both ways"
      (is-= id1 (manager/type-key-index bucket-manager (:key interest1)))
      (is (= 2 (bucket/count (:cache bucket-manager))))
      (bucket/clear id-bucket)
      (is-= id1 (manager/type-key-index bucket-manager (:key interest1)))
      (is-= interest1 (manager/type-lookup bucket-manager id1)))

    (bucket/clear (:cache bucket-manager))
    (is (= 0 (bucket/count id-bucket)))
    (testing "Replacement of bad interests by bad-interest?-fn"
      (let [id1 (manager/type-index! bucket-manager interest1)
            id2 (manager/type-index! bucket-manager interest2)
            extended-interest1 (assoc interest1 :more "more")
            extended-interest2 (assoc interest2 :title "b title")]
        (bucket/clear (:cache bucket-manager))
        (bucket/update id-bucket "key://a" #(assoc-in % [:interest :old?] "true"))
        (bucket/update id-bucket (str "id://" id1) #(assoc-in % [:interest :old?] "true"))
        (is-= id1 (manager/type-index! bucket-manager extended-interest1))
        (is-= id2 (manager/type-index! bucket-manager extended-interest2))
        (is-= (assoc interest1 :old? "true") (manager/type-lookup bucket-manager id1))
        (is-= extended-interest2 (manager/type-lookup bucket-manager id2))
        (is-= id2 (manager/type-key-index bucket-manager (:key interest2)))))

    (testing "Replacement of bad interests by schema validation"
      (let [schema-fail {:type :a :key "z" :title {}}
            good (assoc schema-fail :title "TITLE")]
        (bucket/put id-bucket "key://z" {:id 199 :interest schema-fail})
        (is (thrown? Exception (manager/type-index! bucket-manager schema-fail)))
        (is (= 199 (manager/type-index! bucket-manager good)))
        (is-= good (manager/type-lookup bucket-manager 199))))

    (testing "Auto-fixing of bad publisher images"
      (let [fail {:type :a :key "q" :img {:url "the real img"}}
            good (update-in fail [:img] :url)]
        (bucket/put id-bucket "id://197" {:id 197 :interest fail})
        (is-= good (manager/type-lookup bucket-manager 197))
        (is-= {:id 197 :interest good} (bucket/get id-bucket "id://197"))))))


(deftest ^:slow bucket-manager-concurrency-test
  (let [namespace-size 10
        num-interests 7
        interests (map #(hash-map :key (str "key" %) :type :t) (range num-interests))]
    (dotimes [_ 200]
      (let [id-bucket (bucket/bucket {})
            bucket-manager (bucket-interest-type-manager
                            id-bucket namespace-size 1)
            indices (pmap #(manager/type-index! bucket-manager %) interests)]
        (is-= (count (set indices)) (count interests))
        (doseq [[idx interest] (zipmap indices interests)]
          (is-= (manager/type-lookup bucket-manager idx)
                interest))))))
