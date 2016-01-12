(ns gumby.index-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [plumbing.resource :as resource]
   [gumby.core :as gumby]
   [gumby.index :as index]))

(defn es-bundle-test [f]
  (resource/with-open
      [bundle (resource/bundle-run
               gumby/elasticsearch-bundle
               {:env :test
                :cluster-name "gumby"
                :ec2-tags {}
                :instance {:service-name "gumby-test"}
                :local? true})]
    (f bundle)))

(def +test-index+ ".gumby-test")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest ^:integration index-test
  (es-bundle-test
   (fnk [client]
     (testing "mappings functions"
       (let [mappings {"employee" {:properties {:id {:type "long"} :name {:type "string"}}}}]
         (is (not (index/exists? client +test-index+)))
         (index/set-mappings! client +test-index+ mappings)
         (is (index/exists? client +test-index+))
         (is-= mappings (index/get-mappings client +test-index+))

         (let [updated-mappings (assoc mappings "department" {:properties {:id {:type "long"}}})]
           (index/set-mappings! client +test-index+ updated-mappings)
           (is (index/exists? client +test-index+))
           (is-= updated-mappings (index/get-mappings client +test-index+)))))

     (testing "alias functions"
       (let [alias (str +test-index+ "-alias")]
         (is (not (index/alias-exists? client alias)))
         (is (index/add-alias! client alias +test-index+))
         (is (index/alias-exists? client alias)))))))
