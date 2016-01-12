(ns gumby.core-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [plumbing.resource :as resource]
   [store.bucket :as bucket]
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

(deftest build-query-test
  (let [q (gumby/build-query {:filtered {:filter {:term {"title" "pwn"}}}})]
    ;; TODO
    (is q)))

(deftest ^:integration integration-test
  (es-bundle-test
   (fnk [client]
     (is-= {:id "123" :index +test-index+ :type "user" :version 1 :created? true}
           (gumby/index! client +test-index+ "user" 123 {:id 123}))
     (is (gumby/exists? client +test-index+ "user" 123))
     (is-= {:id 123} (gumby/get client +test-index+ "user" 123))
     (gumby/delete! client +test-index+ "user" 123)
     (is (not (gumby/exists? client +test-index+ "user" 123))))))

(deftest ^:integration bucket-test
  (es-bundle-test
   (fnk [client]
     (let [b (bucket/bucket {:type :elasticsearch
                             :client client
                             :index +test-index+
                             :mapping-type "colors"})]

       (testing "bucket/get + bucket/put"
         (let [id "1"
               document {:name "purple"}]
           (is (bucket/put b id document))
           (is-= document (bucket/get b id))))

       (testing "bucket/exists?"
         (is (false? (bucket/exists? b 10)))
         (is (bucket/put b 10 {:name "black"}))
         (is (true? (bucket/exists? b 10))))

       (testing "bucket/batch-get + bucket/batch-put"
         (let [kvs (for-map [n (range 3 10)] n {:name (str "color " n)})]
           (bucket/batch-put b kvs)
           (is-=-by set kvs (bucket/batch-get b (keys kvs)))))

       (testing "bucket/update"
         (let [ms (millis)
               id "2"]
           (bucket/put b id {:name "orange" :last-seen ms})
           (is-= {:name "orange" :last-seen ms}
                 (bucket/get b id))
           (bucket/update b id #(update % :last-seen inc))
           (is-= {:name "orange" :last-seen (inc ms)}
                 (bucket/get b id))))

       (testing "bucket/count"
         (is-eventually (pos? (bucket/count b)))
         (let [init-count (bucket/count b)]
           (is (bucket/put b 11 {:name "maroon"}))
           (is-eventually (= (inc init-count) (bucket/count b)))))

       (testing "bucket/delete"
         (is (false? (bucket/exists? b 100)))
         (is (bucket/put b 100 {:name "transparent"}))
         (is (true? (bucket/exists? b 100)))
         (is (bucket/delete b 100))
         (is (false? (bucket/exists? b 100)))))

     (let [b (bucket/bucket {:type :elasticsearch
                             :client client
                             :index +test-index+
                             :mapping-type "mlb-teams"})
           teams ["Baltimore Orioles" "Boston Red Sox" "Chicago White Sox" "Cleveland Indians"
                  "Detroit Tigers" "Houston Astros" "Kansas City Royals" "Los Angeles Angels"
                  "Minnesota Twins" "New York Yankees" "Oakland Athletics" "Seattle Mariners"
                  "Tampa Bay Rays" "Texas Rangers" "Toronto Blue Jays" "Arizona Diamondbacks"
                  "Atlanta Braves" "Chicago Cubs" "Cincinnati Reds" "Colorado Rockies"
                  "Los Angeles Dodgers" "Miami Marlins" "Milwaukee Brewers" "New York Mets"
                  "Philadelphia Phillies" "Pittsburgh Pirates" "San Diego Padres"
                  "San Francisco Giants" "St. Louis Cardinals" "Washington Nationals"]
           data (for-map [[idx team] (indexed teams)]
                  idx {:id idx :name team})]

       (bucket/batch-put b data)

       (let [ks [1 3 8 5 2]]
         (is-= (mapv (fn [k] [k (safe-get data k)]) ks)
               (bucket/batch-get b ks)))

       (testing "bucket/keys"
         (is-eventually
          (= (set (map str (keys data)))
             (set (bucket/keys b)))))

       (testing "bucket/vals"
         (is-eventually
          (= (set (vals data))
             (set (bucket/vals b)))))

       (testing "bucket/seq"
         (is-eventually
          (= (map-keys str data)
             (into {} (bucket/seq b))))))

     (testing "multi-index"
       (let [num-indices 4
             num-items 10
             items (for [id (range 10)] [(str id) {:id id}])
             b (bucket/bucket {:type :elasticsearch
                               :client client
                               :index "deli-all"
                               :index-fn (fn-> Long/parseLong (mod num-indices) (->> (str "deli-")))
                               :mapping-type "cheese"})]

         (dotimes [n num-indices] (index/create! client (str "deli-" n) {}))

         (index/add-alias! client "deli-all" "deli-0" "deli-1" "deli-2" "deli-3")

         (let [[k v] (first items)]
           (bucket/put b k v))

         (bucket/batch-put b (rest items))

         (index/refresh! client "deli-all")

         (is-=-by set items (bucket/seq b))
         (is-=-by set (map first items) (bucket/keys b))
         (is-=-by set (map second items) (bucket/vals b))
         (is-= num-items (bucket/count b))

         (doseq [[k v] items]
           (is-= v (bucket/get b k)))

         (is-= items (bucket/batch-get b (map first items)))

         (doseq [n (range num-indices)
                 :let [bi (bucket/bucket
                           {:type :elasticsearch
                            :client client
                            :index (str "deli-" n)
                            :mapping-type "cheese"})]
                 [k v] items]
           (let [x (bucket/get bi k)]
             (if (= n (mod (Long/parseLong k) num-indices))
               (is-= v x)
               (is (nil? x))))))))))
