(ns web.pubsub-test
  (:use clojure.test plumbing.core plumbing.test web.pubsub)
  (:require
   [plumbing.graph :as graph]
   [plumbing.resource :as resource]
   [store.bucket :as bucket]))


(def test-args
  {:env :test
   :ec2-keys {}
   :observer nil
   :server-host "localhost"
   :instance {:service-name "test"}})

(def test-bundle
  (graph/graph
   :pubsub pubsub-resources
   :subscriber* subscribers-resources
   :publisher* publishers-resources
   :pubsub-stats (fnk [] (constantly nil))
   :topic-subscriber (graph/instance raw-subscriber [] {:raw-topic "topic"})
   :topic-publisher (graph/instance raw-publisher [] {:raw-topic "topic"})))

(defrecord Foo [x])

(def +items+
  #{"id" "message" [(java.util.HashMap. {1 2}) (Foo. 2)]})

(deftest pubsub-local-round-trip-test
  (resource/with-open [g (resource/bundle-run test-bundle test-args)]
    (letk [events (atom [])
           [[:pubsub broker] topic-subscriber topic-publisher] g]
      (is-= [["test" {:host "localhost" :id "test" :topic "topic" :uri "/topic"}]]
            (for [[t m] (bucket/seq (topic-bucket broker "topic"))]
              [t (dissoc m :date :port)]))
      (topic-subscriber (partial swap! events conj))
      (doseq [item +items+] (topic-publisher item))
      (is (= +items+ (set @events)))
      (let [unserializable (atom nil)]
        (topic-publisher unserializable) ;; we must be local
        (is (identical? (last @events) unserializable))))))

(deftest pubsub-remote-round-trip-test
  (resource/with-open [g (resource/bundle-run test-bundle (assoc test-args :force-remote? true :drain nil))]
    (letk [events (atom [])
           [[:pubsub broker] topic-subscriber topic-publisher] g]
      (is-= [["test" {:host "localhost" :id "test" :topic "topic" :uri "/topic"}]]
            (for [[t m] (bucket/seq (topic-bucket broker "topic"))]
              [t (dissoc m :date :port)]))
      (topic-subscriber (partial swap! events conj))
      (doseq [item +items+] (topic-publisher item))
      (is (= +items+ (set @events)))
      (is (thrown? Exception (topic-publisher (atom nil))))
      (is (= #{"id" "message" [(java.util.HashMap. {1 2}) (Foo. 2)]} (set @events))))))

(defn test-pubsub []
  (let [data (atom [])
        subs (atom [])]
    {:data-atom data
     :sub-atom subs
     :pub (fn [datum]
            (swap! data conj datum)
            (doseq [f @subs] (f datum)))
     :sub (fn [cb]
            (swap! subs conj cb))}))

(deftest subscriber-concat-test
  (let [{pub1 :pub sub1 :sub} (test-pubsub)
        {pub2 :pub sub2 :sub} (test-pubsub)
        csubscriber (subscriber-concat sub1 sub2)
        out-data (atom [])]
    (csubscriber (fn [m] (swap! out-data conj m)))
    (pub1 10)
    (pub2 20)
    (pub1 30)
    (csubscriber (fn [m] (swap! out-data conj (- m))))
    (pub1 40)
    (is (= @out-data [10 20 30 40 -40]))))
