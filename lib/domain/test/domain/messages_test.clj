(ns domain.messages-test
  (:use clojure.test plumbing.core plumbing.test domain.doc-test-utils domain.messages)
  (:require
   [schema.core :as s]
   [plumbing.serialize :as serialize]
   [domain.doc-test-utils :as doc-test-utils]
   [domain.docs.actions :as actions]
   [domain.docs.fitness-stats :as fitness-stats]
   [domain.docs.views-by-client :as views-by-client]))

(def +test-shares+
  (test-external-shares {:twitter [1 2 3] :facebook [42 5] :rss [7 8]}))

(def +test-activity+
  (test-simple-actions [1 2 {:user-id 3 :action :click}]))

(defn validate-message [m]
  (when-let [es (or (:external-shares m) (:shares m))]
    (is (do (s/validate domain.docs.external_shares.ExternalShares es) true)))
  (when-let [sa (:activity m)]
    (is (do (s/validate [domain.docs.actions.SimpleAction] sa) true)))
  (when-let [ids (:activity-ids m)]
    (is (do (s/validate [long] ids) true)))
  (when-let [c (:comment m)]
    (is (do (s/validate domain.docs.comments.Comment c) true)))
  (when-let [c (:comment-action m)]
    (is (do (s/validate domain.docs.actions.SimpleAction c) true))))

(defn is-=-message [m1 m2]
  (validate-message m1)
  (validate-message m2)
  (is-=-by #(-> %
                (?> (:doc %)
                    (update-in [:doc] core-doc-fields))
                (?> (:fitness-stats %)
                    (update-in [:fitness-stats] fitness-stats/write-fitness-stats)))
           m1 m2))

(deftest message-round-trip-test
  (doseq [[reader writer messages]
          [[read-new-shares-message write-new-shares-message
            [(new-shares-message "http://unresolved" {:doc "hints"} +test-shares+)]]
           [read-fetch-doc-message write-fetch-doc-message
            [(fetch-doc-message 10 "http://resolved" {:doc "hints"})]]
           [read-message write-message
            [(add-external-shares-message 10 {:doc "hints"} +test-shares+)
             (add-activity-message 10 +test-activity+)
             (add-doc-message (test-external-doc {:id 10 :external-shares {:rss ["A"]}}))
             (let [doc (test-external-doc {:id 11})]
               (add-full-doc-message doc (test-fetched-page doc)))
             (merge-doc-message 12 {:topic-predictions [{:id 10 :score 10 :confidence 10}]})
             (delete-doc-message 4)
             (re-cluster-message 10 20)
             (delete-activity-message 12 [100 200 300])
             (add-post-message (test-post {:submitter-id 20}))
             (add-comment-message 20 (test-comment 30))
             (add-comment-action-message 10 20 (test-simple-action 20))
             (update-stats-message 10 (fitness-stats/fitness-stats))
             (add-tag-message 10 {:type :user :user-id 10 :date 123 :tag "offensive" :info nil})
             (untag-message 10 :auto "asdf")
             (update-dwell-message 1 3 19 :iphone 100)
             (mark-viewed-message 1 3 19 :iphone)]]]
          message messages]
    (testing (:action message)
      (let [round-trip (->> message
                            writer
                            reader
                            writer
                            (serialize/serialize serialize/+default+)
                            serialize/deserialize
                            reader)]
        (is-=-message message round-trip)))))

(deftest update-dwell-test
  (let [base (doc-test-utils/test-doc {})
        action (fn [user-id action]
                 (actions/map->SimpleAction
                  {:id 0 :date 0 :user-id user-id :action action}))
        a1 (action 10 :share)
        a2 (action 7 :click)
        a3 (action 10 :click)]
    (doseq [actions [[a3] [a1 a2 a3] [a2 a3 a1]]]
      (let [doc (assoc base :activity actions)
            final (reduce
                   (fn [doc [user-id dwell]]
                     (update-dwell doc (update-dwell-message 1 user-id 0 :iphone dwell)))
                   doc
                   [[10 1000] [10 2000] [5 1000] [10 1000]])
            split-target (fn [d] ((juxt filter remove)
                                  (fnk [action user-id]
                                    (and (= user-id 10) (= action :click)))
                                  (safe-get d :activity)))
            [[init-target] init-others] (split-target doc)
            [[final-target] final-others] (split-target final)]
        (is-= (dissoc doc :activity) (dissoc final :activity))
        (is (= (count init-others) (count final-others) (dec (count (safe-get doc :activity)))))
        (is-= init-others final-others)
        (is-= final-target (assoc init-target :dwell-ms 2000))))))


(deftest mark-viewed-test
  (let [d (doc-test-utils/test-doc {})
        after (mark-viewed! d (mark-viewed-message 1 3 19 :iphone))]
    (is-= {:iphone [3]}
          (map-vals vec (views-by-client/viewing-users (:views-by-client after))))
    (is (identical? d after))))

(use-fixtures :once validate-schemas)
