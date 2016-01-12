(ns domain.interests.manager-test
  (:use clojure.test plumbing.core plumbing.test domain.interests.manager)
  (:require
   [plumbing.index :as index]
   store.bucket
   [domain.interests.type.number :as number]
   [domain.interests.type.bucket :as bucket]
   ))

(deftest type-list-test
  ;; we are checking that these indices are unchanged, because we have saved data that rely on them
  (is-= :topic (+type-list+ 0))
  (is-= [:topic :feed :activity :grabbag-poster :raw :wiki-topic]
        (map +type-list+ (range 6))))

(deftest interest-manager-test
  (let [type->type-manager (assoc (for-map [k (drop 1 +type-list+)]
                                    k (number/->NumberInterestTypeManager k))
                             (first +type-list+) (bucket/bucket-interest-type-manager (store.bucket/bucket {}) 10 10))
        interest-manager (->InterestManager type->type-manager)
        interests (map-indexed (fn [idx type] {:key (if (= 0 idx) (str "ID" idx) idx) :type type})
                               +type-list+)
        indices (mapv (fn [interest] (index! interest-manager interest)) interests)]

    (doseq [[idx interest] (zipmap indices interests)]
      (is-= (assoc interest :id idx)
            (lookup interest-manager idx))
      (is-= (assoc interest :id idx)
            (lookup interest-manager (format "%s:%s" (name (:type interest)) (:key interest)))))))
