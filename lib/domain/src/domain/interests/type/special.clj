(ns domain.interests.type.special
  (:use plumbing.core)
  (:require
   [plumbing.index :as index]
   [plumbing.logging :as log]
   [domain.interests.manager :as manager]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Interest Type Manager for special interest types that have a small, known, set of possible
;;; keys.

(defrecord IndexedInterestTypeManager [type interest-indexer key->idx]
  manager/PInterestTypeManager
  (type-index! [this interest] (manager/type-key-index this (:key interest)))
  (type-lookup [this index] (if-let [interest (index/item interest-indexer index)]
                              interest
                              (log/throw+ {:message (format "Called type-lookup on %s, but this type manager only accepts indices  up to %s"
                                                            index
                                                            (dec (count key->idx)))
                                           :log-level :error})))
  (type-key-index [this key] (if-let [idx (key->idx key)]
                               idx
                               (log/throw+ {:message (format "Called type-key-index on %s, but this type manager only accepts %s as interest keys"
                                                             key
                                                             (keys key->idx))
                                            :log-level :error}))))

(defn special-interest-type-manager
  "ordered-keys means we index based on the order of the keys, and you should pass them in a
   consistent order of you want consistent indexing"
  [ordered-interests]
  (let [type (doto (->> ordered-interests
                        (map :type)
                        distinct
                        singleton)
               assert)
        indexer (index/static ordered-interests)]
    (->IndexedInterestTypeManager type
                                  indexer
                                  (for-map [k ordered-interests]
                                    (:key k) (index/get-index indexer k)))))

(def +singleton-key+ "-default-")

(def +special-interest-managers+
  (for-map [[t interests] {:personal [{:title "Home"
                                       :des "Stories from your interests"}]
                           :social [{:title "Social"
                                     :des "Stories you friends are talking about"}]
                           :global [{:title "World News"
                                     :des "Stories the world is talking about"}]
                           :grabbag-social [{:title "Social"
                                             :des "Stories you friends are talking about"}]
                           :suggestions (for [k ["all" "twitter" "facebook" "google"]]
                                          {:title "Suggestions"
                                           :key k
                                           :des "Suggestions for you"})}]
    t
    (special-interest-type-manager
     (map #(merge {:type t :key +singleton-key+} %) interests))))
