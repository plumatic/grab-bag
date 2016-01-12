(ns domain.interests.manager
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.error :as err]
   [plumbing.index :as index]
   [plumbing.logging :as log]
   [domain.interests.core :as interests]
   [domain.interests.indexer :as indexer]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private fields and schema

;;; Order here should not change!
(def +type-list+
  [:topic
   :feed
   :activity
   :grabbag-poster
   :raw
   :wiki-topic
   :personal
   :global
   ;; this is the current line where we're only saving off stuff from above here
   :tag
   :facebook-sharer
   :twitter-sharer
   :suggestions
   :grabbag-social
   :related
   :social
   :debug
   :ab
   ])

(def ^:private +type-index+ (index/static +type-list+))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Protocol for interest types (for use by InterestManager)

(defprotocol PInterestTypeManager
  (type-index! [this interest] "Return an index; interest typically has a :type but no :id")
  (type-lookup [this index] "Inverse of index!, only defined on return values from past calls.")
  (type-key-index [this key] "Returns an index"))


(defprotocol PInterestManager
  (index! [manager interest] "Return an interest id")
  (lookup [manager ^indexer/Id id] "id can be long or type:key string, result will have :id, :type, :key at least."))

(defn interest-id [type key-index]
  (assert type (str "Type is nil for: " [type key-index]))
  (assert key-index (str "Key-index is nil for: " [type key-index]))
  (indexer/index-of +type-index+ type key-index))

(defn deprecated-lookup [interest-manager type key]
  (assert type (str "Type is nil for: " [type key]))
  (assert key (str "Key is nil for: " [type key]))
  (lookup interest-manager (format "%s:%s" (name type) key)))

(defn deprecated-interest-id [interest-manager type key]
  (-> (deprecated-lookup interest-manager type key)
      (safe-get :id)))

(defn id-type [interest-id]
  (indexer/type-of +type-index+ interest-id))

(defn id-key-id ^long [^long interest-id]
  (indexer/key-of interest-id))

(defn split-id [interest-id]
  ((juxt id-type id-key-id) interest-id))

(defnk sanitize-interest [type :as interest]
  (-> interest
      (update-in [:type] keyword)
      (select-keys [:id :type :key :title :display-key :slug :name :img :highres-img :des])))

(defn sanitized-interest? [interest]
  (= interest (sanitize-interest interest)))

(potemkin/defrecord+ InterestManager [type->type-manager]
  PInterestManager
  (index! [this interest]
    (let [interest (sanitize-interest interest)]
      (letk [[type] interest]
        (indexer/index-of +type-index+ type (type-index! (safe-get type->type-manager type) interest)))))

  (lookup [this id]
    (sanitize-interest
     (cond
      (number? id) (let [[type key] (indexer/value-of +type-index+ id)
                         interest (type-lookup (safe-get type->type-manager type) key)]
                     (if interest
                       (assoc interest :id id)
                       (throw (RuntimeException. (format "No interest found for: %s" [type key])))))

      (string? id) (letk [[type key] (indexer/extract-type-key id)]
                     (let [type-manager (safe-get type->type-manager type)
                           index (type-key-index type-manager key)
                           interest (when index (type-lookup type-manager index))]
                       (if interest
                         (assoc interest :id (interest-id type index))
                         (throw (RuntimeException. (format "No interest found for: %s" [type key]))))))))))

(defn lookup-by-key-index [^InterestManager interest-manager type key-index]
  (lookup interest-manager (interest-id (keyword type) key-index)))

(defn user-id->maybe-follow-interest [interest-manager user-id]
  (when-let [interest (-> interest-manager
                          (safe-get-in [:type->type-manager :activity])
                          (type-lookup user-id))]
    (assoc interest :id (interest-id :activity user-id))))

(defn canonicalize [interest-manager interest]
  (log/with-elaborated-exception {:interest-to-canonicalize interest}
    (if-let [id (:id interest)]
      (lookup interest-manager id)
      (->> interest
           (index! interest-manager)
           (lookup interest-manager)))))

(defn keep-canonical [interest-manager interests]
  (keep (fn [interest]
          (err/?debug (str "Missing Interest Key " interest)
                      (canonicalize interest-manager interest)))
        interests))

(defn keep-ids [interest-manager interests]
  (->> interests
       (keep-canonical interest-manager)
       (map #(safe-get % :id))))
