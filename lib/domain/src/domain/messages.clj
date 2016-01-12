(ns domain.messages
  "Definitions for messages involving docs and shares that are passed between
   services."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [schema.utils :as schema-utils]
   [plumbing.logging :as log]
   [domain.docs :as docs]
   [domain.docs.core :as docs-core]
   [domain.docs.actions :as actions]
   [domain.docs.comments :as comments]
   domain.docs.external-shares
   [domain.docs.fitness-stats :as fitness-stats]
   [domain.docs.tags :as tags]
   [domain.docs.views-by-client :as views-by-client])
  (:import
   [domain.docs Doc]
   [domain.docs.actions SimpleAction]
   [domain.docs.comments Comment]
   [domain.docs.external_shares ExternalShares]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; helpers for doc-hints

(s/defschema DocHints (s/maybe {s/Any s/Any}))

(defn write-doc-hints [doc-hints]
  (update-in-when doc-hints [:fitness-stats] fitness-stats/write-fitness-stats))

(defn read-doc-hints [doc-hints]
  (update-in-when doc-hints [:fitness-stats] fitness-stats/read-fitness-stats))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; new-shares messages: doc-poller --> doc-poller (internal, plus persisted in URL cache)

(s/defschema NewSharesMessage
  {:action (s/eq :new-shares)
   :url String  ;; possibly resolved.
   :doc-hints DocHints
   :external-shares ExternalShares})

(s/defn ^:always-validate new-shares-message :- NewSharesMessage
  [possibly-resolved-url doc-hints external-shares]
  {:action :new-shares
   :url possibly-resolved-url ;; used in both ways in different places.
   :doc-hints doc-hints
   :external-shares external-shares})

(s/defn ^:always-validate write-new-shares-message [message :- NewSharesMessage]
  (-> message
      (update-in [:doc-hints] write-doc-hints)
      (dissoc :external-shares)
      (assoc :shares (docs/downgrade-and-write-shares (safe-get message :external-shares) nil))))

(s/defn read-new-shares-message :- NewSharesMessage [message]
  (letk [[action url doc-hints shares] message
         [external-shares activity] (docs/read-and-upgrade-shares shares)]
    (assert (= action :new-shares))
    (assert (empty? activity))
    (new-shares-message url (read-doc-hints doc-hints) external-shares)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; fetch-doc-message: doc-poller --> doc-fetcher

(s/defschema FetchDocMessage
  {:action (s/eq :fetch-doc) :id long :url String :doc-hints DocHints})

(s/defn ^:always-validate fetch-doc-message :- FetchDocMessage
  [doc-id url doc-hints]
  {:action :fetch-doc :id doc-id :url url :doc-hints doc-hints})

(s/defn ^:always-validate write-fetch-doc-message [message :- FetchDocMessage]
  (update-in message [:doc-hints] write-doc-hints))

(s/defn read-fetch-doc-message :- FetchDocMessage [message]
  (update-in message [:doc-hints] read-doc-hints))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; doc-related messages used in index-master and api

;; doc-poller --> index-master --> api
(defn add-external-shares-message [doc-id doc-hints external-shares]
  {:action :add-external-shares :id doc-id :doc-hints doc-hints
   :external-shares external-shares})

;; doc-fetcher --> index-master --> api
(defn add-doc-message [doc]
  {:action :add-doc :id (safe-get doc :id) :doc doc})

;; index-master --> search-index
(defn add-full-doc-message
  "Similar to add-doc-message, but includes fetched-page HTML data (from url-cache bucket)"
  [doc fetched-page]
  {:action :add-full-doc
   :id (safe-get doc :id)
   :doc doc
   :fetched-page fetched-page})

;; index-master --> api only
(defn delete-doc-message [doc-id]
  {:action :delete-doc :id doc-id})

(defn re-cluster-message [doc-id new-cluster-id]
  {:action :re-cluster :id doc-id :new-cluster-id new-cluster-id})

(s/defschema DocUpdate
  (->> Doc
       ^schema.core.Record (schema-utils/class-schema)
       .schema
       (map-keys (fn [k]
                   (assert (s/specific-key? k))
                   (s/optional-key (s/explicit-schema-key k))))))

(s/defn ^:always-validate merge-doc-message
  "Nuclear option, not meant to be used in normal system operation.
   Can make arbitrary changes to docs: semantics is to replace entire
   provided top-level keys with new values, leaving other top-level
   keys alone."
  [doc-id :- long doc-update :- DocUpdate]
  (when-let [id (:id doc-update)]
    (assert (= id doc-id)))
  {:action :merge-doc :id doc-id :doc-update doc-update})



;;; api --> index-master --> api

;; NOTE: in-memory format different than wire, now grabbag only
(s/defn add-activity-message [doc-id activity :- [SimpleAction]]
  {:action :add-activity :id doc-id :activity activity})

(s/defn ^:always-validate update-dwell-message
  [doc-id :- Long user-id :- Long date :- Long client-type :- docs-core/ActionClientType dwell-ms :- Long]
  {:action :update-dwell :id doc-id :user-id user-id :date date :client-type client-type :dwell-ms dwell-ms})

(defn delete-activity-message [doc-id activity-ids]
  {:action :delete-activity :id doc-id :activity-ids activity-ids})

(defn add-post-message [post]
  {:action :add-post :id (safe-get post :id) :doc post})

(defn add-comment-message [doc-id ^Comment comment]
  {:action :add-comment :id doc-id :comment comment})

(defn add-comment-action-message [doc-id comment-id ^SimpleAction comment-action]
  {:action :add-comment-action :id doc-id :comment-id comment-id :comment-action comment-action})

(defn delete-comment-action-message [doc-id comment-id action-id]
  {:action :delete-comment-action :id doc-id :comment-id comment-id :action-id action-id})

(s/defn ^:always-validate mark-viewed-message
  [doc-id :- long user-id :- long date :- long client-type :- docs-core/ActionClientType]
  {:action :mark-viewed :id doc-id :user-id user-id :date date :client-type client-type})

(defn update-stats-message
  "Old mark-viewed message, can eventually be removed once fitness-stats no longer used for views."
  [doc-id fitness-stats]
  {:action :update-stats :id doc-id :fitness-stats fitness-stats})

(s/defn ^:always-validate add-tag-message
  [doc-id :- long tag :- tags/Tag]
  {:action :add-tag :id doc-id :tag tag})

(s/defn ^:always-validate untag-message
  [doc-id :- long tag-type :- (s/enum :admin :auto) tag-value :- String]
  {:action :untag :id doc-id :tag-type tag-type :tag-value tag-value})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Applying messages to docs (more functionality should probably be pulled out here)

(defn update-dwell
  "Returns updated doc, and warns if a click can't be found to attach"
  [doc message]
  (letk [[action id user-id date client-type dwell-ms] message]
    (assert (= action :update-dwell))
    (let [[head [target & tail]] (split-with (fn [^SimpleAction action]
                                               (not (and (= (.action action) :click)
                                                         (= (.user-id action) user-id))))
                                             (safe-get doc :activity))]
      (if target
        (assoc doc
          :activity (vec (concat head
                                 [(update-in target [:dwell-ms] #(max (or % 0) dwell-ms))]
                                 tail)))
        (do (log/warnf "Missing click applying update-dwell action %s" message)
            doc)))))

(defn mark-viewed!
  "Mutates doc and returns it."
  [doc message]
  (letk [[action user-id date client-type] message]
    (assert (= action :mark-viewed))
    (views-by-client/add-view! (safe-get doc :views-by-client) client-type user-id))
  doc)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Converting messages to and from data/records

(def +known-action-types+
  #{:add-external-shares :add-activity :add-doc :add-full-doc :delete-doc :merge-doc :re-cluster
    :delete-activity :add-post :add-comment :add-comment-action :delete-comment-action :update-stats
    :add-tag :untag :update-dwell :mark-viewed})

(defn write-message [message]
  (assert (long (:id message)))
  (assert (+known-action-types+ (:action message))
          (format "Unknown action type : %s" (:action message)))
  (-> message
      (?> (-> message :doc-hints :fitness-stats)
          (update-in [:doc-hints :fitness-stats] fitness-stats/write-fitness-stats))
      (?> (-> message :fitness-stats)
          (update-in [:fitness-stats] fitness-stats/write-fitness-stats))
      (?> (:doc message)
          (update-in [:doc] docs/write-doc))
      (?> (:comment message)
          (update-in [:comment]
                     (fn [c]
                       (s/validate domain.docs.comments.Comment c)
                       (comments/write-comment c))))
      (?> (:comment-action message)
          (update-in [:comment-action]
                     (fn [ca]
                       (s/validate domain.docs.actions.SimpleAction ca)
                       (actions/Action->old-grabbag-share ca))))
      (?> (= :add-activity (:action message))
          (update-in [:activity] (partial map actions/Action->old-grabbag-share)))
      (?> (= :add-external-shares (:action message))
          (update-in [:external-shares] #(docs/downgrade-and-write-shares % [])))))

(defn read-message [message]
  (assert (+known-action-types+ (:action message)) message)
  (-> message
      (?> (-> message :doc-hints :fitness-stats)
          (update-in [:doc-hints :fitness-stats] fitness-stats/read-fitness-stats))
      (?> (-> message :fitness-stats)
          (update-in [:fitness-stats] fitness-stats/read-fitness-stats))
      (?> (:doc message)
          (update-in [:doc] docs/read-doc))
      (?> (:comment message)
          (update-in [:comment] comments/read-comment))
      (?> (:comment-action message)
          (update-in [:comment-action] actions/share-data->Action))
      (?> (= :add-external-shares (:action message))
          (update-in [:external-shares] #(safe-get (docs/read-and-upgrade-shares %) :external-shares)))
      (?> (= :add-activity (:action message))
          (update-in [:activity] (partial map actions/share-data->Action)))))

(set! *warn-on-reflection* false)
