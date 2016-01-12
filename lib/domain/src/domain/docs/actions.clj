(ns domain.docs.actions
  (:use plumbing.core)
  (:require
   potemkin
   [schema.core :as s]
   [domain.docs.core :as docs-core])
  (:import
   [domain.docs.core IShare]))

(set! *warn-on-reflection* true)

;; bookmark == like
(def +simple-action-types+ #{:click :bookmark :share :remove :email :save :submit})

(def +comment-action-types+ #{:comment-bookmark :comment-share :comment-email})

(potemkin/definterface+ IAction
  (^long id [this])
  (^long user-id [this])
  (action [this]) ;; (apply s/enum +grabbag-action-types+)
  (^long date [this])
  (client-type [this]))

;; clicks will have dwell-ms assoc'ed on as an extra key for now.
(s/defrecord SimpleAction
    [id :- long
     user-id :- long
     action :- (apply s/enum (concat +simple-action-types+ +comment-action-types+))
     date :- long
     client-type :- (s/maybe docs-core/ActionClientType) ;; nil pre-Nov 2014
     ;; Below fields should be unused, just here to roundtrip to old doc format.
     username :- (s/maybe String)
     image :- (s/maybe String)]
  IAction
  (id [this] id)
  (action [this] action)
  (user-id [this] user-id)
  (date [this] date)
  (client-type [this] client-type)

  IShare
  (distinct-by-key [this] [action user-id]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Converting to and from records and old on-disk format

(defn share-data->Action [share]
  (letk [[[:sharer type [:id :as sharer-id] name username image favicon]
          id action comment date source-set {dwell-ms nil} {client-type nil}] share]
    (assoc-when
     (map->SimpleAction
      {:id id
       :user-id sharer-id
       :action action
       :date date
       :username username
       :image image
       :client-type client-type})
     :dwell-ms dwell-ms)))

(defn Action->old-grabbag-share [^SimpleAction action]
  (letk [[id user-id action date username image {dwell-ms nil} client-type] action]
    {:id id
     :action action
     :sharer {:type :grabbag
              :id user-id
              :name nil
              :username username
              :image image
              :favicon nil}
     :date date
     :comment nil
     :source-set nil
     :client-type client-type
     :dwell-ms dwell-ms}))

(set! *warn-on-reflection* false)
