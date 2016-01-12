(ns domain.push-notification-messages
  "Definition of messages pertaining to push notifications."
  (:use plumbing.core)
  (:require
   [schema.experimental.abstract-map :as abstract-map]
   [schema.core :as s]))

(s/defschema MessagePayload
  (abstract-map/abstract-map-schema
   :type
   {:message String
    ;; optional for backwards compatibility
    (s/optional-key :notification-id) String}))

(abstract-map/extend-schema CommentPayload MessagePayload [:comment] {:doc-id long :comment-id long})
(abstract-map/extend-schema SocialDigest MessagePayload [:social-digest] {})

(s/defschema AlertMessage
  {:type (s/eq :alert)
   :user-id Long
   :payload MessagePayload})

(s/defn ^:always-validate alert-message :- AlertMessage
  "A push notification message indicating an alert"
  [user-id payload]
  {:type :alert :user-id user-id :payload payload})
