(ns domain.events.notifications
  "Definition of notification events for analytics"
  (:use plumbing.core)
  (:require
   [schema.core :as s]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Notification Schemas

(s/defschema ServiceType
  "Notification service types"
  (s/enum "email" "push"))

(def NotificationType
  "Represents the type of notification (e.g. social digest, comment, etc.)"
  String)

(s/defschema ToggleSubscriptionEvent
  {:type (s/eq "subscription-toggle")
   :category ServiceType
   :notification-type String
   :subscribe Boolean})

(s/defschema UnregisterPushTokenEvent
  {:type (s/eq "push-token-unregister")
   :category (s/enum "ios") ;; for now
   :token-id String
   :user-id Long
   :date Long})

(s/defschema NotificationSentEvent
  {:type (s/eq "notification-sent")
   :category NotificationType
   :service ServiceType
   :notification-id String
   :notification-campaign (s/named String "A/B testing campaign")
   :user-id Long
   :date Long})

(s/defschema NotificationClickEvent
  {:type (s/eq "notification-click")
   :category NotificationType
   :link-type String
   :service ServiceType
   :notification-id String})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constructors

(s/defn ^:always-validate toggle-subscription :- ToggleSubscriptionEvent
  [service-type notification-type subscribe?]
  {:type "subscription-toggle"
   :category service-type
   :notification-type notification-type
   :subscribe subscribe?})

(s/defn ^:always-validate unregister-push-token :- UnregisterPushTokenEvent
  [service-type push-token user-id]
  {:type "push-token-unregister"
   :category service-type
   :token-id push-token
   :user-id user-id
   :date (millis)})

(s/defn ^:always-validate notification-sent :- NotificationSentEvent
  [service-type notification-type notification-id user-id notification-campaign]
  {:type "notification-sent"
   :service service-type
   :category notification-type
   :notification-id notification-id
   :notification-campaign notification-campaign
   :user-id user-id
   :date (millis)})

(s/defn ^:always-validate notification-click :- NotificationClickEvent
  [service-type notification-type link-type notification-id]
  {:type "notification-click"
   :category notification-type
   :link-type link-type
   :service service-type
   :notification-id notification-id})
