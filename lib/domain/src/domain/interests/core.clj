(ns domain.interests.core
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [schema.experimental.abstract-map :as abstract-map]
   [domain.suggestions :as suggestions]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema

;; Interests

(def Interest
  (s/pred (fnk [type key :as interest]
            (and (if (= type :activity)
                   (and (instance? Long key)
                        (string? (safe-get interest :display-key)))
                   (string? key))
                 (every? #(or (string? %) (nil? %)) (vals (select-keys interest [:img :highres-img :title])))
                 (every? keyword? (keys interest))))
          'valid-interest?))

(s/defschema InterestContext
  (abstract-map/abstract-map-schema :type {}))

;; default interest context
(abstract-map/extend-schema
 UnknownContext InterestContext
 [:unknown]
 {})

(s/defschema Position
  "Stores the screen, section, and index within the section of the suggestion.
   All indices are 0-based."
  {:screen Long
   :section Long
   :index Long})

(s/defn ^:always-validate position :- Position
  ([screen-index section-index position-index]
     {:screen screen-index :section section-index :index position-index})
  ([section-index position-index]
     {:screen 0 :section section-index :index position-index}))

(abstract-map/extend-schema
 OnboardingSuggestionsContext InterestContext
 [:onboarding-suggestions]
 {:session-id String
  :auto-follow Boolean
  :group suggestions/SuggestionGroup
  :position Position})

(abstract-map/extend-schema
 DefaultFollowContext InterestContext
 [:default-follow]
 {})

(abstract-map/extend-schema
 EmailContext InterestContext
 [:email]
 {})

(abstract-map/extend-schema
 RichRemoveContext InterestContext
 [:rich-remove]
 {})
