(ns domain.docs.fetched-page
  (:use plumbing.core)
  (:require
   [schema.core :as s]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas and Constructors

(s/defschema FetchedPage
  {:url (s/named String "canonical first casing of url that is observed")
   :resolved (s/named String "resolved version of url that comes from response")
   :fetch-date Long
   :html String
   s/Keyword s/Any})

(s/defn ^:always-validate fetched-page :- FetchedPage
  [url resolved fetch-date html]
  {:url url
   :resolved resolved
   :fetch-date fetch-date
   :html html})
