(ns domain.docs.views-by-client
  "Mutable type to hold viewing users by client for a doc."
  (:use plumbing.core)
  (:require
   potemkin
   [plumbing.map :as map]
   [schema.core :as s]
   [domain.docs.core :as docs-core])
  (:import
   [java.util HashMap]
   [gnu.trove TIntHashSet]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: Constructor and methods

(s/defschema ViewsByClient
  (s/named HashMap "{docs-core/ActionClientType TIntHashSet}"))

(defn views-by-client []
  (java.util.HashMap. 5))

(s/defn add-view!
  [views-by-client
   client :- docs-core/ActionClientType
   user-id :- long]
  (locking views-by-client
    (let [^TIntHashSet s (map/get! views-by-client client (TIntHashSet. 1))]
      (.add s (int user-id)))))

(s/defn viewing-users :- {docs-core/ActionClientType longs}
  [views-by-client]
  (locking views-by-client
    (map-vals #(long-array (.toArray ^TIntHashSet %)) views-by-client)))

(s/defn viewed? :- Boolean
  [views-by-client client user-id]
  (locking views-by-client
    (boolean
     (when-let [^TIntHashSet hs (get views-by-client client)]
       (.contains hs (int user-id))))))

(s/defn view-count :- long
  [views-by-client client]
  (locking views-by-client
    (long
     (if-let [^TIntHashSet hs (get views-by-client client)]
       (.size hs)
       0))))

(s/defn view-counts :- {docs-core/ActionClientType long}
  [views-by-client]
  (locking views-by-client
    (map-vals #(long (.size ^TIntHashSet %)) views-by-client)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: Serialization

(s/defschema ViewsByClientData
  "serialization-friendly format"
  {docs-core/ActionClientType ints})

(s/defn ^:always-validate read-views-by-client :- ViewsByClient
  [data :- ViewsByClientData]
  (HashMap. ^java.util.Map (map-vals #(TIntHashSet. ^ints %) data)))

(s/defn ^:always-validate write-views-by-client :- ViewsByClientData
  [views-by-client :- ViewsByClient]
  (locking views-by-client
    (map-vals #(locking % (.toArray ^TIntHashSet %)) views-by-client)))

(s/defn clone :- ViewsByClient
  [views-by-client :- ViewsByClient]
  (-> views-by-client write-views-by-client read-views-by-client))

(set! *warn-on-reflection* false)
