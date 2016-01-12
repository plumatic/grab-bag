(ns gumby.index
  "Management for Elasticsearch indices"
  (:use plumbing.core)
  (:require
   [clojure.walk :as walk]
   [schema.core :as s]
   [gumby.util :as util])
  (:import
   [java.util Map]
   [org.elasticsearch.action.admin.indices.alias.exists AliasesExistResponse]
   [org.elasticsearch.action.admin.indices.exists.indices IndicesExistsResponse]
   [org.elasticsearch.action.admin.indices.mapping.get GetMappingsResponse]
   [org.elasticsearch.action.support.master AcknowledgedResponse]
   [org.elasticsearch.client Client IndicesAdminClient]
   [org.elasticsearch.cluster.metadata MappingMetaData]
   [org.elasticsearch.common.collect ImmutableOpenMap]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(s/defschema IndexMapping
  "See https://www.elastic.co/guide/en/elasticsearch/guide/current/mapping-intro.html
   See https://www.elastic.co/guide/en/elasticsearch/reference/1.5/mapping.html"
  {:properties {s/Keyword s/Any}
   s/Keyword s/Any})

(s/defschema IndexMappings
  {(s/named String "mapping-type") IndexMapping})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(def do-ack-action! (comp #(.isAcknowledged ^AcknowledgedResponse %) util/do-action!))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(s/defn indices-client :- IndicesAdminClient
  [client :- Client]
  (-> client .admin .indices))

(s/defn exists? :- Boolean
  "Returns true if index by name exists, otherwise false"
  [client :- Client
   index-name :- String]
  (-> (indices-client client)
      (.prepareExists (util/str-array [index-name]))
      ^IndicesExistsResponse (util/do-action!)
      .isExists))

(s/defn create! :- Boolean
  "Creates an index"
  [client :- Client
   index-name :- String
   index-options :- {s/Keyword s/Any}]
  (-> (indices-client client)
      (.prepareCreate index-name)
      (.setSource ^Map (walk/stringify-keys index-options))
      do-ack-action!))

(s/defn refresh!
  "Performs a refresh request, making all operations performed since last (automatic) refresh
   avaialble for search."
  [client :- Client & indices]
  (-> (indices-client client)
      (.prepareRefresh (util/str-array indices))
      util/do-action!))

(s/defn get-mappings
  "Gets mappings for specified index"
  [client :- Client
   index-name :- String]
  (-> (indices-client client)
      (.prepareGetMappings (util/str-array [index-name]))
      ^GetMappingsResponse (util/do-action!)
      .getMappings
      (.get index-name)
      (as-> ^ImmutableOpenMap mappings
            (map-from-keys
             #(util/to-clj (.getSourceAsMap ^MappingMetaData (.get mappings %)))
             (.toArray (.keys mappings))))))

(s/defn put-mapping! :- Boolean
  "Updates mapping of existing index"
  [client :- Client
   index-name :- String
   mapping-type :- String
   mapping :- IndexMapping]
  (-> (indices-client client)
      (.preparePutMapping (util/str-array [index-name]))
      (.setType mapping-type)
      (.setSource ^Map (walk/stringify-keys mapping))
      do-ack-action!))

(s/defn set-mappings!
  "Sets the mappings for an index. Creates index if it doesn't exist and returns :created,
   otherwise updates index and returns :updated."
  [client :- Client
   index-name :- String
   mappings :- IndexMappings]
  (if-not (exists? client index-name)
    (do (create! client index-name {:mappings mappings})
        :created)
    (do (doseq [[mapping-type mapping] mappings]
          (put-mapping! client index-name mapping-type mapping))
        :updated)))

(s/defn alias-exists? :- s/Bool
  "Checks whether an alias exists"
  [client :- Client alias :- String]
  (-> (indices-client client)
      (.prepareAliasesExist (util/str-array [alias]))
      ^AliasesExistResponse (util/do-action!)
      .exists))

(s/defn add-alias! :- s/Bool
  "Adds an alias for one or more indices"
  [client :- Client
   alias :- String
   & indices]
  (-> (indices-client client)
      .prepareAliases
      (.addAlias (util/str-array indices) alias)
      do-ack-action!))

(s/defn put-template! :- s/Bool
  "Puts an index template. Returns true if acknowledged, otherwise false."
  [client :- Client template-name :- String template]
  (-> (indices-client client)
      (.preparePutTemplate template-name)
      (.setSource ^Map (walk/stringify-keys template))
      do-ack-action!))

(set! *warn-on-reflection* false)
