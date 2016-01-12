(ns gumby.core
  (:refer-clojure :exclude [get])
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.error :as err]
   [plumbing.graph :as graph]
   [plumbing.json :as json]
   [store.bucket :as bucket]
   [gumby.util :as util])
  (:import
   [org.elasticsearch.action.bulk BulkItemResponse BulkResponse]
   [org.elasticsearch.action.count CountResponse]
   [org.elasticsearch.action.delete DeleteResponse]
   [org.elasticsearch.action.get GetResponse
    MultiGetItemResponse
    MultiGetResponse]
   [org.elasticsearch.action.index IndexRequestBuilder
    IndexResponse]
   [org.elasticsearch.action.search SearchRequestBuilder
    SearchResponse]
   [org.elasticsearch.action.suggest SuggestResponse]
   [org.elasticsearch.client Client]
   [org.elasticsearch.client.transport TransportClient]
   [org.elasticsearch.common.settings ImmutableSettings
    ImmutableSettings$Builder
    Settings]
   [org.elasticsearch.common.transport InetSocketTransportAddress]
   [org.elasticsearch.common.unit TimeValue]
   [org.elasticsearch.index.query FilterBuilder FilterBuilders
    QueryBuilder QueryBuilders]
   [org.elasticsearch.node Node NodeBuilder]
   [org.elasticsearch.search SearchHit SearchHitField]
   [org.elasticsearch.search.sort SortOrder]
   [org.elasticsearch.search.suggest SuggestBuilders]
   [org.elasticsearch.search.suggest.completion CompletionSuggestion$Entry$Option]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas & Constants

(s/defschema ClientHosts
  [(s/pair #"^[\d\.]+$" "ip-address" Long "port")])

(s/defschema QuerySpec
  "An EDN version of Elasticsearch Query DSL (JSON).
   Strict schematization would be difficult.

   See: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html"
  (s/either
   QueryBuilder
   {(s/named s/Keyword "query-tag") (s/named s/Any "query-params")}))

(s/defschema FilterSpec
  (s/either
   FilterBuilder
   {(s/named s/Keyword "filter-tag") (s/named s/Any "filter-params")}))

(s/defschema SearchType
  (s/enum :dfs-query-then-fetch :count :scan :query-and-fetch :dfs-query-and-fetch))

(def +scroll-timeout-ms+
  "The default timeout for scan-and-scroll cursors to stay open."
  (* 5 60 1000))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(s/defn search-hit-id [hit :- SearchHit] (.getId hit))

(s/defn search-hit-source :- {s/Keyword s/Any}
  "Returns a hit's source data, including any additional fields that were requested."
  [hit :- SearchHit]
  (merge
   (util/to-clj (.getSource hit))
   (for-map [[k ^SearchHitField field] (.getFields hit)]
     (keyword k) (util/to-clj (.getValue field)))))

(s/defn index-request :- IndexRequestBuilder
  "Builds an index request.
   Options:
     :create? Set true to error if document already exists
     :refresh? Set true to force document to be searchable after returning
     :version Sets the expected version to be updating. Will error if actual version differs.
     :ttl Set the TTL of document (in milliseconds)"
  [client :- Client index mapping-type id data {:keys [create? refresh? version ttl]}]
  (let [req (-> client
                (.prepareIndex index mapping-type (str id))
                (.setSource (json/generate-string data)))]
    (when create? (.setCreate req true))
    (when refresh? (.setRefresh req true))
    (when version (.setVersion req version))
    (when ttl (.setTTL req ttl))
    req))

(s/defn index-response
  "Returns map of data from IndexResponse"
  [^IndexResponse resp]
  {:id (.getId resp)
   :index (.getIndex resp)
   :type (.getType resp)
   :version (.getVersion resp)
   :created? (.isCreated resp)})

(s/defn perform-get :- GetResponse
  "Executes a Get request for document in index by id"
  [client :- Client index :- String mapping-type :- String id]
  (-> client (.prepareGet index mapping-type (str id)) .get))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public - Nodes & Clients

(s/defn build-settings :- Settings
  "Converts a Clojure map into ImmutableSettings object"
  [m]
  (if (seq m)
    (let [^ImmutableSettings$Builder sb (ImmutableSettings/settingsBuilder)]
      (doseq [[k v] m] (.put sb (name k) (str v)))
      (.build sb))
    ImmutableSettings$Builder/EMPTY_SETTINGS))

(s/defn build-node :- Node
  [settings :- {s/Keyword (s/either String Boolean Long)}]
  (-> (NodeBuilder/nodeBuilder) (.settings (build-settings settings)) .build))

(s/defn transport-client :- Client
  [host-port-pairs settings]
  (let [client (TransportClient. (build-settings settings))]
    (doseq [[^String host ^long port] host-port-pairs]
      (.addTransportAddress client (InetSocketTransportAddress. host port)))
    client))

(defnk memory-node-settings
  "Settings to create an in-memory node (for integration tests)"
  [cluster-name node-name]
  {:cluster.name cluster-name
   :node.name node-name
   :node.local true
   :node.client false
   :discovery.zen.ping.multicast.enabled false
   :discovery.zen.minimum_master_nodes 1
   :index.store.type "memory"
   :index.store.fs.memory.enabled true
   :index.gateway.type "none"
   :gateway.type "none"
   :path.data "target/elasticsearch/data"
   :path.logs "target/elasticsearch/logs"
   :index.number_of_shards 1
   :index.number_of_replicas 1})

(defnk ec2-node-settings
  "Settings to create a Node client to connect to cluster running in EC2"
  [cluster-name node-name ec2-tags]
  (merge
   {:cluster.name cluster-name
    :node.name node-name
    :node.client true
    :node.data false
    :discovery.type "ec2"}
   (map-keys #(keyword (str "discovery.ec2.tag." (name %))) ec2-tags)))

(s/defn start-node :- Node
  [node :- Node]
  (doto node (.start)))

(s/defn node-client :- Client
  [node :- Node]
  (.client node))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public - Requests

(s/defn get
  "Returns value of mapping-type stored in index under id if found, otherwise false"
  [client :- Client index :- String mapping-type :- String id]
  (util/to-clj (.getSource (perform-get client index mapping-type id))))

(s/defn exists? :- s/Bool
  "Returns true if specific document of specified type exists in index, otherwise false"
  [client :- Client index :- String mapping-type :- String id]
  (.isExists (perform-get client index mapping-type id)))

(s/defn index!
  "Indexes (upsert) a document. Builds an IndexRequest and executes it, returning response data"
  ([client index mapping-type id data]
     (index! client index mapping-type id data nil))
  ([client :- Client index mapping-type id data opts]
     (-> (index-request client index mapping-type id data opts) .get index-response)))

(s/defn delete! :- s/Bool
  "Deletes a document in index. Returns true if found (and deletd), otherwise false."
  [client :- Client index :- String mapping-type :- String id]
  (-> client
      (.prepareDelete index mapping-type (str id))
      ^DeleteResponse (.get)
      .isFound))

(s/defn document-count :- Long
  "Returns number of searchable documents of type in given indices.
   Note: documents may not be immediately searchable after indexing."
  [client :- Client indices mapping-types]
  (-> client
      (.prepareCount (util/str-array indices))
      (.setTypes (util/str-array mapping-types))
      ^CountResponse (.get)
      .getCount))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public - Search

(s/defn build-query :- QueryBuilder
  "Builds an Elasticsearch query using the JSON DSL.
   See: https://www.elastic.co/guide/en/elasticsearch/reference/current/_introducing_the_query_language.html
   See: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html"
  [spec :- QuerySpec]
  (cond
   (instance? QueryBuilder spec) spec
   (map? spec) (QueryBuilders/wrapperQuery (json/generate-string spec))
   :else (throw (ex-info "Invalid query spec" {:query spec}))))

(s/defn build-filter :- FilterBuilder
  "Builds an Elasticsearch filter using the JSON DSL."
  [spec :- FilterSpec]
  (cond
   (instance? FilterBuilder spec) spec
   (map? spec) (FilterBuilders/wrapperFilter (json/generate-string spec))
   :else (throw (ex-info "Invalid filter spec" {:filter spec}))))

(defnk build-search-request :- SearchRequestBuilder
  "Builds a request to perform search
   See: http://javadoc.kyubu.de/elasticsearch/v1.5.2/org/elasticsearch/action/search/SearchRequestBuilder.html"
  [client :- Client
   {indices :- [String] []} {types :- [String] nil}
   {fields :- [s/Keyword] nil}
   {includes :- [s/Keyword] []} {excludes :- [s/Keyword] []}
   {search-type :- SearchType nil} {scroll-timeout-ms :- long nil}
   {query :- QuerySpec nil}
   {post-filter :- FilterSpec nil}
   {from :- long nil} {size :- long nil}
   {sort :- [(s/pair String "field" (s/enum :asc :desc) "order")] nil}
   {timeout-ms :- long nil}
   {explain? :- Boolean nil} {fetch-source? :- Boolean nil}]
  (let [^SearchRequestBuilder srb (.prepareSearch client (util/str-array indices))]
    (when types (.setTypes srb (util/str-array types)))
    (when fields (.addFields srb (util/str-array fields)))
    (.setFetchSource srb (util/str-array includes) (util/str-array excludes))
    (when search-type (.setSearchType srb (name search-type)))
    (when scroll-timeout-ms (.setScroll srb (TimeValue. scroll-timeout-ms)))
    (when query (.setQuery srb (build-query query)))
    (when post-filter (.setPostFilter srb (build-filter post-filter)))
    (when from (.setFrom srb (int from)))
    (when size (.setSize srb (int size)))
    (doseq [[field order] sort]
      (.addSort srb field (case order :asc SortOrder/ASC :desc SortOrder/DESC)))
    (when timeout-ms (.setTimeout srb (TimeValue. timeout-ms)))
    (when explain? (.setExplain srb true))
    (when fetch-source? (.setFetchSource srb fetch-source?))
    srb))

(def search
  "Convenience for building search request and get response.
   See (doc build-search-request)"
  (comp util/do-action! build-search-request))

(s/defn search-hits
  "Returns lazy seq of search hits converted to Clojure data"
  [search-resp :- SearchResponse]
  (->> search-resp .getHits .getHits (map search-hit-source)))

(s/defn search-hit-total :- long
  "Returns total number of hits that maches the search request"
  [search-resp :- SearchResponse]
  (-> search-resp .getHits .getTotalHits))

(s/defn search-ms :- long
  "Returns how long the seach took in milliseconds"
  [search-resp :- SearchResponse]
  (.getTookInMillis search-resp))

(s/defn search-timed-out? :- s/Bool
  [search-resp :- SearchResponse]
  (.isTimedOut search-resp))

(s/defn scroll-id :- (s/maybe String)
  "Returns the scroll-id of a scrolled search. If search's type isn't :scan, returns nil."
  [search-response :- SearchResponse]
  (.getScrollId search-response))

(s/defn scroll :- SearchResponse
  "Fetches next batch of results from a scan-and-scroll (pagination) request."
  [client :- Client
   scroll-timeout-ms :- long
   scroll-resp :- SearchResponse]
  (-> client
      (.prepareSearchScroll (scroll-id scroll-resp))
      (.setScroll (TimeValue. scroll-timeout-ms))
      .execute .actionGet))

(s/defn scroll-complete? :- Boolean
  "Returns true if response from scan-and-scroll query is the last page, otherwise false.
   Note: Always pass SearchResponse returned from `scroll`. Returns true for un-scrolled search."
  [scroll-resp :- SearchResponse]
  (zero? (-> scroll-resp .getHits .getHits alength)))

(s/defn scroll-seq
  "Returns a lazy seq of SearchHit from a scan-and-scroll search.
   Immediately issues request for next page of hits; lazily requests next page.
   Must consume and request next page within given scroll-timeout-ms, otherwise ES will evict scroll-id"
  [client :- Client scroll-timeout-ms scan-resp :- SearchResponse]
  {:pre [(scroll-id scan-resp)]}
  (let [^SearchResponse resp (scroll client scroll-timeout-ms scan-resp)]
    (lazy-cat
     (-> resp .getHits .getHits)
     (when-not (scroll-complete? resp)
       (lazy-seq (scroll-seq client scroll-timeout-ms resp))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/defschema CompletionResult
  "Result from performing suggestion with Completion Suggester"
  {:text String
   :score int
   :payload {s/Keyword s/Any}})

(defnk suggest-completion :- [CompletionResult]
  "Performs a suggestion request for Completion Suggester given some text.
   See: https://www.elastic.co/guide/en/elasticsearch/reference/1.6/search-suggesters-completion.html"
  [client :- Client
   indices :- [String]
   field :- String
   text :- String
   {size :- (s/maybe int) nil}]
  (let [sugg-name "suggest"
        sugg (doto (SuggestBuilders/completionSuggestion sugg-name)
               (.field field)
               (.text text))]
    (when size (.size sugg size))
    (-> client
        (.prepareSuggest (util/str-array indices))
        (.addSuggestion sugg)
        ^SuggestResponse (util/do-action!)
        .getSuggest
        (.getSuggestion sugg-name)
        .getEntries
        ^CompletionSuggestion$Entry (pci/safe-singleton)
        (->> (map (fn [^CompletionSuggestion$Entry$Option opt]
                    {:text (str (.getText opt))
                     :score (.getScore opt)
                     :payload (util/to-clj (.getPayloadAsMap opt))}))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public - Bucket

(defprotocol IElasticsearchBucket
  (specify-index [this ^String index]
    "Returns new bucket with same arguments as this but only uses specified index, i.e. clears any index-fn option"))

(defmethod bucket/bucket :elasticsearch
  #_ "Returns a bucket for Elasticsearch documents of mapping-type in an index.
An index is analagous database in the sense of relation databases,
and a mapping-type is analagous to a table.

Bucket interface is for CRUD operations on documents-by-id; all other access
to documents (search queries) are not within scope.

Supports dynamic indexing strategies, such as including a time component in index name
(e.g. 'index-2015-07', 'index-2015-08', and so on) by passing optional `index-fn` function,
a single-argument function that's passed a bucket key and returns name of index to
read/write the bucket value.

NOTE when using `index-fn`, you are still required to pass the `index` option for
multi-index operations, such as bucket/keys, bucket/vals, or bucket/count.
If using multiple indices, `index` should be an Index Alias that references each
individual index.

NOTE lazy seqs from bucket can expire:
Bucket methods that return lazy seqs (keys, vals, seq) use scan-and-scroll search,
which s similar to cursor in traditional db. The results expire after a certain amount
of time of no activity. By default, this timeout is 5 minutes.

Currently assumes index specified at instantiation has been created. May throw
IndexMissingException if index hasn't been created.

bucket/update is not thread-safe when updating the same document. Will retry upto 3
times, so f should be free of side-effects."
  [opts]
  (letk [[client :- Client index :- String mapping-type :- String {index-fn (constantly index)}] opts
         scan (fn []
                (->> (build-search-request
                      {:client client
                       :indices [index]
                       :types [mapping-type]
                       :query {:match_all {} :fields []}
                       :search-type :scan
                       :scroll-timeout-ms +scroll-timeout-ms+})
                     .get
                     (scroll-seq client +scroll-timeout-ms+)))]
    (reify
      IElasticsearchBucket
      (specify-index [this index]
        (-> opts (assoc :index index) (dissoc :index-fn) bucket/bucket))
      bucket/IReadBucket
      (get [this k]
        (get client (index-fn k) mapping-type k))
      (batch-get [this ks]
        (let [hits
              (->> ks
                   (group-by index-fn)
                   (map (fn [[^String idx idx-ks]]
                          (-> client
                              .prepareMultiGet
                              (.add idx mapping-type (util/str-array (map str idx-ks)))
                              ^MultiGetResponse (.get)
                              .getResponses
                              (->> (into {} (map (fn [^MultiGetItemResponse item]
                                                   (when-not (.isFailed item)
                                                     [(.getId item) (-> item .getResponse .getSource util/to-clj)]))))))))
                   (apply merge))]
          (map (fn [k] [k (hits (str k))]) ks)))
      (exists? [this k]
        (exists? client (index-fn k) mapping-type k))
      (keys [this]
        (map search-hit-id (scan)))
      (vals [this]
        (map search-hit-source (scan)))
      (seq [this]
        (map (juxt search-hit-id search-hit-source) (scan)))
      (count [this]
        (document-count client [index] [mapping-type]))

      bucket/IWriteBucket
      (put [this k v]
        (index! client (index-fn k) mapping-type k v)
        v)
      (batch-put [this kvs]
        (let [bulk-req (.prepareBulk client)]
          (doseq [[k v] kvs]
            (.add bulk-req (index-request client (index-fn k) mapping-type k v nil)))
          (let [^BulkResponse resp (.get bulk-req)]
            (when (.hasFailures resp)
              (throw (ex-info "Failures performing batch-put"
                              {:failures (filter #(.isFailed ^BulkItemResponse %)
                                                 (.getItems resp))}))))))
      (delete [this k]
        (delete! client (index-fn k) mapping-type k))
      (update [this k f]
        (let [idx (index-fn k)]
          (err/with-retries 3 1
            #(throw (ex-info "Failed to update after 3 retries" {:index idx :type mapping-type :key k} %))
            (let [get-resp (perform-get client idx mapping-type k)]
              (if (.isExists get-resp)
                (let [v (f (util/to-clj (.getSource get-resp)))]
                  (index! client idx mapping-type k v {:version (.getVersion get-resp)})
                  v)
                (bucket/put this k (f nil)))))))
      (sync [this])
      (close [this]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public - Resources

(def elasticsearch-bundle
  "Elasticsearch resources. Node object is included in graph because it's closable.
   Starts & connects to a single node cluster in test environment.

   There are 3 possible modes of use:
   -   Client to cluster running in EC2. This runs a \"Node\" client.
       See: https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.5/client.html
   -   Client to cluster running locally. This starts an in-memory cluster then connects to it.
   -   No client. This does nothing and *client is nil*. Useful for (non-integration) testing
       where Elasticsearch access can be mocked by memory bucket.

   Inputs:
   :ec2-tags      Map of tag name to value to discover cluster nodes
   :cluster-name  Name of cluster to connect to

   Options:
   :local?  If true, starts a single in-memory cluster that client will connect to."
  (graph/graph
   :node (fnk [env ec2-tags cluster-name [:instance service-name] {local? false}]
           (some-> (cond
                    local?
                    (memory-node-settings
                     {:cluster-name cluster-name
                      :node-name service-name})
                    (seq ec2-tags)
                    (ec2-node-settings
                     {:cluster-name cluster-name
                      :node-name service-name
                      :ec2-tags ec2-tags}))
                   build-node
                   start-node))

   :client (fnk [node] (when node (node-client node)))))

(set! *warn-on-reflection* false)
