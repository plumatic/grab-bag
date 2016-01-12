(ns model-explorer.isis.db
  "Both a generic-ish sql-based training data store, and implementations for
   twitter users and tweets."
  (:use plumbing.core)
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.graph :as graph]
   [plumbing.logging :as log]
   [plumbing.parallel :as parallel]
   [plumbing.serialize :as serialize]
   [store.bucket :as bucket]
   [store.sql :as sql]
   [model-explorer.core :as model-explorer]
   [model-explorer.templates :as templates]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; DB stuff

(def table-spec
  [[:id "varchar(255) PRIMARY KEY"]
   [:created :timestamp "default current_timestamp"]
   [:updated :bigint]
   [:datum "BLOB NOT NULL"]
   [:labels "BLOB"]
   [:note "BLOB"]])

(def model-table-spec
  [[:id "varchar(255) NOT NULL"]
   [:data_type "varchar(255) NOT NULL"]
   [:query "BLOB NOT NULL"]
   [:model_info "BLOB NOT NULL"]
   [:trainer_params "BLOB NOT NULL"]
   [:training_data_params "BLOB NOT NULL"]
   ["primary key(data_type, id)"]])

(def +relations-table-spec+
  (let [ks [:name :fromid :toid]]
    (conj (mapv #(vector % "varchar(255) NOT NULL") ks)
          [(format "primary key(%s)" (str/join "," (map name ks)))])))


(def env->+db-spec+
  {:stage {:delimiters "`"
           :port 3306
           :host "HOST"
           :user "USER"
           :password "PASSWORD"
           :db "DB"
           :classname "com.mysql.jdbc.Driver"
           :subprotocol "mysql"}

   :test (assoc sql/h2-test-connection-spec
           :table-spec {"tweet" table-spec
                        "tweeter" table-spec
                        "url" table-spec
                        "model" model-table-spec
                        "relations" +relations-table-spec+})})

(def db-bucket
  "Partial spec of db bucket.  You fill in table, primary-key, and ->mem etc."
  (graph/instance bucket/bucket-resource [env {connection-pool nil}]
    {:env env
     :type :sql
     :ec2-keys nil
     :connection-spec (or connection-pool (safe-get env->+db-spec+ env))
     :full-cache true}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Datum schemas.

(s/defschema Geo
  {:type (s/enum "Point"), :coordinates [Double]})

(s/defschema TwitterUser
  {:description String,
   :profile_link_color String,
   :profile_sidebar_border_color String,
   :is_translation_enabled Boolean,
   :profile_image_url String,
   :profile_use_background_image Boolean,
   :default_profile Boolean,
   :profile_background_image_url String,
   :is_translator Boolean,
   :profile_text_color String,
   :name String,
   :profile_background_image_url_https String,
   :favourites_count Long,
   :screen_name String,
   :entities
   {:description
    {:urls
     [{:url String,
       :expanded_url String,
       :display_url String,
       :indices [Long]}]},
    (s/optional-key :url)
    {:urls
     [{:url String,
       :expanded_url (s/maybe String)
       (s/optional-key :display_url) String,
       :indices [Long]}]}},
   :listed_count Long,
   :profile_image_url_https String,
   :statuses_count Long,
   :has_extended_profile Boolean,
   (s/optional-key :profile_banner_url) String,
   :contributors_enabled Boolean,
   :following Boolean,
   :lang String,
   :utc_offset (s/maybe Long),
   :notifications Boolean,
   :default_profile_image Boolean,
   :profile_background_color String,
   :id Long,
   :follow_request_sent Boolean,
   :url (s/maybe String),
   :time_zone (s/maybe String),
   :profile_sidebar_fill_color String,
   :protected Boolean,
   :profile_background_tile Boolean,
   :id_str String,
   :geo_enabled Boolean,
   :location String,
   :followers_count Long,
   :friends_count Long,
   :verified Boolean,
   :created_at String
   s/Keyword s/Any})

(s/defschema Tweet
  "Tweet, ignoring stuff we don't care about."
  {:id Long,
   :in_reply_to_screen_name (s/maybe String),
   :geo (s/maybe Geo)
   :coordinates (s/maybe Geo)
   :in_reply_to_status_id (s/maybe Long)
   :is_quote_status Boolean
   (s/optional-key :quoted_status_id) Long
   (s/optional-key :quoted_status) (s/recursive #'Tweet)
   :entities
   {:hashtags [{:text String, :indices [Long]}],
    :symbols [s/Any],
    :user_mentions
    [{:screen_name String,
      :name String,
      :id Long,
      :id_str String,
      :indices [Long]}],
    :urls
    [{:url String,
      :expanded_url String
      :display_url String,
      :indices [Long]}],
    (s/optional-key :media)
    [s/Any]}
   :source String,
   :lang String,
   :in_reply_to_user_id (s/maybe Long),
   :id_str String,
   (s/optional-key :possibly_sensitive) Boolean,
   :favorited Boolean,
   (s/optional-key :retweeted_status) (s/recursive #'Tweet)
   :user TwitterUser
   :retweet_count Long,
   :favorite_count Long,
   :created_at String,
   (s/optional-key :extended_entities) s/Any,
   :text String
   s/Keyword s/Any})

(s/defschema URL
  "A web page, just a measly unresolved URL for now"
  {:url String})

(defn sub-tweets [tweet]
  (remove nil? (map tweet [:quoted_status :retweeted_status])))

(defmethod model-explorer/render-html :tweet [_ tweet] (templates/tweet tweet))
(defmethod model-explorer/render-html :tweeter [_ tweeter] (templates/tweeter tweeter))
(defmethod model-explorer/render-html :url [_ tweeter] (templates/url tweeter))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Bucket, conversion, and ops

(def +tables+
  (for-map [[table id-extractor item-schema] [[:tweet #(safe-get % :id_str) Tweet]
                                              [:tweeter #(safe-get % :id_str) TwitterUser]
                                              [:url #(templates/truncate-url (safe-get % :url)) URL]]]
    table
    (let [schema {:id String
                  :type (s/eq table)
                  :created s/Any
                  :updated long
                  :datum item-schema
                  :labels (s/maybe model-explorer/Labels)
                  :note s/Any}]
      {:table table
       :id-extractor id-extractor
       :ddl table-spec
       :schema schema
       :->mem (s/fn ^:always-validate to-mem :- schema [d]
                (-> d
                    (update :datum serialize/deserialize)
                    (update :labels #(when % (serialize/deserialize %)))
                    (update :note #(when % (serialize/deserialize %)))
                    (assoc :type table)))
       :mem-> (s/fn ^:always-validate from-mem [d :- (map-keys s/optional-key schema)]
                (assert (= (:type d) table))
                (-> d
                    (update :datum #(serialize/serialize serialize/+default+ %))
                    (update :labels #(when % (serialize/serialize serialize/+default+ %)))
                    (update :note #(when % (serialize/serialize serialize/+default+ %)))
                    (dissoc :type)
                    (assoc :updated (millis))))})))

(defn training-data-bucket [table]
  (letk [[->mem mem->] (+tables+ table)]
    (graph/instance db-bucket []
      {:table (name table)
       :primary-key [:id]
       :->mem ->mem
       :mem-> mem->})))

(defn ->entry [type datum & [labels note]]
  (letk [[id-extractor] (safe-get +tables+ type)]
    {:id (id-extractor datum)
     :type type
     :datum datum
     :labels labels
     :note note}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Relations

(s/defschema RelationClass
  {:name s/Keyword
   :eman s/Keyword
   :source (s/enum :manual :auto)
   :fromtype s/Keyword
   :totype s/Keyword
   (s/optional-key :from->to) (s/=> [model-explorer/ID] model-explorer/Datum)
   (s/optional-key :to->from) (s/=> [model-explorer/ID] model-explorer/Datum)})

(s/defn invert :- RelationClass
  [rc :- RelationClass]
  (set/rename-keys
   rc
   {:name :eman :eman :name
    :fromtype :totype :totype :fromtype
    :from->to :to->from :to->from :from->to}))

(s/def +relation-classes+ :- {s/Keyword RelationClass}
  {:author {:name :author
            :eman :tweet
            :source :auto
            :fromtype :tweet
            :totype :tweeter
            :from->to #(vector (safe-get-in % [:user :id_str]))}
   :follower {:name :follower
              :eman :friend
              :source :manual
              :fromtype :tweeter
              :totype :tweeter}
   :shared {:name :shared
            :eman :sharer
            :source :manual
            :fromtype :tweeter
            :totype :url}
   :linksto {:name :linksto
             :eman :linkedfrom
             :source :manual
             :fromtype :tweet
             :totype :url}})

(s/defschema RelationInstance
  {:name s/Keyword
   :fromid String
   :toid String})

(def relation-bucket
  (graph/instance db-bucket []
    {:table "relations"
     :primary-key [:name :fromid :toid]
     :full-cache true
     :mem-> #(update % :name name)
     :->mem #(update % :name keyword)}))

(defn fill-auto-relations!
  "Batch job to fill automatic relations."
  [training-data-buckets relation-bucket]
  (doseq [relation (vals +relation-classes+)]
    (letk [[name source fromtype totype {from->to nil} {to->from nil}] relation]
      (case source
        :manual nil
        :auto
        (let [[froms tos] (for [[t extractor src-k trg-k]
                                [[fromtype from->to :fromid :toid]
                                 [totype to->from :toid :fromid]]]
                            (for [d (->> (safe-get training-data-buckets fromtype)
                                         model-explorer/all-data)
                                  :let [src-id (safe-get d :id)]
                                  trg-id (when extractor (extractor (safe-get d :datum)))]
                              {:name name src-k src-id trg-k trg-id}))
              all (distinct (concat froms tos))]
          (log/infof "Found %s distinct %s relations (%s forward, %s backwards)"
                     (count all) name (count froms) (count tos))
          (bucket/batch-put
           relation-bucket
           (for [i all]
             [(mapv i [:name :fromid :toid]) i])))))))

(defnk fill-url-relations!
  "Batch job to fill url relations."
  [stores [:relations relation-bucket]]
  (->> (safe-get stores :tweet)
       model-explorer/all-data
       (mapcat (fnk [[:datum
                      id_str
                      [:user [:id_str :as user-id]]
                      [:entities urls]]]
                 (aconcat
                  (for [url-entity urls
                        :let [raw-url (safe-get url-entity :expanded_url)
                              url (templates/truncate-url raw-url)]]
                    [{:name :shared :fromid user-id :toid url}
                     {:name :linksto :fromid id_str :toid url}]))))
       (mapv (fn [i] [(update (mapv i [:name :fromid :toid]) 0 name) i]))
       (bucket/batch-put relation-bucket)))

(defnk fill-urls!
  "Batch job to fill urls."
  [stores]
  (->> (safe-get stores :tweet)
       model-explorer/all-data
       (mapcat (fnk [[:datum [:entities urls]]]
                 (for [url-entity urls
                       :let [raw-url (safe-get url-entity :expanded_url)]]
                   (->entry :url {:url raw-url}))))
       (model-explorer/put! (safe-get stores :url))))

(defn fill-follow-relations!
  [train-data-graph twitter-client]
  (letk [[stores [:relations relation-bucket]] train-data-graph]
    (let [all-ids (sort (map :id (model-explorer/all-data (safe-get stores :tweeter))))
          id-set (set all-ids)]
      (doseq [[i ids] (indexed (partition-all 100 all-ids))]
        (println i)
        (->> ids
             (parallel/map-work 10 (fn [id]
                                     (for [friend (first (twitter-client :following id)) ;; just the first page for now.
                                           :when (id-set (str friend))]
                                       {:name :follower :fromid id :toid (str friend)})))

             aconcat
             (mapv (fn [i] [(update (mapv i [:name :fromid :toid]) 0 name) i]))
             ((fn [rs] (println i "..." (count rs)) rs))
             (bucket/batch-put relation-bucket))))))

(s/defschema Relations
  {(s/named s/Keyword "source datatype")
   {(s/named s/Keyword "relation name")
    {(s/named model-explorer/ID "source id")
     [(s/named model-explorer/ID "target id")]}}})

(s/defn extract-relations :- Relations
  [relation-bucket]
  (reduce
   (fn [m r]
     (letk [[name fromid toid] r
            [eman fromtype totype] (safe-get +relation-classes+ name)]
       (-> m
           (update-in [fromtype name fromid] conj toid)
           (update-in [totype eman toid] conj fromid))))
   {}
   (bucket/vals relation-bucket)))

(def relation-graph
  (graph/graph
   :relation-bucket relation-bucket
   :index (fnk [relation-bucket]
            (extract-relations relation-bucket))))

(s/defn relations :- Relations
  [relations-graph]
  (safe-get relations-graph :index))

;; relation indexing and lookup.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; TrainingDataStore implementation.

(defrecord BucketTrainingDataStore [b]
  model-explorer/TrainingDataStore
  (id [this] (keyword (sql/table-name b)))
  (labels [this]
    (->> b bucket/vals (mapcat (fnk [labels] (keys labels))) distinct))
  (data [this label]
    (->> b bucket/vals (filter (fnk [labels] (contains? labels label)))))
  (datum [this id]
    (bucket/get b [id]))
  (all-data [this]
    (bucket/vals b))
  (put! [this data]
    (bucket/batch-put b (for [item data] [[(safe-get item :id)] item])))
  (label! [this ids new-label label-data]
    (model-explorer/put!
     this
     (for [id ids]
       (update (model-explorer/datum this id) :labels assoc new-label label-data))))
  (unlabel! [this ids label]
    (model-explorer/put!
     this
     (for [id ids]
       (update (model-explorer/datum this id) :labels dissoc label)))))

(def training-data-graph
  (graph/graph
   :stores (fnk [connection-pool env :as args]
             (map-vals
              (fnk [table]
                (->BucketTrainingDataStore
                 ((training-data-bucket table) args)))
              +tables+))
   :relations relation-graph))

(defn all-data-with-neighbors
  "Get all datums of a type, filling in neighboring values (depth 1)
   for each relation.  Will eventually need something faster/smarter."
  [tdg typ datum->auto-labels]
  (letk [[stores [:relations :as relations-graph]] tdg
         rels (typ (relations relations-graph))
         target-stores (for-map [k (keys rels)]
                         k
                         (->> (concat (vals +relation-classes+)
                                      (map invert (vals +relation-classes+)))
                              (filter #(= (:name %) k))
                              pci/safe-singleton
                              (<- (safe-get :totype))
                              (safe-get stores)))]
    (for [d (model-explorer/all-data (safe-get stores typ))]
      (reduce
       (fn [d [k rels]]
         (assoc d k (for [i (rels (safe-get d :id))]
                      (doto (model-explorer/datum (target-stores k) i) assert))))
       (assoc d :auto-labels (datum->auto-labels d))
       rels))))



;; labels separate from datums?

;; TODO: pull docs, followers, etc.

(comment
  (require
   '[model-explorer.isis.db :as db]
   '[clojure.java.jdbc.deprecated :as jdbc]
   '[store.sql :as sql])

  (plumbing.resource/with-open [conn (sql/connection-pool (:stage db/+db-spec+))]
    (jdbc/with-connection conn
      (jdbc/do-commands
       (throw (Exception.
               "CREATE DATABASE DBNAME
                DEFAULT CHARACTER SET utf8
                DEFAULT COLLATE utf8_general_ci;")))))

  (plumbing.resource/with-open [conn (sql/connection-pool (:stage db/+db-spec+))]
    (doseq [[t {:keys [ddl]}] db/+tables+]
      (jdbc/with-connection conn
        (apply sql/create-table-if-missing! (name t) ddl))))

  (def g (plumbing.resource/bundle-run db/training-data-graph {}))
  (apply db/put! g (for [t all-tweets] (db/->entry :tweet t)))
  (apply db/put! g (for [u (distinct-by :id (map :user all-tweets))] (db/->entry :tweeter u)))
  )
