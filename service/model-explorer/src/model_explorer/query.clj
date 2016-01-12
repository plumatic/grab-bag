(ns model-explorer.query
  "Query language for querying over documents.
   Ultimate goal is for to incorporate labels and other models,
   relations and aggregation, functions, and serve as
   select and where clauses as well as feature fns."
  (:refer-clojure :exclude [some])
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.math :as math]
   [web.data :as web-data]))

;; TODO: optional keys?
;; TODO: auto-labels

(s/defschema Query
  s/Any)

(s/defschema FeatureMap
  {String s/Num})

(s/defschema FeatureName [s/Any])

(s/defschema NestedFeatureMap
  "Represents a mapping from feature fns to value mappings.
   May eventually want to flatten for perf."
  {FeatureName FeatureMap})

(s/defn ensure-feature-map :- NestedFeatureMap
  "Take the output of a function and transform it to a feature map."
  [query o]
  (cond (not o) {[query] {}}
        (number? o) {[query] {"value" (double o)}}
        (string? o) {[query] {o 1.0}}
        (true? o) {[query] {"true" 1.0}}
        (and (map? o) (or (empty? o) (number? (first (vals o)))))
        {[query] (reduce-kv (fn [m k v] (if (zero? v) (dissoc m k) m)) o o)}

        :else o))

(defn local-eval [e]
  (binding [*ns* (the-ns 'model-explorer.query)] (eval e)))

(s/defn compile-query
  "Take a 'read' query and return a function from datum to feature map.

   Here is an example query that demonstrates all the features:

     {:datum {:screen_name #\"sam\"}
      :tweet (concat
               count
               [mean {:datum {:text tokens}}])}

   which yields an output NestedFeatureMap like:

     {(:datum :screen_name #\"sam\") {\"sam\" 1.0},
      (:tweet count) {\"value\" 4.0},
      (:tweet :datum :text tokens) {\"bokoharam\" 0.75,
                                    \"jihad\" 0.5,
                                    \"isis\" 0.5,
                                    \"taliban\" 0.25}}

   The outer map matches the LabeledDatum, with each key matching corresponding
   sub-parts of the datum.

   Leaves are functions from data to FeatureMaps; or they can return Booleans
   or numbers which are converted into FeatureMaps.  Regexes can also serve
   as boolean leaf functions.

   Square brackets match lists in the data.  They must contain a pair of an
   aggregation function and a subquery.  The aggregation is a function
   from a sequence of NestedFeatureMaps to a single NestedFeatureMap.

   Finally, function call expressions take one or more subqueries, which
   are all executed on the data and passed into the function, which must
   return a single NestedFeatureMap."
  [query]
  (cond (map? query) ;; nested query
        (let [k-fns (map-vals compile-query query)]
          (fn [d]
            (for [[k f] k-fns
                  [keypath fm] (f (safe-get d k))]
              [(cons k keypath) fm])))

        (vector? query) ;; list aggregation
        (let [[f-expr subquery] query
              f (local-eval f-expr)
              subquery-fn (compile-query subquery)]
          (assert (= (count query) 2) "Only one subquery permitted in list aggregation")
          (fn list-aggregation [ds]
            (for [[ks fm] (f (map subquery-fn ds))]
              [(cons f-expr ks) fm])))

        (and (list? query) (not (#{'fn 'fn*} (first query)))) ;; function call
        (let [[f-expr & subqueries] query
              f (local-eval f-expr)
              subquery-fns (mapv compile-query subqueries)]
          (fn function-call [d]
            (apply f (map #(% d) subquery-fns))))

        (instance? java.util.regex.Pattern query) ;; literal string match
        (fn regex-leaf [d] (ensure-feature-map query (re-find query d))) ;; not sure if re-find is right semantics

        :else (let [q (local-eval query)] ;; leaf feature fn
                (fn leaf-feature-fn [x] (ensure-feature-map query (q x))))))

(s/defn sort->fn [query :- String]
  (->> (str "(" query ")")
       read-string
       (cons `fn->)
       local-eval))

(defn query-map->fn [query-map]
  (->> query-map
       compile-query
       (comp #(into {} %))))

(s/defn query->fn [query :- String]
  (-> query
      read-string
      query-map->fn))

(defn succeeded? [fm]
  (every? #(seq (second %)) fm))

(defn matches-where? [query-fn datum]
  (succeeded? (query-fn datum)))

(s/defn print-feature-name :- String
  "Try to print the feature name for human consumption."
  [feature-name :- FeatureName]
  (->> feature-name
       (remove #{identity})
       (mapv #(if (fn? %)
                (-> %
                    class
                    str
                    (.split "\\$" 2)
                    second
                    symbol)
                %))
       pr-str))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Functions that query can access.

(defn char-ngrams
  [order ^String s]
  (let [s (.toLowerCase s)
        len (count s)]
    (for-map [l (range 1 (inc order))
              start (range 0 (- len l -1))]
      (subs s start (+ start l))
      1.0)))

(defn bigrams [s]
  ;; deleted
  )

(defn char-trigrams [x] (char-ngrams 3 x))

(defn total [fvs]
  (->> fvs
       (apply concat)
       (reduce
        (fn [m [ks fm]]
          (update m ks #(merge-with + % fm)))
        {})))

(defn mean [fvs]
  (let [n (count fvs)]
    (->> fvs
         total
         (map-vals (fn->> (map-vals #(/ % (double n))))))))

(def +no+ "Arbitrary data representing unsuccessful query" {[] {}})
(def +yes+ "Arbitrary data representing successful query" {[] {"yes" 1.0}})

(defn some
  "Aggregation that takes the first clause that will match the where, or fails."
  [fvs]
  (or (first (filter succeeded? fvs))
      +no+))

(defn at-least
  "Aggregation for query only which returns truthy if at least k entities pass."
  [k]
  (fn [fvs]
    (if (>= (count-when succeeded? fvs) k) +yes+ +no+)))

(defn histogram
  "Aggregation that makes binary features representing a histogram of the proportion
   of times a feature appears in a collection."
  ([fvs]
     (histogram [0.0 0.02 0.1 0.2 0.5 0.9] fvs))
  ([levels fvs]
     (->> fvs
          mean
          (map-vals (fn [fm]
                      (for-map [[k v] fm
                                thresh (take-while #(> v %) levels)]
                        (str k " > " (long (* thresh 100)) "%") v))))))

(defn top-ten [fv]
  (for [[ks fm] fv]
    [ks (->> fm (sort-by #(Math/abs (double (second %)))) (take-last 10) (into {}))]))

(defn cross-product
  ([fv] fv)
  ([fv1 fv2] (for [[ks1 fm1] fv1 [ks2 fm2] fv2]
               [[:cross ks1 ks2]
                (for-map [[k1 v1] fm1 [k2 v2] fm2]
                  (str k1 "||" k2 )
                  (* v1 v2))]))
  ([fv1 fv2 & fvs]
     (reduce cross-product (cross-product fv1 fv2) fvs)))

(defn url-domain [url]
  (last (web-data/subdomain-split (web-data/host url))))

(defn order-of-magnitude [x]
  (str (long (math/log2 (inc x)))))

(def user-features
  (query-map->fn
   {:description bigrams
    :is_translation_enabled identity
    :default_profile identity
    :is_translator identity
    :name char-trigrams
    :screen_name char-trigrams
    :favourites_count order-of-magnitude
    :entities {:description {:urls #(boolean (seq %))}}
    :listed_count order-of-magnitude
    :statuses_count order-of-magnitude
    :has_extended_profile identity
    :contributors_enabled identity
    :lang identity
    :notifications identity
    :default_profile_image identity
    :url boolean
    :time_zone bigrams
    :geo_enabled identity
    :location bigrams
    :followers_count order-of-magnitude
    :friends_count order-of-magnitude
    :verified identity}))

(def tweet-only-features
  (query-map->fn
   {:coordinates boolean
    :geo boolean
    :in_reply_to_status_id boolean
    :is_quote_status boolean
    :lang identity
    :source identity
    ;;:retweeted_status boolean
    :entities
    {:hashtags [total {:text identity}]
     :urls [total {:expanded_url url-domain}]}
    :text bigrams}))


(def tweet-features
  (query-map->fn
   {:datum (list concat tweet-only-features {:user user-features})}))

(def tweeter-features
  (query-map->fn
   {:datum user-features
    :tweet [histogram {:datum tweet-only-features}]}))
