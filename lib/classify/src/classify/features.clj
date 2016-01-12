(ns classify.features
  "Code for defining feature fns, feature sets, and indexing into
   feature and weight vectors."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.index :as index]
   [plumbing.logging :as log]
   [flop.map :as map]
   flop.weight-vector)
  (:import
   [gnu.trove TLongArrayList TLongDoubleHashMap]
   [plumbing.index Index]
   [flop LongDoubleFeatureVector]
   [flop.weight_vector SparseWeightVector]))


(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(s/defschema GlobalContext
  "Global context to pass when featurizing datums.  Requirements depend on features."
  {s/Keyword s/Any})

(s/defschema FeatureMap
  (s/either TLongDoubleHashMap
            {(s/named s/Any "target-feature-key") Number}))

(s/defschema NestedFeatureMap
  {(s/named s/Keyword "feature-type") FeatureMap})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core feature protocols

(defprotocol PFeatureFn
  (feature-type [this]
    "the name of the feature-type, e.g. :topic, :favorites")
  (key-index [this]
    "Create a plumbing.indexer.Index for indexing the keys into at most 56-bit longs,
     or :identity if the keys are already suitable without indexing.")
  (features [this global-context datum]
    "features on the datum.
     returns a FeatureMap, e.g. {\"fave-feed\" 1.0, \"fave-topic\" 3.0}."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Helpers for implementing features

(defn feature-fn
  "A convenience implementation for simple feature fns."
  [feature-type make-key-index datum-feature-fn & [share-features?]]
  (reify
    PFeatureFn
    (feature-type [this] feature-type)
    (key-index [this] (make-key-index))
    (features [this global-context datum]
      (datum-feature-fn global-context datum))))

(defn identity-index [] :identity)

(defn default-index [] (index/dynamic))

(defn I [x] (if x 1.0 0.0))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core FeatureSet record

(s/defrecord FeatureSet
    [type-index :- Index
     key-indices :- {s/Keyword Index}
     features :- {s/Keyword (s/protocol PFeatureFn)}])

(defn feature-set [features]
  (assert (apply distinct? (map feature-type features)))
  (FeatureSet.
   (index/static (map feature-type features))
   (for-map [f features] (feature-type f) (key-index f))
   (for-map [f features] (feature-type f) f)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Simple indexing and de-indexing

(defn feature-fns [feature-set]
  (vals (safe-get feature-set :features)))

(defn get-feature-fn [feature-set t]
  (doto (->> feature-set feature-fns (filter #(= t (feature-type %))) first)
    assert))

(defn- ^long combine-indices [^long type-index ^long key-index]
  (+ type-index (bit-shift-left key-index 8)))

(defn type-index
  ([^long id] (mod id 256))
  ([feature-set type]
     (index/index! (safe-get feature-set :type-index) type)))

(defmacro with-indexer-of
  "Executes the body with a locked indexer bound to index-fn-sym"
  [[index-fn-sym feature-set type] & body]
  `(let [feature-set# ~feature-set
         type# ~type
         t# (type-index feature-set# type#)
         type-indexer# (safe-get-in feature-set# [:key-indices type#])]
     (if (= :identity type-indexer#)
       (let [~index-fn-sym (fn ^long [^long k#]
                             (combine-indices t# k#))]
         ~@body)
       (locking type-indexer#
         (let [~index-fn-sym (fn ^long [k#]
                               (log/with-elaborated-exception {:feature-type type# :feature-key k#}
                                 (combine-indices
                                  t#
                                  (index/index! type-indexer# k#))))]
           ~@body)))))

(defn index-of! [feature-set type key]
  (with-indexer-of [indexer feature-set type]
    (indexer key)))

(defn type-of [feature-set ^long id]
  (index/item (safe-get feature-set :type-index) (type-index id)))

(defn value-of [feature-set ^long id]
  (let [type (type-of feature-set id)
        indexer (safe-get-in feature-set [:key-indices type])
        shifted (unsigned-bit-shift-right id 8)]
    [type
     (if (= :identity indexer) shifted (index/item indexer shifted))]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Composite indexing and de-indexing

(defmacro put-nested-features!
  "Generic method for putting nested features into trove or map or ldfv"
  [feature-set target nested-feature-map]
  `(let [feature-set# ~feature-set
         target# ~target]
     (doseq [[t# feature-map#] ~nested-feature-map]
       (with-indexer-of [indexer# feature-set# t#]
         (if (instance? TLongDoubleHashMap feature-map#)
           (map/do-trove [[k# v#] ^TLongDoubleHashMap feature-map#]
                         (when-not (zero? v#)
                           (.put target# (.invokePrim ^clojure.lang.IFn$LL indexer# k#) v#)))
           (doseq [[k# v#] feature-map#
                   :when (not (zero? v#))]
             (.put target# (indexer# k#) v#)))))))

(s/defn nested-features->trove :- TLongDoubleHashMap
  "Take a nested feature map where the values can be map or trove."
  [feature-set nested-feature-map :- NestedFeatureMap]
  (let [m (TLongDoubleHashMap.)]
    (put-nested-features! feature-set m nested-feature-map)
    m))

(defn nested-features->wv
  "Take a nested feature map where the values can be map or trove."
  [feature-set nested-feature-map]
  (SparseWeightVector. (nested-features->trove feature-set nested-feature-map)))

(defn fv->nested-features [feature-set ^flop.LDReducable m]
  (.reduce m
           (fn [m ^long k ^double v]
             (assoc-in m (value-of feature-set k) v))
           {}))

(defn trove->nested-features [feature-set ^TLongDoubleHashMap ldhm]
  (fv->nested-features feature-set (SparseWeightVector. ldhm)))

(defn fv-remove-types! [feature-set ^LongDoubleFeatureVector fv type-pred]
  (let [to-remove (TLongArrayList.)]
    (map/do-fv [[^long k ^double v] fv]
               (when (type-pred (type-of feature-set k))
                 (.add to-remove k)))
    (dotimes [idx (.size to-remove)]
      (.remove fv (.get to-remove idx)))))

(defn fv-type-filter-fn
  "Return a fn that projects feature vectors onto only types that pass type-pred."
  [feature-set type-pred]
  (let [good-types (->> (feature-fns feature-set)
                        (map feature-type)
                        (filter type-pred)
                        (map #(type-index feature-set %))
                        set)]
    (fn [^LongDoubleFeatureVector fv]
      (let [res (LongDoubleFeatureVector.)]
        (map/do-fv [[^long k ^double v] fv]
                   (when (contains? good-types (type-index k))
                     (.put res k v)))
        res))))

(defn fv-put-features!
  "Take a LDFV and extract features on datum from feature-fn and increment
   corresponding [feat-type feat-key] and feat-val pairs
   The provided pluggable indexer fn receives a type and key and returns a long."
  [feature-set
   global-context
   ^LongDoubleFeatureVector fv
   datum
   feature-fn ;:- (s/protocol PFeatureFn) (commented for potential perf reasons.)
   ]
  (put-nested-features!
   feature-set
   fv
   {(feature-type feature-fn) (features feature-fn global-context datum)}))

(defn feature-vector
  [feature-set global-context datum]
  (let [datum-feature-vector (LongDoubleFeatureVector.)]
    (doseq [simple (feature-fns feature-set)]
      (fv-put-features! feature-set global-context datum-feature-vector datum simple))
    datum-feature-vector))

(defn nested-feature-map
  [feature-fns global-context datum]
  (for-map [feature-fn feature-fns]
    (feature-type feature-fn) (features feature-fn global-context datum)))

(s/defn ^Double nested-map-dot-product
  "Dot product of two nested maps. Iterates over m1, so pass the smaller map first for
   efficiency."
  [m1 :- NestedFeatureMap
   m2 :- NestedFeatureMap]
  (sum (for [[t inner-m1] m1
             :let [inner-m2 (get m2 t)]
             :when inner-m2
             [k v1] inner-m1
             :let [v2 (get inner-m2 k)]
             :when v2]
         (* v1 v2))))

(set! *warn-on-reflection* false)
