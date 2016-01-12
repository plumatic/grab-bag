(ns plumbing.schema.infer
  "Experimental thing for inferring schemas from a collection of data."
  (:use plumbing.core)
  (:require
   [clojure.set :as set]
   [schema.core :as s])
  (:import
   [schema.core AnythingSchema EnumSchema Maybe]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Infer by building up a schema incrementally.
;;; Assumes certain kinds of norms for now -- just fixed-key maps, uniform seqs, enums

(def UnknownSchema
  (reify s/Schema
    (spec [this] (throw (UnsupportedOperationException.)))
    (explain [this] ::Unknown)))

(defrecord LeafWithExample [schema ex]
  s/Schema
  (spec [this] (s/spec schema))
  (explain [this] (list 'class-ex schema ex)))

(defn class-ex [leaf ex]
  (LeafWithExample. leaf ex))

(declare extend-schema)

(defn enum-elt? [x] (or (string? x) (keyword? x)))

(defn default-schema [x]
  (cond (nil? x) (s/maybe UnknownSchema)
        (enum-elt? x) (s/enum x)
        (map? x) (for-map [[k v] x] (s/required-key k) (default-schema v))
        (instance? Iterable x) [(reduce extend-schema UnknownSchema x)]
        :else (class-ex (class x) x)))

(defn incorporate-present [x s]
  (reduce
   (fn [s [k v]]
     (let [[kx vs] (or (find s (s/required-key k))
                       (find s (s/optional-key k))
                       [(s/optional-key k) (default-schema v)])]
       (assoc s kx (extend-schema vs v))))
   s
   x))

(defn downgrade-required [x s]
  (for-map [[k v] s]
    (if (and (s/required-key? k)
             (not (contains? x (s/explicit-schema-key k))))
      (s/optional-key (s/explicit-schema-key k))
      k)
    v))

(defn lub [& classes]
  (->> classes
       (map #(set (cons % (ancestors %))))
       (apply set/intersection)
       (<- (disj java.io.Serializable Cloneable))
       (apply max-key (comp count ancestors))))

(defn upgrade-enum [e]
  (let [vs (safe-get e :vs)]
    (if (> (count vs) 5)
      (class-ex (apply lub (map class vs)) (first (sort vs)))
      e)))

(defn extend-schema [s x]
  (let [s (if (= s UnknownSchema) (default-schema x) s)]
    (cond (instance? AnythingSchema s)
          s

          (instance? Maybe s)
          (if (nil? x)
            s
            (s/maybe (extend-schema (.schema ^Maybe s) x)))

          (nil? x)
          (s/maybe s)

          (instance? EnumSchema s)
          (if (enum-elt? x)
            (upgrade-enum (update-in s [:vs] conj x))
            (do (printf "invalid enum %s element %s\n" (s/explain s) x)
                s/Any))



          (and (map? s) (not (instance? clojure.lang.IRecord s)))
          (->> s
               (incorporate-present x)
               (downgrade-required x))

          (vector? s)
          (if (instance? Iterable x)
            [(reduce extend-schema (first s) x)]
            s/Any)

          (isa? s Number)
          (if (instance? s x)
            s
            (do (assert (number? x))
                Number))

          (instance? LeafWithExample s)
          (class-ex (extend-schema (safe-get s :schema) x) (safe-get s :ex))

          :else
          (if (s/check s x)
            s/Any
            s))))

(defn infer-schema [xs]
  (first (doto (default-schema xs) identity #_(s/validate xs))))

(set! *warn-on-reflection* false)
