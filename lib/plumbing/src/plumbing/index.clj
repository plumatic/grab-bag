(ns plumbing.index
  (:refer-clojure :exclude [reset!])
  (:use plumbing.core)
  (:require
   [potemkin :as potemkin]
   [plumbing.io :as io]
   [schema.core :as s])
  (:import
   [java.util ArrayList HashMap List Map]))

(set! *warn-on-reflection* true)

(potemkin/definterface+ IIndexer
  (index! ^long [this item])
  (get-index ^long [this item] "like index! but return -1 for missing")
  (safe-get-index ^long [this item] "like safe-get-index, but throw when missing")
  (item [this ^long idx])
  (contains [this item])
  (reset! [this] "Clear all entries if unlocked, else do nothing")
  (lock! [this])
  (unlock! [this])
  (locked? [this]))

;; Note: not thread-safe.
(deftype Index [^Map index-map ^List index-list ^:volatile-mutable locked?]
  IIndexer
  (lock! [this] (locking this (set! locked? true)))
  (unlock! [this] (locking this (set! locked? false)))
  (locked? [this] locked?)
  (index! [this item]
    (or (.get index-map item)
        (if locked?
          (throw (RuntimeException. (format "Cannot index %s; index locked" item)))
          (let [idx (.size index-list)]
            (.add index-list item)
            (.put index-map item idx)
            idx))))
  (get-index [this item]
    (or (.get index-map item) -1))
  (safe-get-index [this item]
    (or (.get index-map item)
        (throw (RuntimeException. (format "Cannot get index for %s" item)))))
  (item [this i]
    (when-not (< i (.size index-list))
      (throw (RuntimeException. (format "Cannot unindex %s; max index %s" i (.size index-list)))))
    (.get index-list i))
  (contains [this item]
    (.containsKey index-map item))
  (reset! [this]
    (when-not locked?
      (.clear index-map)
      (.clear index-list)))

  clojure.lang.Seqable
  (seq [this] (seq index-list))

  clojure.lang.Counted
  (count [this]
    (.size index-list))

  plumbing.io.PDataLiteral
  (to-data [this] [::index [locked? (vec (seq index-list))]])

  java.lang.Object
  (toString [this] (.toString index-map)))

(defn ^Index dynamic
  "Make a new dynamic indexer that will index new elements with sequential Ids."
  []
  (Index. (HashMap.) (ArrayList.) false))

(defn ^Index static
  "Make a new static indexer for a fixed collection of elements (which will be
   given sequential ids."
  [items]
  (let [idx (dynamic)]
    (doseq [i items] (index! idx i))
    (lock! idx)
    idx))

(defmethod io/from-data ::index [[_ [locked? xs]]]
  (let [index (dynamic)]
    (doseq [x xs] (index! index x))
    (if locked? (lock! index))
    index))

;; read old classifiers.  TODO: test.
(defmethod io/from-data :classify.index/index [[_ [locked? xs]]]
  (io/from-data [::index [locked? xs]]))
