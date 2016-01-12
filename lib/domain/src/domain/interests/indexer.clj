(ns domain.interests.indexer
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.index :as index])
  (:import
   [clojure.lang Keyword]
   [plumbing.index Index]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(def Id (s/either String ;; of form type:display-key
                  Long))

(def ^:const high-bits 8)
(def ^:const low-bits (- 64 8))
(def ^:const low-bitmask (dec (bit-shift-left 1 low-bits)))
(def ^:const high-bitmask (bit-not low-bitmask))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public; generic methods for encoding/decoding type keyword + key index to single long

(s/defn key-of :- long
  [id :- long]
  (bit-and id low-bitmask))

(s/defn type-of :- Keyword
  [type-index :- Index id :- long]
  (index/item type-index (unsigned-bit-shift-right id low-bits)))

(s/defn value-of :- [(s/one Keyword "type") (s/one long "value")]
  [type-index :- Index id :- long]
  [(type-of type-index id) (key-of id)])

(s/defn index-of :- long
  [type-index :- Index
   type :- Keyword
   key :- long]
  (assert (zero? (bit-and high-bitmask key)) (format "key too large: %s:%s" type key))
  (+ key (bit-shift-left (index/index! type-index type) low-bits)))

(s/defn extract-type-key :- {:type Keyword :key String}
  [x :- String]
  (let [[type key] (.split x ":" 2)]
    {:type (keyword type)
     :key key}))
