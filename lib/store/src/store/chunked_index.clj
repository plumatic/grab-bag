(ns store.chunked-index
  (:use plumbing.core)
  (:require
   [store.bucket :as bucket]
   [store.dynamo :as dynamo])
  (:import [java.io DataInputStream DataOutputStream]))


;; Support for chunked indexes built on top of dynamo.
;; We manage the values under a single hash key,
;; supporting a view of a decreasing seq of [key value] pairs,
;; stored in dynamo as batches of a specified size
;; with a specified serialization function.
;; range key is a double.

;; TODO: move hash-key out of protocol, if we ctually find another use.
;; TODO: this should actually be a bucket?

(set! *warn-on-reflection* true)

(defprotocol PChunkedSortedIndex
  (find-page [this hash-key start?]
    "Get the max-value page taht includes at least one item <= start, defaulted to infinity,
     as a descending sorted seq of [key value] pairs], or nil")
  (index-get [this hash-key range-key])
  (index-put! [this hash-key range-key value])
  (index-delete! [this hash-key range-key]))

;; TODO: chunked versino that uses scan?
(defn index-get-all [index hash-key & [start]]
  (lazy-seq
   (when-let [vals (find-page index hash-key start)]
     (lazy-cat
      (drop-while #(when start (> (first %) start)) vals)
      (index-get-all index hash-key (dec (first (last vals))))))))

(defn deserialize-chunk [read-value ^bytes chunk]
  (with-open [is (java.io.DataInputStream. (java.io.ByteArrayInputStream. chunk))]
    (vec
     (for [i (range (.readInt is))]
       [(.readLong is) (read-value is)]))))

(defn serialize-chunk ^bytes [write-value! chunk]
  (let [baos (java.io.ByteArrayOutputStream.)
        os (java.io.DataOutputStream. baos)]
    (.writeInt os (count chunk))
    (doseq [[k v] chunk]
      (.writeLong os k)
      (write-value! os v))
    (.close os) (.close baos)
    (.toByteArray baos)))

;; This is meant to do well for sequential writes, may get arbitrarily fragmented
;; with out-of-order writes (worst case 1 entry per chunk if we write in backwards order).
;; dynamo bucket should have :raw serialize-method, be ready to accept map to byte array.
(defrecord SimpleChunkedIndex [get-next put! batch-size write-value! read-value]
  PChunkedSortedIndex
  (find-page [this hash-key start?]
    (when-let [v (get-next hash-key start?)]
      (deserialize-chunk read-value v)))

  (index-get [this hash-key range-key]
    (when-let [chunk (find-page this hash-key range-key)]
      (second (first (filter #(= (first %) range-key) chunk)))))

  ;; TODO: compact or be smarter
  (index-put! [this hash-key range-key value]
    (let [put! (fn [vs] (put! hash-key (first (last vs)) (serialize-chunk write-value! vs)))]
      (if-let [p (find-page this hash-key range-key)]
        (if-let [pos (first (positions #(= (first %) range-key) p))]
          (put! (assoc p pos [range-key value]))
          (let [np (sort-by (comp - first) (cons [range-key value] p))]
            (if (> (count np) batch-size)
              (let [[split1 split2] ((juxt drop-last take-last) batch-size np)]
                (put! split2)
                (put! split1))
              (put! np))))
        (put! [[range-key value]]))))   ;; we've reached a new low

  (index-delete! [this hash-key range-key]
    (when-let [p (find-page this hash-key range-key)]
      (when-let [pos (first (positions #(= (first %) range-key) p))]
        (let [np (concat (take pos p) (drop (inc pos) p))
              ork (first (last p))
              nrk (first (last np))]
          (when nrk
            (put! hash-key nrk (serialize-chunk write-value! np)))
          (when-not (= ork nrk) (put! hash-key ork nil)))
        (second (nth p pos))))))


;; WARNING: I expect that dynamo-bucket has :raw serialization type.
;; also works on mem buckets.
(defn dynamo-chunked-index [dynamo-bucket batch-size write-value read-value]
  (SimpleChunkedIndex.
   #(when-let [[k v] (ffirst (dynamo/simple-query dynamo-bucket %1 (when %2 (inc %2)) true 1))]
      v)
   (fn [hash-key range-key val]
     (if (seq val)
       (bucket/put dynamo-bucket [hash-key range-key] val)
       (bucket/delete dynamo-bucket [hash-key range-key])))
   batch-size write-value read-value))

(defnk long-chunked-index
  "A chunked index where values are longs, with a default size to fit into 1k per chunk"
  [bucket ;; dynamo or mem
   {chunk-size 50}]  ;; empirically, 50 fits in 1k (~62 for small vals)
  (dynamo-chunked-index
   bucket chunk-size #(.writeLong ^DataOutputStream %1 %2) #(.readLong ^DataInputStream %)))

(set! *warn-on-reflection* false)
