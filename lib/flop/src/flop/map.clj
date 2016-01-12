(ns flop.map
  (:require
   [hiphip.double :as dbl]
   [hiphip.long :as lng])
  (:import
   [gnu.trove TLongDoubleHashMap TLongLongHashMap
    TLongObjectHashMap TObjectDoubleHashMap TDoubleFunction]
   [flop LongDoubleFeatureVector]))


(set! *warn-on-reflection* true)

(defmacro do-trove [[[k v] m] & body]
  `(let [m# ~m
         ks# (.keys m#)]
     (dotimes [i# (alength ks#)]
       (let [~k (aget ks# i#)
             ~v (.get m# ~k)]
         ~@body))
     m#))

;; TODO: bad name, should be increment all
(defn increment! [^TLongDoubleHashMap t1 ^TLongDoubleHashMap t2]
  (do-trove [[k v2] t2]
            (.put t1 k (+ (.get t1 k) v2))))

;; TODO: bad name, should be increment all, and merged with increment above
(defn increment-by! [^TLongDoubleHashMap t1 ^TLongDoubleHashMap t2 ^double mul]
  (do-trove [[k v2] t2]
            (.put t1 k (+ (.get t1 k) (* mul v2)))))

(defn increment-key! [^TLongDoubleHashMap t ^long key ^double amount]
  (when-not (.adjustValue t key amount)
    (.put t key amount))
  t)

(defn scale-key! [^TLongDoubleHashMap t ^long key ^double scale-factor]
  (let [val (.get t key)]
    (.put t key (* val scale-factor)))
  t)

(defn scale-by! [^TLongDoubleHashMap t scale]
  (let [scale (double scale)]
    (do-trove [[k v] t]
              (.put t k (* scale v)))
    t))

(defn add [^TLongDoubleHashMap t1 ^TLongDoubleHashMap t2]
  (doto (TLongDoubleHashMap.)
    (increment! t1)
    (increment! t2)))

(defn trove->map [^TLongDoubleHashMap t]
  (into {}
        (for [k (.keys t)]
          [k (.get t k)])))

(defn otrove->map [^TObjectDoubleHashMap t]
  (into {}
        (for [k (.keys t)]
          [k (.get t k)])))

(defn lotrove->map [^TLongObjectHashMap t]
  (into {}
        (for [k (.keys t)]
          [k (.get t k)])))

(defn value-sum ^double [^TLongDoubleHashMap tld]
  (dbl/asum (.getValues tld)))

(defn value-sum-sq ^double [^TLongDoubleHashMap tld]
  (dbl/asum [v (.getValues tld)] (* v v)))

(defmacro copy! [output input ks]
  `(doseq [k# ~ks
           :when (.containsKey ~input k#)]
     (.put ~output k# (.get ~input k#))))

(defn ^TLongObjectHashMap map->lotrove [m]
  (let [res (TLongObjectHashMap.)]
    (doseq [[k v] m] (.put res (long k) v))
    res))

(defn ^TObjectDoubleHashMap map->otrove [m]
  (let [res (TObjectDoubleHashMap.)]
    (doseq [[k v] m] (.put res k (double v)))
    res))

(defn ^TLongObjectHashMap map->lotrove [m]
  (let [res (TLongObjectHashMap.)]
    (doseq [[k v] m] (.put res k v))
    res))

(defn ^TLongDoubleHashMap map->trove [m]
  (let [res (TLongDoubleHashMap.)]
    (doseq [[k v] m] (.put res k (double v)))
    res))

(defn ^TLongLongHashMap map->lltrove [m]
  (let [res (TLongLongHashMap.)]
    (doseq [[k v] m] (.put res k (long v)))
    res))

(defn lltrove->map [^TLongLongHashMap tll]
  (into {}
        (for [k (.keys tll)]
          [k (.get tll (long k))])))

(defn ld-select-keys
  "Copy ks subset from long-double trove and return new copy"
  ^TLongDoubleHashMap [^TLongDoubleHashMap tld ks]
  (doto (TLongDoubleHashMap. (count ks))
    (copy! tld ks)))

(defn ll-select-keys
  "Copy ks subset from tlo and return new copy"
  ^TLongLongHashMap [^TLongLongHashMap tll ks]
  (doto (TLongLongHashMap. (count ks))
    (copy! tll ks)))

(defn lo-select-keys
  "Copy ks subset from tlo and return new copy"
  ^TLongObjectHashMap [^TLongObjectHashMap tlo ks & [copy-fn]]
  (let [ret (TLongObjectHashMap. (count ks))
        copy-fn (if copy-fn copy-fn identity)]
    (doseq [k ks
            :let [v (.get tlo (long k))]
            :when v]
      (.put ret (long k) (copy-fn v)))
    ret))

(defmacro double-fn [bind & body]
  (assert (= (count bind) 1))
  `(reify TDoubleFunction
     (execute [this ~(first bind)] ~@body)))

(defn normalize! [^TLongDoubleHashMap m]
  (let [s (value-sum m)]
    (.transformValues m (double-fn [x] (/ x s)))
    m))

(defn pivot-normalize! [^TLongDoubleHashMap m ^double pivot]
  (let [s (max pivot (value-sum m))]
    (.transformValues m (double-fn [x] (/ x s)))
    m))

(defmacro trove-dot-product [h1 h2]
  `(let [h1# ~h1
         h2# ~h2]
     (if (< (.size h1#) (.size h2#)) ;; clunky but oh well
       (let [keys# (.keys h1#)]
         (loop [sum# 0.0 i# (dec (alength keys#))]
           (if (< i# 0)
             sum#
             (recur
              (let [k# (aget keys# i#)]
                (+ sum# (* (.get h1# k#) (.get h2# k#))))
              (dec i#)))))
       (let [keys# (.keys h2#)]
         (loop [sum# 0.0 i# (dec (alength keys#))]
           (if (< i# 0)
             sum#
             (recur
              (let [k# (aget keys# i#)]
                (+ sum# (* (.get h1# k#) (.get h2# k#))))
              (dec i#))))))))

(defn explain-trove-dot-product [^TLongDoubleHashMap h1 ^TLongDoubleHashMap h2]
  (let [keys (.keys h1)]
    (loop [i (dec (alength keys)) feats []]
      (if (< i 0)
        (sort-by #(- (Math/abs (double (last %)))) feats)
        (let [k (aget keys i)
              v1 (.get h1 k)
              v2 (.get h2 k)
              prod (* v1 v2)]
          (recur
           (dec i)
           (if (not (zero? prod))
             (conj feats [k v1 v2 prod])
             feats)))))))

(comment
  ;; this version would be 2x faster if Clojure's type inference wasn't broken...
  (defmacro trove-dot-product [h1 h2]
    `(let [h1# ~h1
           h2# ~h2
           ]
       (if (< (.size h1#) (.size h2#)) ;; ugly but avoids type hinting games
         (let [it# (.iterator h1#)]
           (loop [sum# 0.0]
             (if (.hasNext it#)
               (do (.advance it#)
                   (recur (+ sum# (* (.value it#) (.get h2# (.key it#))))))
               sum#)))
         (let [it# (.iterator h2#)]
           (loop [sum# 0.0]
             (if (.hasNext it#)
               (do (.advance it#)
                   (recur (+ sum# (* (.value it#) (.get h1# (.key it#))))))
               sum#)))))))

;; see above
(defn fast-trove-dot-product [^TLongDoubleHashMap t1 ^TLongDoubleHashMap t2]
  (flop.LongDoubleFeatureVector/dotProduct t1 t2))

(defmacro trove-dot-product-and-count [h1 h2]
  `(let [h1# ~h1
         h2# ~h2
         keys# (.keys h1#)]
     (loop [sum# 0.0 i# (dec (alength keys#)) num# 0]
       (if (< i# 0)
         [sum# num#]
         (let [k# (aget keys# i#)
               s1# (.get h1# k#)
               s2# (.get h2# k#)]
           (recur (+ sum# (* s1# s2#))
                  (dec i#)
                  (if (or (= 0.0 s1#) (= 0.0 s2#))
                    num#
                    (inc num#))))))))

(defn ^LongDoubleFeatureVector feature-vector [& [initial-capacity]]
  (LongDoubleFeatureVector. (int (or initial-capacity 0))))

;; Note: feature vectors aren't synchronized, and won't know if you add a feature twice,
;; and they have pointer equality.
;; They're just super-fast to iterate through and compact.
;; (about 2x faster dot product than trove-on-trove, and load factor + a bit smaller.)
(defn ^LongDoubleFeatureVector map->fv [^java.util.Map m] (LongDoubleFeatureVector. m))
(defn ^LongDoubleFeatureVector trove->fv [^TLongDoubleHashMap m] (LongDoubleFeatureVector. m))
(defn ^java.util.Map fv->map [^LongDoubleFeatureVector f] (into {} f))
(defn ^TLongDoubleHashMap fv->trove [^LongDoubleFeatureVector f] (.asTrove f))

(defmacro do-fv [bind & body]
  (let [kv (first bind)]
    (assert (= (count bind) 2))
    (assert (= (count kv) 2))
    `(let [^flop.LongDoubleFeatureVector fv# ~(second bind)]
       (.forEachEntry fv# (fn [~(with-meta (first kv) {:tag 'long})
                               ~(with-meta (second kv) {:tag 'double})]
                            ~@body)))))

(set! *warn-on-reflection* false)
