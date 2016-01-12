(ns plumbing.serialize
  (:use plumbing.core)
  (:require
   [clojure.java.io :as java-io]
   [plumbing.json :as json])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream
    DataInputStream DataOutputStream InputStream
    ObjectInputStream ObjectOutputStream OutputStream]
   [java.nio ByteBuffer]
   [org.apache.commons.codec.binary Base64]
   [org.apache.commons.io IOUtils]
   [org.xerial.snappy SnappyInputStream SnappyOutputStream Snappy]
   [plumbing SeqInputStream]
   [plumbing Serializer]))


(set! *warn-on-reflection* true)

(defn read-object-filthy-classloader-hack
  "Apparently the classloader used to instantiate objects by readObject is taken from
   the class in which the bytecode is embedded.  Thus, a direct call to readObject in
   plumbing.Serializer.java would use the Java classloader, which is unable to find
   Clojure's deftype classes.  Instead, we create this useless function and call it
   from plumbing.Serializer.java so that default serialization actually works on
   Clojure deftypes."
  [^ObjectInputStream ois]
  (.readObject ois))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public serialization and deserialization interfaces

(defmulti serialize-impl
  "Serializes x to output stream os based on type. Do not output a leading tag byte."
  (fn [type os x] type))

(defmulti deserialize-impl
  "Deserializes a single object from input stream based on type.
   Input stream should not contain a leading tag byte."
  (fn [type is] type))


(defn serialize-stream
  "Serializes x to output stream os based on type. Outputs a leading tag byte so that
   deserialization can dispatch based on the leading byte."
  [type ^OutputStream os x]
  (.write os (int type))
  (serialize-impl type os x))

(defn deserialize-stream
  "Deserializes a single tagged object from is based on leading byte."
  [^InputStream is]
  (deserialize-impl (.read is) is))


(defn ^{:tag "[B"} serialize
  "Serializes x to byte array with tag. See serialize-stream above."
  [type x]
  (let [baos (ByteArrayOutputStream.)]
    (serialize-stream type baos x)
    (.toByteArray baos)))

(defn deserialize
  "Deserialize a tagged object from either byte array or input stream.
   See deserialize-stream above."
  [x]
  (deserialize-stream
   (if (instance? InputStream x)
     x
     (ByteArrayInputStream. ^bytes x))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Type implementations for serialize-impl and deserialize-imple

(def +clojure+ 0)

(defmethod serialize-impl +clojure+ [_ ^OutputStream os x]
  (.write os (.getBytes (pr-str x) "UTF-8")))

(defmethod deserialize-impl +clojure+ [_ ^InputStream is]
  (-> (IOUtils/toByteArray is) (String. "UTF-8") read-string))


;; 1 is reserved for deprecated method
(def +deprecated-serialize-method+ 1)


(def +java+ 2)

(defmethod serialize-impl +java+ [_ ^OutputStream os x]
  (.writeObject (ObjectOutputStream. os) x))

(defmethod deserialize-impl +java+ [_ ^InputStream is]
  (.readObject (ObjectInputStream. is)))


(def +json+ 3)

(defmethod serialize-impl +json+ [_ ^OutputStream os x]
  (.write os (.getBytes ^String (json/generate-string x) "UTF-8")))

(defmethod deserialize-impl +json+ [_ ^InputStream is]
  (-> (IOUtils/toByteArray is) (String. "UTF-8") json/parse-string))


(def +default+ 4)

(defn clj-serializer [^OutputStream os obj]
  (let [dos (DataOutputStream. os)]
    (Serializer/serialize dos obj)
    (.flush dos)))

(defn clj-deserializer [^InputStream is]
  (Serializer/deserialize (DataInputStream. is)))

(defmethod serialize-impl +default+ [_ ^OutputStream os x]
  (clj-serializer (SnappyOutputStream. os) x))

(defmethod deserialize-impl +default+ [_ ^InputStream is]
  (clj-deserializer (SnappyInputStream. is)))


(def +default-seq+ 5)

(defmethod serialize-impl +default-seq+ [_ ^OutputStream os xs]
  (let [o (DataOutputStream. (SnappyOutputStream. os))]
    (doseq [x xs] (Serializer/serialize o x))
    (.flush o)))


(defn lazy-deserialize [^InputStream is finally-fn]
  (let [is (DataInputStream. (SnappyInputStream. is))]
    ((fn lazy-deserialize-helper []
       (lazy-seq
        (try
          (cons (Serializer/deserialize is)
                (lazy-deserialize-helper))
          (catch java.io.EOFException e
            (finally-fn)
            nil)))))))

(defmethod deserialize-impl +default-seq+ [_ ^InputStream is]
  (lazy-deserialize is (constantly nil)))


(def +default-uncompressed+ 6)

(defmethod serialize-impl +default-uncompressed+ [_ ^OutputStream os x]
  (clj-serializer os x))

(defmethod deserialize-impl +default-uncompressed+ [_ ^InputStream is]
  (clj-deserializer is))

(def +clojure-snappy+ 7)

(defmethod serialize-impl +clojure-snappy+ [_ ^OutputStream os x]
  (with-open [w (java-io/writer (SnappyOutputStream. os))]
    (binding [*out* w]
      (pr x))))

(defmethod deserialize-impl +clojure-snappy+ [_ ^InputStream is]
  (with-open [r (java.io.PushbackReader. (java-io/reader (SnappyInputStream. is)))]
    (read r)))

(defn round-trip
  "Serializes and deserializes an object for checking serializability"
  [obj & [serialize-method]]
  (->> obj
       (serialize (or serialize-method +default+))
       deserialize))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Make an input stream with +default+ serialization out of a lazy sequence

(defn- chunk-seq [xs chunk-size]
  (let [b (ByteArrayOutputStream.)
        extract-chunk! (fn [] (let [bytes (.toByteArray b)] (.reset b) bytes))
        d (DataOutputStream. (SnappyOutputStream. b))
        chunk-size (int chunk-size)]
    ((fn serializer [xs]
       (when xs
         (loop [xs xs]
           (if (not xs)
             (do (.flush d)
                 [(extract-chunk!)])
             (do (Serializer/serialize d (first xs))
                 (if (>= (.size b) chunk-size)
                   (cons (extract-chunk!) (lazy-seq (serializer (next xs))))
                   (recur (next xs))))))))

     (seq xs))))

(defn ^InputStream seq->ring-input-stream [xs & [chunk-size]]
  (->> (chunk-seq xs (or chunk-size 65536))
       (cons (byte-array [(Byte. (byte +default-seq+))]))
       (SeqInputStream.)))

(defn clj->input-stream [x & [chunk-size]]
  (if (sequential? x)
    (seq->ring-input-stream x chunk-size)
    (ByteArrayInputStream. ^bytes (serialize +default+ x))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Helpers

;; TODO: this still makes it easy to leak file handles by not consuming lazy seq results.
(defn deserialize-and-close-stream
  "Deserialize from stream and then close it.  Closes the stream immediately except for
   streaming results, for which the entire lazy seq must be realized before the stream is closed."
  [^InputStream is]
  (try (let [t (.read is)]
         (if (= t +default-seq+)
           (lazy-deserialize is #(.close is))
           (with-open [is is] (deserialize-impl t is))))
       (catch Throwable t
         (.close is)
         (throw t))))

;;; For storage in dynamo, etc.

(defn utf8-serialize
  ([x] (utf8-serialize +default+ x))
  ([t x]
     (String.
      (->> x
           (serialize t)
           (Base64/encodeBase64))
      "UTF-8")))

(defn utf8-deserialize [^String x]
  (->> (.getBytes x "UTF-8")
       Base64/decodeBase64
       deserialize))

;;; For packing multiple objects into a single byte array with a hard size limit.

(defn fill!
  "Stuff byte arrays into byte buffer, returning whatever wouldn't fit."
  [^ByteBuffer bb byte-arrays]
  (when-let [[^bytes f & more] (seq byte-arrays)]
    (if (<= (+ 4 (alength f)) (.remaining bb))
      (do (.putInt bb (alength f))
          (.put bb f)
          (recur bb more))
      byte-arrays)))

(defn safe-limit
  "The maximal size in bytes of uncompressed data that is guaranteed to be compressible
   into at most max-size bytes."
  [max-size]
  (->> (range)
       (take-while #(<= (Snappy/maxCompressedLength %) max-size))
       last))

(defn get-bytes
  ([^ByteBuffer bb]
     (get-bytes bb (.remaining bb)))
  ([^ByteBuffer bb ^long n]
     (let [a (byte-array n)]
       (.get bb a)
       a)))

(defn stream-packer
  "Return a function that can be passed byte arrays to concatenate and compress
   into compressed chunks of some integer number of inputs, guaranteed to be
   at most max-size.   Returns sequences of byte array that can be unpacked
   into seqs using the unpack fn.

   Each input array must be slightly smaller than max-size, or an exception is
   thrown.

   The buffer can also be flushed by calling with zero arguments, which returns a
   sequence of blocks representing all the remaining data in the buffer.

   Because none of the available compression methods can do true streaming,
   uses a binary search-type method to try to get close to max-size without
   using too much cpu.

   Does outer compression, so typically byte arrays passed in should not be
   precompressed."
  [^long max-size]
  (let [uncompressed (ByteBuffer/allocateDirect (* max-size 100))
        safe-limit (- (safe-limit max-size) 4)
        compressed (ByteBuffer/allocateDirect (Snappy/maxCompressedLength (* max-size 100)))
        state (atom {:next-size 1 :data nil :buffer []})

        consume! (fn [i]
                   ;; attempt to move up to i datums from buffer into the data array,
                   ;; returning true if i datums are successfully moved.
                   (letk [[next-size buffer] @state]
                     (assert (>= (count buffer) i))
                     (assert (pos? i))
                     (let [o (.duplicate uncompressed)
                           [process more] (split-at i buffer)
                           remaining (fill! o process)
                           new-size (.position o)]
                       (when (> (.position o) (.position uncompressed))
                         (do (.clear compressed)
                             (.flip o)
                             (Snappy/compress o compressed)
                             (when (<= (.remaining compressed) max-size)
                               (let [b (get-bytes compressed)]
                                 (.position uncompressed new-size)
                                 (reset! state
                                         {:next-size (* 1.5 next-size)
                                          :data b
                                          :buffer (vec (concat remaining more))})
                                 (empty? remaining))))))))
        consume-if-needed! (fn []
                             ;; keep consuming elements from the buffer in next-size
                             ;; chunks (which grows exponentially starting at 1)
                             ;; until we don't have next-size elements left or
                             ;; adding next-size elements will exceed max-size.
                             ;; returns true iff the latter, in which case we
                             ;; need to commit! to return block(s) to the caller.
                             (letk [[next-size buffer] @state]
                               (when (>= (count buffer) next-size)
                                 (if (consume! (long next-size))
                                   (recur)
                                   true))))
        commit! (fn commit! []
                  ;; flush the latest data block, and any additional full blocks
                  ;; that are produced by consume-if-needed on the remaining
                  ;; buffered data.
                  (letk [[data buffer] @state]
                    (when-not data (assert (empty? buffer)))
                    (when data
                      (.clear uncompressed)
                      (reset! state {:next-size 1 :data nil :buffer buffer})
                      (if (consume-if-needed!)
                        ;; when cannot consume more
                        (cons data (commit!))
                        [data]))))]
    (fn packer
      ([] (locking uncompressed
            (letk [[buffer] @state]
              (if (or (empty? buffer) (consume! (count buffer)))
                (commit!)
                (vec (concat (commit!) (packer)))))))
      ([^bytes x]
         (locking uncompressed
           (when (> (alength x) safe-limit)
             (throw (RuntimeException. "Object too large.")))
           (swap! state update :buffer conj x)
           (when (consume-if-needed!)
             (commit!)))))))

(defn unpack
  "Unpack an array from stream-packer."
  [^bytes pack]
  (let [bb (ByteBuffer/wrap (Snappy/uncompress pack))]
    (loop [res []]
      (if (zero? (.remaining bb))
        res
        (let [start (+ 4 (.position bb))
              s (.getInt bb)]
          (recur (conj res (get-bytes bb s))))))))

(defn serialized-stream-packer
  [max-size serialize-method]
  (let [p (stream-packer max-size)]
    (fn serialized-packer
      ([] (p))
      ([x] (p (serialize serialize-method x))))))

(defn serialized-unpack
  [pack]
  (map deserialize (unpack pack)))

(defn pack
  "Helper for tests that packs a sequence of items and returns blocks"
  [packer data]
  (let [lead-blocks (vec (mapcat packer data))]
    (vec (concat lead-blocks (packer)))))

(set! *warn-on-reflection* false)
