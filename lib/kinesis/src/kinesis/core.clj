(ns kinesis.core
  "Shared utils between publisher and client, such as message framing / schemas."
  (:use plumbing.core)
  (:require
   [plumbing.serialize :as serialize])
  (:import
   [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Encoding and decoding messages into records.

(defn- prepend-timestamp [^bytes b]
  (let [bb (ByteBuffer/allocate (+ 8 (alength b)))]
    (.putLong bb (millis))
    (.put bb b)
    (.array bb)))

(defn record-encoder
  "Create an encoder function that can be passed Clojure messages, and periodically emits
   sequences of byte arrays corresponding to Kinesis records.  Each byte array will have up
   to 50k of compressed and +default+-serialized messages, plus a timestamp to track
   latency of kinesis processing."
  []
  (comp #(mapv prepend-timestamp %)
        (serialize/serialized-stream-packer
         49992 ;; 50k kinesis limit minus 8-byte timestamp
         serialize/+default-uncompressed+)))

(defn decode-record
  "Decode a byte array from 'record-encoder' into a timestamp and sequence of messages."
  [^bytes m]
  (let [bb (ByteBuffer/wrap m)
        date (.getLong bb)]
    {:messages (serialize/serialized-unpack (serialize/get-bytes bb))
     :date date}))

(defn env-stream
  "Construct an env'd kinesis stream name from a base name and environment."
  [env stream]
  (str stream "-" (name env)))

(set! *warn-on-reflection* false)
