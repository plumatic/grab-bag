(ns hadoop-wrapper.io
  "Utilities for wrapping hadoop io"
  (:use plumbing.core)
  (:import
   [hadoop_wrapper SeekablePositionedReadableByteArrayInputStream]
   [org.apache.hadoop.conf Configuration]
   [org.apache.hadoop.fs FSDataInputStream]
   [org.apache.hadoop.io BytesWritable LongWritable SequenceFile$Reader
    SequenceFile$Reader$Option Writable]
   [java.io EOFException]
   [org.apache.hadoop.util ReflectionUtils]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(defn unpack [^Writable w]
  ;; NOTE: this may need to be extended for additional types
  (cond
   (instance? LongWritable w) (.get ^LongWritable w)
   (instance? BytesWritable w) (.getBytes ^LongWritable w)
   :else (.toString w)))

(defn reader-seq
  "Returns a lazy sequence of maps representing data within a sequence file."
  [^SequenceFile$Reader rdr key-writable val-writable]
  (try
    (if (.next rdr key-writable val-writable)
      (cons
       [(unpack key-writable) (unpack val-writable)]
       (lazy-seq (reader-seq rdr key-writable val-writable)))
      (do (.close rdr) nil))
    (catch EOFException e
      nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(defn sequence-file-reader
  "Take bytes representing a hadoop sequence file.
   Returns lazy seq of maps representing data within the file."
  [^bytes bytes]
  (let [conf (Configuration.)
        reader (->> bytes
                    SeekablePositionedReadableByteArrayInputStream.
                    FSDataInputStream.
                    SequenceFile$Reader/stream
                    vector
                    (into-array SequenceFile$Reader$Option)
                    (SequenceFile$Reader. conf))]
    (reader-seq
     reader
     (-> reader .getKeyClass (ReflectionUtils/newInstance conf))
     (-> reader .getValueClass (ReflectionUtils/newInstance conf)))))
