(ns plumbing.io
  (:use plumbing.core)
  (:require
   [clojure.java.io :as java-io]
   [clojure.string :as str]
   [schema.core :as s])
  (:import
   [java.io File]
   [org.apache.commons.io FileUtils IOUtils]))


(set! *warn-on-reflection* true)

(defprotocol PDataLiteral
  (to-data [this] "convert object to a clojure data literal"))

(defmulti from-data (fn [[type data]] type))
;; (from-data type data)

(doseq [c [clojure.lang.PersistentList,
           clojure.lang.PersistentVector,
           clojure.lang.PersistentHashMap,
           clojure.lang.PersistentTreeMap,
           clojure.lang.PersistentArrayMap,
           clojure.lang.PersistentStructMap,
           clojure.lang.PersistentHashSet
           String
           Number
           clojure.lang.Keyword,
           ]]
  (extend c PDataLiteral {:to-data (fn [x] [::identity x])}))

(defmethod from-data ::identity [[type data]] data)

(defn schematized-record
  "Call this from to-data for a schematized record to validate on read."
  [r]
  [::schematized-record r])

(defmethod from-data ::schematized-record [[_ data]]
  (s/validate (class data) data))

(defmacro with-test-dir
  "creates a tmp dir for testing within body.  deletes tmp dir after exiting body."
  [bindings & body]
  (assert (and (vector? bindings) (= (count bindings) 2)))
  (let [[name path] bindings]
    `(try (FileUtils/deleteDirectory (java-io/file ~path))
          (java-io/make-parents (java-io/file ~path "touch"))
          (let [~name ~path] ~@body)
          (finally (FileUtils/deleteDirectory (java-io/file ~path))))))

(defn ^String temp-dir
  "Create a directory that will be deleted on JVM exit."
  []
  (.getAbsolutePath
   (doto (File. (format "/tmp/grabbag_%s" (System/currentTimeMillis)))
     (.mkdir)
     (.deleteOnExit))))

(defmacro with-temp-dir
  "Like with-test-dir, but generates a name, and does not delete on exception
   until JVM exit to assist debugging."
  [dirname-sym & body]
  `(let [~dirname-sym (temp-dir)]
     (try (let [r# (do ~@body)]
            (FileUtils/deleteDirectory (java-io/file ~dirname-sym))
            r#)
          (catch Exception e#
            (throw (RuntimeException.
                    (format "Error processing in temp dir %s" ~dirname-sym)
                    e#))))))

(defn ^bytes file-bytes [^String path]
  (IOUtils/toByteArray (java-io/input-stream path)))

(defn ^File copy-to-file
  "Copyable is InputStream, Reader, File, byte[], or String."
  [copyable ^File file]
  (java-io/copy copyable file)
  file)

(defn ^File create-temp-file
  "Creates a temporary file that is conditionally deleted on JVM exit."
  [^String file-ext & [delete-on-exit?]]
  (let [file (File/createTempFile "tmp-resource" file-ext)]
    (if delete-on-exit?
      (doto file .deleteOnExit)
      file)))

(defn ^File copy-to-temp-file
  "Write the copyable data to a temporary file with new random name and given extension.
   The file is requested to be deleted on JVM exit.
   Returns the temporary file."
  [copyable ^String file-ext]
  (copy-to-file copyable (create-temp-file file-ext true)))

(defn ^File get-temp-file
  "Returns a File object for the specified name in the system temp directory.
   The file may or may not exist on disk."
  [^String file-name]
  (java-io/file (str (.get (System/getProperties) "java.io.tmpdir")) file-name))

(defn resource-to-temp-file [resource-name file-ext]
  (copy-to-temp-file (ClassLoader/getSystemResourceAsStream resource-name) file-ext))


(defn delete-quietly [f]
  (FileUtils/deleteQuietly (java-io/file f)))

(defn file-exists? [f] (.exists (java-io/file f)))

(defn list-files-recursively [f]
  (remove #(.isDirectory ^java.io.File %)
          (file-seq (java-io/file f))))

(defn delimited-stream [^java.io.InputStream is delim-pattern]
  (let [scanner (doto (java.util.Scanner. is "UTF-8")
                  (.useDelimiter ^java.util.regex.Pattern delim-pattern))]
    (iterator-seq scanner)))

(defn sh!
  "Like clojure.java.shell/sh but output streams to log, and returns nothing."
  [^java.util.List commands]
  (let [inherit java.lang.ProcessBuilder$Redirect/INHERIT
        process (.start
                 (doto (ProcessBuilder. commands)
                   (.redirectOutput inherit)
                   (.redirectError inherit)))]
    (.waitFor process)
    (assert (zero? (.exitValue process)))))

(defn- path-safe [s]
  (str/replace s #"[^\w.]" ""))

(s/defn shell-fn
  "Execute a sequence of shell commands, taking and returning string data
   as maps of input and output files."
  [inputs :- {s/Keyword String}
   outputs :- [s/Keyword]
   commands-fn
   output-fn]
  (with-temp-dir d
    (let [filenames (fn [pfx names]
                      (for-map [[i n] (indexed names)]
                        n
                        (str pfx i "_" (path-safe (name n)))))
          in-files (filenames (str d "/in") (keys inputs))
          out-files (filenames (str d "/out") outputs)]
      (doseq [[k s] inputs]
        (spit (safe-get in-files k) s))
      (doseq [commands (commands-fn {:in in-files :out out-files})]
        (sh! commands))
      (output-fn (map-vals slurp out-files)))))



(set! *warn-on-reflection* false)
