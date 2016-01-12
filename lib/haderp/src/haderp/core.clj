(ns haderp.core
  "Main namespace for integrating emr with clojure-haderp to define
   and launch jobs.

   Provides a job definition language that allows, from the repl
    - create an uberjar for current project
    - optionally create / upload input data
    - (optionally create a cluster) and submit a job to it"
  (:refer-clojure :exclude [run!])
  (:use plumbing.core)
  (:require
   [clojure.string :as str]
   [schema.core :as s]
   [plumbing.graph :as graph]
   [plumbing.io :as io]
   [plumbing.string :as string]
   [store.bucket :as bucket]
   [store.s3 :as s3]
   [aws.core :as aws]
   [crane.config :as crane-config]
   [crane.core :as crane]
   [crane.osx :as osx]
   [crane.ssh :as ssh]
   [aws.emr :as emr]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: helpers

(def creds (delay (try (crane-config/read-dot-crane)
                       (catch Throwable t aws/+iam-creds+))))
(def emr (delay (emr/emr @creds)))

(def +job-bucket-name+ "grabbag-emr")
(def +absolute-cluster-log-path+ (format "s3://%s/cluster-logs/" +job-bucket-name+))

(def job-bucket
  (graph/instance bucket/bucket-resource []
    {:type :s3
     :name +job-bucket-name+
     :key-prefix ""
     :serialize-method :raw}))

(def +jar-path+ "job.jar")
(def +input-path+ "input")
(def +output-path+ "output/")

(defn uberjar! []
  (println "\nCreating project uberjar...")
  (->> (crane/safe-run-local-cmd ["lein" "uberjar" "clojure_hadoop.job"])
       (re-find #"Created (.*?standalone\.jar)")
       second
       (<- (doto assert))))

(defn ->bytes [input-data]
  (cond (instance? (Class/forName "[B") input-data)
        input-data

        (string? input-data)
        (.getBytes ^String input-data "UTF-8")

        :else
        (do (s/validate [String] input-data)
            (->bytes (str/join "\n" input-data)))))

(defn upload! [bucket key bytes & [context]]
  (let [path (s3/s3-url bucket key)]
    (println (format "\nUploading %s to %s%s (%3.5f MB)..."
                     key path context (/ (count bytes) 1024.0 1024.0)))
    (bucket/put bucket key bytes)
    (println "Done uploading" key)
    path))

(defn hadoop-cli-args [job-sym input-path output-path]
  ["-job" (str job-sym)
   "-input" input-path
   "-output" output-path])

(defnk prep-job!
  "Return job-config plus job-id."
  [job-sym
   [:step-opts name {input-path nil} {input-data nil} & step-opts]
   {creds @creds}
   {job-id (str (millis) "-" name)}
   {job-bucket (s3/sub-bucket (job-bucket {:ec2-keys creds}) (str job-id "/"))}]
  (assert (= 1 (count-when identity [input-path input-data])))
  (assert (empty? (bucket/keys job-bucket)))
  (let [local-jar (uberjar!)
        remote-jar (upload! job-bucket +jar-path+ (io/file-bytes local-jar) (str " from " local-jar))
        output-path (s3/s3-url job-bucket +output-path+)
        input-path (or input-path
                       (upload! job-bucket +input-path+ (->bytes input-data)))]
    {:job-info {:job-path (s3/s3-url job-bucket "")
                :job-bucket job-bucket
                :output-path output-path
                :output-bucket (s3/sub-bucket job-bucket +output-path+)
                :jar-path remote-jar}
     :step-config (merge
                   {:name name
                    :jar-path remote-jar
                    :main-class nil
                    :cli-args (hadoop-cli-args job-sym input-path output-path)}
                   step-opts)}))

(def +default-instances-opts+
  {:master-instance-type "m3.xlarge"
   :key-name "learner"
   :availability-zone "us-east-1d"
   :core-instances {:num 2 :type "m3.xlarge"}
   :hadoop-version "2.4.0"})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(s/defschema QualifiedSymbol
  (s/both clojure.lang.Symbol (s/pred namespace 'namespace)))

(s/defschema JobInfo
  {:job-path String
   :job-bucket s/Any
   :output-path String
   :output-bucket s/Any
   :jar-path String
   :cluster-id String})

(s/defn ^:private local-runner [run! step-opts]
  (io/with-test-dir [d "/tmp/hadoop-job"]
    {:job (run! {:input (or (:input-path step-opts)
                            (.getAbsolutePath
                             (io/copy-to-temp-file
                              (->bytes (safe-get step-opts :input-data))
                              ".txt")))
                 :output d})
     :output (into {} (bucket/seq (bucket/bucket {:type :fs :path d :name "" :serialize-method :raw})))}))

(s/defn run-in-jvm!
  "Run this job-sym inside the current jvm.  Dynamically loads clojure-hadoop.job so
   we don't have to pull the hadoop jars into this project."
  [job-sym
   step-opts]
  (require 'clojure-hadoop.job)
  (local-runner #((resolve 'clojure-hadoop.job/run) (merge ((resolve job-sym)) %)) step-opts))

(def ^:dynamic *hadoop-path* (str (System/getProperty "user.home") "/sw/hadoop-2.6.0/bin/hadoop"))

(defn run-locally!
  "Run the job in a local installation of hadoop"
  [job-sym step-opts]
  (assert (.exists (java.io.File. ^String *hadoop-path*)))
  (let [jar (uberjar!)]
    (local-runner
     (fnk [input output]
       (let [command (concat
                      [*hadoop-path* "jar" jar]
                      (hadoop-cli-args job-sym input output))]
         (println "\nLaunching hadoop with command: " (str/join " " command))
         (crane/safe-run-local-cmd command)))
     step-opts)))


(s/defn launch! :- JobInfo
  "emr-job-config must have at least :instances (merged with +default-instances-opts+ above),
     and can include other overrides for emr/run-job-flow-request).
   step-opts are parameters to be merged into the step from emr/step-config, plus either:
    - input-data, a byte array, String or list of strings, or
    - input-path, an s3 path from which to find input-data.
   Returns map with info about job.  By default, cluster will terminate after job."
  [emr-job-config
   job-sym :- QualifiedSymbol
   step-opts]
  (assert (not (:steps emr-job-config)))
  (let [cluster-name (or (:name emr-job-config) (namespace job-sym))]
    (letk [[job-info step-config] (prep-job! {:job-sym job-sym
                                              :step-opts (merge {:name cluster-name} step-opts)})]
      (println "\nLaunching cluster!...")
      (assoc job-info
        :cluster-id (emr/start-job-flow!
                     @emr
                     (merge
                      {:steps [step-config]
                       :log-uri (str +absolute-cluster-log-path+ cluster-name "/")
                       :name cluster-name}
                      (update emr-job-config :instances (partial merge +default-instances-opts+))))))))

(s/defn run! :- JobInfo
  "Like launch!, but uses existing cluster."
  [cluster-id :- String
   job-sym :- QualifiedSymbol
   step-opts]
  (let [cluster-name (emr/cluster-name (emr/describe-cluster @emr cluster-id))]
    (letk [[job-info step-config] (prep-job! {:job-sym job-sym
                                              :step-opts (merge {:name cluster-name} step-opts)})]
      (println "\nAdding step to cluster!...")
      (emr/add-steps! @emr cluster-id [step-config])
      (assoc job-info :cluster-id cluster-id))))

(defn- byte-array-concat [byte-arrays]
  (let [out (byte-array (sum count byte-arrays))]
    (loop [i 0 arrs byte-arrays]
      (when-let [[^bytes a & more] (seq arrs)]
        (System/arraycopy a 0 out i (alength a))
        (recur (+ i (alength a)) more)))
    out))

(defn results
  "Get the results and concatenate into a single byte array."
  [output-bucket]
  (assert (bucket/get output-bucket "_SUCCESS") "job was successful")
  (-> (s3/sub-bucket output-bucket "part")
      (bucket/pseq 10)
      (->> (map second))
      byte-array-concat))

(defn results->map
  [^bytes job-output]
  (->> (.split (String. job-output "UTF-8") "\n")
       (map #(vec (.split ^String % "\t")))
       (into {})))

(def result-map (comp results->map results))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Sshing to clusters and doing lower-level hadoop stuff (programmatic)

(defn ssh-master-command
  "Command to ssh to the master.  forwards ports for web UIs."
  [cluster-id]
  (let [host (emr/job-flow-master @emr cluster-id)]
    (format "ssh -i ~/.crane.pem%s hadoop@%s"
            (apply str
                   (for [port [9026 9101 9046]]
                     (format " -L %s:%s:%s" port host port)))
            host)))

(defn run-on-master! [cluster-id command]
  (ssh/run-remote-cmds
   (assoc @creds :user "hadoop") (emr/job-flow-master @emr cluster-id)
   [command]))

(defn hadoop-job-info
  "Get low-level job ids and management urls."
  [cluster-id]
  (->> (run-on-master! cluster-id "bin/hadoop job -list")
       first
       (java.io.StringReader.)
       (java.io.BufferedReader.)
       line-seq
       (drop-while #(not (.startsWith ^String % "Total jobs:")))
       drop-last
       next
       string/from-tsv))

(defn kill-all-jobs! [cluster-id]
  (doseq [job (hadoop-job-info cluster-id)]
    (letk [[job-id] job]
      (println "Killing job" job-id)
      (run-on-master! cluster-id (format "bin/hadoop job -kill %s" job-id)))))

(defn get-split
  "Get the data processed by a partiular mapper.  Takes a url from the loc like:
   s3://bucket/bla:start+length"
  [s3-url]
  (let [[_ bucket key start length] (re-find #"s3://(.*?)/(.*):(\d+)\+(\d+)" s3-url)
        start (Long/parseLong start)
        length (Long/parseLong length)]
    (java.util.Arrays/copyOfRange
     ^bytes
     (bucket/get
      (bucket/bucket-resource {:ec2-keys @creds :type :s3 :serialize-method :raw :name bucket})
      key)
     start (+ start length))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Opening UIs to clusters.

(defn ssh-master! [cluster-id]
  (osx/open-in-terminal! (ssh-master-command cluster-id)))

(defn open-cluster-web-ui! [cluster-id]
  (ssh-master! cluster-id)
  (osx/open-url! "http://localhost:9026"))

(defn open-name-node-web-ui! [cluster-id]
  (ssh-master! cluster-id)
  (osx/open-url! "http://localhost:9011"))

(defn open-job-web-ui! [cluster-id]
  (ssh-master! cluster-id)
  (doseq [job (hadoop-job-info cluster-id)]
    (osx/open-url! (.replaceAll ^String (safe-get job :am-info) "http://.*:9046" "http://localhost:9046"))))


(set! *warn-on-reflection* false)
