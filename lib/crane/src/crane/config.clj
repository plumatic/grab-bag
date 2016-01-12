(ns crane.config
  "Schemas and utilities for working with new-style crane config.clj files."
  (:use plumbing.core)
  (:require
   [clojure.java.io :as java-io]
   [schema.core :as s]
   [plumbing.map :as map]
   [aws.ec2 :as ec2]
   [crane.ssh :as ssh]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; The final config format

(s/defschema EBS {(s/optional-key :snapshot) String
                  (s/optional-key :device) String
                  (s/optional-key :size) long
                  (s/optional-key :id) String})

(s/defschema Owner
  "Values for the `owner` tag, used for billing accounting"
  (s/enum
   "grabbag-corp" ;; e.g. blog, monitoring
   "grabbag-news"
   "content-ingest" ;; main doc pipeline
   "ads-model" ;; should also have customer tag
   "ads-dashboard" ;; should also have customer tag
   "content-index" ;; machines populating and storing feeds API index
   "public-api"))

(s/defschema Machine
  {:zone String
   :groups [String]
   :instance-type s/Keyword
   :image String
   :user String
   :ebs [EBS]
   :replicated? Boolean
   :tags {:owner Owner
          s/Keyword String}})

(s/defschema Service
  {:type String
   :service-name-override (s/maybe (s/named String "service name-env override"))
   :elb-names [String]
   :server-port long ;; services no longer have a builtin server, but this still goes in nameserver.  maybe remove?
   :profile? Boolean
   :max-gb long
   :jvm-opts String
   :install-nodejs? Boolean
   :pseudo-env (s/named s/Keyword "A real env or an element of envs")})

(s/defschema Address {:host String :port long})
(s/defschema Addresses {:public Address :private Address})

(s/defschema Instance
  {:service-name (s/named String "env/instance-id'd version of service type")
   :instance-id String
   :addresses Addresses})

(def +envs+ #{:test :local :prod :stage})
(s/defschema Env (apply s/enum +envs+))

(s/defschema AbstractConfig
  "A service config, not yet bound to a specific instance"
  {:machine Machine
   :service Service
   :env Env
   (s/named s/Keyword "Parameter") s/Any})

(s/defschema Config
  "The final config for a single instance of the service.
   The input to the service graph."
  (assoc AbstractConfig
    :instance Instance
    :ec2-keys ec2/EC2Creds))

;; no more service-name; replaced by [instance service-name]

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helpers for dealing with configs

(defn remote? [env] (#{:prod :stage} env))
(defn replicated? [config]
  (safe-get-in config [:machine :replicated?]))
(defn public-host [config]
  (safe-get-in config [:instance :addresses :public :host]))
(declare enved-service-type)
(defnk base-service-name [service env]
  (enved-service-type service env))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema for config.clj

(s/defschema EnvConfigSpec
  {(s/optional-key :machine) (map-keys s/optional-key Machine)
   (s/optional-key :service) (map-keys s/optional-key Service)
   (s/optional-key :parameters) {s/Keyword s/Any}})

(s/defschema ConfigSpec
  "A schema for config.clj, which allows missing values and env-specific overrides.
   The inner env must be present for non-standard envs."
  (assoc EnvConfigSpec
    (s/optional-key :envs)
    {s/Keyword (assoc EnvConfigSpec (s/optional-key :env) Env)}))

(s/defn ^:always-validate str->config-spec :- ConfigSpec
  "For now, just read-string, but eventaully we may want to eval or something."
  [s :- String]
  (read-string s))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Applying env-specific overrides to a ConfigSpec to get an EnvedConfigSpec

(s/defschema EnvedConfigSpec
  "A config with the env-specific overrides applied."
  (assoc EnvConfigSpec
    :env Env))

(s/defn ^:always-validate enved-config-spec :- EnvedConfigSpec
  [config-spec :- ConfigSpec
   env :- s/Keyword]
  (let [env-spec (get-in config-spec [:envs env])]
    (if (+envs+ env)
      (assert (= env (get env-spec :env env)))
      (assert (+envs+ (get env-spec :env))))
    (merge-with
     merge
     (dissoc config-spec :envs)
     (merge {:env env} env-spec)
     {:service {:pseudo-env env}})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Applying default values to an EnvedConfigSpec to get an AbstractConfig

(defn- enved-service-type [service-spec env]
  (or (get service-spec :service-name-override)
      (str (safe-get service-spec :type) "-" (name env))))

(defn- with-machine-defaults [machine-spec service-spec env]
  (let [instance-type (:instance-type machine-spec :cc2.8xlarge)]
    (merge
     {:zone "us-east-1d"
      :instance-type instance-type
      :groups (or (:groups machine-spec)
                  ["woven" (str "grabbag-" (enved-service-type service-spec env))])
      :image (or (:image machine-spec)
                 (safe-get-in ec2/instance-types [instance-type :image]))
      :user "ubuntu"
      :ebs []
      :replicated? false}
     machine-spec)))

(defn- with-service-defaults [service-spec env machine-spec]
  (let [max-gb (or (:max-gb service-spec)
                   (safe-get-in ec2/instance-types [(:instance-type machine-spec) :max-gb]))]
    (merge
     {:service-name-override nil
      :elb-names nil
      :server-port -1 ;; no server by default, you can put one to get it in nameserver.
      :max-gb max-gb
      :profile? true
      :install-nodejs? false
      :jvm-opts (str
                 (or (:jvm-opts service-spec) (format " -Xmx%sg " max-gb))
                 " -server -XX:HeapDumpPath=/mnt/java_pid.hprof -XX:MaxPermSize=256M -XX:+CMSClassUnloadingEnabled "
                 (when (:profile? service-spec true)
                   " -agentpath:/home/ubuntu/yourkit/bin/linux-x86-64/libyjpagent.so=disableall"))}
     (dissoc service-spec :jvm-opts))))

(s/defn ^:always-validate abstract-config :- AbstractConfig
  [config-spec :- EnvedConfigSpec project-name :- String]
  (let [env (safe-get config-spec :env)
        service-spec (merge {:type project-name} (:service config-spec))
        machine (with-machine-defaults (:machine config-spec) service-spec env)]
    (map/merge-disjoint
     {:machine machine
      :service (with-service-defaults service-spec env machine)
      :env env}
     (:parameters config-spec))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Producing a final config from an abstract-config, ec2 keys, and instance id.

(defn- service-name [abstract-config instance-id]
  (str (enved-service-type
        (safe-get abstract-config :service)
        (safe-get abstract-config :env))
       (if (replicated? abstract-config) (str "-" instance-id) "")))

(defn- addresses [abstract-config ec2-instance]
  (letk [[publicDnsName privateDnsName] ec2-instance
         [[:service server-port]] abstract-config]
    (map-vals
     (fn [^String addr] {:port server-port :host (.replaceAll addr "http://" "")})
     {:public publicDnsName :private privateDnsName})))

(s/defn ^:always-validate config :- Config
  [abstract-config :- AbstractConfig
   ec2-creds :- ec2/EC2Creds
   ec2-instance]
  (let [instance-id (safe-get ec2-instance :instanceId)]
    (assoc abstract-config
      :instance {:service-name (service-name abstract-config instance-id)
                 :instance-id instance-id
                 :addresses (addresses abstract-config ec2-instance)}
      :ec2-keys ec2-creds)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Producing a set of final configs from abstract-config and crane params.

;;; Reading .crane file to get EC2Creds

(defn dot-crane-path []
  (java-io/file (System/getProperty "user.home") ".crane"))

(defn expand-tilde [^String path]
  (if (and (>= (.length path) 2) (= "~/" (.substring path 0 2)))
    (str (System/getProperty "user.home") (.substring path 1))
    path))

(s/defn read-dot-crane :- ec2/EC2Creds
  [& [path]]
  (-> (or path (dot-crane-path))
      slurp
      read-string
      (update-in [:key-path] expand-tilde)))


;;; Dealing with instances and slave args.

(defn- instances [ec2-creds abstract-config slave-arg]
  "Return the set of EC2 instances matching this abstract-config and slave arg (if replicated)"
  (let [instances (ec2/running-instances
                   (ec2/ec2 ec2-creds)
                   (safe-get-in abstract-config [:machine :groups]))]

    (if (and (replicated? abstract-config) (not= slave-arg "all"))
      (do (assert slave-arg "Must pass slave-arg (all, any, or instance-id) for replicated service")
          (if (= slave-arg "any")
            [(first instances)]
            (let [matching (filter (fnk [^String instanceId] (.contains instanceId slave-arg)) instances)]
              (assert (= (count matching) 1)
                      (format "Instance-id %s matches %s instances" slave-arg (count matching)))
              matching)))
      instances)))

(defn configs
  "Get the configs for current services matching this abstract-config and slave-arg (nil
   for non-replicated services)."
  [ec2-creds abstract-config & [slave-arg]]
  (map (partial config abstract-config ec2-creds)
       (instances ec2-creds abstract-config slave-arg)))

(defn- instances!
  "Return instances for abstract config, launching as necessary"
  [ec2-creds abstract-config slave-arg]
  (letk [[[:machine instance-type image groups zone]] abstract-config]
    (or (seq (when-not (and (replicated? abstract-config) (= slave-arg "new"))
               (instances ec2-creds abstract-config slave-arg)))
        (ec2/run-instances ec2-creds zone instance-type image groups #(ssh/ssh-able? ec2-creds %)))))

(defn configs!
  "Get the configs for services matching this abstract-config and slave-arg (nil for
   non-replicated services), creating instances as needed."
  [ec2-creds abstract-config & [slave-arg]]
  (map (partial config abstract-config ec2-creds)
       (instances! ec2-creds abstract-config slave-arg)))

(defn remote-config
  "Get the config for a remote machine, relying on environmental vars to get current inst id."
  [ec2-creds abstract-config]
  (let [instance-id (System/getProperty "crane.instanceId")
        instances (instances ec2-creds abstract-config instance-id)]
    (assert (= (count instances) 1)
            (format "Found %s instances of machine for %s" (vec instances) instance-id))
    (when (seq instance-id)
      (assert (= (safe-get (first instances) :instanceId) instance-id)
              (format "Instance id mismatch: %s %s" (first instances) instance-id)))
    (config abstract-config ec2-creds (first instances))))

(def +local-ec2-instance+
  {:instanceId "LOCAL"
   :publicDnsName "localhost"
   :privateDnsName "localhost"})

(def +test-ec2-creds+
  (map-vals (constantly "TEST") ec2/EC2Creds))

(defn local-config
  "Get the config for running locally or in test mode."
  [abstract-config & [ec2-creds]]
  (config abstract-config (or ec2-creds +test-ec2-creds+) +local-ec2-instance+))
