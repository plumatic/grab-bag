(ns aws.emr
  "EMR library, adapted from https://github.com/TheClimateCorporation/lemur"
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [aws.core :as aws])
  (:import
   [com.amazonaws.auth AWSCredentialsProvider]
   [com.amazonaws.services.elasticmapreduce AmazonElasticMapReduce
    AmazonElasticMapReduceClient]
   [com.amazonaws.services.elasticmapreduce.model ActionOnFailure
    AddJobFlowStepsRequest
    Cluster
    DescribeClusterRequest
    DescribeJobFlowsRequest
    HadoopJarStepConfig
    InstanceGroupConfig
    JobFlowDetail
    JobFlowInstancesConfig
    KeyValue
    PlacementType
    RunJobFlowRequest
    StepConfig
    TerminateJobFlowsRequest]))


(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public:  Client

(defnk emr :- AmazonElasticMapReduceClient
  [key secretkey]
  (doto (AmazonElasticMapReduceClient. (aws/credentials-provider key secretkey))
    (.setEndpoint (throw (Exception. "ENDPOINT")))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public:  Creating and destroying job flows

(defnk instance-group-config :- InstanceGroupConfig
  [group :- String type :- String num :- s/Int {spot-price :- (s/maybe String) nil}]
  (let [g (InstanceGroupConfig. group type (Integer. (int num)))]
    (when spot-price
      (.setBidPrice g (str spot-price))
      (.setMarket g "SPOT"))
    g))

(defnk instances-config :- JobFlowInstancesConfig
  "Instances for a job:
    - key-name: AWS keypair name for instances
    - master-instance-type: instance type for the master node
    - core-instances and task-instances: maps with type, num and optionally spot-price
    - keep-alive?: whether to keep the cluster alive after job finishes
    - subnet-id: VPC subnet id
    - other options should be obvious."
  [key-name
   master-instance-type
   {core-instances {:num 0}}
   {task-instances {:num 0}}
   {keep-alive? false} {hadoop-version nil} {availability-zone nil} {subnet-id nil}]
  (let [jf (doto (JobFlowInstancesConfig.)
             (.setKeepJobFlowAliveWhenNoSteps keep-alive?)
             (.setInstanceGroups (for [g [{:group "MASTER" :type master-instance-type :num 1}
                                          (assoc core-instances :group "CORE")
                                          (assoc task-instances :group "TASK")]
                                       :when (pos? (:num g))]
                                   (instance-group-config g)))
             (.setEc2KeyName key-name))]
    (when hadoop-version
      (.setHadoopVersion jf hadoop-version))
    (when availability-zone
      (.setPlacement jf (PlacementType. availability-zone)))
    (when subnet-id
      (.setEc2SubnetId jf subnet-id))
    jf))

(def +failure-actions+
  {:cancel-and-wait ActionOnFailure/CANCEL_AND_WAIT
   :terminate-job-flow ActionOnFailure/TERMINATE_JOB_FLOW})

(defnk step-config :- StepConfig
  [name jar-path main-class {cli-args :- [String] []} {failure-action :terminate-job-flow} {properties {}}]
  (doto (StepConfig.
         name
         (doto (HadoopJarStepConfig.)
           (.setJar jar-path)
           (.setMainClass main-class)
           (.setArgs (vec cli-args)) ;collection of strings
           (.setProperties (map (fn [[k v]] (KeyValue. k v)) properties))))
    (.setActionOnFailure (str (safe-get +failure-actions+ failure-action)))))

(s/defn ->step :- StepConfig
  [c]
  (if (instance? StepConfig c) c (step-config c)))

(defnk run-job-flow-request :- RunJobFlowRequest
  "Create a request for job flow; options are:
    - name: the name for the cluster, displayed in AWS
    - instance: see instances-config above
    - steps: a sequence of StepConfigs or arguments to step-config
    - job-flow-role: an IAM role for the job flow
    - service-role: the IAM role that will be used to access AWS from EMR.
    - ami-version hich AMI to use (see RunJobFlowRequest#setAmiVersion in the AWS Java SDK)
    - others are straight passthroughs to AWS API."
  [name instances steps
   {log-uri nil} {job-flow-role "EMR_EC2_DefaultRole"} {service-role "EMR_DefaultRole"}
   {ami-version "latest"} {visible-to-all-users true} {supported-products []}]
  (doto (RunJobFlowRequest.)
    (.setName name)
    (.setInstances (instances-config instances))
    (.setSteps (mapv ->step steps))
    (.setLogUri log-uri) ;can be nil (i.e. no logs)
    (.setAmiVersion ami-version)
    (.setServiceRole service-role)
    (.setJobFlowRole job-flow-role)
    (.setSupportedProducts supported-products)
    (.setVisibleToAllUsers visible-to-all-users)))

(s/defn ->run-job-flow-request :- RunJobFlowRequest
  [request]
  (if (instance? RunJobFlowRequest request)
    request
    (run-job-flow-request request)))

(s/defn start-job-flow! :- String
  [^AmazonElasticMapReduceClient emr request]
  (.getJobFlowId (.runJobFlow emr (->run-job-flow-request request))))

(defn add-steps!
  "Add a step to a running jobflow. Steps is a seq of StepConfig objects.
  Use (step-config) to create StepConfig objects."
  [^AmazonElasticMapReduceClient emr job-flow-id steps]
  (.addJobFlowSteps emr (AddJobFlowStepsRequest. job-flow-id (map ->step steps))))

(defn terminate-job-flow!
  [^AmazonElasticMapReduceClient emr job-flow-id]
  (.terminateJobFlows emr (TerminateJobFlowsRequest. ^java.util.List [job-flow-id])))

(s/defn describe-cluster :- Cluster
  [emr :- AmazonElasticMapReduce cluster-id]
  (.getCluster (.describeCluster emr (.withClusterId (DescribeClusterRequest.) cluster-id))))

(s/defn cluster-name :- String
  [cluster :- Cluster]
  (.getName cluster))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: Querying about job flows

(defn ^JobFlowDetail job-flow-detail
  [^AmazonElasticMapReduceClient emr job-flow-id]
  (->> [job-flow-id]
       (DescribeJobFlowsRequest.)
       (.describeJobFlows emr)
       .getJobFlows
       first))

(defn job-flow-status
  "Return the state of the specified cluster."
  [emr job-flow-id]
  (.. (job-flow-detail emr job-flow-id) getExecutionStatusDetail getState))

(s/defn job-flow-master :- String
  "Return the master public DNS name."
  [emr job-flow-id]
  (.getMasterPublicDnsName (describe-cluster emr job-flow-id)))

(set! *warn-on-reflection* false)
