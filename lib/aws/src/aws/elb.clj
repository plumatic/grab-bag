(ns aws.elb
  (:refer-clojure :exclude [remove])
  (:use plumbing.core)
  (:require
   [plumbing.logging :as log]
   [aws.core :as aws])
  (:import
   [com.amazonaws.auth AWSCredentialsProvider]
   [com.amazonaws.services.elasticloadbalancing AmazonElasticLoadBalancingClient]
   [com.amazonaws.services.elasticloadbalancing.model DeregisterInstancesFromLoadBalancerRequest
    DescribeInstanceHealthRequest
    Instance
    RegisterInstancesWithLoadBalancerRequest]))

(set! *warn-on-reflection* true)

(defnk client :- AmazonElasticLoadBalancingClient [key secretkey]
  (AmazonElasticLoadBalancingClient. (aws/credentials-provider key secretkey)))

(defn instances [ec2-creds load-balancer-name]
  (->> (DescribeInstanceHealthRequest. load-balancer-name)
       (.describeInstanceHealth (client ec2-creds))
       .getInstanceStates
       (map (comp #(select-keys % [:instanceId :reasonCode :state]) bean))))

(defn add [ec2-creds load-balancer-name inst-ids]
  (->> inst-ids
       (map #(Instance. %) )
       (RegisterInstancesWithLoadBalancerRequest. load-balancer-name)
       (.registerInstancesWithLoadBalancer (client ec2-creds))
       .getInstances
       (map #(.getInstanceId ^Instance %))))

(defn remove [ec2-creds load-balancer-name inst-ids]
  (->> inst-ids
       (map #(Instance. %) )
       (DeregisterInstancesFromLoadBalancerRequest. load-balancer-name)
       (.deregisterInstancesFromLoadBalancer (client ec2-creds))
       .getInstances
       (map #(.getInstanceId ^Instance %))))

(defn healthy? [ring-inst]
  (= (:state ring-inst) "InService"))

(defn elb-action [ec2-keys elb-names action-name action]
  (doseq [ring-name elb-names]
    (let [insts (action ec2-keys ring-name [(System/getProperty "crane.instanceId")])]
      (log/infof "%s self to/from elb %s, final instances:" action-name ring-name (pr-str insts)))))

(set! *warn-on-reflection* false)
