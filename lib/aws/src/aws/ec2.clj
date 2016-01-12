(ns aws.ec2
  (:use plumbing.core)
  (:require
   [clojure.set :as set]
   [schema.core :as s]
   [plumbing.logging :as log]
   [plumbing.new-time :as new-time]
   [aws.core :as aws])
  (:import
   [com.amazonaws.auth AWSCredentialsProvider]
   [com.amazonaws.services.ec2 AmazonEC2 AmazonEC2Client]
   [com.amazonaws.services.ec2.model AuthorizeSecurityGroupIngressRequest
    BlockDeviceMapping
    CreateSecurityGroupRequest
    CreateTagsRequest
    DeleteSecurityGroupRequest
    DescribeInstancesRequest
    DescribeSpotPriceHistoryRequest
    EbsBlockDevice
    IamInstanceProfileSpecification
    InstanceState InstanceType
    IpPermission Placement
    RebootInstancesRequest
    Reservation
    RunInstancesRequest
    SpotPrice
    StartInstancesRequest
    StopInstancesRequest Tag
    TerminateInstancesRequest]
   [java.util Collection]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema and client

(s/defschema EC2Creds
  "Contents of .crane file."
  {:key String
   :secretkey String
   :key-path String
   :key-name String
   :user String})

(defnk ec2 :- AmazonEC2Client
  [key secretkey]
  (AmazonEC2Client. (aws/credentials-provider key secretkey)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Groups

(defn describe-security-groups [^AmazonEC2 ec2]
  (map bean (.getSecurityGroups (.describeSecurityGroups ec2))))

(defn describe-security-group [^AmazonEC2 ec2 group]
  (first
   (filter #(= group (:groupName %)) (describe-security-groups ec2))))

(defn create-security-group
  [^AmazonEC2 ec2 ^String name ^String desc]
  (.createSecurityGroup ec2 (CreateSecurityGroupRequest. name desc)))

(defn delete-security-group
  [^AmazonEC2 ec2 ^String name]
  (.deleteSecurityGroup ec2 (DeleteSecurityGroupRequest. name)))

(defn ensure-groups [ec2 groups]
  (doseq [g groups]
    (when-not (describe-security-group ec2 g)
      (create-security-group ec2 g g))))

(defn auth-ports
  "(ec2/auth-ports ec \"default\" \"tcp\" 22 22 \"0.0.0.0/0\")"
  ([ec2 group port] (auth-ports ec2 group "tcp" port port "0.0.0.0/0"))
  ([^AmazonEC2 ec2 group protocol from-port to-port cidr-ip]
     (.authorizeSecurityGroupIngress
      ec2
      (AuthorizeSecurityGroupIngressRequest.
       group
       [(doto (IpPermission.)
          (.setIpProtocol (str protocol))
          (.setFromPort (int from-port))
          (.setToPort (int to-port))
          (.setIpRanges ^Collection [cidr-ip]))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Finding and manipulating instances

(defn reservation-instances [^Reservation r]
  (for [i (.getInstances r)]
    (assoc (bean i) :groups (.getGroupNames r))))

(defn describe-instances
  "Gets all instances, with security groups, as a map."
  [^AmazonEC2 ec2 & [instance-ids]]
  (let [req (DescribeInstancesRequest.)]
    (when (seq instance-ids)
      (.setInstanceIds req instance-ids))
    (->> (.describeInstances ec2 req)
         (.getReservations)
         (mapcat reservation-instances))))

(s/defn tag-instances!
  "Apply given tags to instances"
  [ec2 :- AmazonEC2
   instance-ids
   tags :- {s/Keyword String}]
  (let [tags-seq (for [[k v] tags]
                   (Tag. (name k) v))]
    (.createTags
     ec2
     (doto (CreateTagsRequest. )
       (.setTags tags-seq)
       (.setResources instance-ids)))))

(defn is-running? [instance]
  (= (.getName ^InstanceState (:state instance)) "running"))

(defn list-instances [ec2 groups]
  (filter #(set/subset? (set groups) (set (:groups %))) (describe-instances ec2)))

(defn running-instances [ec2 groups]
  (filter is-running? (list-instances ec2 groups)))

(defn ->security-name [service-name service-env]
  (format "grabbag-%s-%s" service-name (name service-env)))

(defn start
  [^AmazonEC2 ec2 instances]
  (.startInstances ec2 (StartInstancesRequest. ^Collection instances)))

(defn reboot [^AmazonEC2 ec2 instances]
  (.rebootInstances ec2 (RebootInstancesRequest. instances)))

(defn stop
  [^AmazonEC2 ec2 instances force]
  (.stopInstances ec2 (doto (StopInstancesRequest. ^Collection instances)
                        (.setForce (boolean force)))))

(defn terminate
  [^AmazonEC2 ec2 instance-ids]
  (.terminateInstances ec2 (TerminateInstancesRequest. instance-ids)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Spot instances

(defnk spot-price-history
  "Fetches spot instance pricing history for an instance type over a period of time,
   by default the past 24 hours."
  [ec2 :- AmazonEC2 instance-type :- String
   {start :- long (new-time/time-ago 1 :days)}
   {end :- long (millis)}
   {availability-zone :- String nil}
   {max-results :- long nil}]
  (let [req (doto (DescribeSpotPriceHistoryRequest.)
              (.setInstanceTypes [instance-type])
              (.setStartTime (java.util.Date. start))
              (.setEndTime (java.util.Date. end)))]
    (when availability-zone (.setAvailabilityZone req availability-zone))
    (when max-results (.setMaxResults req (int max-results)))
    (for [^SpotPrice sp (.getSpotPriceHistory (.describeSpotPriceHistory ec2 req))]
      {:availability-zone (.getAvailabilityZone sp)
       :timestamp (.getTime (.getTimestamp sp))
       :price (.getSpotPrice sp)})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Launching new instances

(def +default-block-devices+
  {:cc2.8xlarge ;; filthy city hack
   {;"/dev/sda1" {:snapshot "snap-093a6255" :size 100 :terminate? true}
    "/dev/xvdb" "ephemeral0" "/dev/xvdc" "ephemeral1" "/dev/xvdd" "ephemeral2" "/dev/xvde" "ephemeral3" "/dev/xvdg" "ephemeral4"}})

(defn block-device-mapping [^String d m]
  (let [bd (-> (BlockDeviceMapping.) (.withDeviceName d))]
    (if (string? m)
      (.withVirtualName bd ^String m)
      (.withEbs
       bd
       (-> (EbsBlockDevice.)
           (.withSnapshotId (safe-get m :snapshot))
           (.withVolumeSize (Integer. (int (safe-get m :size))))
           (.withDeleteOnTermination (safe-get m :terminate?)))))))

(def instance-types
  (let [default "ami-34cc7a5c"] ;; ubuntu/images/hvm-ssd/ubuntu-precise-12.04-amd64-server-20140927
    {:m1.large {:image "ami-ad184ac4" :max-gb 6 :type InstanceType/M1Large}
     :m2.xlarge {:image "ami-ad184ac4" :max-gb 15 :type InstanceType/M2Xlarge}
     :c1.medium {:image "ami-71589518" :max-gb 13 :type InstanceType/C1Medium}
     :cc1.4xlarge {:image default :max-gb 21 :type InstanceType/Cc14xlarge}
     :cc2.8xlarge {:image default :max-gb 50 :type  InstanceType/Cc28xlarge}
     :r3.xlarge {:image default :max-gb 25 :type InstanceType/R3Xlarge}
     :r3.2xlarge {:image default :max-gb 50 :type InstanceType/R32xlarge}
     :r3.4xlarge {:image default :max-gb 102 :type InstanceType/R34xlarge}
     :r3.8xlarge {:image default :max-gb 220 :type InstanceType/R38xlarge}
     :c3.large {:image default :max-gb 3 :type InstanceType/C3Large}
     :c3.xlarge {:image default :max-gb 6 :type InstanceType/C3Xlarge}
     :c3.xlarge.docker {:image "ami-65116700" :max-gb 6 :type InstanceType/C3Xlarge}
     :c3.2xlarge {:image default :max-gb 12 :type InstanceType/C32xlarge}
     :c3.4xlarge {:image default :max-gb 25 :type InstanceType/C34xlarge}
     :c3.8xlarge {:image default :max-gb 50 :type InstanceType/C34xlarge}
     :m3.medium {:image default :max-gb 3 :type InstanceType/M3Medium}}))

(defn wait-for-instances
  [ec2 ec2-creds ready?-fn insts]
  (log/infof "waiting for new instance(s): %s" (mapv :instanceId insts))
  (let [inst-ids (mapv :instanceId insts)
        new-insts (describe-instances ec2 inst-ids)]
    (if (and (= (count new-insts) (count insts))
             (every? (fn [inst] (ready?-fn (safe-get inst :publicDnsName))) new-insts))
      new-insts
      (do (Thread/sleep 1000) (recur ec2 ec2-creds ready?-fn insts)))))

(defn run-instances
  "Provision a (singleton) set of ec2 instances.  Returns the instance.
   If ready?-fn is non-nil, blocks until it passes on all instance hostnames."
  [ec2-creds zone instance-type image groups ready?-fn]
  (letk [[key-name] ec2-creds]
    (let [ec2 (ec2 ec2-creds)
          instances 1
          iamrole (doto (IamInstanceProfileSpecification.) (.withName "GrabbagBackend"))
          user-data (.getBytes "")
          block-devices (+default-block-devices+ instance-type)]
      (ensure-groups ec2 groups)
      (->> (doto (RunInstancesRequest. image (Integer. (int instances)) (Integer. (int instances)))
             (.setBlockDeviceMappings
              (for [[device mapping] block-devices]
                (block-device-mapping device mapping)))
             (.setPlacement (Placement. zone))
             (.setInstanceType ^InstanceType (safe-get-in instance-types [instance-type :type]))
             (.setKeyName key-name)
             (.setSecurityGroups groups)
             (.setIamInstanceProfile ^IamInstanceProfileSpecification iamrole)
             (.setUserData (String. ^bytes user-data)))
           (.runInstances ec2)
           (.getReservation)
           reservation-instances
           (?>> ready?-fn (wait-for-instances ec2 ec2-creds ready?-fn))))))

(set! *warn-on-reflection* false)
