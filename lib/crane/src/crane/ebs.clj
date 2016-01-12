(ns crane.ebs
  "Machine Cconfig should have top-level :ebs elements with map value supporting...
    :device device path, default /dev/sdf
    :mount where to mount ebs device, default /ebs
    :size How large in GB, default 500 gb
    :id a specific id to use if available"
  (:use plumbing.core)
  (:require
   [clojure.tools.logging :as log]
   [schema.core :as s]
   [plumbing.error :as err]
   [crane.config :as config]
   [crane.core :as crane]
   [aws.ec2 :as ec2]
   [crane.ssh :as ssh])
  (:import
   [com.amazonaws.services.ec2 AmazonEC2]
   [com.amazonaws.services.ec2.model
    AttachVolumeRequest CreateVolumeRequest DeleteVolumeRequest DescribeVolumesRequest
    DetachVolumeRequest]))

(set! *warn-on-reflection* true)

(defn describe-volumes
  "Describe volumes for given EC2 account.
   If volume IDs are provided, only those are described."
  ([^AmazonEC2 ec2 & [ids]]
     (let [vs (map bean (.getVolumes (.describeVolumes ec2 (DescribeVolumesRequest. (or ids [])))))]
       (map (fn [v] (update-in v [:attachments] #(map bean %))) vs))))

(defn find-volume [ec2 vol-id]
  (->> (describe-volumes ec2)
       (filter (comp (partial = vol-id) :volumeId))
       first))

(defn attach-volume [^AmazonEC2 ec2 instance-id volume-id device]
  (.attachVolume ec2 (AttachVolumeRequest. ^String volume-id ^String instance-id ^String device)))

(defn detach-volume [^AmazonEC2 ec2 ^String vol-id ^String i-id ^String dev force?]
  (.detachVolume ec2 (doto (DetachVolumeRequest. vol-id)
                       (.setInstanceId  i-id)
                       (.setDevice dev)
                       (.setForce (boolean force?)))))

(defn delete-volume
  "Delete an EBS volume."
  [^AmazonEC2 ec2 id]
  (.deleteVolume ec2 (DeleteVolumeRequest. id)))


(defn public-host [config]
  (safe-get-in config [:instance :addresses :public :host]))

(defn attach-fresh-volume [ec2-creds ^AmazonEC2 ec2 config ebs]
  (err/assert-keys [:device :size] ebs)
  (let [zone (safe-get-in config [:machine :zone])
        instance-id (safe-get-in config [:instance :instance-id])
        vol (bean (.getVolume (.createVolume ec2 (if (:snapshot ebs)
                                                   (CreateVolumeRequest. ^String (:snapshot ebs) ^String zone)
                                                   (CreateVolumeRequest. (Integer. (int (or (-> ebs :size) 500))) ^String zone)))))]
    (log/info (format "Creating Fresh EBS Volume %s" (:volumeId vol)))
    (assert vol)
    (attach-volume ec2 instance-id (:volumeId vol) (:device ebs))
    (when-not (:snapshot ebs)
      (Thread/sleep 5000)
      (log/info (format "Formatting new ebs volume, may take a minute or two..."))
      (crane/display-errors
       (ssh/run-remote-cmds ec2-creds (public-host config)
                            [["sudo" "mkfs.xfs" "-f" (:device ebs)]])))))

(defn ensure-attached [ec2-creds ec2 config ebs]
  (let [{:keys [id device]} ebs]
    (if-not id
      (attach-fresh-volume ec2-creds ec2 config ebs)
      (let [vol (find-volume ec2 id)
            instance-id (safe-get-in config [:instance :instance-id])]
        (if-let [a (first (:attachments vol))]
          (assert (= instance-id (:instanceId a)))
          (do (assert vol)
              (assert device)
              (attach-volume ec2 instance-id (safe-get vol :volumeId) device)))))))

(defn mount-cmds [device mount]
  (assert device)
  (assert mount)
  [ ["sudo" "cp" "/etc/fstab" "fstab"]
    ["sudo" "bash" "-c" (format "'echo %s %s xfs defaults,noatime,nobootwait,comment=cloudconfig 0 0 >> fstab'" device mount)]
    ["sudo" "cp" "fstab" "/etc/fstab"]
    ["sudo" "mkdir" "-p" mount]
    ["sudo" "mount" "-a"]])

(defn mounted? [ec2-creds ebs host]
  (letk [[device mount] ebs]
    (let [output (first (ssh/run-remote-cmds ec2-creds host [["sudo" "mount"]]))]
      (->>
       (.split ^String output "\r?\n")
       seq
       (filter
        (fn [line]
          (.startsWith ^String line (format "%s on %s" device mount))))
       ((complement empty?))))))

(defn mount [ec2-creds ebs host]
  (letk [[device mount] ebs]
    (ssh/run-remote-cmds ec2-creds host (mount-cmds device mount))))

(def ^:private defaults {:mount "/ebs" :device "/dev/sdf" :size 500 })

(s/defn ^:always-validate ensure-volumes
  [ec2-creds :- ec2/EC2Creds
   config :- config/Config]
  (doseq [ebs (safe-get-in config [:machine :ebs])]
    (let [ebs (merge defaults ebs)
          host (public-host config)]
      (if (mounted? ec2-creds ebs host)
        (log/info (format "EBS volume attached: %s" ebs))
        (do (ensure-attached ec2-creds (ec2/ec2 ec2-creds) config ebs)
            (Thread/sleep 2000)
            (mount ec2-creds ebs host)
            (if (mounted? ec2-creds ebs host)
              (log/info "Verified correct EBS counting")
              (do
                (log/error "Didn't mound EBS correctly")
                (System/exit 32))))))))

(set! *warn-on-reflection* false)
