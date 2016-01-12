(ns aws.cost-report
  "Library to produce fine-grained cost reports of AWS resources.

   AWS pricing data is not programatically available via the main API, but some
   can be fetched from http://info.awsstream.com/ (from Amazon) and others
   are hardcoded in the file.

   The 'report' functions at the bottom of each section are the interface."
  (:use plumbing.core)
  (:require
   [clojure.string :as str]
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.new-time :as new-time]
   [plumbing.json :as json]
   [aws.dynamo :as dynamo]
   [aws.ec2 :as ec2])
  (:import
   [com.amazonaws.services.dynamodbv2 AmazonDynamoDBClient]
   [com.amazonaws.services.ec2 AmazonEC2Client]
   [com.amazonaws.services.ec2.model InstanceStatus ReservedInstances]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Helpers

(defn index-by
  [ks maps]
  (->> maps
       (group-by #(select-keys % ks))
       (map-vals #(apply dissoc (pci/safe-singleton %) ks))))

(def +hours-per-year+ (new-time/convert 1 :year :hours))
(def +days-per-month+ 30)
(def +hours-per-month+ (* +days-per-month+ (new-time/convert 1 :day :hours)))
(def +bytes-per-gigabyte+ (* 1024.0 1024 1024))

(defn csv
  ([ms]
     (csv (keys (first ms)) ms))
  ([ks ms]
     (->> ms
          (cons (map-from-keys name ks))
          (map #(str/join ", " (map % ks)))
          (str/join "\n"))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; DynamoDB Report

(s/defschema DynamoThroughputPricing
  {:region String
   :operation (s/enum "read" "write")
   :pricing (s/enum "od" "reserved")
   :cost-per-unit-hour Double})

(s/def +dynamo-throughput+ :- [DynamoThroughputPricing]
  "as of 7/13/2015"
  [{:region "us-east-1"
    :operation "read"
    :pricing "od"
    :cost-per-unit-hour (/ 0.0065 50)}
   {:region "us-east-1"
    :operation "write"
    :pricing "od"
    :cost-per-unit-hour (/ 0.0065 10)}
   {:region "us-east-1"
    :operation "read"
    :pricing "reserved"
    :cost-per-unit-hour (/ (+ 0.0025 (/ 30 +hours-per-year+)) 100)}
   {:region "us-east-1"
    :operation "write"
    :pricing "reserved"
    :cost-per-unit-hour (/ (+ 0.0128 (/ 150 +hours-per-year+)) 100)}])

(def +indexed-dynamo-throughput+
  (index-by [:region :operation :pricing] +dynamo-throughput+))

(def +dynamo-price-per-gb-month+
  "as of 7/13/2015 for us-east"
  0.25)

(defn dynamo-table [client table-name]
  (let [des (dynamo/describe-table {:client client :name table-name})
        cap (.getProvisionedThroughput des)
        read-cap (.getReadCapacityUnits cap)
        write-cap (.getWriteCapacityUnits cap)
        size (.getTableSizeBytes des)
        region "us-east-1"
        [unreserved reserved] (for [pricing ["od" "reserved"]]
                                (sum (fn [[op cap]]
                                       (* cap
                                          +hours-per-month+
                                          (safe-get-in
                                           +indexed-dynamo-throughput+
                                           [{:region region
                                             :operation (name op)
                                             :pricing pricing}
                                            :cost-per-unit-hour])))
                                     {:read read-cap :write write-cap}))]
    (array-map
     :name table-name
     :read read-cap
     :write write-cap
     :items (.getItemCount des)
     :bytes size
     :monthly-storage (* +dynamo-price-per-gb-month+
                         (/ size +bytes-per-gigabyte+))
     :monthly-throughput unreserved
     :monthly-throughput-if-reserved reserved)))

(defn dynamo-tables [ec2-keys]
  (dynamo/with-dynamo-client [c ec2-keys]
    (mapv #(dynamo-table c %) (dynamo/list-tables c))))

(defn total-row [ks rs]
  (assoc (apply merge-with + (map #(select-keys % ks) rs))
    :name "Total"))

(defn dynamo-report [ec2-keys]
  (let [t (dynamo-tables ec2-keys)]
    (csv
     (concat
      t
      [(total-row
        [:bytes :monthly-storage :monthly-throughput :monthly-throughput-if-reserved]
        t)]))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; S3 report (not including all aspects such as cloudfront, for now)

;; Unfortunately, the only way to count s3 usage through the API is to iterate
;; through all files.
;; http://blog.nuxi.pl/2015/aws-tip-of-the-day-get-s3-disk-usage-per-bucket.html
;; provides an alternative: log into the AWS console as root, go to
;; https://console.aws.amazon.com/billing/home?#/reports/usage
;; and download S3 usage report in CSV for the last **full** day.
;; (from yesterday to today, report granularity in Days)
;; then pass the filename into the code below.

(s/defschema StoragePricing
  {:id Long

   ;; primary key
   :region String,
   :kind (s/enum "ebsssd" "ebspiops" "ebsstandard" "ebssnap" "s3" "s3reduced" "glacier")
   :unit (s/enum "Million IOPS Requests" "GB/month" "Provisioned IPOPS/month")

   ;; values
   :created_at String
   :updated_at String
   :price Double})

(s/defn ^:always-validate storage-pricing :- [StoragePricing]
  []
  (-> "http://info.awsstream.com/storage.json?"
      slurp
      (json/parse-string true)))

(defn indexed-storage-pricing []
  (index-by [:region :kind :unit]
            (storage-pricing)))

(defn us-east-storage-pricing []
  (safe-get-in (indexed-storage-pricing)
               [{:region "us-east-1" :kind "s3" :unit "GB/month"} :price]))

(s/defschema UsageEntry
  {:service (s/eq "AmazonS3")
   :operation (s/enum
               "GetObject" "PutObject" "HeadBucket" "ListBucket"
               "HeadObject" "DeleteObject" "StandardStorage" "WebsiteGetObject"
               "ListAllMyBuckets" "MultiObjectDelete" "CreateBucket")
   :usagetype (s/enum
               "C3DataTransfer-Out-Bytes" "C3DataTransfer-In-Bytes" ;; to/from AWS
               "DataTransfer-Out-Bytes" "DataTransfer-In-Bytes" ;; to/from internet
               "CloudFront-Out-Bytes"
               "Requests-Tier2"  "Requests-Tier1" "Requests-NoCharge"
               "StorageObjectCount" "TimedStorage-ByteHrs")
   :resource String
   :usagevalue Double})

(defn drop-region-prefix
  "For now, assume everything is in US east."
  [^String s]
  (if (.startsWith s "US")
    (subs s 5)
    s))

(s/defn ^:always-validate parse-usage-report :- [UsageEntry]
  [s :- String]
  (let [[head & rows] (map #(.split ^String % ",") (.split s "\n"))
        head (map (comp keyword str/trim str/lower-case) head)]
    (for [r rows]
      (-> (zipmap head r)
          (dissoc :starttime :endtime)
          (update :usagevalue #(Double/parseDouble %))
          (update :usagetype drop-region-prefix)))))

(s/defn entry-monthly-cost :- (s/maybe (s/pair s/Keyword "type" Double "monthly cost"))
  "Values updated on 7/13/2015.
   some explanation here: https://forums.aws.amazon.com/thread.jspa?threadID=26865"
  [storage-cost :- double
   entry :- UsageEntry]
  (letk [[usagetype usagevalue] entry]
    (case usagetype
      "DataTransfer-Out-Bytes" [:transfer (* 0.09 +days-per-month+ (/ usagevalue +bytes-per-gigabyte+))]
      "Requests-Tier1" [:requests (* (/ 0.005 1000) +days-per-month+ usagevalue)]
      "Requests-Tier2" [:requests (* (/ 0.004 10000) +days-per-month+ usagevalue)]
      "TimedStorage-ByteHrs" [:storage (* storage-cost (/ usagevalue +bytes-per-gigabyte+ 24))]

      ("C3DataTransfer-Out-Bytes" ;; free to US-East, not otherwise
       "CloudFront-Out-Bytes"
       "Requests-NoCharge"
       "StorageObjectCount"
       "DataTransfer-In-Bytes"
       "C3DataTransfer-In-Bytes") nil)))

(defn s3-tables [filename]
  (let [storage-cost (us-east-storage-pricing)]
    (->> filename
         slurp
         parse-usage-report
         (reduce (fn [m e]
                   (if-let [[t cost] (entry-monthly-cost storage-cost e)]
                     (update-in m [(safe-get e :resource) t] #(if % (+ % cost) cost))
                     m))
                 {})
         (map (fn [[k v]] (assoc v :bucket k))))))

(defn s3-report
  "See comment at top of section"
  [filename]
  (csv [:bucket :transfer :requests :storage] (s3-tables filename)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Instance report (instances only, not counting storage or reservation payments)

(s/defschema InstancePricing
  {:id Long

   ;; primary key
   :region (s/enum "us-east-1")
   :model String
   :os (s/enum "linux")
   :ebsoptimized Boolean
   :term (s/enum 0 1 3)
   :pricing (s/enum "partial" "od" "no" "all")

   ;; values
   :created_at String
   :updated_at String
   :latest Boolean
   :upfront Double
   :hourly Double})

(s/defn ^:always-validate us-east-instance-pricing :- [InstancePricing]
  []
  (-> "http://info.awsstream.com/instances.json?top=10000&region=us-east-1&os=linux"
      slurp
      (json/parse-string true)))

(defn indexed-instance-pricing []
  (index-by [:region :model :os :ebsoptimized :term :pricing]
            (us-east-instance-pricing)))

(defn price-info
  [iip inst term pricing]
  (let [g (assoc (select-keys inst [:region :model :ebsoptimized])
            :os "linux" :term term :pricing pricing)]
    (or (get iip g)
        ;; some instances not supported, data is backwards i think.
        (safe-get iip (update g :ebsoptimized not)))))

(defn hourly-price
  [iip inst term pricing]
  (safe-get (price-info iip inst term pricing) :hourly))

(defn effective-hourly-price
  [iip inst term pricing]
  (letk [[upfront hourly] (price-info iip inst term pricing)]
    (+ hourly (new-time/convert upfront :hour :year))))

(defn service-name [tags groups]
  (str/trim
   (cond (some #{"SearchLayer"} tags)
         (pci/safe-singleton
          (filter (fn [^String s]
                    (and (.startsWith s "Elastic")
                         (not (.contains s " - "))))
                  tags))

         (and (seq tags) (seq (first tags)))
         (pci/safe-singleton tags)

         :else
         (pci/safe-singleton (remove #{"woven"} groups)))))

(defn az-region [az]
  (subs az 0 (dec (count az))))

(defn instances [ec2]
  (for [i (ec2/describe-instances ec2)
        :when (= "running" (.getName (:state i)))]
    (letk [[instanceId instanceType spotInstanceRequestId groups placement ebsOptimized tags] i
           az (.getAvailabilityZone placement)
           tags (map #(.getValue %) tags)]
      (assert (not spotInstanceRequestId)) ;; not handling spot for now
      {:id instanceId
       :model instanceType
       :az az
       :region (az-region az)
       :groups groups
       :tags tags
       :service (service-name tags groups)
       :ebsoptimized ebsOptimized})))

(defn on-demand-service-costs
  "Table of costs per service"
  [pricing insts]
  (->> insts
       (group-by #(select-keys % [:region :model :ebsoptimized :service]))
       (map (fn [[g [i :as is]]]
              (letk [[service model region] i
                     g (dissoc g :service)
                     price (hourly-price pricing i 0 "od")]
                (array-map
                 :service service
                 :model model
                 :region region
                 :instances (count is)
                 :on-demand-monthly-cost (* (count is) price +hours-per-month+)))))))

(defn reservations [^AmazonEC2Client ec2]
  (for [^ReservedInstances r (.getReservedInstances (.describeReservedInstances ec2))
        :when (= (.getState r) "active")]
    (let [recurring (sum (fn [rc] (assert (= (.getFrequency rc) "Hourly")) (.getAmount rc))
                         (.getRecurringCharges r))
          usage (.getUsagePrice r)]
      (assert (= "USD" (.getCurrencyCode r)))
      (assert (or (zero? usage) (zero? recurring))) ;; old vs new way
      {:az (.getAvailabilityZone r)
       :duration-days (long (new-time/convert (.getDuration r) :seconds :days))
       :instances (.getInstanceCount r)
       :start (.getStart r)
       :end (.getEnd r)
       :model (.getInstanceType r)
       :offering (.getOfferingType r) ;; "Light Utilization", No Upfront", etc.
       :fixed-price (.getFixedPrice r)
       :hourly-price (+ usage recurring)})))

(defn reservations-by-type
  "Table of active reservations"
  [pricing insts resvs]
  (let [num-used (->> insts
                      (map #(select-keys % [:az :model]))
                      frequencies)]
    (for [[g ress] (group-by #(select-keys % [:az :model]) resvs)]
      (letk [[az model] g
             n-used (num-used g 0)
             first-end (first (sort-by :end ress))
             n-reserved (sum :instances ress)
             total-monthly (sum (fnk [instances fixed-price hourly-price duration-days]
                                  (* instances
                                     (+ (* (/ fixed-price duration-days) +days-per-month+)
                                        (* hourly-price +hours-per-month+))))
                                ress)]
        (array-map
         :az az
         :model model
         :num-reserved n-reserved
         :first-ending (:end first-end)
         :num-ending (:instances first-end)
         :num-unused (max 0 (- n-reserved n-used))
         :total-reserved-effective-monthly-cost total-monthly
         :savings-percent (* 100 (- 1 (/ (/ total-monthly n-reserved +hours-per-month+)
                                         (hourly-price
                                          pricing
                                          (assoc g :ebsoptimized false :region (az-region (:az g)))
                                          0 "od")))))))))

(defn unreserved-by-type
  "Table of unreserved instances by type and potential savings"
  [pricing insts resvs]
  (let [num-reserved (->> resvs
                          (group-by #(select-keys % [:az :model]))
                          (map-vals #(sum :instances %)))]
    (for [[g insts] (group-by #(select-keys % [:az :region :model :ebsoptimized]) insts)
          :let [n-reserved (num-reserved (dissoc g :ebsoptimized :region) 0)
                n-used (count insts)
                n-unreserved (max 0 (- n-used n-reserved))
                od-price (effective-hourly-price pricing g 0 "od")
                yearp-price (try (effective-hourly-price pricing g 1 "partial")
                                 (catch Throwable t nil))]
          :when (pos? n-unreserved)]
      (letk [[az model ebsoptimized] g]
        (array-map
         :az az
         :model model
         :ebsoptimized ebsoptimized
         :total-instances n-used
         :unreserved-instances n-unreserved
         :1yearpartialupfront-savings-percent (if yearp-price (* 100 (- 1 (/ yearp-price od-price))) "missing pricing data")
         :total-unreserved-monthly-cost (* n-unreserved od-price +hours-per-month+)
         :potential-monthly-savings (if yearp-price (* n-unreserved (- od-price yearp-price) +hours-per-month+) "missing pricing data"))))))

(defn instances-report [ec2-keys]
  (let [pricing (indexed-instance-pricing)
        ec2 (ec2/ec2 ec2-keys)
        insts (instances ec2)
        resvs (reservations ec2)]
    (str
     "On demand costs per service (not counting reservations)\n"
     (csv (on-demand-service-costs pricing insts))
     "\n\nCurrent reservations\n"
     (csv (reservations-by-type pricing insts resvs))
     "\n\nCurrent unreserved instances and potential savings\n"
     (csv (unreserved-by-type pricing insts resvs)))))
