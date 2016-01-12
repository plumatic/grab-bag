(ns aws.cloudfront
  "Client & functionality for CloudFront CDN.

   CloudFront Basics:
   *   Origin
       *   Location of original content. CF contacts Origin upon cache miss.
       *   Can be S3 bucket or custom HTTP server (URL). This uses S3 bucket.
   *   Invalidations
       *   Can invalidate a single URL or URLs matching a pattern
       *   Two states: in-progress & complete.
       *   Can take a long time to complete (10-20m)"
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.resource :as resource]
   [aws.core :as aws])
  (:import
   [com.amazonaws.auth AWSCredentialsProvider]
   [com.amazonaws.services.cloudfront AmazonCloudFrontClient]
   [com.amazonaws.services.cloudfront.model CreateInvalidationRequest
    Distribution
    DistributionSummary
    GetDistributionRequest
    GetInvalidationRequest
    Invalidation
    InvalidationBatch
    ListDistributionsRequest
    Paths]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public - Constants

(def +status-incomplete+ "Invalidation status when incomplete/in-progress"
  "InProgress")

(def +status-completed+ "Invalidation status when completed"
  "Completed")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public - Client

(defn ^AmazonCloudFrontClient cloudfront-client
  ([{access-key :key secret-key :secretkey}]
     (cloudfront-client access-key secret-key))
  ([k sk] (AmazonCloudFrontClient. (aws/credentials-provider k sk))))

(defn shutdown-client [^AmazonCloudFrontClient c] (.shutdown c))

(extend-protocol resource/PCloseable
  AmazonCloudFrontClient
  (close [this] (shutdown-client this)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(s/defn list-distributions :- [DistributionSummary]
  "Lists all CloudFront distributions. Returns a seq of DistributionSummary objects"
  [client :- AmazonCloudFrontClient]
  (-> client
      (.listDistributions (ListDistributionsRequest.))
      .getDistributionList
      .getItems))

(s/defn get-distribution :- Distribution
  "Returns an existing distribution if found, otherwise false.
   Throws exception if dist-id is not a valid distribution id."
  [client :- AmazonCloudFrontClient dist-id :- String]
  (-> client
      (.getDistribution (GetDistributionRequest. dist-id))
      .getDistribution))

(s/defn invalidate! :- Invalidation
  "Creates invalidation and returns it.
   Throws exception if dist-id is not a valid distribution id."
  [client :- AmazonCloudFrontClient dist-id :- String paths :- [String]]
  (-> client
      (.createInvalidation
       (CreateInvalidationRequest.
        dist-id
        (InvalidationBatch.
         (doto (Paths.)
           (.setItems paths)
           (.setQuantity (count paths)))
         (name (gensym)))))
      .getInvalidation))

(s/defn get-invalidation :- (s/maybe Invalidation)
  "Returns an existing invalidation if found, otherwise returns nil.
   Throws exception if dist-id is not a valid distribution id.
   Throws exception if inv-id is not a valid invalidation id."
  [client :- AmazonCloudFrontClient dist-id :- String inv-id :- String]
  (-> client
      (.getInvalidation (GetInvalidationRequest. dist-id inv-id))
      .getInvalidation))

;;; Getters

(s/defn distribution-id [dist :- Distribution] (.getId dist))

(s/defn distribution-status [dist :- Distribution] (.getStatus dist))

(s/defn distribution-domain [dist :- Distribution] (.getDomainName dist))

(s/defn invalidation-id [inv :- Invalidation] (.getId inv))

(s/defn invalidation-status [inv :- Invalidation] (.getStatus inv))

(s/defn invalidation-completed? :- s/Bool
  [inv :- Invalidation]
  (= +status-completed+ (invalidation-status inv)))

(set! *warn-on-reflection* false)
