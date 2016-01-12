(ns aws.dynamo
  (:use plumbing.core)
  (:require
   [aws.core :as aws])
  (:import
   [com.amazonaws.auth AWSCredentialsProvider]
   [com.amazonaws.services.dynamodbv2 AmazonDynamoDBClient]
   [com.amazonaws.services.dynamodbv2.model
    AttributeDefinition
    AttributeValue
    Condition
    ConsumedCapacity
    CreateTableRequest
    DeleteTableRequest
    DescribeTableRequest
    KeySchemaElement
    ProvisionedThroughput
    TableDescription
    UpdateTableRequest]
   [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

;; Create and destroy clients

(defn ^AmazonDynamoDBClient  dynamo-client
  ([{access-key :key secret-key :secretkey}]
     (dynamo-client access-key secret-key))
  ([k sk] (AmazonDynamoDBClient. (aws/credentials-provider k sk))))

(defn shutdown-client [^AmazonDynamoDBClient c] (.shutdown c) )

(defmacro with-dynamo-client [[var ec2-keys] & body]
  `(let [~var (dynamo-client ~ec2-keys)]
     (try
       ~@body
       (finally
         (shutdown-client ~var)))))

;;; Table schemata

(def ^String +hash+ "HASH")
(def ^String +range+ "RANGE")
(def ^String +string+ "S")
(def ^String +number+ "N")
(def ^String +binary+ "B")
(def ^String +total+ "TOTAL")

(defn ^AttributeDefinition to-attr-definition [attr-name ^String attr-type]
  (AttributeDefinition. (name attr-name) attr-type))

(defn ^KeySchemaElement to-schema-element [attr-name ^String key-type]
  (KeySchemaElement. (name attr-name) key-type))

(defn key-name [^KeySchemaElement e] (keyword (.getAttributeName e)))

(defn capacity-units [^ConsumedCapacity cc] (.getCapacityUnits cc))

(defn ^ProvisionedThroughput to-provision [^long write-units ^long read-units]
  (doto (ProvisionedThroughput.)
    (.setWriteCapacityUnits write-units)
    (.setReadCapacityUnits read-units)))

(defn from-provision [^ProvisionedThroughput pt]
  [(.getWriteCapacityUnits pt) (.getReadCapacityUnits pt)])

;;; Actual table ops

(defn list-tables [^AmazonDynamoDBClient c]
  (seq (.getTableNames (.listTables c))))

(defn create-table* [^AmazonDynamoDBClient c ^String name key-schema-els attr-defs ^ProvisionedThroughput pt]
  (.getTableDescription
   (.createTable c
                 (doto (CreateTableRequest.)
                   (.setTableName name)
                   (.setKeySchema key-schema-els)
                   (.setAttributeDefinitions attr-defs)
                   (.setProvisionedThroughput pt)))))

(defnk create-table [client name write-throughput read-throughput hash-key {range-key nil}]
  (assert (keyword? (first hash-key)))
  (when range-key (assert (keyword? (first range-key))))
  (let [[hash-key-attr hash-key-type] hash-key
        [range-key-attr range-key-type] range-key]
    (create-table*
     client
     name
     (conj-when [(to-schema-element hash-key-attr +hash+)]
                (when range-key (to-schema-element range-key-attr +range+)))
     (conj-when [(to-attr-definition hash-key-attr hash-key-type)]
                (when range-key (to-attr-definition range-key-attr range-key-type)))
     (to-provision write-throughput read-throughput))))

(defnk delete-table [^AmazonDynamoDBClient client ^String name]
  (.deleteTable client (DeleteTableRequest. name)))

(defnk update-table [^AmazonDynamoDBClient client ^String name write-throughput read-throughput]
  (.updateTable client
                (doto (UpdateTableRequest.)
                  (.setTableName name)
                  (.setProvisionedThroughput (to-provision write-throughput read-throughput)))))

(defnk describe-table :- TableDescription
  [^AmazonDynamoDBClient client ^String name]
  (.getTable (.describeTable client (doto (DescribeTableRequest.) (.setTableName name)))))

(defn table-key-spec
  "Returns key names for table. Hash key is always first"
  [^TableDescription td]
  (let [ks (.getKeySchema td)]
    (if (or (= 1 (count ks))
            (= +hash+ (.getKeyType ^KeySchemaElement (first ks))))
      (mapv key-name ks)
      (mapv key-name (reverse ks)))))

;;; Attributes and keys

(defn ^AttributeValue to-numeric-attribute [^String n]
  (doto (AttributeValue.) (.setN n)))

(defn ^AttributeValue to-string-attribute [^String n]
  (doto (AttributeValue.) (.setS n)))

(defn ^AttributeValue to-binary-attribute [^bytes n]
  (doto (AttributeValue.) (.setB n)))

(defn ^AttributeValue to-attribute [x]
  (cond (or (float? x) (integer? x))
        (to-numeric-attribute (str x))

        (string? x)
        (to-string-attribute x)

        :else
        (to-binary-attribute (ByteBuffer/wrap (bytes x)))))

(defn from-attribute [^AttributeValue a]
  (or (when-let [b (.getB a)]
        (.array b))
      (or (.getS a)
          (let [n (.getN a)]
            (assert n)
            (read-string n)))))

;;; Range conditions

(defn range-between [from until]
  "Produce a range condition suitable for use with `query-page` and `query` over range keys
   which fall in [from, until).

   These values must convert to the right key-schema values when passed through `to-attribute`
   (eg. If your range-key is a string, then the values must be strings when you call this)."
  (doto (Condition.)
    (.setComparisonOperator "BETWEEN")
    (.setAttributeValueList
     [(to-attribute from)
      (to-attribute until)])))

(set! *warn-on-reflection* false)
