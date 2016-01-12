(ns aws.core
  "Utility functions for accessing AWS. Eventually a lot of stuff from crane will move here"
  (:import
   [com.amazonaws.auth BasicAWSCredentials AWSCredentialsProvider InstanceProfileCredentialsProvider]))

(def +iam-creds+ {:key "iam" :secretkey "iam"})

(defn credentials-provider ^AWSCredentialsProvider [key secretkey]
  (if (= key "iam")
    (InstanceProfileCredentialsProvider.)
    (let [basic-credentials (BasicAWSCredentials. key secretkey)]
      (reify AWSCredentialsProvider
        (refresh [this] nil)
        (getCredentials [this] basic-credentials)))))
