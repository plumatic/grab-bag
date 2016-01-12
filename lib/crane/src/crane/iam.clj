(ns crane.iam
  (:use plumbing.core)
  (:require [schema.core :as s]
            [crane.config :as config])

  (:import
   [com.amazonaws.auth BasicAWSCredentials]
   [com.amazonaws.services.identitymanagement AmazonIdentityManagementClient]
   [com.amazonaws.services.identitymanagement.model
    CreateUserRequest AddUserToGroupRequest CreateLoginProfileRequest CreateAccessKeyRequest]
   [java.util Collection]))

(defnk iam-client :- AmazonIdentityManagementClient
  [key secretkey]
  (AmazonIdentityManagementClient. (BasicAWSCredentials. key secretkey)))

(defn aws-console-url-from-arn [arn]
  (format "https://%s.signin.aws.amazon.com/console" (second (re-find #"iam::(\d+):" arn))))

(defn- create-user! [iam-client user-name password]
  (let [create-user-request (.createUser iam-client (CreateUserRequest. user-name))]
    (let [user (.getUser create-user-request)]
      (doto iam-client
        (.addUserToGroup (AddUserToGroupRequest. "engineering" user-name))
        (.createLoginProfile (CreateLoginProfileRequest. user-name password))
        (.createAccessKey  (doto (CreateAccessKeyRequest.) (.withUserName user-name))))
      user)))

(defn create-iam-user! [user-name password]
  (let [iam-client (iam-client (select-keys (config/read-dot-crane) [:key :secretkey]))
        user (create-user! iam-client user-name password)]
    (println
     (format
      "User created succesfully. Use this URL %s to login to the aws console"
      (aws-console-url-from-arn (.getArn user))))))
