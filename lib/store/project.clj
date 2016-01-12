(defproject store "0.2.5-SNAPSHOT"
  :description "Distributed Datastorage Abstraction"
  :internal-dependencies [plumbing aws]
  :external-dependencies [com.amazonaws/aws-java-sdk
                          com.h2database/h2
                          com.sleepycat/je
                          honeysql
                          ring/ring-core
                          c3p0/c3p0
                          org.clojure/java.jdbc]
  :dependencies [[com.novemberain/monger "1.0.0-SNAPSHOT"]]
  :repositories {"oracle" "http://download.oracle.com/maven"}
  :jvm-opts ["-Xmx600m"])
