(defproject crane "0.1.2-SNAPSHOT"
  :description "heavy lifting for service deployment"
  :internal-dependencies [plumbing email aws]
  :external-dependencies [prismatic/schema clj-ssh com.amazonaws/aws-java-sdk])
