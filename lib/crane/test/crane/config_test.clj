(ns crane.config-test
  (:use plumbing.core plumbing.test clojure.test)
  (:require
   [schema.core :as s]
   [crane.config :as config]))

(defn instantiate [service-spec env & [project-name instance]]
  (-> service-spec
      (assoc-in [:machine :tags] {:owner "grabbag-corp"})
      (config/enved-config-spec env)
      (config/abstract-config (or project-name "test-service"))
      (config/config config/+test-ec2-creds+ (or instance config/+local-ec2-instance+))))

(deftest full-pipeline-test
  "Test that all the defaults come through, before we get into fancier tests."
  (is-= {:env :test
         :service {:type "test-service",
                   :service-name-override nil,
                   :elb-names nil,
                   :server-port -1,
                   :max-gb 50,
                   :profile? true,
                   :install-nodejs? false
                   :jvm-opts " -Xmx50g  -server -XX:HeapDumpPath=/mnt/java_pid.hprof -XX:MaxPermSize=256M -XX:+CMSClassUnloadingEnabled  -agentpath:/home/ubuntu/yourkit/bin/linux-x86-64/libyjpagent.so=disableall"
                   :pseudo-env :test},
         :machine {:zone "us-east-1d",
                   :instance-type :cc2.8xlarge,
                   :tags {:owner "grabbag-corp"}
                   :groups ["woven" "grabbag-test-service-test"],
                   :image "ami-34cc7a5c",
                   :user "ubuntu",
                   :ebs [],
                   :replicated? false},
         :instance {:service-name "test-service-test",
                    :instance-id "LOCAL"
                    :addresses {:private {:host "localhost", :port -1},
                                :public {:host "localhost", :port -1}}},
         :ec2-keys config/+test-ec2-creds+
         :a-param :a-value
         :another-param :another-value}
        (instantiate
         {:machine {:tags {:owner "grabbag-corp"}}
          :parameters {:a-param :a-value
                       :another-param :a-value}
          :envs {:test {:parameters {:another-param :another-value}}}}
         :test)))

(deftest enved-config-spec-test
  (is-= {:service {:pseudo-env :test} :env :test}
        (config/enved-config-spec {} :test))
  (let [base-spec {:service {:type "api"}
                   :machine {:user "kitten25"
                             :tags {:owner "grabbag-corp"}}
                   :parameters {:p1 :v1}}
        test-spec {:service {:type "api-test"}
                   :machine {:user "puppy77"
                             :tags {:owner "grabbag-corp"}}
                   :parameters {:p1 :v2}}
        with-envs (assoc base-spec
                    :envs {:test test-spec
                           :orange {:env :test
                                    :parameters {:p2 :v2}}})]
    (is-= (-> base-spec (assoc :env :prod) (assoc-in [:service :pseudo-env] :prod))
          (config/enved-config-spec with-envs :prod))
    (is-= (-> test-spec (assoc :env :test) (assoc-in [:service :pseudo-env] :test))
          (config/enved-config-spec with-envs :test))
    (is-= (-> base-spec
              (assoc :env :test)
              (assoc-in [:service :pseudo-env] :orange)
              (assoc-in [:parameters :p2] :v2))
          (config/enved-config-spec with-envs :orange))))

(deftest abstract-config-test
  (testing "User provided values survive"
    (let [ac {:machine {:zone "test-zone"
                        :user "bobby"
                        :tags {:owner "grabbag-corp"}}
              :service {:type "apples"
                        :elb-names ["a" "b"]
                        :pseudo-env :test}
              :env :test}
          params {:p1 :v1}]
      (is-subset (merge ac params)
                 (config/abstract-config (assoc ac :parameters params) "zzzzz")))))

(deftest config-test
  (testing "proper service name from type and env"
    (testing "not replicated"
      (is-subset {:instance {:service-name "social-graph-test"}}
                 (instantiate {:service {:type "social-graph"}} :test)))
    (testing "replicated"
      (is-subset {:instance {:service-name "api-test-LOCAL"}}
                 (instantiate {:machine {:replicated? true
                                         :tags {:owner "grabbag-corp"}}
                               :service {:type "api"}}
                              :test)))
    (testing "override"
      (is-subset {:instance {:service-name "pwned"}}
                 (instantiate {:service {:type "api" :service-name-override "pwned"}} :test))))

  (testing "proper addresses for remote env"
    (is-subset {:instance {:instance-id "I-123"
                           :addresses {:public {:host "pub" :port 999}
                                       :private {:host "prv" :port 999}}}}
               (instantiate {:service {:server-port 999}} :prod "zzz"
                            {:instanceId "I-123"
                             :publicDnsName "http://pub"
                             :privateDnsName "http://prv"}))))
