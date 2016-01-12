{:machine {:groups ["woven" "grabbag-analytics-prod"]
           :tags {:owner "grabbag-news"}}
 :service {:server-port 80
           :jvm-opts " -XX:+UseConcMarkSweepGC -Xmx2g "}
 :parameters {:swank-port 6684
              :forward-ports {6666 :swank-port}}
 :envs {:stage {:service {:server-port 8080}
                :parameters {:swank-port 6685}}
        :local {:service {:server-port 8080}
                :parameters {:email-level :fatal}}}}
