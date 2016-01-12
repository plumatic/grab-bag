(ns crane.provision-test
  (:use clojure.test crane.provision)
  (:require
   [clojure.java.io :as java-io]
   [clojure.set :as set]
   [crane.config :as config]))

(deftest rsync-cmd-test
  (is (= ["rsync" "-avzL" "--delete"
          "-e" "ssh -o StrictHostKeyChecking=no -i keypath"
          "foo/" "bar/"
          "bob@foobar.com:dir"]
         (rsync-cmd
          {:key "" :secretkey "" :key-path "keypath" :key-name "" :user "bob"}
          "foobar.com" ["foo/" "bar/"] "dir"))))

(deftest crane-init-cmds-test
  (is (= ["mkdir -p ~/.crane.d/bin"
          "sudo cp ~/.crane /root/.crane"
          "unzip -o yas/*-standalone.jar bin/* -d ~/.crane.d/"
          "chmod +x ~/.crane.d/bin/*"
          "sudo ln -s ~/.crane.d/bin/crane /usr/local/bin/crane"]
         (crane-init-cmds
          (-> {:service {:jvm-opts "-Xmx2g"
                         :service-name-override "yas"}
               :machine {:tags {:owner "grabbag-corp"}}}
              (config/enved-config-spec :test)
              (config/abstract-config "bla")
              (config/config config/+test-ec2-creds+ config/+local-ec2-instance+))))))
