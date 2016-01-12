(ns crane.provision
  "Code for provisioning new machines and deploying new services to machines."
  (:use plumbing.core)
  (:require
   [clojure.string :as str]
   [schema.core :as s]
   [crane.config :as config]
   [aws.ec2 :as ec2]))

(defn app-name [config]
  (str (-> config :service :type) "-" (-> config :env name)))

(defn nodejs-config [config]
  (when (get-in config [:service :install-nodejs?])
    {:node-version "v0.12.2"
     :node-path (str "/home/ubuntu/node-" (app-name config))}))

(defn install-nodejs
  "Returns command to install node.js locally to `~/node-<app-name>` when nodejs configured"
  [config]
  (when-let [{:keys [node-version node-path]} (nodejs-config config)]
    (letk [node-file-name (str "node-" node-version "-linux-x64.tar.gz")
           node-download-dir "/home/ubuntu"]
      (str "if [ -d " node-path " ]; then echo '" node-path " exists'; "
           "else ("
           (->> [["mkdir" "-p" node-download-dir node-path]
                 ;; download nodejs archive
                 ["cd" node-download-dir]
                 ["wget" (format "http://nodejs.org/dist/%s/%s" node-version node-file-name)]
                 ;; barf archive contents into node-path
                 ["cd" node-path]
                 ["tar" "--strip-components 1" "-xzf" (str node-download-dir "/" node-file-name)]
                 ["echo" (format "'installed Node.js to %s'" node-path)]]
                (map (partial str/join " "))
                (str/join " && "))
           ") "
           "fi"))))

(defn install-cmds [config pkgs]
  (let [defaults [;; setup for java 7
                  "sudo apt-get -q -y install python-software-properties"
                  "sudo add-apt-repository -y ppa:webupd8team/java"
                  "sudo apt-get -q -y update"

                  "echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections"
                  "echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections"

                  "sudo apt-get -q -y install oracle-java7-installer"
                  "sudo apt-get -y install ntp"
                  "sudo apt-get -y install unzip"
                  "sudo apt-get -y install daemontools-run"
                  "sudo apt-get -y install xfsprogs"
                  (install-nodejs config)
                  "sudo service svscan start"]
        installs (map
                  #(str "sudo apt-get -y install " %)
                  pkgs)]
    (filter identity (concat defaults installs))))

(s/defn ^:always-validate rsync-cmd
  [ec2-creds :- ec2/EC2Creds
   host :- String
   srcs :- [String]
   dest :- String]
  (letk [[key-path user] ec2-creds]
    (flatten ["rsync" "-avzL" "--delete"
              "-e" (format "ssh -o StrictHostKeyChecking=no -i %s" key-path)
              srcs (str user "@" host ":" dest)])))

;; unzipping standalone.jar is probably not the right thing to do here...
(defnk crane-init-cmds [[:service jvm-opts] :as config]
  (concat ["mkdir -p ~/.crane.d/bin"
           "sudo cp ~/.crane /root/.crane"
           (format "unzip -o %s/*-standalone.jar bin/* -d ~/.crane.d/" (config/base-service-name config))
           "chmod +x ~/.crane.d/bin/*"
           "sudo ln -s ~/.crane.d/bin/crane /usr/local/bin/crane"]))

(defnk service-init-cmds
  [[:service jvm-opts pseudo-env] [:instance instance-id] :as config]
  (let [service (config/base-service-name config)]
    [(format "sudo mkdir -p /etc/service.new/%s/log/main" service)
     (format "sudo mkdir -p /etc/service.new/%s/env" service)
     (format "sudo mkdir -p /etc/service.old/%s/" service)
     (format "sudo mkdir -p /etc/service")
     (format "sudo sh -c \"echo /etc/service > /etc/service.new/%s/env/SVC_HOME\"" service)
     (format "sudo sh -c \"echo %s > /etc/service.new/%s/env/SVC_NAME\"" service service)
     (format "sudo sh -c \"echo /home/ubuntu/%s > /etc/service.new/%s/env/CRANE_PROJ_PATH\"" service service)
     (format "sudo sh -c \"echo %s > /etc/service.new/%s/env/CRANE_CONFIG\"" (name pseudo-env) service)
     (format "sudo sh -c \"echo %s > /etc/service.new/%s/env/JVM_OPTS\"" jvm-opts service)
     (format "sudo sh -c \"echo %s > /etc/service.new/%s/env/INSTANCE_ID\"" instance-id service)
     (format "sudo sh -c \"echo %s > /etc/service.new/%s/env/NODEJS_HOME\"" (str (:node-path (nodejs-config config))) service)
     (format "sudo cp ~/.crane.d/bin/svc_log_run /etc/service.new/%s/log/run" service)
     (format "sudo cp ~/.crane.d/bin/svc_run /etc/service.new/%s/run" service)
     (format "sudo chmod +x /etc/service.new/%s/log/run" service)
     (format "sudo chmod +x /etc/service.new/%s/run" service)
     (format "sudo rm -rf /etc/service.old/%s" service)
     (format "sudo cp -r /etc/service/%s /etc/service.old/%s" service service)
     (format "sudo mv /etc/service.new/%s /etc/service/%s" service service)]))
