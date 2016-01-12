(ns crane.task
  (:use plumbing.core)
  (:require
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [plumbing.parallel :as parallel]
   [plumbing.new-time :as new-time]
   [email.sendgrid :as sendgrid]
   [crane.config :as config]
   [crane.core :as crane]
   [crane.ebs :as ebs]
   [aws.ec2 :as ec2]
   [aws.elb :as elb]
   [crane.provision :as provision]
   [crane.ssh :as ssh]))

(defn- shell [cmd]
  (str/trim (second (crane/run-local-cmd cmd))))

(defn safe-base-service-name [abstract-config]
  (let [base-service-name (config/base-service-name abstract-config)]
    (assert (not (re-find #"\s" base-service-name)))
    base-service-name))

(defn apply-tags!
  "Update the AWS tags for this instance"
  [ec2-creds abstract-config instance-ids]
  (letk [[[:machine tags] env] abstract-config]
    (ec2/tag-instances!
     (ec2/ec2 ec2-creds)
     instance-ids
     (merge
      {:Name (safe-base-service-name abstract-config)
       :stack (name env)}
      tags))))

(defn- deploy-git-hook [abstract-config]
  (let [base-service-name (safe-base-service-name abstract-config)
        branch (str "DEPLOYED-" base-service-name)
        warn (fn [s]
               (let [stars (apply str (repeat 80 "*"))]
                 [" " " " " " stars s stars]))
        messages (concat
                  [(format "At %s %s (%s) is deploying commit %s to %s"
                           (java.util.Date.)
                           (shell "git config --get user.name")
                           (shell "git config --get user.email")
                           (shell "git rev-parse HEAD")
                           base-service-name)
                   " "
                   (format "https://github.com/plumatic/grab-bag/tree/%s" branch)
                   " "
                   (format "https://github.com/plumatic/grab-bag/commit/%s" (shell "git rev-parse HEAD"))
                   " "
                   "Watch deploy status at http://dashboard.example.com/admin"
                   " "]
                  (when-not (zero? (first (crane/run-local-cmd "git diff-index --quiet HEAD")))
                    (warn "WARNING: deploying with uncommitted changes"))
                  (when-not (empty? (second (crane/run-local-cmd "git ls-files --others --exclude-standard")))
                    (warn "WARNING: deploying with untracked files")))]
    (crane/safe-run-local-cmd (format "git push --no-verify -f origin HEAD:%s" branch))
    (doseq [l messages]
      (log/infof "%s" l))
    (sendgrid/send
     {:to (format "eng+deploy-%s@example.com" (name (safe-get abstract-config :env)))
      :from "eng@example.com"
      :subject (format "[deploy start] %s" base-service-name)
      :text (str/join "\n" messages)})))

(defn deploy [ec2-creds abstract-config & [slave-arg]]
  (let [base-service-name (config/base-service-name abstract-config)
        configs (config/configs! ec2-creds abstract-config slave-arg)]
    (deploy-git-hook abstract-config)
    (log/infof "Deploying to %d machine(s): %s"
               (count configs)
               (str/join ", " (map :instance configs)))
    (log/info "")
    (doseq [config configs
            :let [h (config/public-host config)]]
      (crane/install ec2-creds h config)
      (ebs/ensure-volumes ec2-creds config)
      (crane/push ec2-creds base-service-name h)
      (ssh/run-remote-cmds
       ec2-creds h
       (concat (provision/crane-init-cmds config)
               (provision/service-init-cmds config)
               [(format "sudo svc -tu /etc/service/%s" base-service-name)])))))

(defn shell-command
  "run arbitrary shell command on grabbag hosts"
  ([ec2-creds abstract-config & [maybe-slave-arg command]]
     (let [commands (if (config/replicated? abstract-config) [command] [maybe-slave-arg])
           configs (if (config/replicated? abstract-config)
                     (config/configs ec2-creds abstract-config maybe-slave-arg)
                     (config/configs ec2-creds abstract-config nil))]
       (ssh/safe-run-remote-cmds-on-hosts
        ec2-creds
        (map config/public-host configs)
        commands)))
  ([ec2-creds command]
     ;; TODO(manish) when we have dozens of machines, do this in parallel. Not worth
     ;; the trouble just yet.
     (ssh/safe-run-remote-cmds-on-hosts
      ec2-creds
      (map :publicDnsName (ec2/describe-instances (ec2/ec2 ec2-creds)))
      [command])))

(defn- elb-reporter [ec2-creds abstract-config elb-name]
  (let [last-report (atom nil)
        inst-ids (map #(safe-get-in % [:instance :instance-id])
                      (config/configs ec2-creds abstract-config "all"))
        report-keys [:instanceId :state :reasonCode]]
    (fn []
      (let [insts (->> (concat
                        (map #(select-keys % report-keys)
                             (elb/instances ec2-creds elb-name))
                        (for [i inst-ids] {:instanceId i :state "MISSING" :reasonCode "MISSING"}))
                       (distinct-by :instanceId)
                       (sort-by :instanceId))]
        (when-not (= @last-report insts)
          (reset! last-report insts)
          (log/infof "\nCurrent load balancer state for %s: %s" elb-name (if (seq insts) "" "NO INSTANCES"))
          (when (seq insts)
            (pprint/print-table report-keys insts)))
        (for-map [{:keys [instanceId] :as ring-inst} insts]
          instanceId (elb/healthy? ring-inst))))))

(defn- rolling-redeploy [ec2-creds abstract-config slave-arg & [dont-push-new-jar?]]
  (let [elb-names (doto (seq (safe-get-in abstract-config [:service :elb-names])) assert)
        configs (config/configs ec2-creds abstract-config slave-arg)
        base-service-name (config/base-service-name abstract-config)
        elb-reporters (map-from-keys #(elb-reporter ec2-creds abstract-config %) elb-names)]
    (log/infof "Rolling redeploy to %d machine(s): %s"
               (count configs)
               (str/join ", " (map :instance configs)))
    (log/infof "Waiting for all current machines healthy...")
    (parallel/wait-until #(every? (fn [elb-report] (every? val (elb-report))) (vals elb-reporters)))
    (doseq [config configs]
      (let [instanceId (safe-get-in config [:instance :instance-id])
            publicDnsName (config/public-host config)]
        (log/infof "Redeploying to %s: %s" instanceId publicDnsName)
        ;; (elb/remove config ring-name [instanceId])
        ;; (log/infof "Removed %s from load balancer %s" instanceId ring-name)
        (when-not dont-push-new-jar?
          (crane/money-push ec2-creds base-service-name publicDnsName))
        (ssh/run-remote-cmds ec2-creds publicDnsName
                             [(format "sudo svc -t /etc/service/%s" base-service-name)])
        (log/infof "Waiting for instance %s to remove itself from elb (or become unhealthy)" instanceId)
        (parallel/wait-until #(not-any? (fn [elb-report] (get (elb-report) instanceId)) (vals elb-reporters)))
        (log/infof "Waiting for instance %s to add itself to elb and all healthy" instanceId)
        (parallel/wait-until #(every? (fn [elb-report] (let [r (elb-report)] (and (contains? r instanceId) (every? val r))))
                                      (vals elb-reporters)))
        (log/infof "Done redeploying to instance %s: %s" instanceId publicDnsName)))))

(def ^:private hosts (comp (partial map config/public-host) config/configs))

;; Creates a ssh config entry of the form:
;; Host api-prod-1
;;     HostName <<HOSTNAME>>
;;     User ubuntu
;;     IdentityFile ~/.crane.pem
(defn- generate-ssh-config-entry [logical-name ec2-name]
  (str "\nHost " logical-name "\n\t" "HostName " ec2-name "\n\tUser ubuntu\n\tIdentityFile ~/.crane.pem\n"))

(defn- create-logical-host-names [abstract-config ec2-host-names]
  (let [logical-host-name-fn (fn [idx]
                               (str (-> abstract-config :service :type)
                                    "-"
                                    (-> abstract-config :env name)
                                    (when idx
                                      (str "-" (inc idx)))))]
    (if (> (count ec2-host-names) 1)
      (map-indexed (fn [idx host-name] (logical-host-name-fn idx)) ec2-host-names)
      [(logical-host-name-fn nil)])))

(defn ssh-config
  "Generates ssh config for this service/env pair so we can log in directly with ssh instead of crane ssh"
  [ec2-creds abstract-config & [slave-arg]]
  (let [ec2-host-names (sort (hosts ec2-creds abstract-config slave-arg))
        logical-host-names (vec (create-logical-host-names abstract-config ec2-host-names))]
    (let [ssh-config (str (System/getenv "HOME") "/.ssh/config")
          ssh-config-contents (try
                                (slurp ssh-config)
                                (catch Exception e ""))]
      (spit ssh-config (str "# Generated by crane ssh-config on " (new-time/pst-date (millis)) "\n"))
      (doseq [[logical-name ec2-host-name] (zipmap logical-host-names ec2-host-names)]
        (log/infof "Generating ssh entry for %s" logical-name)
        (spit ssh-config (generate-ssh-config-entry logical-name ec2-host-name) :append true))
      (spit ssh-config ssh-config-contents :append true))))

(defn- run-remote-cmd [config cmd]
  (letk [[[:ec2-keys user key-path]] config]
    (let [c (format "%s | ssh -i %s %s@%s" cmd key-path user (config/public-host config))]
      (println c)
      (crane/run-local-cmd c))))

(defn- svc [[ec2-creds abstract-config & [slave-arg] :as args] op]
  (doseq [config (apply config/configs args)]
    (run-remote-cmd
     config
     (format "echo 'sudo svc -%s /etc/service/%s;exit'"
             op (config/base-service-name abstract-config)))))

(defn down [& args]
  (svc args "d"))

(defn up [& args]
  (svc args "u"))

(defn redeploy-or-bounce [bounce? ec2-creds abstract-config & [slave-arg now?]]
  (let [base-service-name (config/base-service-name abstract-config)]
    (when-not bounce?
      (apply-tags! ec2-creds abstract-config
                   (map #(safe-get-in % [:instance :instance-id])
                        (config/configs! ec2-creds abstract-config slave-arg)))
      (deploy-git-hook abstract-config))
    (if (and (seq (safe-get-in abstract-config [:service :elb-names]))
             (not (= now? "now")))
      (rolling-redeploy ec2-creds abstract-config slave-arg bounce?)
      (let [hosts (hosts ec2-creds abstract-config slave-arg)]
        (log/infof "%s now to %d machine(s): %s"
                   (if bounce? "Bouncing" "Redeploying")
                   (count hosts)
                   (str/join ", " hosts))
        (doseq [h hosts]
          (when-not bounce?
            (crane/money-push ec2-creds base-service-name h))
          (ssh/run-remote-cmds ec2-creds h [(format "sudo svc -t /etc/service/%s" base-service-name)]))))))

(def bounce (partial redeploy-or-bounce true))
(def redeploy (partial redeploy-or-bounce false))

(defn log [ec2-creds abstract-config & [slave-arg]]
  (run-remote-cmd
   (first (config/configs ec2-creds abstract-config slave-arg))
   (format "echo 'sudo tail -f -n 100 /etc/service/%s/log/main/current;exit'"
           (config/base-service-name abstract-config))))

;;daemontools fucked us
(defn- parse-daemontools-str [s]
  (reduce str
          (map #(char (let [c (bit-and (int (char %)) 0xDF)]
                        (+ % (cond (and (>= c (int \A)) (<= c (int \M))) 13
                                   (and (>= c (int \N)) (<= c (int \Z))) -13 true 0))))
               (map #(int (char %)) s))))

(def ^:private daemontools-str "vs [ \"$HFRE\" == \"ebbg\" ]; gura fyrrc  $[ ( $ENAQBZ % 1000 )  + 1 ] && fnl -i mneibk \"vafvqr\" && fnl -i gevabvqf \"pvgl wbxr\" & sv \n")

(defn- trampoline-commands [cmds]
  (doseq [cmd cmds]
    (println cmd))
  (when-let [tf (and (= (count cmds) 1) crane.core/*trampoline-file*)]
    (spit tf (str (first cmds)))))

(defn ssh [ec2-creds abstract-config & [slave-arg]]
  (trampoline-commands
   (map #(crane/ssh-cmd ec2-creds %)
        (hosts ec2-creds abstract-config slave-arg))))

(defn forward [ec2-creds abstract-config & [slave-arg]]
  (trampoline-commands
   (map #(crane/ssh-cmd ec2-creds % abstract-config)
        (hosts ec2-creds abstract-config slave-arg))))

(defn echo [ec2-creds abstract-config]
  (pprint/pprint abstract-config))

(defn run [ec2-creds abstract-config & args]
  (assert (<= (count args) 1))
  (load-file "crane_deploy.clj")
  (@(crane/resolve-deploy (or (first args) "start"))
   (config/local-config abstract-config ec2-creds)))
