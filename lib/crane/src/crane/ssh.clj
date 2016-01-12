(ns crane.ssh
  (:use plumbing.core)
  (:require
   [clojure.string :as str]
   [clj-ssh.ssh :as ssh]
   [plumbing.logging :as log]))

(defn join-cmd [cmd]
  (if (string? cmd)
    cmd
    (str/join " " cmd)))

(defn run-remote-cmds [ec2-creds h cmds]
  (let [{:keys [key-path user]} ec2-creds
        agent (ssh/ssh-agent {})]
    (ssh/add-identity agent {:private-key-path key-path})
    (let [sess (ssh/session agent h {:username user :strict-host-key-checking :no})]
      (ssh/with-connection sess
        (->> cmds
             (map join-cmd)
             (map (fn [cmd] (log/info (format "remote exec: %s" cmd)) cmd))
             (map (fn [cmd] (ssh/ssh sess {:in cmd})))
             (map (fnk [out] (log/info (format "remote result: %s" out)) out))
             doall)))))

(defn safe-run-remote-cmds [ec2-creds h cmds]
  (try
    (run-remote-cmds ec2-creds h cmds)
    true
    (catch Exception e
      (println (str "Error running commands on " h))
      false)))

(defn safe-run-remote-cmds-on-hosts [ec2-creds hosts cmds]
  "Run commands on all hosts and print a warning about failures at the end."
  (let [failures (atom [])]
    (doseq [host hosts
            :when (not (str/blank? host))]
      (when-not (safe-run-remote-cmds ec2-creds host cmds)
        (swap! failures conj host)))
    (when (seq @failures)
      (log/errorf "Commands failed on : %s" (str/join "," @failures)))))

(defn ssh-able? [ec2-creds h]
  (try
    (run-remote-cmds ec2-creds h ["ls"])
    (catch Exception e
      (println (str "not ssh-able " h " " e)))))
