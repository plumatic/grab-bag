(ns crane.core
  (:use plumbing.core)
  (:require
   [clojure.java.shell :as shell]
   [clojure.tools.logging :as log]
   [plumbing.error :as err]
   [plumbing.io :as io]
   [aws.core :as aws]
   [crane.config :as config]
   [crane.provision :as provision]
   [crane.ssh :as ssh])
  (:import com.jcraft.jsch.JSch))

(JSch/setLogger (reify com.jcraft.jsch.Logger
                  (isEnabled [this level] false)
                  (log [this level message] nil)))

;; Janky args rebound by lein crane plugin.
(def ^{:dynamic true} *uberjar-file* nil)
(def ^{:dynamic true} *trampoline-file* nil)

(defn run-local-cmd [cmd]
  ((juxt :exit :out) (apply shell/sh (if (string? cmd) ["/bin/sh" "-c" cmd] cmd))))

(defn safe-run-local-cmd [cmd]
  (let [[exit out] (run-local-cmd cmd)]
    (when-not (zero? exit)
      (throw (RuntimeException. (format "Command %s errored with exit code %s, out %s" (pr-str cmd) exit out))))
    out))

(defn display-errors [run-results]
  (doseq [result run-results]
    (let [[code out] result]
      (when (not= 0 code)
        (log/error (str "stdout:" out))
        (log/error (str  "exit code:" code)))
      [code out])))

;;TODO: extract out ssh-agent stuff that's also in init
(defn install
  "installs packages from apt"
  [ec2-creds h config]
  (->> (provision/install-cmds config nil)
       (ssh/run-remote-cmds ec2-creds h)
       display-errors))

(defn money-city-path []
  (assert *uberjar-file*)
  (assert (.contains ^String *uberjar-file* "-standalone.jar"))
  (assert (.exists (java.io.File. ^String *uberjar-file*)))
  *uberjar-file*)

(defn- clean-old-jars [ec2-creds service-name host]
  (let [older-than-days 15]
    (log/info (format "Cleaning jars older than %s days" older-than-days))
    ;; redirect stderr to /dev/null. rm complains if we don't have any old jars.
    (ssh/run-remote-cmds ec2-creds host [(format "find %s -name *.jar.*  -mtime +%s | xargs rm &> /dev/null" service-name older-than-days)])))

(defn money-push [ec2-creds service-name host]
  (clean-old-jars ec2-creds service-name host)
  (let [cmd (concat (provision/rsync-cmd ec2-creds host [(money-city-path)] (format "%s/" service-name))
                    ["-b" "--suffix=.`date +%s`"])
        _   (log/info (format "Pushing: %s" (pr-str cmd)))
        rsync-result (safe-run-local-cmd cmd)]
    (log/info (format "Result: %s" (pr-str rsync-result)))))

(defn push [ec2-creds service-name host]
  (->> [(format "mkdir -p %s" service-name)]
       (ssh/run-remote-cmds ec2-creds host)
       display-errors)
  (money-push ec2-creds service-name host)

  (let [dot-crane (io/get-temp-file ".crane")]
    (spit dot-crane (merge ec2-creds aws/+iam-creds+))
    (let [cmd (provision/rsync-cmd ec2-creds host [(.getAbsolutePath dot-crane)] "./")]
      (log/info (format "Push .crane: %s" (pr-str cmd)))
      (let [rsync-result (run-local-cmd cmd)]
        (log/info (format "Result: %s" (pr-str rsync-result)))))))

(defn ssh-cmd [{:keys [user key-path] :as c} host & [forward-params]]
  (format "ssh -i %s %s %s@%s"
          key-path
          (if forward-params
            (let [forward (safe-get forward-params :forward-ports)]
              (apply str
                     (for [[local-port remote-k] forward]
                       (format "-L %s:localhost:%s " local-port (safe-get forward-params remote-k)))))
            "")
          user
          host))

(defn resolve-deploy [x]
  (ns-resolve (create-ns 'crane-deploy) (symbol x)))

(defn init-main!
  "Init the crane JVM"
  []
  (err/init-logger! :info)
  (com.amazonaws.services.s3.AmazonS3Client. (com.amazonaws.auth.BasicAWSCredentials. "shutthe" "fuckup"))
  (err/set-log-level-regex!  #".*amazonaws.*" :warn))
