(ns dashboard.data.db-config)

(def db-spec
  (let [template {:delimiters "`"
                  :port 3306
                  :host ""
                  :classname "com.mysql.jdbc.Driver"
                  :subprotocol "mysql"}
        dws-template (assoc template
                       :user     "USER"
                       :password "PASSWORD")
        subname (fn [host db] (format "//%s/%s" host db))]
    (->>
     {:local (assoc template
               :host     "127.0.0.1"
               :user     "root"
               :password ""
               :db       "datawarehouse_local")
      :test  (assoc template
               :host     "127.0.0.1"
               :user     "root"
               :password "")
      :stage (assoc dws-template
               :host    "HOST"
               :db "datawarehouse_prod")
      :prod  (assoc dws-template
               :host    "HOST"
               :db "datawarehouse_prod")

      :analysis-local (assoc dws-template
                        :host "127.0.0.1"
                        :db   "analysis_local")
      :analysis-stage (assoc dws-template
                        :host    "HOST"
                        :db      "analysis_prod")
      :analysis-prod  (assoc dws-template
                        :host    "HOST"
                        :db      "analysis_prod")}
     (map (fn [[k {:keys [host db] :as v}]]
            [k (assoc v :subname (subname host db))]))
     (into {}))))
