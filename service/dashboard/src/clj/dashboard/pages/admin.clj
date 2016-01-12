(ns dashboard.pages.admin
  (:use plumbing.core)
  (:require
   [plumbing.graph :as graph]
   [plumbing.parallel :as parallel]
   [store.snapshots :as snapshots]
   [service.nameserver :as nameserver]
   [dashboard.pages.admin.api :as api]))

(set! *warn-on-reflection* true)

(def admin-resources
  (graph/graph
   :snapshots-cache (graph/instance parallel/refreshing-resource [nameserver get-snapshot-store]
                      {:secs 15
                       :f (fn []
                            (for-map [[nm info] (nameserver/service-map nameserver)]
                              nm {:info info
                                  :last-snapshot (try (api/sanitize (snapshots/read-latest-snapshot (get-snapshot-store nm)))
                                                      (catch Exception e {:snapshot-read-error (pr-str e)}))}))})
   ))
