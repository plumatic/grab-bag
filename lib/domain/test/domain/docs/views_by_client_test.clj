(ns domain.docs.views-by-client-test
  (:use plumbing.core plumbing.test clojure.test)
  (:require
   [domain.docs.views-by-client :as views-by-client]))

(deftest views-by-client-test
  (let [vbc (views-by-client/views-by-client)]
    (is-= {} (views-by-client/viewing-users vbc))
    (is-= {} (views-by-client/view-counts vbc))

    (views-by-client/add-view! vbc :iphone 1)
    (views-by-client/add-view! vbc :web 2)
    (views-by-client/add-view! vbc :iphone 3)

    (is-= {:web [2] :iphone [1 3]}
          (map-vals sort (views-by-client/viewing-users vbc)))
    (is-= {:web 1 :iphone 2} (views-by-client/view-counts vbc))

    (doseq [[t copy] {"write/read" (-> vbc
                                       views-by-client/write-views-by-client
                                       views-by-client/read-views-by-client)
                      "clone" (views-by-client/clone vbc)}]
      (testing t
        (is-= {:web [2] :iphone [1 3]}
              (map-vals sort (views-by-client/viewing-users copy)))
        (is-= vbc copy)
        (views-by-client/add-view! copy :iphone 7)
        (is-= {:web [2] :iphone [1 3 7]}
              (map-vals sort (views-by-client/viewing-users copy)))
        (is-= {:web [2] :iphone [1 3]}
              (map-vals sort (views-by-client/viewing-users vbc)))))))
