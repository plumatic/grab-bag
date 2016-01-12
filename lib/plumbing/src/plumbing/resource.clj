(ns plumbing.resource
  (:refer-clojure :exclude [with-open])
  (:require
   [schema.core :as s]
   [plumbing.core :as plumbing]
   [plumbing.fnk.pfnk :as pfnk]
   [plumbing.fnk.schema :as schema]
   [plumbing.graph :as graph]
   [plumbing.logging :as log]
   [plumbing.map :as map]
   [plumbing.observer :as observer]
   [plumbing.timing :as timing]))


;; Basic idea is that service graph nodes just return the raw resource, which
;; can implement PCloseable for resource-like behavior.

(set! *warn-on-reflection* true)

(defprotocol PCloseable
  (close [this]))

(extend-protocol PCloseable
  java.io.Closeable
  (close [this] (.close ^java.io.Closeable this))

  java.lang.AutoCloseable
  (close [this] (.close ^java.lang.AutoCloseable this))

  Object
  (close [this])

  nil
  (close [this]))

(defprotocol HasId
  (identifier [this]))

(defmacro with-open [bindings & body]
  (if (empty? bindings)
    `(do ~@body)
    `(let ~(subvec bindings 0 2)
       (try (with-open ~(subvec bindings 2) ~@body)
            (finally (close ~(bindings 0)))))))

(defn observer-transform [g]
  (map/map-leaves-and-path
   (fn [keyseq node]
     (if (schema/possibly-contains? (pfnk/input-schema node) :observer)
       (graph/comp-partial-fn node (plumbing/fnk [observer] {:observer (reduce observer/sub-observer observer keyseq)}))
       node))
   g))

(defn- resource-wrap [instantiated-atom-key keyseq node]
  (let [wants-iak? (contains? (pfnk/input-schema node) instantiated-atom-key)]
    (pfnk/fn->fnk
     (fn [m]
       (let [r (node (if wants-iak? m (dissoc m instantiated-atom-key)))]
         (swap! (instantiated-atom-key m) conj [keyseq r])
         r))
     [(assoc (pfnk/input-schema node)
        instantiated-atom-key s/Any)
      (pfnk/output-schema node)])))

(defn resource-transform [instantiated-atom-key g]
  (assert (not (contains? g instantiated-atom-key)))
  (assoc (map/map-leaves-and-path (partial resource-wrap instantiated-atom-key) g)
    instantiated-atom-key (plumbing/fnk [] (atom []))))

(defn force-walk
  "Ensure the (presumably lazy) graph instance inst has its nodes instantiated
   in the same order as graph-spec (from which is was produced).
   Easier than making sure that all transforms preserve order, when you care about it."
  [graph-spec graph-inst]
  (map/map-leaves-and-path
   (fn [keyseq node]
     (log/infof "Starting %s ..." keyseq)
     (let [t (timing/get-time (get-in graph-inst keyseq))]
       (when (> t 10)
         (log/infof "Starting %s took %s ms" keyseq t))))
   graph-spec)
  graph-inst)

(defn shutdown! [instantiated-resources]
  (doseq [[keyseq res] (reverse instantiated-resources)]
    (try (log/infof "Stopping %s..." keyseq)
         (let [t (timing/get-time (close res))]
           (when (> t 10)
             (log/infof "Stopping %s took %s ms" keyseq t)))
         (catch Throwable t (log/errorf t "Error shutting down %s" keyseq)))))

(defrecord Bundle []
  java.io.Closeable
  (close [this] (when-let [c (::shutdown (meta this))] (c))))

(defn make-bundle [m shutdown]
  (with-meta (map->Bundle m)
    {::shutdown shutdown}))

(defn bundle-compile [g]
  (let [gk (keyword (gensym "bundle-resources"))
        gf (->> g
                observer-transform
                (resource-transform gk)
                graph/lazy-compile)]
    (pfnk/fn->fnk
     (fn [m]
       (let [o (force-walk g (gf m))
             ir @(gk o)]
         (make-bundle
          (dissoc o gk)
          #(shutdown! ir))))
     (pfnk/io-schemata gf))))

(def bundle (comp bundle-compile graph/graph))

(defn bundle-run [g m] ((bundle-compile g) m))


;; TODO: these probably belong in graph

(defn upstream-subgraph
  "Return the portion of graph g relevant for computing input-schema."
  [g input-schema]
  (let [g (graph/->graph g)]
    (apply
     array-map
     (second
      (reduce
       (fn [[is sub-g] [prev-k prev-node]]
         (if-let [schema-k (schema/schema-key is prev-k)]
           (do (schema/assert-satisfies-schema (get is schema-k) (pfnk/output-schema prev-node))
               [(schema/union-input-schemata
                 (dissoc is schema-k)
                 (pfnk/input-schema prev-node))
                (concat [prev-k prev-node] sub-g)])
           [is sub-g]))
       [input-schema nil]
       (reverse g))))))

(defn downstream-subgraph
  "Return the portion of graph g that depends (directly or indirectly) on output-schema."
  [g output-schema]
  (apply
   array-map
   (second
    (reduce
     (fn [[os-keys sub-g] [prev-k prev-node]]
       (let [input-keys (set (pfnk/input-schema-keys prev-node))]
         (if (some input-keys os-keys)
           [(conj os-keys prev-k) (concat sub-g [prev-k prev-node])]
           [os-keys sub-g])))
     [(keys (schema/explicit-schema-key-map output-schema)) nil]
     (graph/->graph g)))))

(set! *warn-on-reflection* false)
