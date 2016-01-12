(ns plumbing.cache
  (:use plumbing.core)
  (:require
   [plumbing.chm :as chm]))

(defmacro first-not-nil
  ([] nil)
  ([x] x)
  ([x & next]
     `(let [nilor# ~x]
        (if-not (nil? nilor#)
          nilor#
          (first-not-nil ~@next)))))

;; TODO(jw): move to store, build these on top of buckets

;; TODO: maybe MapMaker has a proper concurrent weak hashmap?
(defn strong-interner
  "Return an interner that canonicalizes all objects passed through it.
   Equivalent to (memoize identity), but has java equality semantics and is
   ~7x faster."
  ([] (strong-interner identity))
  ([key-fn]
     (let [m (chm/chm)]
       (fn [x]
         (let [k (key-fn x)]
           (first-not-nil (.get m k)
                          (.putIfAbsent m k x)
                          (let [v (.get m k)]
                            (assert (not (nil? v)))
                            v)))))))

(defn bounded-memoize
  "Bounded memoize, with optional entry weight (weight k v) --> int.
   Function cannot take or return nil values.
   May compute value multiple times simultaneously."
  [max-size f & [entry-weight-fn]]
  (let [m (chm/lru-chm max-size entry-weight-fn)]
    (fn memoized [k]
      (first-not-nil
       (.get m k)
       (let [v (f k)]
         (.put m k v)
         v)))))

(defn volatile-bounded-memoize
  "Like bounded-memoize, but entries time out and failure values are
   not saved.

  key-fn is applied to the memoized function's argument before interacting
  with the cache, so that you can memoize functions with arguments that
  don't influence the result. For example, if a function takes a 'url' parameter
  that appears in logging output but does not affect any observable behavior of
  the function, that parameter shouldn't be used in the cache key."
  [max-size timeout-millis f & [key-fn entry-weight-fn]]
  (let [m (chm/lru-chm max-size entry-weight-fn)]
    (fn volatile-memoized [arg]
      (let [key-fn (or key-fn identity)
            k (key-fn arg)
            [timestamp val] (.get m k)]
        (if (and timestamp val (< (- (millis) timestamp) timeout-millis))
          val
          (when-let [computed (f arg)]
            (.put m k [(millis) computed])
            computed))))))

(defn thread-local [f]
  (let [^ThreadLocal tl
        (proxy [ThreadLocal] [] (initialValue [] (f)))]
    (reify clojure.lang.IDeref
      (deref [_] (.get tl))
      clojure.lang.IFn
      (invoke [_] (.get tl)))))

(defmacro cache-with
  "Cache results of expr under k-sym in map with locking.  Stores delayed values in map."
  [map-sym k-sym expr]
  `(let [^java.util.Map m# ~map-sym
         k# ~k-sym]
     @(locking m#
        (or (.get m# k#)
            (let [v# (delay ~expr)]
              (.put m# k# v#)
              v#)))))
