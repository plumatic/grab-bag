(ns plumbing.hash
  (:import
   [plumbing MurmurHash]))

;; This is OK for hash functions but bad for collisions -- 31 is too small.
;; IE 2 char strings on average collide with 8 others.
(defn hash64 ^long [s]
  (loop [h 1125899906842597
         s (seq (name s))]
    (if s
      (recur (unchecked-add (unchecked-multiply 31 h) (long (int (first s)))) (next s))
      h)))

;; About the same speed, it seems, and many fewer collisions.
(defn murmur64 ^long [^String s]
  (MurmurHash/hash64 s))
