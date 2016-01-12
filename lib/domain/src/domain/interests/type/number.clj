(ns domain.interests.type.number
  (:use plumbing.core)
  (:require
   [domain.interests.manager :as manager]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Interest Type Manager for interests that are already assigned numeric keys

(defrecord NumberInterestTypeManager [type]
  manager/PInterestTypeManager
  (type-index! [this interest] (safe-get interest :key))
  (type-lookup [this index] {:type type :key index})
  (type-key-index [this key] (if (string? key)
                               (Long/parseLong key)
                               (do (assert (integer? key)) key))))
