(ns tubes.macros
  (:require
   [clojure.repl :as repl]))

(defmacro import-vars [ns & syms]
  `(do
     ~@(for [sym syms]
         (read-string (repl/source-fn (symbol (name ns) (name sym)))))))
