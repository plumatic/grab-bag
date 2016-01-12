(ns tubes.event-emitter-test
  (:use-macros
   [cljs-test.macros :only [deftest is is=]])
  (:require
   [cljs-test.core :as test]
   [tubes.event-emitter :as event-emitter]))

(deftest event-emitter
  (let [counter (atom 0)
        e (event-emitter/event-emitter)]

    (event-emitter/listen! e :test :foo #(swap! counter inc))
    (is= 0 @counter)
    (event-emitter/fire e :test nil)
    (is= 1 @counter)

    (event-emitter/unlisten! e :test :foo)
    (event-emitter/fire e :test nil)
    (is= 1 @counter)

    (event-emitter/listen! e :test #(swap! counter inc))
    (is= 1 @counter)
    (event-emitter/fire e :test nil)
    (is= 2 @counter)

    (event-emitter/listen! e :test #(is= "hello" %))
    (event-emitter/listen! e :test #(swap! counter inc))
    (event-emitter/fire e :test "hello")
    (is= 4 @counter)))

(deftest wait-for-all!
  (let [single (event-emitter/event-emitter)
        ees (repeatedly 3 event-emitter/event-emitter)
        truths (atom 0)
        fire-ees (fn [idx bool]
                   ;; note: NOT idepotent
                   (swap! truths (if bool inc dec))
                   (event-emitter/fire (nth ees idx) :test bool))]
    (event-emitter/wait-for-all! [single] :test (fn [bool] (is= true bool)))
    (event-emitter/fire single :test true)

    (event-emitter/wait-for-all! ees :test (fn [bool] (is= bool (= 3 @truths))))
    (fire-ees 0 true)
    (fire-ees 1 true)
    (fire-ees 0 false)
    (fire-ees 2 true)
    (fire-ees 0 true)
    (fire-ees 1 false)
    (event-emitter/wait-for-all! ees :test (fn [bool] (is= true bool)) [false true true])
    (event-emitter/fire (first ees) :test true)))