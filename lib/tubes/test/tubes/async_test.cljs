(ns tubes.async-test
  (:use-macros
   [cljs-test.macros :only [deftest is is=]])
  (:require
   [cljs-test.core :as test]
   [tubes.async :as async]))

(deftest wait*
  (let [counter (atom 0)
        a (js/Array.)]
    (async/wait* (repeat 5 (fn [cb] (.push a cb)))
                 #(swap! counter inc))
    (is= 5 (.-length a))
    (doseq [cb a] (cb))
    (is= 1 @counter)
    (async/wait* nil #(swap! counter inc))
    (is= 2 @counter)))

(deftest map*
  (let [a (js/Array.)]
    (async/map* (fn [item cb] (.push a #(cb (inc item))))
                (range 10)
                #(is= (seq %) (map inc (range 10))))
    (is= 10 (.-length a))
    (doseq [cb a] (cb))))

(deftest parallel*
  (let [a (js/Array.)
        push (fn [v] #(.push a [v %]))]
    (async/parallel*
     {:foo (push "food")
      :bar (push "bard")
      :baz (push "bazd")
      :pwn (push "pwnd")}
     (fn [res-map err-map]
       (doseq [[k v] res-map]
         (is= (str (name k) "d") v))))
    (is= 4 (.-length a))
    (doseq [[v cb] a] (cb v))))

(deftest take*
  (let [test-data (atom (range 1 15))
        buffer (async/refilling-buffer
                (fn [also-return cb]
                  (if (and (seq also-return) (nil? (last also-return)))
                    (cb nil)
                    (let [return
                          (concat also-return (take 5 @test-data))]
                      (swap! test-data #(drop 5 %))
                      (cb return))))
                5
                nil)]
    (async/take*
     buffer 4 (fn [data]
                (is= data [1 2 3 4])
                [[1 2] 99]))
    (async/take*
     buffer 1 (fn [data]
                (is= data [1])
                [[] 98]))
    (async/take*
     buffer 5 (fn [data]
                (is= data [2 5 99 6 7])
                [[99] 97]))
    (async/take*
     buffer 4 (fn [data]
                (is= data [99 8 9 10])
                [[] 96]))
    (async/take*
     buffer 5 (fn [data]
                (is= data [98 97 11 12 13])
                [[] 95]))
    (async/take*
     buffer 5 (fn [data]
                (is= data [14 96 95])
                [[] nil]))
    (is= (async/depleted? buffer) true)))