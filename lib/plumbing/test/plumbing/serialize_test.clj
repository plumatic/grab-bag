(ns plumbing.serialize-test
  (:use clojure.test plumbing.core plumbing.serialize plumbing.test)
  (:require [plumbing.core :as plumbing])
  (:import
   org.apache.commons.io.IOUtils
   [java.io DataOutputStream ByteArrayOutputStream
    DataInputStream  ByteArrayInputStream]))

(def serialization-methods [+clojure+ +java+ +json+ +default+ +default-seq+ +clojure-snappy+])

(deftest roundtrip
  (let [data (seq [{"foo" (range 100)
                    "bar" {"bhh" 3}}])]
    (doseq [[m ser]
            (cons
             [:seq-whore #(-> % seq->ring-input-stream IOUtils/toByteArray)]
             (for [m serialization-methods]
               [m (partial serialize m)]))]
      (testing m
        (is (= (-> data ser deserialize) data))))))

(deftest roundtrip-with-keywords
  (let [data (seq [{:foo (range 100)
                    :bar {"bhh" 5}}])]
    (doseq [[m ser]
            (cons
             [:seq-whore #(-> % seq->ring-input-stream IOUtils/toByteArray)]
             (for [m serialization-methods]
               [m (partial serialize m)]))]
      (testing m
        (is (= (-> data ser deserialize)
               (if (= m +json+) [(plumbing/map-keys name (first data))] data)))))))

(def a-data-structure
  [1 {:goo "1 2 3"
      (Integer. "1") (Integer. "1")
      (Long. "124332423423423") (Long. "124332423423423")
      "test" ["a" :b 123]}])

(def a-fancier-data-structure
  (assoc-in a-data-structure [1 :foo/goo] :ro/bo))

(deftest roundtrip-fancier
  (doseq [[m ser]
          (cons
           [:seq-whore #(-> % seq->ring-input-stream IOUtils/toByteArray)]
           (for [m (remove #{+json+} serialization-methods)]
             [m (partial serialize m)]))]
    (testing m
      (is (= (-> a-fancier-data-structure ser deserialize)
             a-fancier-data-structure)))))

(defn generate-ser-data []
  (vec
   (for [ser
         (cons
          #(-> % seq->ring-input-stream IOUtils/toByteArray)
          (for [m (remove #{+json+} serialization-methods)
                :when (not (#{+json+} m))]
            (partial serialize m)))]
     (-> a-data-structure ser vec))))

(def old-ser-data
  [[0 91 49 32 123 58 103 111 111 32 34 49 32 50 32 51 34 44 32 49 32 49 44 32 49 50 52 51 51 50 52 50 51 52 50 51 52 50 51 32 49 50 52 51 51 50 52 50 51 52 50 51 52 50 51 44 32 34 116 101 115 116 34 32 91 34 97 34 32 58 98 32 49 50 51 93 125 93]
   ;; only readable in 1.7.0 and later
   [2 -84 -19 0 5 115 114 0 29 99 108 111 106 117 114 101 46 108 97 110 103 46 80 101 114 115 105 115 116 101 110 116 86 101 99 116 111 114 72 -86 -96 81 81 31 -16 -8 2 0 5 73 0 3 99 110 116 73 0 5 115 104 105 102 116 76 0 5 95 109 101 116 97 116 0 29 76 99 108 111 106 117 114 101 47 108 97 110 103 47 73 80 101 114 115 105 115 116 101 110 116 77 97 112 59 76 0 4 114 111 111 116 116 0 36 76 99 108 111 106 117 114 101 47 108 97 110 103 47 80 101 114 115 105 115 116 101 110 116 86 101 99 116 111 114 36 78 111 100 101 59 91 0 4 116 97 105 108 116 0 19 91 76 106 97 118 97 47 108 97 110 103 47 79 98 106 101 99 116 59 120 114 0 30 99 108 111 106 117 114 101 46 108 97 110 103 46 65 80 101 114 115 105 115 116 101 110 116 86 101 99 116 111 114 64 -58 -114 -34 89 -85 -21 -101 2 0 2 73 0 5 95 104 97 115 104 73 0 7 95 104 97 115 104 101 113 120 112 -1 -1 -1 -1 -1 -1 -1 -1 0 0 0 2 0 0 0 5 112 115 114 0 34 99 108 111 106 117 114 101 46 108 97 110 103 46 80 101 114 115 105 115 116 101 110 116 86 101 99 116 111 114 36 78 111 100 101 -119 62 1 40 19 45 96 106 2 0 1 91 0 5 97 114 114 97 121 113 0 126 0 3 120 112 117 114 0 19 91 76 106 97 118 97 46 108 97 110 103 46 79 98 106 101 99 116 59 -112 -50 88 -97 16 115 41 108 2 0 0 120 112 0 0 0 32 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 112 117 113 0 126 0 8 0 0 0 2 115 114 0 14 106 97 118 97 46 108 97 110 103 46 76 111 110 103 59 -117 -28 -112 -52 -113 35 -33 2 0 1 74 0 5 118 97 108 117 101 120 114 0 16 106 97 118 97 46 108 97 110 103 46 78 117 109 98 101 114 -122 -84 -107 29 11 -108 -32 -117 2 0 0 120 112 0 0 0 0 0 0 0 1 115 114 0 31 99 108 111 106 117 114 101 46 108 97 110 103 46 80 101 114 115 105 115 116 101 110 116 65 114 114 97 121 77 97 112 -30 65 -80 10 80 36 -43 45 2 0 2 76 0 5 95 109 101 116 97 113 0 126 0 1 91 0 5 97 114 114 97 121 113 0 126 0 3 120 114 0 27 99 108 111 106 117 114 101 46 108 97 110 103 46 65 80 101 114 115 105 115 116 101 110 116 77 97 112 93 124 47 3 116 32 114 123 2 0 2 73 0 5 95 104 97 115 104 73 0 7 95 104 97 115 104 101 113 120 112 -1 -1 -1 -1 -1 -1 -1 -1 112 117 113 0 126 0 8 0 0 0 8 115 114 0 20 99 108 111 106 117 114 101 46 108 97 110 103 46 75 101 121 119 111 114 100 -30 -55 56 -39 53 -58 -82 -3 2 0 2 73 0 6 104 97 115 104 101 113 76 0 3 115 121 109 116 0 21 76 99 108 111 106 117 114 101 47 108 97 110 103 47 83 121 109 98 111 108 59 120 112 -75 79 -37 -28 115 114 0 19 99 108 111 106 117 114 101 46 108 97 110 103 46 83 121 109 98 111 108 16 -121 108 25 -15 -71 44 35 2 0 4 73 0 7 95 104 97 115 104 101 113 76 0 5 95 109 101 116 97 113 0 126 0 1 76 0 4 110 97 109 101 116 0 18 76 106 97 118 97 47 108 97 110 103 47 83 116 114 105 110 103 59 76 0 2 110 115 113 0 126 0 22 120 112 23 24 98 43 112 116 0 3 103 111 111 112 116 0 5 49 32 50 32 51 115 114 0 17 106 97 118 97 46 108 97 110 103 46 73 110 116 101 103 101 114 18 -30 -96 -92 -9 -127 -121 56 2 0 1 73 0 5 118 97 108 117 101 120 113 0 126 0 12 0 0 0 1 115 113 0 126 0 26 0 0 0 1 115 113 0 126 0 11 0 0 113 20 101 -18 -91 -65 115 113 0 126 0 11 0 0 113 20 101 -18 -91 -65 116 0 4 116 101 115 116 115 113 0 126 0 0 -1 -1 -1 -1 -1 -1 -1 -1 0 0 0 3 0 0 0 5 112 113 0 126 0 7 117 113 0 126 0 8 0 0 0 3 116 0 1 97 115 113 0 126 0 18 88 88 -13 86 115 113 0 126 0 21 -70 33 121 -99 112 116 0 1 98 112 115 113 0 126 0 11 0 0 0 0 0 0 0 123]
   [4 -126 83 78 65 80 80 89 0 0 0 0 1 0 0 0 1 0 0 0 86 100 24 11 0 0 0 2 3 0 9 1 20 1 10 0 0 0 4 1 12 72 3 103 111 111 1 0 0 0 5 49 32 50 32 51 2 0 0 0 1 5 5 32 3 0 0 113 20 101 -18 -91 -65 21 9 1 38 16 4 116 101 115 116 1 74 0 3 1 14 64 1 97 0 0 0 0 1 98 3 0 0 0 0 0 0 0 123]
   [5 -126 83 78 65 80 80 89 0 0 0 0 1 0 0 0 1 0 0 0 82 95 4 3 0 9 1 20 1 10 0 0 0 4 1 12 72 3 103 111 111 1 0 0 0 5 49 32 50 32 51 2 0 0 0 1 5 5 32 3 0 0 113 20 101 -18 -91 -65 21 9 1 38 36 4 116 101 115 116 11 0 0 0 3 1 14 64 1 97 0 0 0 0 1 98 3 0 0 0 0 0 0 0 123]
   [7 -126 83 78 65 80 80 89 0 0 0 0 1 0 0 0 1 0 0 0 63 77 124 91 49 32 123 58 103 111 111 32 34 49 32 50 32 51 34 44 32 49 32 49 44 32 49 50 52 51 51 50 52 50 51 9 3 62 16 0 88 44 32 34 116 101 115 116 34 32 91 34 97 34 32 58 98 32 49 50 51 93 125 93]])

(deftest stable-deserialization []
  (doseq [x old-ser-data]
    (is (= a-data-structure (deserialize (byte-array (map #(Byte. (byte %)) x)))))))

(def map-data
  {:keyword :foo
   :string "bar"
   :integer 3
   :long 52001110638799097
   :bigint (BigInteger. "9223372036854775808")
   :double 1.23
   :float (float 123/456789)
   :boolean true
   :nil nil
   :map {"hi" 9 "low" 0}
   :vector ["a" "b" "c"]
   :set #{"a" "b" "c"}
   :emptylist '()
   :serializable (java.util.Date.)
   :list '(1 2 3)
   :big-int (bigint 12)})

(deftest map-roundtrip-test
  (is-serializable? map-data)
  (is (thrown? Exception (round-trip (assoc map-data :promise (promise))))))

(defrecord Test-Record [x y z])

(deftest record-roundtrip-test
  (let [z (Test-Record. ["vec"] {:m "map"} 6)
        rec (Test-Record. "x" 1.23 z)
        round-tripped (round-trip rec)]
    (is (instance? Test-Record round-tripped))
    (is (instance? Test-Record (:z round-tripped)))
    (is-= rec round-tripped)
    (is-=-by class rec round-tripped)))

(deftest test-eof-on-incomplete-bytes
  (is (thrown? java.io.EOFException (deserialize (byte-array (take 6 (seq (serialize +default-uncompressed+ "foobar"))))))))

(defrecord Foo [x])

(deftest clojure-snappy-record-test
  (let [r (Foo. 1)
        rt (round-trip r +clojure-snappy+)]
    (is (= r rt))
    (is (instance? Foo rt))))

(deftest stream-packer-test
  (testing "simple usage"
    (let [packer (serialized-stream-packer 1000 +default-uncompressed+)]
      (is (not (packer "Hi world")))
      (is (thrown? Exception (packer (repeatedly 10000 rand))))
      (is (not (packer -42)))
      (let [[b :as chunks] (packer)]
        (is (= 1 (count chunks)))
        (is (= ["Hi world" -42] (serialized-unpack b))))))
  (testing "round trips"
    (doseq [[method expected-blocks data]
            [[+clojure+ 1 [1 "2" {3 [5]}]]
             [+default-uncompressed+ 1 (repeat 1000 {:foo :bar :baz :bat})]
             [+default-uncompressed+ 2 (repeat 4000 {:foo :bar :baz :bat})]
             [+java+ 62 (let [r (java.util.Random. 1)]
                          (for [i (range 100)]
                            (let [b (byte-array (mod (.nextInt r) 4000))]
                              (.nextBytes r b)
                              b)))]]]
      (let [packer (serialized-stream-packer 5000 method)
            blocks (pack packer data)]
        (is-= expected-blocks (count blocks))
        (is-= data (mapcat serialized-unpack blocks)))))
  (testing "compression ratio"
    (let [data (for [i (range 10000)] {:foo i :bar {:baz (hash i)}})
          packer (serialized-stream-packer 5000 +default-uncompressed+)
          blocks (pack packer data)
          packed-size (sum count blocks)
          individual-size (sum #(count (serialize +default+ %)) data)
          group-size (count (serialize +default+ data))]
      (is (<= packed-size (* 1.2 group-size)))
      (is (<= packed-size (* 0.25 individual-size))))))
