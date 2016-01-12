(ns plumbing.auth
  (:use plumbing.core)
  (:require
   [clojure.string :as str]
   [schema.core :as s]
   [plumbing.serialize :as serialize]
   [plumbing.new-time :as new-time])
  (:import
   [javax.crypto Mac SecretKey SecretKeyFactory]
   [javax.crypto.spec SecretKeySpec PBEKeySpec]
   [org.apache.commons.codec.binary Base64 Hex]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(def +default-algorithm+ "HmacSHA1")

(defn ^String sha512-hex
  [^String data]
  (let [md (. java.security.MessageDigest getInstance "sha-512")]
    (. md update (.getBytes data))
    (reduce #(str %1 (format "%02x" %2)) "" (. md digest))))

(defn- ^bytes hmac-bytes
  [^String algorithm ^String key ^String data]
  (let [signing-key (SecretKeySpec. (.getBytes key) algorithm)
        mac (doto (Mac/getInstance algorithm) (.init signing-key))]
    (.doFinal mac (.getBytes data))))

(defn ^String url-safe-base64
  "URL safe base 64"
  [^bytes b]
  (Base64/encodeBase64URLSafeString b))

(defn decode-base64
  "URL safe base 64"
  ^bytes [^String s]
  (Base64/decodeBase64 s))

(def +random-secret-key+ "SECRET_KEY_REDACTED")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(let [secure-random (java.security.SecureRandom/getInstance "SHA1PRNG")]
  (defn rand-str
    "Generate a random string in base 36 (0-9,a-z) of the provided length"
    [length]
    (loop [raw (.toString (new BigInteger (int (* 6 length)) secure-random) 36)]
      (if (>= (count raw) length)
        (subs raw 0 length)
        (recur (str "0" raw))))))

(s/defn hmac :- String
  "Calculate HMAC signature for given data."
  ([key :- String data :- String]
     (hmac +default-algorithm+ key data))
  ([algorithm :- String key :- String data :- String]
     (String. (Base64/encodeBase64 (hmac-bytes algorithm key data)) "UTF-8")))

(s/defn hmac-hex :- String
  "Calculate HMAC signature for given data."
  [algorithm :- String key :- String data :- String]
  (Hex/encodeHexString (hmac-bytes algorithm key data)))

(s/defn token-key :- String
  "Generate a secret key from the single secret above.  Advantage over rand-str is we don't
   scatter secret keys throughout the codebase."
  [s :- String]
  (hmac +random-secret-key+ s))

(s/defn secure-token :- String
  "Make a secure token hashed with encryption key, such that payloads can later
   be recovered with high assurance that the user hasn't tampered with them."
  [key :- String payloads :- [String]]
  (let [payload (str/join "." (map #(url-safe-base64 (.getBytes ^String %)) payloads))]
    (str payload "." (url-safe-base64 (hmac-bytes +default-algorithm+ key payload)))))

(s/defn token-data :- (s/maybe [String])
  "Return the payloads used to create the secure token, or nil if the hash doesn't match."
  [key :- String token :- String ]
  (let [pieces (.split token "\\.")
        payloads (map #(String. (decode-base64 %) "UTF-8") (drop-last pieces))]
    (when (= (secure-token key payloads) token)
      payloads)))

(s/defn expiring-token :- String
  "Make a secure token that is timestamped, for use with expiring-token-data"
  ([key :- String payloads :- [String]]
     (expiring-token key (millis) payloads))
  ([key :- String ts :- long payloads :- [String]]
     (secure-token key (cons (str ts) payloads))))

(s/defn expiring-token-data :- (s/maybe [String])
  "Return the data passed to expiring-token, or nil if the hash doesn't match or
   more than expiration-ms have passed since its creation."
  [key :- String expiration-ms :- long token :- String]
  (when-let [[created & payloads] (token-data key token)]
    (when (< (new-time/time-since (Long/parseLong created) :ms) expiration-ms)
      payloads)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; encryption, currently used for securely storing session info in cookie

(defrecord Cipher [^javax.crypto.Cipher encrypter ^javax.crypto.Cipher decrypter])

(defnk ^:always-validate make-cipher :- Cipher
  [salt :- String
   passphrase :- String
   {enc-mode :- (s/enum :ECB :basic) :ECB}]
  (let [salt (.getBytes ^String salt)
        iterations 10000
        ^SecretKeyFactory factory (SecretKeyFactory/getInstance "PBKDF2WithHmacSHA1")
        ^SecretKey tmp (.generateSecret factory (PBEKeySpec. (.toCharArray ^String passphrase) salt iterations 128))
        ^SecretKeySpec key-spec (SecretKeySpec. (.getEncoded tmp) "AES")
        cipher-enc-instance (case enc-mode :ECB "AES/ECB/PKCS5Padding" :basic "AES/ECB/NoPadding")]
    (map->Cipher
     {:encrypter (doto (javax.crypto.Cipher/getInstance cipher-enc-instance)
                   (.init javax.crypto.Cipher/ENCRYPT_MODE key-spec))
      :decrypter (doto (javax.crypto.Cipher/getInstance cipher-enc-instance)
                   (.init javax.crypto.Cipher/DECRYPT_MODE key-spec))})))

(s/defn encrypt :- String
  [cipher :- Cipher
   s :- String]
  (-> cipher
      ^javax.crypto.Cipher (.encrypter)
      (.doFinal (serialize/serialize serialize/+default+ s))
      url-safe-base64))

(s/defn decrypt :- String
  [cipher :- Cipher
   s :- String]
  (-> cipher
      ^javax.crypto.Cipher (.decrypter)
      (.doFinal (decode-base64 s))
      serialize/deserialize))

(set! *warn-on-reflection* false)
