(ns web.client-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [clojure.java.io :as io]
   [schema.core :as s]
   [plumbing.resource :as resource]
   [web.client :as client]
   [web.data :as data]
   [web.fnhouse :as fnhouse]
   [web.handlers :as handlers])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream]
   [java.util Arrays]
   [java.util.zip DeflaterInputStream GZIPOutputStream]
   [org.apache.commons.io IOUtils]
   [org.apache.http.client.params ClientPNames CookiePolicy]
   [org.apache.http.impl.client DefaultHttpClient]))


(defn gzip
  "Returns a gzip'd version of the given byte array."
  [b]
  (let [baos (ByteArrayOutputStream.)
        gos  (GZIPOutputStream. baos)]
    (IOUtils/copy (ByteArrayInputStream. b) gos)
    (.close gos)
    (.toByteArray baos)))

(defn deflate
  "Returns a deflate'd version of the given byte array."
  [b]
  (IOUtils/toByteArray (DeflaterInputStream. (ByteArrayInputStream. b))))

(defnk $get$GET
  {:responses {200 s/Any}}
  []
  (handlers/html-response "get"))

(defnk $head$HEAD
  {:responses {200 s/Any}}
  []
  {:status 200})

(defnk $content-type$GET
  {:responses {200 s/Any}}
  [[:request headers]]
  (handlers/html-response
   (safe-get headers "content-type")))

(defnk $header$GET
  {:responses {200 s/Any}}
  [[:request headers]]
  (handlers/html-response
   (safe-get headers "x-my-header")))

(defnk $post$POST
  {:responses {200 s/Any}}
  [[:request body]]
  (handlers/html-response body))

(defnk $error$GET
  {:responses {500 s/Any}}
  []
  {:status 500 :body "o noes"})

(defnk $moved$GET
  {:responses {301 s/Any}}
  []
  (handlers/permanent-redirect "http://localhost:8080/get"))

(defnk $moved-absolute$GET
  {:responses {301 s/Any}}
  []
  (handlers/permanent-redirect "/get"))

(defnk $moved-relative$GET
  {:responses {301 s/Any}}
  []
  (handlers/permanent-redirect "get"))

(defnk $bounce$GET
  {:responses {302 s/Any}}
  []
  (handlers/url-redirect "http://localhost:8080/moved"))

(use-fixtures :once
  (fn [f]
    (resource/with-open [s (resource/bundle-run
                            (fnhouse/simple-fnhouse-server-resource {"" 'web.client-test})
                            {:env :test :port 8080})]
      (f))))

(def base-req
  {:scheme "http"
   :host "localhost"
   :port 8080})

(deftest roundtrip
  (let [resp (client/fetch :get (merge base-req {:uri "/get"}))]
    (is (= 200 (:status resp)))
    (is (= "close" (get-in resp [:headers "connection"])))
    (is (= "get" (:body resp)))))

(defn is-passed [middleware req]
  (let [client (middleware identity)]
    (is (= req (client req)))))

(defn is-applied [middleware req-in req-out]
  (let [client (middleware identity)]
    (is (= req-out (client req-in)))))

(deftest apply-on-compressed
  (let [resp (client/decompress {:body (-> "foofoofoo"
                                           .getBytes
                                           gzip
                                           java.io.ByteArrayInputStream.)
                                 :headers {"content-encoding" "gzip"}})]
    (is (= "foofoofoo" (-> resp :body  slurp)))))

(deftest apply-on-deflated
  (let [resp (client/decompress {:body (-> "barbarbar" .getBytes
                                           deflate
                                           java.io.ByteArrayInputStream.)
                                 :headers {"content-encoding" "deflate"}})]
    (is (= "barbarbar" (slurp (:body resp))))))

(deftest pass-on-non-compressed
  (let [resp (client/decompress {:body "foo"})]
    (is (= "foo" (:body resp)))))

(deftest apply-on-accept
  (is (=
       {:headers {"Accept" "application/json"}}
       (client/accept {:accept :json}))))

(deftest pass-on-no-accept
  (let [req {:uri "/foo"}]
    (is (= req (client/accept req)))))

(deftest apply-on-accept-encoding
  (is (=  {:headers {"Accept-Encoding" "identity, gzip"}}
          (client/wrap-accept-encoding
           {:accept-encoding [:identity :gzip]}))))

(deftest pass-on-no-accept-encoding
  (let [req {:uri "/foo"}]
    (is (= req (client/wrap-accept-encoding req)))))


(deftest apply-on-output-coercion
  (let [resp (client/output-coercion
              :byte-array
              {:body (io/input-stream (.getBytes "foo"))})]
    (is (Arrays/equals (.getBytes "foo") (:body resp)))))

(deftest input-stream-output-coercion
  (let [resp (client/output-coercion
              :input-stream
              {:body (io/input-stream (.getBytes "foo"))})]
    (is (instance? java.io.InputStream (:body resp)))
    (is (Arrays/equals (.getBytes "foo")
                       (org.apache.commons.io.IOUtils/toByteArray (:body resp))))))

(deftest pass-on-no-output-coercion
  (let [resp (client/output-coercion nil {:body nil})]
    (is (nil? (:body resp))))
  (let [resp (client/output-coercion
              :byte-array
              {:body :thebytes})]
    (is (= :thebytes (:body resp)))))

(deftest apply-on-input-coercion
  (let [resp (client/input-coercion {:body "foo"})]
    (is (= "UTF-8" (:character-encoding resp)))
    (is (Arrays/equals (data/utf8-bytes "foo") (:body resp)))))

(deftest pass-on-no-input-coercion
  (let [req {:body (data/utf8-bytes "foo")}]
    (is (= req
           (client/input-coercion req)))))

(deftest apply-on-content-type
  (is (= {:content-type "application/json"}
         (client/content-type
          {:content-type :json}))))

(deftest pass-on-no-content-type
  (let [req {:uri "/foo"}]
    (is (= req (client/content-type req)))))

(deftest apply-on-query-params
  (is (= {:query-string "foo=bar&dir=%3C%3C"}
         (client/query-params
          {:query-params {"foo" "bar" "dir" "<<"}}))))

(deftest pass-on-no-query-params
  (let [req {:uri "/foo"}]
    (is (= req
           (client/query-params req)))))

(deftest apply-on-basic-auth
  (is (= {:headers {"Authorization" "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="}}
         (client/basic-auth
          {:basic-auth ["Aladdin" "open sesame"]}))))

(deftest pass-on-no-basic-auth
  (let [req {:uri "/foo"}]
    (is (= req
           (client/basic-auth req)))))

(deftest encode-illegal-characters-test
  (testing "encode non-ascii characters"
    (is-= "wikia.com/wiki/%ED%8F%AC%EC%BC%93%EB%AA%AC_%EA%B8%80%EB%A1%9C%EB%B2%8C_%EB%A7%81%ED%81%AC"
          (client/encode-illegal-characters "wikia.com/wiki/포켓몬_글로벌_링크")))
  (testing "don't mess up already-encoded characters"
    (is-= "/wiki/%ED%8F%AC%EC%BC%93%EB%AA%AC_%EA%B8%80%EB%A1%9C%EB%B2%8C_%EB%A7%81%ED%81%AC"
          (client/encode-illegal-characters
           "/wiki/%ED%8F%AC%EC%BC%93%EB%AA%AC_%EA%B8%80%EB%A1%9C%EB%B2%8C_%EB%A7%81%ED%81%AC")))
  (testing "accented letters"
    (is-= "%C3%89,%20%C3%A9%20(e-acute)%20is%20a%20letter%20of%20the%20Latin%20alphabet."
          (client/encode-illegal-characters
           "É, é (e-acute) is a letter of the Latin alphabet.")))
  (testing "round trip"
    (is-= "/fish/É크"
          (data/url-decode (client/encode-illegal-characters "/fish/É크")))))

(deftest parse-url-test
  (is-= {:scheme "http"
         :host "google.com"
         :port 8080
         :uri "/foo"
         :query-string "bar=bat"}
        (client/parse-url "http://google.com:8080/foo?bar=bat"))
  (is-= {:scheme "http"
         :host "example.com"
         :port nil
         :uri "/topic/Science%20Fiction"
         :query-string nil}
        (client/parse-url "http://example.com/topic/Science Fiction"))
  (is-= {:scheme "grabbag"
         :host "create_account"
         :port nil
         :uri ""
         :query-string nil}
        (client/parse-url "grabbag://create_account")))

(deftest parse-url-with-hash
  (let [u (client/parse-url "http://gizmodo.com/#!5789093/the-near+future-of-mobile-gaming-is-going-to-be-pretty-epic")]
    (is (= "/#!5789093/the-near+future-of-mobile-gaming-is-going-to-be-pretty-epic" (:uri  u)))))

(deftest ^{:slow true} url-with-hash
  (let [u "http://gizmodo.com/#!5789093/the-near+future-of-mobile-gaming-is-going-to-be-pretty-epic"]
    (is (= u
           (:url (client/fetch :get u))))))

(deftest strip-bad-punc-test
  (is (= "utf-8"
         (client/strip-punc "utf-8;")))
  (is
   (= "iso-8859-2"
      (client/strip-punc "iso-8859-2"))))

(deftest charset-in-body
  (let [body "<html>
<head>
<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>
<meta name=\"author\" content=\"I Wayan Saryada\"/></head></html>"]
    (is (= "utf-8" (client/charset {:body body})))))

(deftest charset-test
  (is (= "windows-1250"
         (client/charset {:body  "
<html><head>
<meta http-equiv=\"Content-Type\" content=\"text/html; charset=windows-1250\"/>
<meta http-equiv=\"cache-control\" content=\"no-cache\"/>
<meta name=\"robots\" content=\"all\"/>
<title>Zemřel Pavel Vondruška, muzikant a jeden z 'Cimrmanů' - www.lidovky.cz</title>
</head><body></body></html>"}))))

(deftest charset-meta-charset
  (is (= "EUC-JP"
         (client/charset {:body
                          "<html><head><META http-equiv=\"Content-Type\" content=\"text/html; charset=EUC-JP\"></head><body></body></html>"}))))

(deftest html5-charset-test
  (is (= "fake-charset"
         ( client/charset
           {:body"<!doctype html>
<html>
 <head>
   <meta charset=\"fake-charset\">
   <title>Example document</title>
 </head>
 <body>
   <p>Example paragraph</p>
 </body>
</html>"}))))

(deftest charset-test
  (is (= "windows-1250"
         (client/charset {:body "<!DOCTYPE html PUBLIC \"-//Lidovky//DTD HTML 4//EN\" \"http://g.lidovky.cz/dtd/n3_uni.dtd\">
<html><head>
<meta http-equiv=\"Content-Type\" content=\"text/html; charset=windows-1250\">
<meta http-equiv=\"cache-control\" content=\"no-cache\">
<meta name=\"robots\" content=\"all\">
<title>Zemřel Pavel Vondruška, muzikant a jeden z 'Cimrmanů' - www.lidovky.cz</title>
</head><body></body></html>"}))))

;;http://www.boston.com/sports/colleges/extras/colleges_blog/2011/03/harvard_wins_sh.html
#_(deftest charset-prod-example-breakage
    (is (= "ISO-8859-1"
           (client/charset
            {:body
             " <html lang=\"en\">
  <head>
<meta http-equiv=\"Content-Type\" content=\"text/html; charset=\"ISO-8859-1\">"}))))

(deftest redirect-path-test
  (is (= [[:302 "http://localhost:8080/moved"]
          [:301 "http://localhost:8080/get"]]
         (:redirects (client/fetch :get "http://localhost:8080/bounce")))))

(def base-req
  {:scheme "http"
   :host "localhost"
   :port 8080})

(defn request
  ([req]
     (client/request (merge base-req req)))
  ([client req]
     (client/request client (merge base-req req))))

(defn slurp-body [req]
  (slurp (:body req)))

(deftest basic-http-client-test
  (let [c (client/basic-http-client)]
    (is (= (doto (.getParams (DefaultHttpClient.))
             (.setParameter ClientPNames/COOKIE_POLICY
                            CookiePolicy/BROWSER_COMPATIBILITY)))
        (.getParams (client/basic-http-client)))))

(deftest makes-get-request
  (let [resp (request {:request-method :get :uri "/get"})]
    (is (= 200 (:status resp)))
    (is (= "get" (slurp-body resp)))))

(deftest makes-get-request-ignore-ssl
  (let [resp (request (client/wrap-ignore-ssl (client/basic-http-client)) {:request-method :get :uri "/get"})]
    (is (= 200 (:status resp)))
    (is (= "get" (slurp-body resp)))))

(deftest makes-head-request
  (let [resp (request {:request-method :head :uri "/head"})]
    (is (= 200 (:status resp)))
    (is (nil? (:body resp)))))

(deftest makes-head-request-ignore-ssl
  (let [resp (request (client/wrap-ignore-ssl (client/basic-http-client)) {:request-method :head :uri "/head"})]
    (is (= 200 (:status resp)))
    (is (nil? (:body resp)))))

(deftest sets-content-type-with-charset
  (let [resp (request {:request-method :get :uri "/content-type"
                       :content-type "text/plain" :character-encoding "UTF-8"})]
    (is (= "text/plain; charset=UTF-8" (slurp-body resp)))))

(deftest sets-content-type-without-charset
  (let [resp (request {:request-method :get :uri "/content-type"
                       :content-type "text/plain"})]
    (is (= "text/plain" (slurp-body resp)))))

(deftest sets-arbitrary-headers
  (let [resp (request {:request-method :get :uri "/header"
                       :headers {"X-My-Header" "header-val"}})]
    (is (= "header-val" (slurp-body resp)))))

(deftest sends-and-returns-byte-array-body
  (let [resp (request {:request-method :post :uri "/post"
                       :body (data/utf8-bytes "contents")})]
    (is (= 200 (:status resp)))
    (is (= "contents" (slurp-body resp)))))

(deftest returns-arbitrary-headers
  (let [resp (request {:request-method :get :uri "/get"})]
    (is (string? (get-in resp [:headers "date"])))))

(deftest returns-status-on-exceptional-responses
  (let [resp (request {:request-method :get :uri "/error"})]
    (is (= 500 (:status resp)))))

(deftest redirect-moved-test
  (let [resp (request {:request-method :get :uri "/moved"})]
    (is (= 200 (:status resp))))
  (let [resp (request {:request-method :get :uri "/moved-absolute"})]
    (is (= 200 (:status resp))))
  (let [resp (request {:request-method :get :uri "/moved-relative"})]
    (is (= 200 (:status resp)))))

(deftest redirect-strategy-test
  (let [resp (client/fetch :get {:scheme "http"
                                 :host "localhost"
                                 :port 8080
                                 :uri "/bounce"})]
    (is (= [[:302 "http://localhost:8080/moved"]
            [:301 "http://localhost:8080/get"]]
           (:redirects resp)))))

(deftest clean-url-query-string-test
  (let [bbc-url "http://www.bbc.com/travel/feature/20140908-coolmhhbc"
        nyt-url "http://nytimes.com"]
    (is (= bbc-url
           (client/clean-url-query-string (str bbc-url "?ocid=twtvl"))))
    (is (= (str nyt-url "?ocid=twitter"))
        (client/clean-url-query-string (str nyt-url "?ocid=twtvl")))
    (is (= nyt-url
           (client/clean-url-query-string
            (str nyt-url
                 "?amp;utm_campaign=partner&amp;utm_medium=twitter&amp;utm_source=wired&mbid=social_twitter"))))
    (testing "query-params are sorted"
      (is (= (str nyt-url "?a=10&b=20")
             (client/clean-url-query-string (str nyt-url "?b=20&a=10")))))))

(deftest resolved-url-test
  (testing "redirects"
    (is (=
         "http://paul.kedrosky.com/archives/2011/04/coachella-glish.html"
         (client/resp->resolved-url
          {:redirects [[:301 "http://paul.kedrosky.com/archives/2011/04/coachella-glish.html?utm_source=feedburner&utm_medium=feed&utm_campaign=Feed%3A+InfectiousGreed+%28Paul+Kedrosky%27s+Infectious+Greed%29%22"]]})))))


(deftest ^{:slow true} expand-that-motherfucker
  (is (= "http://www.wiggle.co.uk/team-sky"
         (client/expand-url client/spoofed-http-client "http://t.co/T1eO78I")))
  (is (= "http://3.bp.blogspot.com/-_ik30OHzTp4/TkhxxtKO6RI/AAAAAAABVAo/0x7_PAn7CfE/s1600/00_mylove_14_woodstock_morrow.jpg"
         (client/expand-url client/spoofed-http-client "http://cultr.me/rq5Oin"))))


(deftest body->canonical-url-test
  (testing "canonical"
    (let [end "</head>"
          rel "<link  rel=\"canonical\"  href=\"rel/canonical\">"
          rel-link "rel/canonical"
          og  "<meta  property=\"og:url\"  content=\"og/url\">"
          og-link "og/url"]
      (is (= rel-link
             (client/body->canonical-url (str "lakjsdflkjasdfljksdf" rel og end) 0)))
      (is (= og-link
             (client/body->canonical-url (str "lakjsdflkjasdfljksdf" og rel end) 0)))
      (is (= rel-link
             (client/body->canonical-url
              (.replace (str "lakjsdflkjasdfljksdf" rel og end) "\"" "'") 0)))
      (is (= og-link
             (client/body->canonical-url
              (.replace (str "lakjsdflkjasdfljksdf" og rel end) "\"" "'") 0)))
      (is (= nil
             (client/body->canonical-url (str "lakjsdflkjasdfljksdf" end og rel) 0)))
      (is (= nil
             (client/body->canonical-url (str "lakjsdflkjasdfljksdf" og rel end) 20)))))
  (testing "meta-refresh"
    (is (= "http://www.brainpickings.org/2014/02/20/the-benjamin-franklin-effect-mcraney/"
           (client/body->canonical-url
            "<noscript><META http-equiv=\"refresh\" content=\"0;URL=http://www.brainpickings.org/2014/02/20/the-benjamin-franklin-effect-mcraney/\"></noscript><script>location.replace(\"http:\\/\\/www.brainpickings.org\\/2014\\/02\\/20\\/the-benjamin-franklin-effect-mcraney\\/\")</script>"
            0)))))
