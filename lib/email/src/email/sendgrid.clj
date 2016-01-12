(ns email.sendgrid
  (:refer-clojure :exclude [send])
  (:use plumbing.core)
  (:require
   [plumbing.json :as json]
   [plumbing.logging :as log])
  (:import
   [javax.activation DataHandler DataSource FileDataSource]
   [javax.mail Authenticator Part PasswordAuthentication Session Transport]
   [javax.mail.internet InternetAddress MimeBodyPart MimeMessage MimeMultipart]))

(set! *warn-on-reflection* true)

(defnk session [host port password user]
  (Session/getInstance
   (doto (java.util.Properties.)
     (.put "mail.smtp.host" host)
     (.put "mail.smtp.port" port)
     (.put "mail.smtp.user" user)
     (.put "mail.smtp.socketFactory.fallback" false)
     (.put "mail.smtp.starttls.enable" true)
     (.put "mail.transport.protocol" "smtps")
     (.put "mail.smtp.auth" "true"))
   (proxy [Authenticator] []
     (getPasswordAuthentication []
       (PasswordAuthentication. user password)))))

;; TODO: fix crappy image warning.

(defn sanitize-subject [^String subject]
  "http://docs.oracle.com/javaee/1.4/api/javax/mail/internet/MimeMessage.html#setSubject(java.lang.String)
   says it's our job to remove line breaks from the subject.  If we don't, users who can control
   the subject can inject arbitrary headers into the email."
  (.replaceAll subject "(\r|\n)+" " "))

(def unsubscribe-header
  {:filters
   {:subscriptiontrack
    {:settings
     {"enable" 1
      "replace" "-unsub-"}}}})

(defn set-sendgrid-header! [^MimeMessage msg m]
  (let [header (json/generate-string m)]
    (.addHeader msg "X-SMTPAPI" header)))

(defn ^MimeBodyPart mime-body-part [^String id ^DataSource s]
  (doto (MimeBodyPart.)
    (.setDataHandler (DataHandler. s))
    (.setHeader "Content-ID", (str "<" id ">"))
    (.setFileName id)
    (.setDisposition Part/INLINE)))

;; TODO: still UTF-8 broken for names in from
(defnk send [session
             {^String from "noreply@example.com"}
             ^String to, ^String subject, ^String text,
             {html? false}
             {^String reply-to nil}
             {^String cc nil}
             {^String bcc nil}
             {attachment-paths nil}
             {attachment-data-sources nil} ;; more general, map from id to DataSource
             {unsubscribe? nil}]
  (assert (not unsubscribe?)) ;; Don't use sendgrid's unsubscribe anymore.
  (try
    (log/infof "Sending email to %s with subject %s" to subject)
    (let [msg     (MimeMessage. ^Session session)
          ^MimeMultipart multipart (MimeMultipart. "related")]
      ;;add body
      (.addBodyPart
       multipart
       (doto (MimeBodyPart.)
         (.setContent text (if html? "text/html; charset=UTF-8" "text/plain; charset=UTF-8"))
         (.setDisposition Part/INLINE)))

      (when unsubscribe?
        (set-sendgrid-header! msg unsubscribe-header))

      ;;add attachments
      (doseq [^String path attachment-paths]
        (.addBodyPart multipart (mime-body-part (last (.split path "/")) (FileDataSource. path))))
      (doseq [[id data-source] attachment-data-sources]
        (.addBodyPart multipart (mime-body-part id data-source)))

      ;; (.setDebug session true)
      (.setFrom msg (if (coll? from)
                      (InternetAddress. ^String (first from) ^String (second from) "UTF-8")
                      (InternetAddress. from)))
      (.setRecipients msg
                      (javax.mail.Message$RecipientType/TO)
                      (InternetAddress/parse to))
      (when cc
        (.setRecipients msg
                        (javax.mail.Message$RecipientType/CC)
                        (InternetAddress/parse cc)))
      (when bcc
        (.setRecipients msg
                        (javax.mail.Message$RecipientType/BCC)
                        (InternetAddress/parse bcc)))
      (.setSubject msg (sanitize-subject subject) "UTF-8")
      (.setContent msg multipart)

      (when reply-to
        (.setReplyTo msg (into-array  InternetAddress [(InternetAddress. reply-to)])))

      (Transport/send msg))
    (catch Throwable t
      (log/warnf t "Error sending email %s to %s" subject to))))

(set! *warn-on-reflection* false)
