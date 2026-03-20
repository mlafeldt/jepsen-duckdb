(ns jepsen.duckdb.client
  "Common functions for our little HTTP client."
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clj-http.client :as http]
            [clojure [edn :as edn]]
            [clojure.tools.logging :refer [info warn]])
  (:import (java.net ConnectException
                     SocketException)
           (org.apache.http NoHttpResponseException)))


(def common-opts
  "Options we pass to every HTTP request"
  {:content-type       "application/edn"
   :socket-timeout     5000
   :connection-timeout 1000})

(defn post
  "Wrapper for `clj-http.client/post`. Always hits localhost on the given port.
  Makes an HTTP post request, sending the given Clojure data structure as EDN,
  and deserializing an EDN response. Returns body, or throws."
  ([port path]
   (post port path nil))
  ([port path body]
   (post port path body nil))
  ([port path body opts]
   (let [res (http/post (str "http://localhost:" port path)
                        (merge common-opts
                               {:body (pr-str body)}
                               opts))]
     (edn/read-string (:body res)))))

(defmacro with-errors
  "Intended for converting HTTP errors back to Jepsen failed operations. Takes
  an invoke op, runs body and returns its result, catching errors and
  converting some to fail/info ops."
  [op & body]
  `(try+ ~@body
         (catch ConnectException e#
           (assoc ~op :type :fail, :error :conn-refused))
         (catch SocketException e#
           (assoc ~op :type :info, :error [:socket (.getMessage e#)]))
         (catch NoHttpResponseException e#
           (assoc ~op :type :info, :error :no-response))
         (catch [:status 409] e#
           (assoc ~op :type :fail, :error :conflict))
         (catch [:status 500] e#
           (assoc ~op :type :info, :error [:server-error (:body e#)]))
         (catch [:status 503] e#
           (assoc ~op :type :info, :error [:server-error (:body e#)]))))

(defn write-logs!
  "Tells the server to write its logs to the DB."
  [port]
  (try (post port "/write-logs")
       (catch Exception e
         (warn e "Couldn't ask local node to write logs:"))))
