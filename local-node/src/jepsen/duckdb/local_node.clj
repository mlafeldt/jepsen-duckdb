(ns jepsen.duckdb.local-node
  "A small HTTP server that does transactions against a local DuckDB file."
  (:gen-class)
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clojure [datafy :refer [datafy]]
                     [edn :as edn]
                     [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn fatal]]
            [dom-top.core :refer [loopr with-retry]]
            [jepsen.duckdb.local-node [client :as c]
                                      [append :as append]]
            [next.jdbc :as j]
            [next.jdbc [protocols :as jp]
                       [result-set :as rs]]
            [next.jdbc.sql.builder :as sqlb]
            [org.httpkit.server :as http])
  (:import (java.io BufferedReader
                    InputStreamReader
                    PushbackReader)
           (java.sql Connection
                     SQLException)))

;; Server

(defn setup!
  "Sets up the DB connection on initial startup."
  [conn opts]
  (c/with-logging opts [conn conn]
    (when (:duckdb-log opts)
      (info "Enabling DuckDB logging")
      (j/execute! conn ["CALL enable_logging()"]))

    (when (:disable-index-scan opts)
      (info "Disabling index scans")
      (j/execute! conn ["SET index_scan_percentage=0"])
      (j/execute! conn ["SET index_scan_max_count=0"]))
    (when (:disable-optimizer opts)
      (info "Disabling optimizer")
      (j/execute! conn ["PRAGMA disable_optimizer"]))

    (info "Setting up tables...")
    (append/create-tables! conn opts)))

(def logs-written? (atom false))

(defn write-logs!
  "Writes logs to the duckdb file. Only callable once."
  [conn opts]
  (if (:duckdb-log opts)
    (locking logs-written?
      (if @logs-written?
        :already-written
        (do ; Write
            (info "Saving DuckDB logs to table `jepsen_logs`")
            (j/execute! conn ["CREATE TABLE jepsen_logs AS SELECT * from duckdb_logs"])
            (reset! logs-written? true)
            :written)))
    :disabled))

(defn handle*
  "Takes a request with a deserialized body and processes it, returning an
  unserialized response body."
  [conn opts req]
  (c/with-conn opts [conn conn]
    (c/with-logging opts [conn conn]
      (let [{:keys [body uri]} req
            _ (when (:log-sql opts) (info "request:" uri (pr-str body)))
            res (case uri
                  "/append"     (append/txn! conn body opts)
                  "/write-logs" (write-logs! conn opts))]
        (when (:log-sql opts) (info "response:" uri (pr-str res)))
        res))))

(defn handler
  "Returns a function that handles HTTP requests."
  [conn opts]
  (fn handle [req]
    (try+
      (let [; Deserialize request body
            ; _ (pprint req)
            body (when (:body req)
                   (edn/read (PushbackReader.
                               (BufferedReader.
                                 (InputStreamReader. (:body req) "UTF-8")))))
            req (assoc req :body body)
            res (handle* conn opts req)]
        {:status  200
         :headers {"Content-Type" "application/edn"}
         :body    (pr-str res)})

      ; Definite exceptions
      (catch :definite? e
        {:status  409
         :headers {"Content-Type" "application/edn"}
         :body    (pr-str (:type e))})

      ; Other exceptions are a 503
      (catch Exception e
        (warn e "Exception handling request")
        {:status  503
         :headers {"Content-Type" "application/edn"}
         :body    (pr-str (datafy e))})

      ; Other Throwables we log and explode
      (catch Throwable t
        (fatal t "Fatal exception handling request")
        (throw t)))))

(defn parse-comma-separated-kws
  "Takes a string of comma-separated values and turns it into a vector of
  keywords."
  [s]
  (->> (str/split s #",")
       (mapv keyword)))

(defn read-opts
  "Reads enviromnent variable and returns a nice parsed option map. For
  example:

    {:db-file   \"/foo/duck.db\"
     :port      10002
     :log-sql   true
     :isolation :repeatable-read
     ...}

  TODO: validate this structure! I'm on a tight schedule here, sorry!"
  []
  (let [store-dir (System/getenv "JEPSEN_STORE_DIR")
        port      (parse-long (System/getenv "JEPSEN_PORT"))
        rw-mode   (keyword (System/getenv "JEPSEN_RW_MODE"))
        opts      (edn/read-string (System/getenv "JEPSEN_OPTS"))]
    (assert (and (string? store-dir) (not= "" store-dir)))
    (assert (#{:ro :rw} rw-mode))
    (assert (pos? port))
    (merge opts
           {:db-file   (str store-dir "/duck.db")
            :rw-mode   rw-mode
            :port      port})))

(defn -main
  "Main entrypoint. All behavior is taken from the following environment
  variables:

  JEPSEN_STORE_DIR  The directory where we should store duck.db etc.

  JEPSEN_PORT       The local HTTP port to bind

  JEPSEN_RW_MODE    Whether to connect in read-write or read-only mode

  JEPSEN_OPTS       An EDN map of CLI options from the test harness. We do this
                    to avoid plumbing every single option through separately.
                    See jepsen.duckdb.cli for what options exist."
  []
  (try
    (let [opts (read-opts)
          conn (c/open opts)]
      (info "Options are:\n" (with-out-str (pprint opts)))
      (setup! conn opts)
      (http/run-server (handler conn opts) (select-keys opts [:port]))
      (info "Waiting for HTTP requests on port" (:port opts))
      (while true
        (Thread/sleep 100000)))
    (catch Throwable t
      (fatal t "Fatal error")
      (System/exit 1))))
