(ns jepsen.duckdb.local-node.client
  "Core functions for opening DuckDB clients, doing transactions, etc."
  (:gen-class)
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn fatal]]
            [dom-top.core :refer [loopr with-retry]]
            [next.jdbc :as j]
            [next.jdbc [protocols :as jp]
                       [result-set :as rs]]
            [next.jdbc.sql.builder :as sqlb])
  (:import (java.sql Connection
                     SQLException)))

;; Read SQL arrays as vectors
(extend-protocol rs/ReadableColumn
  java.sql.Array
  (read-column-by-label [^java.sql.Array a _]
    (vec (.getArray a)))

  (read-column-by-index [^java.sql.Array a _2 _3]
    (vec (.getArray a))))

;; General SQL client stuff

(defmacro with-logging
  "Takes an options map, a binding vector of [conn-name jdbc-connection], and a
  body. Binds `conn-name` to a JDBC connection and evaluates body, presumably
  using `conn-name`. If `(:log-sql opts)` is truthy, `conn-name` will log SQL
  statements to the console."
  [opts [conn-name conn] & body]
  `(let [~conn-name (if (:log-sql ~opts)
                      (j/with-logging ~conn
                        (fn ~'sql-log [op# sql#]
                          (info op# "query" (pr-str sql#)))
                        (fn ~'res-log [op# state# res#]
                          (info op# "result" (pr-str res#))))
                      ~conn)]
     ~@body))

(defn set-transaction-isolation!
  "Sets the transaction isolation level on a connection. Returns conn."
  [conn level]
  (.setTransactionIsolation
    conn
    (case level
      :serializable     Connection/TRANSACTION_SERIALIZABLE
      :repeatable-read  Connection/TRANSACTION_REPEATABLE_READ
      :read-committed   Connection/TRANSACTION_READ_COMMITTED
      :read-uncommitted Connection/TRANSACTION_READ_UNCOMMITTED))
  conn)

(defn open*
  "Raw form of open, which does not retry read-only opens when the file does
  not exist."
  [{:keys [db-file rw-mode]}]
  (info "Opening" db-file)
  (let [; TODO: temp_directory?
        spec {:dbtype "duckdb"
              :dbname db-file
              "duckdb.read_only" (case rw-mode
                                   :rw "false"
                                   :ro "true")}
        ds (j/get-datasource spec)
        conn (j/get-connection ds)]
    conn))

(defn open
  "Opens a connection to the local DuckDB file. Options:

      :db-file          The path to the DB file
      :rw-mode          Either :rw or :ro
      :isolation-level  e.g. :serializable

  For read-only clients, retries when the database does not yet exist; we're
  presumably waiting for the writer to open it."
  [opts]
  (with-retry [i 10]
    (open* opts)
    (catch SQLException e
      (if (and (pos? i)
               (re-find #"in read-only mode: database does not exist"
                        (.getMessage e)))
        (do (info "Waiting for DB to be created...")
            (Thread/sleep 1000)
            (retry (dec i)))
        (throw e)))))

(defn close!
  "Closes a connection"
  [^Connection conn]
  (info "Closing DB connection")
  (.close conn))

(defmacro with-conn-duplicate
  "Multiple threads can use a DuckDB client concurrently, but they can't share
  a single sql.Connection. Instead, we duplicate a connection for the scope of
  a single request. I'm not totally sure what the semantics *should* be here;
  this is a bit of a guess.

  Takes global opts, a binding form `[conn-name conn]` and a body. Duplicates
  `conn`, binds it to `conn-name`, and evaluates body, closing `conn-name` at
  the end."
  [opts [conn-name conn] & body]
  (let [conn (vary-meta conn assoc :tag 'org.duckdb.DuckDBConnection)]
    `(let [~conn-name (.duplicate ~conn)]
       (try ~@body
            (finally
              (.close ~conn-name))))))

(defmacro with-conn-open
  "Multiple threads can use a DuckDB client concurrently, but they can't share
  a single sql.Connection. This version of `with-conn` opens a fresh Connection
  every time."
  [opts [conn-name conn] & body]
  `(let [~conn-name (open ~opts)]
     (try ~@body
          (finally
            (.close ~conn-name)))))

(defmacro with-conn
  "Acquires a connection for the use of a single thread's request. Takes global
  opts, a binding form [conn-name conn], and a body. Evaluates body with
  `conn-name` bound to a connection which is (supposed to be) safe for use by a
  thread."
  [opts binding & body]
  `(with-conn-open ~opts ~binding ~@body))

(defmacro with-errors
  "Takes a body which does some SQL; evals body, throwing typed errors as
  appropriate."
  [& body]
  `(try ~@body
        (catch SQLException e#
          (condp re-find (.getMessage e#)
            #"Conflict on tuple deletion!"
            (throw+ {:type :conflict, :definite? true})

            #"Conflict on update!"
            (throw+ {:type :conflict, :definite? true})

            #"[Dd]uplicate key"
            (throw+ {:type :duplicate-key, :definite? true})

            (throw e#)))))

;; DuckDB doesn't support savepoints yet

(defn set-savepoint!
  "Creates a savepoint on a next.jdbc connection."
  [conn name]
  (info :conn (type conn) "is a Connection?" (instance? Connection conn))
  (let [^Connection conn (if (instance? Connection conn)
                           conn
                           (jp/unwrap conn))]
    (.setSavepoint conn name)))

(defn release-savepoint!
  "Releases a savepoint on a next.jdbc connection."
  [conn savepoint]
  (let [^Connection conn (if (instance? Connection conn)
                           conn
                           (jp/unwrap conn))]
    (.releaseSavepoint conn name)))

(defn rollback-savepoint!
  "Releases a savepoint on a next.jdbc connection."
  [conn savepoint]
  (let [^Connection conn (if (instance? Connection conn)
                           conn
                           (jp/unwrap conn))]
    (.rollbackSavepoint conn name)))

(defn ensure-abort!
  "Ensures any currently-running transaction has been aborted. If no
  transaction is running, does nothing."
  [conn]
  (try
    (j/execute! conn ["ABORT"])
    true
    (catch SQLException e
      (if (re-find #"no transaction is active" (.getMessage e))
        false ; Sure, whatever
        (throw e)))))

(defmacro with-transaction
  "I'm not sure if DuckDB actually respects j/with-txn, so let's try rolling
  our own just to be sure."
  [[conn-name conn] & body]
  `(let [~conn-name ~conn]
     (try (j/execute! ~conn-name ["BEGIN TRANSACTION"])
          (let [res# ~@body]
            (j/execute! ~conn-name ["COMMIT"])
            res#)
          (catch Throwable t#
            (ensure-abort! ~conn-name)
            (throw t#)))))
