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
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [org.httpkit.server :as http])
  (:import (java.io BufferedReader
                    InputStreamReader
                    PushbackReader)
           (java.sql Connection
                     SQLException)))

;; General SQL client stuff

(defmacro with-logging
  "Takes an options map, a binding vector of [conn-name jdbc-connection], and a
  body. Binds `conn-name` to a JDBC connection and evaluates body, presumably
  using `conn-name`. If `(:log-sql opts)` is truthy, `conn-name` will log SQL
  statements to the console."
  [opts [conn-name conn] & body]
  `(let [~conn-name (if (:log-sql ~opts)
                      (j/with-logging ~conn (fn ~'log [op# sql#]
                                              (info op# (pr-str sql#))))
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
  [{:keys [db-file rw-mode isolation-level]}]
  (let [; TODO: temp_directory?
        spec {:dbtype "duckdb"
              :dbname db-file
              "duckdb.read_only" (case rw-mode
                                   :rw "false"
                                   :ro "true")}
        ds (j/get-datasource spec)
        conn (j/get-connection ds)]
    ; We use explicit isolation levels for txns later...
    ; (set-transaction-isolation! conn isolation-level)
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
  (.close conn))

(defmacro with-conn
  "Multiple threads can use a DuckDB client concurrently, but they can't share
  a single sql.Connection. Instead, we duplicate a connection for the scope of
  a single request. I'm not totally sure what the semantics *should* be here;
  this is a bit of a guess.

  Takes a binding form `[conn-name conn]` and a body. Duplicates `conn`, binds
  it to `conn-name`, and evaluates body, closing `conn-name` at the end."
  [[conn-name conn] & body]
  (let [conn (vary-meta conn assoc :tag 'org.duckdb.DuckDBConnection)]
    `(let [~conn-name (.duplicate ~conn)]
       (try ~@body
            (finally
              (.close ~conn-name))))))

(defmacro with-errors
  "Takes a body which does some SQL; evals body, throwing typed errors as
  appropriate."
  [& body]
  `(try ~@body
        (catch SQLException e#
          (condp re-find (.getMessage e#)
            #"Conflict on update!" (throw+ {:type :conflict, :definite? true})

            #"Duplicate key" (throw+ {:type :duplicate-key, :definite? true})

            (throw e#)))))

;; Append workload


(def default-table-count 3)

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn append-using-on-conflict!
  "Appends an element to a key using an INSERT ... ON CONFLICT statement."
  [conn opts table k e]
  (j/execute!
    conn
    [(str "insert into " table " as t"
          " (id, sk, val) values (?, ?, ?)"
          " on conflict (id) do update set"
          " val = CONCAT(t.val, ',', ?) where "
          "t.id"
          ; TODO: secondary keys
          ;(if (< (rand) 0.5) "t.id" "t.sk")
          " = ?")
     k k e e k]))

(defn insert!
  "Performs an initial insert of a key with initial element e. Catches
  duplicate key exceptions, returning true if succeeded. If the insert fails
  due to a duplicate key, it'll break the rest of the transaction, assuming
  we're in a transaction, so we establish a savepoint before inserting and roll
  back to it on failure."
  [^Connection conn opts txn? table k e]
  ; TODO: DuckDB complains about our SQL when we do a savepoint, so this breaks
  (let [savepoint (when txn? (.setSavepoint conn "upsert"))]
    (try
      ;(info (if txn? "" "not") "in transaction")
      (j/execute! conn
                  [(str "insert into " table " (id, sk, val)"
                        " values (?, ?, ?)")
                   k k e])
      (when txn? (.releaseSavepoint conn savepoint))
      true
      ; TODO: figure out what error DuckDB uses for this
      #_(catch org.postgresql.util.PSQLException e
          (if (re-find #"duplicate key value" (.getMessage e))
            (do (info (if txn? "txn") "insert failed: " (.getMessage e))
                (when txn? (.releaseSavepoint conn savepoint))
                false)
            (throw e))))))

(defn update!
  "Performs an update of a key k, adding element e. Returns true if the update
  succeeded, false otherwise."
  [conn opts table k e]
  (let [res (-> conn
                (j/execute-one! [(str "update " table " set val = CONCAT(val, ',', ?)"
                                      " where id = ?") e k]))]
    ;(info :update res)
    (-> res
        :next.jdbc/update-count
        pos?)))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn opts txn? [f k v]]
  (let [table-count (:table-count opts default-table-count)
        table (table-for table-count k)]
    (Thread/sleep (long (rand-int 10)))
    [f k (case f
           :r (let [r (j/execute! conn
                                  [(str "select (val) from " table " where "
                                        ;(if (< (rand) 0.5) "id" "sk")
                                        "id"
                                        " = ? ")
                                   k]
                                  {:builder-fn rs/as-unqualified-lower-maps})]
                (when-let [v (:val (first r))]
                  (mapv parse-long (str/split v #","))))

           :append
           (let [vs (str v)]
             ; TOOD: more sophisticated dispatching here
             (if (:on-conflict (:upsert opts))
               ; Use ON CONFLICT
               (append-using-on-conflict! conn opts table k vs)
               ; TODO: this does not work in DuckDB yet; investigate
               ; Try an update, and if that fails, back off to an insert.
               (or (update! conn opts table k vs)
                   ; No dice, fall back to an insert
                   (insert! conn opts txn? table k vs)
                   ; OK if THAT failed then we probably raced with another
                   ; insert; let's try updating again.
                   (update! conn opts table k vs)
                   ; And if THAT failed, all bets are off. This happens even
                   ; under SERIALIZABLE, but I don't think it technically
                   ; VIOLATES serializability.
                   (throw+ {:type     :conflict
                            :key      k
                            :element  v})))
             v))]))

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
            (j/execute! ~conn-name ["ABORT"])
            (throw t#)))))

(defn append-txn!
  "Takes a list-append transaction and applies it, returning a completed
  transaction, or throwing. Options are:

    :isolation - The isolation level passed to the txn."
  [conn txn opts]
  (with-errors
    (let [use-txn?  (< 1 (count txn))
          _         (info "Using txn")
          txn'      (if use-txn?
                      (with-transaction [t conn
                                         ; Client doesn't support this yet
                                         #_{:isolation (:isolation opts)}]
                        (mapv (partial mop! t opts true) txn))
                      (mapv (partial mop! conn opts false) txn))]
      txn')))

;; Server

(defn setup!
  "Sets up the DB connection on initial startup."
  [conn opts]
  (with-logging opts [conn conn]
    (info "Setting up tables...")
    (j/with-transaction [t conn
                         ; Client doesn't support this yet
                         #_{:isolation (:isolation opts)}]
      (dotimes [i (:table-count opts default-table-count)]
        (j/execute! conn
                    [(str "create table if not exists " (table-name i)
                          " (id int not null primary key,
                          sk int not null,
                          val text)")])))))

(defn handle*
  "Takes a deserialized request body and processes it, returning an
  unserialized response body."
  [conn body opts]
  (with-conn [conn conn]
    (with-logging opts [conn conn]
      (when (:log-sql opts) (info "request:" (pr-str body)))
      (let [res (append-txn! conn body opts)]
        (when (:log-sql opts) (info "response:" (pr-str res)))
        res))))

(defn handler
  "Returns a function that handles HTTP requests."
  [conn opts]
  (fn handle [req]
    (try+
      (let [; Deserialize request body
            ; _ (pprint req)
            body (edn/read (PushbackReader.
                             (BufferedReader.
                               (InputStreamReader. (:body req) "UTF-8"))))
            res (handle* conn body opts)]
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

(defn read-opts
  "Reads enviromnent variable and returns a nice parsed option map. For
  example:

    {:db-file   \"/foo/duck.db\"
     :port      10002
     :log-sql   true
     :isolation :repeatable-read
     :rw-mode   :ro
     :upsert    #{:on-conflict ...}}"
  []
  (let [store-dir (System/getenv "JEPSEN_STORE_DIR")
        port      (parse-long (System/getenv "JEPSEN_PORT"))
        isolation (keyword (System/getenv "JEPSEN_ISOLATION"))
        log-sql   (boolean (System/getenv "JEPSEN_LOG_SQL"))
        rw-mode   (keyword (System/getenv "JEPSEN_RW_MODE"))
        upsert    (set (map keyword
                            (str/split (or (System/getenv "JEPSEN_UPSERT") "")
                                       #",")))]
    (assert (and (string? store-dir) (not= "" store-dir)))
    (assert (pos? port))
    (assert #{:serializable
              :repeatable-read
              :read-committed
              :read-uncommitted} isolation)
    (assert #{:ro :rw} rw-mode)
    (assert (every? #{:on-conflict} upsert))
    {:db-file   (str store-dir "/duck.db")
     :port      port
     :isolation isolation
     :log-sql   log-sql
     :rw-mode   rw-mode
     :upsert    upsert}))

(defn -main
  "Main entrypoint. All behavior is taken from the following environment
  variables:

  JEPSEN_PORT       The local HTTP port to bind

  JEPSEN_LOG_SQL    Whether to log SQL statements. Optional; if set to
                    anything, logs.

  JEPSEN_ISOLATION  e.g. serializable, repeatable-read, etc. Presently ignored;
                    DuckDB will throw if you try to set it.

  JEPSEN_RW_MODE    Either rw (read-write) or ro (read-only)

  JEPSEN_UPSERT     A comma-separated list of tactics we use for upserting,
                    like \"on-conflict,insert\"; see mop! for details."
  []
  (try
    (let [opts (read-opts)
          conn (open opts)]
      (info "Options are:\n" (with-out-str (pprint opts)))
      (setup! conn opts)
      (http/run-server (handler conn opts) (select-keys opts [:port]))
      (info "Waiting for HTTP requests on port" (:port opts))
      (while true
        (Thread/sleep 100000)))
    (catch Throwable t
      (fatal t "Fatal error")
      (System/exit 1))))
