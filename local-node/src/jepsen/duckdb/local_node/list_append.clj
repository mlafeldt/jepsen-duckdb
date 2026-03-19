(ns jepsen.duckdb.local-node.list-append
  "List-append transactions, encoded either a LIST or comma-separated TEXT
  field."
  (:gen-class)
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn fatal]]
            [dom-top.core :refer [loopr with-retry]]
            [jepsen.duckdb.local-node [client :as c]]
            [next.jdbc :as j]
            [next.jdbc [protocols :as jp]
                       [result-set :as rs]]
            [next.jdbc.sql.builder :as sqlb])
  (:import (java.sql Connection
                     SQLException)))


(def default-table-count 3)

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn create-tables!
  "Creates the tables used for this workload."
  [conn opts]
  (info "Creating tables")
  (c/with-transaction [t conn
                       ; Client doesn't support this yet
                       #_{:isolation (:isolation opts)}]
    (dotimes [i (:table-count opts default-table-count)]
      (j/execute! conn
                  [(str "create table if not exists " (table-name i)
                        " (id int not null primary key,
                        sk int not null,
                        val text)")]))))

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

(defn append-using-merge-into!
  "Appends an element to a key using a MERGE INTO statement."
  [conn opts table k e]
  (j/execute!
    conn
    [(str "merge into " table " as t "
          "using ( select ? as id, ? as sk, ? as val) as upserts "
          "using (id) "
          "when matched then update set "
          "val = CONCAT(t.val, ',', ?) "
          "when not matched then insert")
     k k e e]))

(defn insert!
  "Performs an initial insert of a key with initial element e. Catches
  duplicate key exceptions, returning true if succeeded. If the insert fails
  due to a duplicate key, it'll break the rest of the transaction (assuming
  we're in a transaction), so we establish a savepoint before inserting and
  roll back to it on failure."
  [^Connection conn opts txn? table k e]
  ; TODO: DuckDB complains about our SQL when we do a savepoint, so this breaks
  (let [savepoint (when txn? (c/set-savepoint! conn "upsert"))]
    (try
      ;(info (if txn? "" "not") "in transaction")
      (j/execute! conn
                  [(str "insert into " table " (id, sk, val)"
                        " values (?, ?, ?)")
                   k k e])
      (when txn? (c/release-savepoint! conn savepoint))
      true
      (catch SQLException e
          (if (re-find #"[Dd]uplicate key" (.getMessage e))
            (do (info (if txn? "txn") "insert failed: " (.getMessage e))
                (when txn? (c/rollback-savepoint! conn savepoint))
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

(defn append-using-update-insert!
  "Appends an element to a key using an UPDATE, and if that fails, an INSERT,
  and if that fails, an UPDATE again (on the hopes that someone else INSERTed"
  [conn opts txn? table k e]
  (or (update! conn opts table k e)
      (insert! conn opts txn? table k e)
      (update! conn opts table k e)
      ; And if THAT failed, all bets are off. This happens even under
      ; SERIALIZABLE, but I don't think it technically *violates*
      ; serializability.
      (throw+ {:type      :homebrew-upsert-failed
               :definite? true
               :key       k
               :element   e})))

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
             (case (rand-nth (:upsert opts))
               :merge-into
               (append-using-merge-into! conn opts table k vs)

               :on-conflict
               (append-using-on-conflict! conn opts table k vs)

               :update-insert
               (append-using-update-insert! conn opts txn? table k vs))
             v))]))

(defn txn!
  "Takes a list-append transaction structure [[f k v] ...] and applies it,
  returning a completed transaction, or throwing. Options are:

    :table-count  - The number of tables to spread data across
    :upsert       - A vector of upsert tactic keywords
    :isolation    - The isolation level passed to the txn."
  [conn txn opts]
  (c/with-errors
    (let [use-txn?  (< 1 (count txn))
          txn'      (if use-txn?
                      (c/with-transaction [t conn
                                         ; Client doesn't support this yet
                                         #_{:isolation (:isolation opts)}]
                        (mapv (partial mop! t opts true) txn))
                      (mapv (partial mop! conn opts false) txn))]
      txn')))
