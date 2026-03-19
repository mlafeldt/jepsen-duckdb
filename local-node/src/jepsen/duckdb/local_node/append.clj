(ns jepsen.duckdb.local-node.append
  "List-append transactions, encoded either a LIST or comma-separated TEXT
  field."
  (:gen-class)
  (:refer-clojure :exclude [read])
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn fatal]]
            [dom-top.core :refer [loopr with-retry]]
            [jepsen.random :as rand]
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
  (str "append" i))

(defn create-tables!
  "Creates the tables used for this workload. We encode the key two ways:

    id: The primary key
    sk: An un-indexed secondary key

  We store the list for any single key in one of two ways, controlled by
  list-type:

    val_list: a list of integers
    val_text: a comma-separated string"
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
                           val_text text,
                           val_list INTEGER[])")]))))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn list-type
  "Takes options map and a key; returns the keyword list type (e.g. :text or
  :list) used for this key."
  [opts k]
  (let [ts (:list-types opts)]
    (nth ts (mod (hash k) (count ts)))))

(def val-column
  "A map of list types to columns used to encode them."
  {:list "val_list"
   :text "val_text"})

(defn rand-indexed-key-column
  "Picks a random indexed key column name to use for a key, e.g. \"id\"."
  []
  (rand-nth ["id"]))

(defn rand-key-column
  "Picks a random column name to use for a primary key, e.g. 'id', 'sk', ..."
  []
  (rand-nth ["id" "sk"]))

(defn append-using-on-conflict!
  "Appends an element to a key using an INSERT ... ON CONFLICT statement."
  [conn opts table k e]
  (let [kcol (rand-indexed-key-column)]
    (case (list-type opts k)
      :list
      (j/execute!
        conn
        [(str "insert into " table " as t "
              "(id, sk, val_list) values (?, ?, [?]::INTEGER[]) "
              "on conflict (" kcol ") do update "
              "set val_list = list_append(t.val_list, ?) "
              "where t." kcol " = ?")
         k k e e k])
      :text
      (j/execute!
        conn
        [(str "insert into " table " as t"
              " (id, sk, val_text) values (?, ?, ?)"
              " on conflict (" kcol ") do update "
              "set val_text = CONCAT(t.val_text, ',', ?) "
              "where t." kcol " = ?")
         k k (str e) (str e) k]))))

(defn append-using-merge-into!
  "Appends an element to a key using a MERGE INTO statement."
  [conn opts table k e]
  (let [kcol (rand-key-column)]
    (case (list-type opts k)
      :list
      (j/execute!
        conn
        [(str "merge into " table " as t "
              "using (select ? as id, ? as sk, NULL as val_text, [?]::INTEGER[] as val_list) "
              "using (" kcol ") "
              "when matched then update set "
              "val_list = list_append(t.val_list, ?) "
              "when not matched then insert")
         k k e e])
      :text
      (j/execute!
        conn
        [(str "merge into " table " as t "
              "using (select ? as id, ? as sk, ? as val_text, NULL as val_list) as upserts "
              "using (" kcol ") "
              "when matched then update set "
              "val_text = CONCAT(t.val_text, ',', ?) "
              "when not matched then insert")
         k k (str e) (str e)]))))

(defn insert!
  "Performs an initial insert of a key with initial element e. Catches
  duplicate key exceptions, returning true if succeeded. If the insert fails
  due to a duplicate key, it'll break the rest of the transaction (assuming
  we're in a transaction), so we establish a savepoint before inserting and
  roll back to it on failure."
  [^Connection conn opts txn? table k e]
  ; TODO: DuckDB complains about our SQL when we do a savepoint, so this breaks
  ; TODO: since this doesn't work, I'm not teaching it about val_text/val_list
  ; yet, or secondary keys
  (let [savepoint (when txn? (c/set-savepoint! conn "upsert"))]
    (try
      ;(info (if txn? "" "not") "in transaction")
      (j/execute! conn
                  [(str "insert into " table " (id, sk, val)"
                        " values (?, ?, ?)")
                   k k (str e)])
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
                                      " where id = ?") (str e) k]))]
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

(defn read
  "Reads a key's value."
  [conn opts table k]
  (let [kcol (rand-key-column)
        vcol (case (list-type opts k)
               :list "val_list"
               :text "val_text")
        r (-> conn
              (j/execute! [(str "select (" vcol ") from " table " where "
                                kcol " = ?")
                           k]
                          {:builder-fn rs/as-unqualified-lower-maps})
              first
              (get (keyword vcol)))]
    (case (list-type opts k)
      :list r
      :text (when r (mapv parse-long (str/split r #","))))))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn opts txn? [f k v]]
  (let [table-count (:table-count opts default-table-count)
        table (table-for table-count k)]
    ; We deliberately inject some latency here to force longer windows of
    ; transactions overlapping.
    (Thread/sleep (long (rand/long 10)))
    [f k (case f
           :r (read conn opts table k)

           :append
           (do (case (rand-nth (:upsert opts))
                 :merge-into
                 (append-using-merge-into! conn opts table k v)

                 :on-conflict
                 (append-using-on-conflict! conn opts table k v)

                 :update-insert
                 (append-using-update-insert! conn opts txn? table k v))
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
