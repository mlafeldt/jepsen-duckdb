(ns jepsen.duckdb.local-node.fkey-register
  "A rw-register test which, as a complication, uses a foreign key table to
  indirect access to the values. We call the top-level keys used by the test
  'logical' keys, and introduce new 'physical' keys which store the actual
  values.

  - logical: maps our logical keys to physical keys
  - physical: maps physical keys to integer values

  We can either mutate physical values in place, or create new physical rows
  and mutate the logical mapping.

  Our logical transaction structure is straight from elle.wr-register; see the
  docs there for details."
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

(defn create-tables!
  "Creates the tables used for this workload."
  [conn opts]
  ; TODO: lift to multiple tables
  (info "Creating tables")
  (c/with-transaction [t conn]
    (doto conn
      (j/execute-one!
        [(str "CREATE SEQUENCE IF NOT EXISTS physical_id_seq")])
      (j/execute-one!
        [(str "CREATE TABLE IF NOT EXISTS physical ("
              "id INT NOT NULL PRIMARY KEY DEFAULT nextval('physical_id_seq'), "
              "val INT NOT NULL)")])
      (j/execute-one!
        [(str "create table if not exists logical ("
              "id INT NOT NULL PRIMARY KEY, "
              "physical_id INT NOT NULL REFERENCES physical(id))")]))))

(defn read
  "Reads a key."
  [conn k]
  (-> conn
      (j/execute-one!
        [(str "SELECT physical.val FROM physical "
              "INNER JOIN logical ON logical.physical_id = physical.id "
              "WHERE logical.id = ?")
         k]
        {:builder-fn rs/as-unqualified-lower-maps})
      :val))

(defn insert-physical!
  "Inserts a fresh physical row, returning its logical ID."
  [conn opts v]
  (-> conn
      (j/execute-one!
        [(str "INSERT INTO physical (val) VALUES (?) "
              "RETURNING id") v]
        {:builder-fn rs/as-unqualified-lower-maps})
      :id))

(defn write-physical!
  "Writes a value by updating the physical row."
  [conn opts k v]
  ; Look up the physical id
  (if-let [physical-id
           (-> conn
               (j/execute-one!
                 ["SELECT physical_id FROM logical WHERE id = ?" k]
                 {:builder-fn rs/as-unqualified-lower-maps})
               :physical_id)]
    ; And update that row
    (j/execute-one! conn
                    [(str "UPDATE physical SET val = ? WHERE id = ?")
                     v physical-id])
    ; Doesn't exist; create both rows. Either of these paths contends on the
    ; logical key, so transactions should conflict under SI.
    (let [physical-id (insert-physical! conn opts v)]
      (j/execute-one!
        conn [(str "INSERT INTO logical VALUES (?, ?)") k physical-id]))))

(defn write-logical!
  "Writes a value by creating a fresh physical row and pointing to it."
  [conn opts k v]
  (let [physical-id (insert-physical! conn opts v)]
    (j/execute-one!
      conn
      [(str "INSERT OR REPLACE INTO logical VALUES (?, ?)") k physical-id])))

(defn mop!
  "Executes a transaction's micro-operation on a connection. Returns the
  completed micro-op."
  [conn opts [f k v]]
  ; Deliberately inject latency to force longer windows of concurrency.
  ;(Thread/sleep (long (rand/long 10)))
  [f k (case f
         :r (read conn k)
         :w (do ((rand-nth [write-physical! write-logical!])
                 conn opts k v)
                v))])

(defn txn!
  "Takes a write-read register transaction structure [[f k v] ...] and applies
  it, returning a completed transaction or throwing. Options are:"
  [conn txn opts]
  (c/with-errors
    (if (< 1 (count txn))
      (c/with-transaction [conn conn]
        (mapv (partial mop! conn opts) txn))
      (mapv (partial mop! conn opts) txn))))
