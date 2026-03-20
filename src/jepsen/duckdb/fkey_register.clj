(ns jepsen.duckdb.fkey-register
  "A workload for transactional write-read registers accessed through a foreign
  key."
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clojure.tools.logging :refer [info warn]]
            [elle.core :as elle]
            [jepsen [client :as client]
                    [local :as local]]
            [jepsen.duckdb [client :as dc]]
            [jepsen.tests.cycle.wr :as wr]))

; Nothing fancy here; we proxy things straight over to the local node.
(defrecord Client [port]
  client/Client
  (open! [this test node]
    (assoc this :port (local/port test node)))

  (setup! [this test])

  (invoke! [this test op]
    (dc/with-errors op
      (assoc op :type :ok, :value (dc/post port "/fkey-register" (:value op)))))

  (teardown! [this test]
    (dc/write-logs! port))

  (close! [this test]))

(defn workload
  "A foreign-key register workload. Takes CLI options."
  [opts]
  (-> opts
      (select-keys [:key-count
                    :max-txn-length
                    :max-writes-per-key])
      (assoc :min-txn-length     1
             :consistency-models [(:expected-consistency-model opts)])
      wr/test
      (assoc :client (map->Client {}))))
