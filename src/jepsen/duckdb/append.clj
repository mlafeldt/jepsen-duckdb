(ns jepsen.duckdb.append
  "A workload for transactional list-append, checked with Elle"
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clj-http.client :as http]
            [clojure [edn :as edn]]
            [clojure.tools.logging :refer [info warn]]
            [elle.core :as elle]
            [jepsen [client :as client]
                    [local :as local]]
            [jepsen.tests.cycle.append :as append]))

; Nothing fancy here; we proxy things straight over to the local node.
(defrecord Client [port]
  client/Client
  (open! [this test node]
    (assoc this :port (local/port test node)))

  (setup! [this test])

  (invoke! [this test op]
    (try+
      (let [res (http/post (str "http://localhost:" port "/append")
                           {:body               (pr-str (:value op))
                            :content-type       "application/edn"
                            :socket-timeout     5000
                            :connection-timeout 1000})
            txn' (edn/read-string (:body res))]
        (assoc op :type :ok, :value txn'))
      (catch java.net.ConnectException _
        (assoc op :type :fail, :error :conn-refused))
      (catch [:status 409] _
        (assoc op :type :fail, :error :conflict))
      (catch [:status 500] e
        (assoc op :type :info, :error [:server-error (:body e)]))
      (catch [:status 503] e
        (assoc op :type :info, :error [:server-error (:body e)]))))

  (teardown! [this test])

  (close! [this test]))

(defn workload
  "A list-append workload. Takes CLI options."
  [opts]
  (-> opts
      (select-keys [:key-count
                    :max-txn-length
                    :max-writes-per-key])
      (assoc :min-txn-length     1
             :consistency-models [(:expected-consistency-model opts)])
      append/test
      (assoc :client (map->Client {}))))
