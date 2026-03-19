(ns jepsen.duckdb.nemesis
  "Fault injection."
  (:require [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as jdb]
                    [nemesis :as n]
                    [generator :as gen]
                    [util :as util]
                    [random :as rand]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis [combined :as nc]]
            [clj-commons.slingshot :refer [try+ throw+]])
  (:import (jepsen.generator Generator)))

(defn db-gen
  "Normally we're testing distributed systems, so it makes sense to query while
  a node is down. That's not helpful here, where we're testing one node, so we
  wait almost no time between killing and restarting."
  [opts]
  (when (:kill (:faults opts))
    ; Hack hack hack
    (gen/cycle [(gen/once (fn [] (gen/sleep (rand (:interval opts)))))
                {:type :info, :f :kill, :value (rand-nth (:targets (:kill opts)))}
                {:type :info, :f :start, :value :all}])))

(defn package
  "Takes CLI opts and constructs a nemesis package, including a nemesis and
  generator."
  [opts]
  (let [; We can't do partitions, clock skew, etc, but process kills will work
        ; fine.
        packages [(assoc (nc/db-package opts) :generator (db-gen opts))]]
    (nc/compose-packages packages)))
