(ns jepsen.duckdb.cli
  "Command-line entry point for DuckDB tests."
  (:gen-class)
  (:require [clojure [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [local :as local]
                    [os :as os]
                    [tests :as tests]
                    [util :as util :refer [sh]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.duckdb [append :as append]
                           [nemesis :as nemesis]]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps."
  {:append append/workload
   :none   (fn [_] tests/noop-test)})

(def all-workloads
  "A collection of workloads we run by default."
  [:append])

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:kill]
   ])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:kill]})

(defn parse-comma-separated-kws
  "Takes a string of comma-separated values and turns it into a vector of
  keywords."
  [s]
  (->> (str/split s #",")
       (mapv keyword)))

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a set of keyword
  faults."
  [spec]
  (->> (parse-comma-separated-kws spec)
       (mapcat #(get special-nemeses % [%]))
       set))

(def upsert-tactics
  "The ways we can do upserts."
  #{:on-conflict :merge-into})

(def list-types
  "How can we encode lists for append?"
  #{:text :list})

(def short-isolation
  {:strict-serializable "Strict-1SR"
   :serializable        "S"
   :strong-snapshot-isolation "Strong-SI"
   :snapshot-isolation  "SI"
   :repeatable-read     "RR"
   :read-committed      "RC"
   :read-uncommitted    "RU"})

(def local-dir
  "Where does the local node project live?"
  "local-node")

(defonce local-built?
  (atom false))

(defn build-local!
  "Builds the local DB node program."
  []
  (locking local-built?
    (when-not @local-built?
      (info "Building local node...")
      (sh "lein" "uberjar"
          :dir local-dir
          :env (-> (into {} (System/getenv))
                   (dissoc "CLASSPATH")
                   (assoc "LEIN_SNAPSHOTS_IN_RELEASE" "TRUE")))
      (sh "/bin/bash" "-c" "mv target/*-standalone.jar local-node.jar"
          :dir local-dir)
      (reset! local-built? true))))

(defn db
  "Constructs a local DB given CLI opts."
  [opts]
  (let [; Base arguments
        args ["-jar" (.getCanonicalPath (io/file local-dir "local-node.jar"))]
        ; We don't need to do this, but it's slightly easier to read a smaller
        ; map in the logging output, so we'll dissoc some keys that we *know*
        ; don't need to get passed through.
        env-opts (dissoc opts :max-txn-length :rate :concurrency :key-count :max-writes-per-key :expected-consistency-model :leave-db-running? :logging-json? :nemesis-interval :ssh :time-limit :argv :nemesis :test-count)
        env           {"JEPSEN_OPTS" (pr-str env-opts)}
        primary-env   (assoc env "JEPSEN_RW_MODE" "rw")
        secondary-env (assoc env "JEPSEN_RW_MODE" "ro")]
      (local/db {:bin "/usr/bin/java"
                 :node-args (zipmap (:nodes opts) (repeat args))
                 :node-envs (zipmap (:nodes opts)
                                    (cons primary-env (repeat secondary-env)))})))

(defn duckdb-test
  "Given options from the CLI, constructs a test map. As a side effect, builds
  the local node first, once per JVM run."
  [opts]
  (build-local!)
  (let [workload-name (:workload opts :append)
        workload ((workloads workload-name) opts)
        db       (db opts)
        nemesis (nemesis/package
                  {:db       db
                   :nodes    (:nodes opts)
                   :faults   (:nemesis opts)
                   :kill     {:targets [:one :majority :all]}
                   :interval (:nemesis-interval opts)})
        gen (->> (:generator workload)
                 (gen/stagger (/ (:rate opts)))
                 (gen/nemesis (:generator nemesis))
                 (gen/time-limit (:time-limit opts)))]
    (-> tests/noop-test
        (merge
          opts
          {:name (str (name workload-name)
                      " " (short-isolation (:isolation opts)) "("
                      (short-isolation (:expected-consistency-model opts)) ") "
                      (str/join "," (map name (:nemesis opts))))
           :ssh     {:dummy? true}
           :os      os/noop
           :db      db
           :checker (checker/compose
                      {:perf       (checker/perf {:nemeses (:perf nemesis)})
                       :clock      (checker/clock-plot)
                       :stats      (checker/stats)
                       :exceptions (checker/unhandled-exceptions)
                       ;:timeline   (timeline/html)
                       :workload   (:checker workload)})
           :client    (:client workload)
           :nemesis   (:nemesis nemesis)
           :generator gen})
        local/test)))

(def cli-opts
  "Command line options"
  [[nil "--checkpoint-threshold BYTES" "Sets the checkpoint threshold. Try 0 to force frequent auto-checkpointing."
    :parse-fn parse-long
    :validate [(complement neg?) "Must be non-negative"]]

   [nil "--concurrency NUMBER" "How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes."
    :default  "3n"
    :validate [(partial re-find #"^\d+n?$")
               "Must be an integer, optionally followed by n."]]

   [nil "--disable-index-scan" "If set, disables index scans at local node startup."]

   [nil "--disable-optimizer" "If set, disables the DuckDB optimizer via a pragma."]

   [nil "--duckdb-log" "If set, also asks DuckDB to log transactions directly to the duckdb file."]

   [nil "--expected-consistency-model MODEL" "What level of isolation do we *expect* to observe? Defaults to the same as --isolation."
    :default nil
    :parse-fn keyword]

   ["-i" "--isolation LEVEL" "What level of isolation we should set: serializable, repeatable-read, etc."
    :default :strong-snapshot-isolation
    :parse-fn keyword
    :validate [#{:read-uncommitted
                 :read-committed
                 :repeatable-read
                 :serializable}
               "Should be one of read-uncommitted, read-committed, repeatable-read, or serializable"]]

   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--list-types TYPES" "A comma-separated list of column types to use for append."
    :default  (vec list-types)
    :parse-fn parse-comma-separated-kws
    :validate [(partial every? list-types) (cli/one-of list-types)]]

   [nil "--log-sql" "If set, logs SQL statements on each local node."]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  256
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill})
               "Faults must be pause, kill, or the special faults all or none."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default  5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   (cli/repeated-opt "-n" "--node HOSTNAME"
     "Node(s) to run test on. Flag may be submitted many times, with one node per flag."
     ; Ah, so the only two options are single rw, or multiple ro servers--they
     ; use flock to prevent ro clients from opening a DB while a writer has it
     ; open. Maybe down the road we look at mixing these clients by killing the
     ; rw node, opening multiple ros, trying to get them to fight over fctrl by
     ; repeatedly killing and restarting, etc....
     ["l1"])
     ;["l1" "l2" "l3"])

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default  1000
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--upsert TACTICS" "Comma-separated list of tactics to use for upserting values."
    :default [:merge-into :on-conflict]
    :parse-fn parse-comma-separated-kws
    :validate [(fn [tactics]
                 (and (not (empty? tactics))
                      (every? upsert-tactics tactics)))
               (cli/one-of upsert-tactics)]]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   ])

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)] [n] all-nemeses)
        workloads (if-let [w (:workload opts)] [w] all-workloads)]
    (for [i (range (:test-count opts)), n nemeses, w workloads]
      (duckdb-test (assoc opts :nemesis n :workload w)))))

(defn opt-fn
  "Transforms CLI options before execution."
  [parsed]
  ; If not explicitly specified, the expected consistency model is whatever
  ; isolation level we ask for
  (update-in parsed [:options :expected-consistency-model]
             #(or % (get-in parsed [:options :isolation]))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  duckdb-test
                                         :opt-spec cli-opts
                                         :opt-fn   opt-fn})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts
                                      :opt-fn   opt-fn})
                   (cli/serve-cmd))
            args))
