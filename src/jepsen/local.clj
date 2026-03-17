(ns jepsen.local
  "Sometimes you want to test a DB by talking to processes running on the local
  control node, instead of (or in addition to) remote nodes. This namespace
  provides tools to run, talk to, and interfere with local processes.

  We model these processes as virtual nodes: a node name like \"l1\", \"l2\",
  identifies a single process at any given time. A node's process can be killed
  and replaced with a fresh one. If local nodes are used in conjunction with
  remote nodes, they can be assigned using `jepsen.role`.

  Local nodes are run inside the current test's store directory, in
  `<test-dir>/<node-name>/`---this is the same directory structure used for
  data files downloaded from remote nodes. Jepsen automatically saves each
  local process' STDOUT and STDERR to `<test-dir>/<node-name>/stdout.log` and
  `.../stderr.log`, respectively.

  A special key in the test map, `(:local test)`, keeps track of the state to
  manage and talk to local nodes. This is a map of local node names to atoms,
  each containing either `nil` if the node is not running, or a map like:

      {:node \"l1\", :process ...}

  For convenience, local nodes are assigned sequential ports from
  `starting-port`, based on their index in (:nodes test). When started, this
  port is available to the process in `JEPSEN_PORT`. You can use this to (e.g.)
  open HTTP connections to each local node."
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [jepsen [db :as db]
										[store :as store]
                    [util :refer [with-thread-name]]])
  (:import (java.lang Process
                      ProcessBuilder
                      ProcessBuilder$Redirect)
           (java.io File
                    IOException
                    PushbackReader
                    OutputStreamWriter
                    Writer)
           (java.util.concurrent TimeUnit)))

(def starting-port
  "Where we start assigning port numbers."
  47000)

(defn port
  "Takes a test map and a local node; returns the local port assigned to that
  node."
  [test node]
  (let [index (.indexOf ^java.util.List (:nodes test) node)]
    (assert (not (neg? index)))
    (+ starting-port index)))

;; Process management

(defn start-process!
  "Starts a local node's process. Takes a test and options:

     :node         This node's name
     :bin          A program to run
     :args         A list of arguments to the program
     :env          A map of environment variables (converted to strings with
                   `name`) to merge into the environment for this process

  Returns a map of:

     :node            The node name
     :process         The java Process object started
  "
  [test {:keys [node args bin env] :as opts}]
  (info "Starting local node" node bin (pr-str args))
  (let [; Files and directories
        dir         (store/path! test node)
        stdout-file (store/path! test node "stdout.log")
        stderr-file (store/path! test node "stderr.log")
        ; Binary
        bin         (.getCanonicalPath (io/file bin))
        ; Launch process
        builder (-> (ProcessBuilder. ^java.util.List (cons bin (:args opts)))
                    (.directory (io/file (:dir opts)))
                    (.redirectOutput stdout-file)
                    (.redirectError  stderr-file))
        _ (doto (.environment builder)
            (.put "JEPSEN_PORT" (str (port test node))))
        _ (doseq [[k v] env]
            (.put (.environment builder) (name k) (name v)))
        process (.start builder)]
    {:node    node
     :process process}))

(defn kill-process!
  "Kills a local node's process. Takes a local node map with a :node and
  :process key."
  [{:keys [node, ^Process process]}]
  (let [crashed? (not (.isAlive process))]
    (when-not crashed?
      ; Kill
      (info "Killing local node" node)
      (.. ^Process process destroyForcibly (waitFor 5 TimeUnit/SECONDS)))

    {:exit (.exitValue process)}))

;; DBs

; bin is the path to the binary we'll run. node-args is a map of node name to
; an arguments vector used when launching that node.
(defrecord DB [bin node-args node-envs]
  db/Kill
  (kill! [this test node]
    (let [state (get (:local test) node)]
      (assert state (str "Expected (:local test) to contain " node))
      (locking state
        (if-let [s @state]
          (do (kill-process! s)
              (reset! state nil))
          :not-running))))

  (start! [this test node]
    (let [state (get (:local test) node)]
      (assert state (str "Expected (:local test) to contain " node))
      (locking state
        (if @state
          :already-running
          (reset! state (start-process! test
                                        {:node node
                                         :bin  bin
                                         :args (get node-args node)
                                         :env  (get node-envs node)}))))))

  db/DB
  (setup! [this test node]
    (db/start! this test node))

  (teardown! [this test node]
    (db/kill! this test node)))

(defn db
  "Creates a Jepsen DB which manages a local node's process. Supports
  `jepsen.db/Kill`. Options:

  {:bin         The binary to run
   :node-args   A map of node names to argument vectors
   :node-envs   A map of node names to environment maps"
  [opts]
  (map->DB opts))

(defn test
  "Transforms an existing test map, adding the :local key needed to manage
  local nodes. Each node in the test gains a corresponding entry in (:local
  test)."
  [test]
  (-> test
      (assoc :local (zipmap (:nodes test) (repeatedly (partial atom nil))))
      (update :nonserializable-keys conj :local)))
