(defproject jepsen.duckdb "0.0.1"
  :description "A Jepsen test for the DuckDB database"
  :url "https://github.com/jepsen-io/duckdb"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [jepsen "0.3.11-SNAPSHOT"]
                 [clj-http "3.13.1"]]
  :main jepsen.duckdb.cli
  :repl-options {:init-ns jepsen.duckdb.repl}
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"])
