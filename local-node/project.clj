(defproject jepsen.duckdb.local-node "0.0.1"
  :description "The local test node for the Jepsen DuckDB tests."
  :url "https://github.com/jepsen-io/duckdb"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :repositories [["central-snapshots"
                  {:url "https://central.sonatype.com/repository/maven-snapshots/"
                   :snapshots true}]]
  :dependencies [[com.github.seancorfield/next.jdbc "1.3.1070"]
                 [dom-top "1.0.10"]
                 [http-kit "2.8.1"]
                 [org.clojure/clojure "1.12.4"]
                 [org.clj-commons/slingshot "0.13.0"]
                 [org.clojure/tools.logging "1.3.0"]
                 ; [org.duckdb/duckdb_jdbc "1.5.0.0"]
                 ; Fixes a few known issues (not sure what exactly) in 1.5.0.0
                 ; [org.duckdb/duckdb_jdbc "1.5.1.0-7e03e76d-SNAPSHOT"]
                 ; Fixes the data corruption issue we found with strings!
                 [org.duckdb/duckdb_jdbc "1.5.1.0-fb39e9db-SNAPSHOT"]
                 [spootnik/unilog "0.7.32"]]
  :main jepsen.duckdb.local-node
  :repl-options {:init-ns jepsen.duckdb.local-node}
  :jvm-opts ["-Djava.awt.headless=true"
             ; Maybe we want faster startup by skipping server mode?
             ;"-server"
             ]
  :profiles
  {:uberjar {:aot :all}})
