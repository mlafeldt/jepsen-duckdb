# Jepsen DuckDB Test

Jepsen tests for the DucKDB database. Runs locally, rather than on a remote
cluster. The test spawns a collection of local processes which open a DuckDB
file locally, and interacts with them over STDIN/STDOUT.

This is an early prototype. It turns on, runs transactions, checks them for correctness, and reports bugs, but I'm not sure if those bugs are real.

## Installation

You'll need a JDK (21+), Git, Gnuplot, Graphviz, plus
[Leiningen](https://leiningen.org/). Unlike most Jepsen tests this runs
entirely locally; you don't need a cluster of machines, SSH keys, etc.

### Debian

```
sudo apt install openjdk leiningen gnuplot graphviz
```

### OS X

```
brew install openjdk leiningen gnuplot graphviz
```

## Usage

To run a test, try:

```
lein run test
```

DuckDB provides (I suspect) Strong SI by default, and that's what the test checks for. It does allow G2-item, though, which is a violation of Repeatable Read. To demonstrate this, try:

```
lein run test --time-limit 10 --expected-consistency-model serializable --max-writes-per-key 8
```

We're asking to test for ten seconds, to look for violations of
Serializability, and (to generate small, readable examples), to write only 8
elements per key. Examples of G2-item should be available in
`store/latest/elle/G2-item`.

There are several tuning options available. Help for the various options is available through `lein run test --help`.

Test results are written to `store/<test-name>/<date>/`, and symlinked as
`store/latest`. Each of these test directories is self-contained; you can copy
it around, tar it up, analyze one later, delete it, and so on. You can also run
a web server to browse results.

```
lein run serve
```

A [REPL is
available](https://github.com/jepsen-io/jepsen?tab=readme-ov-file#working-with-the-repl); see `lein repl`.

## Structure

The test harness lives in this directory; its project file is `project.clj`,
its source lives in `src/`, and so on.

The test harness runs a separate program, the "local node", which embeds the
DuckDB library and performs transactions against it. The test harness generates
random transactions for a given workload, submits them over HTTP to the local
node, and journals the results of those transactions, checking at the end for
various transactional anomalies. We look for Strong Snapshot Isolation, using
the Elle checker (https://github.com/jepsen-io/elle).

The local node has a small HTTP server which receives abstract transactions
(e.g. "read key x, then set y to 5") from the test harness, and translates them
into transactions run against the DuckDB JDBC driver.

We can inject one kind of fault: process kills.

# Workloads

We have two workloads.

The first, *append*, runs transactions which append unique integers to lists,
and reads the contents of those lists. Each list lives in a single row, spread
across several tables. Lists are identified by primary key or an unindexed
secondary key. Lists are encoded either as text fields or as DuckDB INTEGER{]
lists. Mutation is done either with INSERT ON CONFLICT UPDATE or MERGE INTO.

The second, *fkey-register*, performs reads and writes of integer registers. In
DuckDB, we store those registers in two tables. A *logical* table maps keys to
physical IDS, with a foreign key. A *physical* table maps physical IDs to
values. We use a straightforward JOIN between the two to read. Writes are
performed either by updating the physical row, or by creating a new physical
row and altering the logical pointer to it. I just got this workload to turn on
today; it runs, but it's not polished yet.

## License

Copyright © 2026 Jepens, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
https://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
