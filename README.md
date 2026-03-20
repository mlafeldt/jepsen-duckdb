# Jepsen DuckDB Test

Jepsen tests for the DucKDB database. Runs locally, rather than on a remote
cluster. The test spawns a collection of local processes which open a DuckDB
file locally, and interacts with them over STDIN/STDOUT.

This is an early prototype. It turns on, runs transactions, checks them for correctness, and reports bugs, but I'm not sure if those bugs are real.

## Installation

You'll need a JDK (21+), Git, Gnuplot, Graphviz, plus
[Leiningen](https://leiningen.org/). Unlike most Jepsen tests this runs
entirely locally; you don't need a cluster of machines, SSH keys, etc.

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

Because we want to test what happens when you kill a process running DuckDB,
the actual code that talks to the DuckDB library lives in a separate process,
called a *local node*. The `local-node` directory is its own Clojure project,
which the test harness automatically builds and runs. The harness communicates
with one or more local nodes via HTTP.

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
