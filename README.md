# Jepsen DuckDB Test

This is not ready yet.

Jepsen tests for the DucKDB database. Runs locally, rather than on a remote
cluster. The test spawns a collection of local processes which open a DuckDB
file locally, and interacts with them over STDIN/STDOUT.

## Usage

lein run test-all

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
