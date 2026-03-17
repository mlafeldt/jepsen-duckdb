# jepsen.duckdb.local-node

This is a small program which embeds the DuckDB Java libary and exposes a small
HTTP server. The test builds this project, spins up instances of it, and sends
requests to them to execute transactions.

## Testing

As a quick sanity check, try:

```
JEPSEN_STORE_DIR="." JEPSEN_PORT=8000 JEPSEN_ISOLATION=serializable JEPSEN_RW_MODE=rw JEPSEN_UPSERT=on-conflict lein run
```

## License

Copyright © 2026 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
https://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
