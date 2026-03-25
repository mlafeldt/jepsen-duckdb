# DuckDB Jepsen Test Results

Results from running [Jepsen](https://jepsen.io) consistency tests against
DuckDB 1.5.0.0 on 2026-03-24.

## How to run

```bash
# Prerequisites: Docker

# Default test (Strong SI, 10s)
docker compose run --rm jepsen

# Find write skew (G2-item) violations
docker compose run --rm jepsen run test \
  --time-limit 10 \
  --expected-consistency-model serializable \
  --max-writes-per-key 8

# Custom options (pass any CLI flags after `run test`)
docker compose run --rm jepsen run test --help
```

Results are written to `store/latest/` (volume-mounted from the container).

## Test 1: Strong Snapshot Isolation (default)

```
lein run test --time-limit 10
```

**Result: PASS** — no transactional anomalies detected.

Typical runs produce ~2000-2500 transactions in 10 seconds, with roughly
half failing due to contention (expected under this workload). Exact counts
vary per run. All checkers pass: workload, clock, stats, exceptions, perf.

This means DuckDB's behavior is consistent with its Strong Snapshot Isolation
claim — every transaction saw a valid snapshot and no forbidden anomalies
were observed.

## Test 2: Serializable (expected to fail)

```
lein run test --time-limit 10 --expected-consistency-model serializable --max-writes-per-key 8
```

This test asks "does DuckDB behave as if transactions ran one at a time?"
The answer is no — DuckDB provides Strong Snapshot Isolation, not
Serializability.

Results vary across runs:

- Some runs produce a clean **`:valid? false`** with G2-item anomalies,
  meaning Elle successfully proved that two concurrent transactions each
  read a key the other wrote, creating a dependency cycle impossible under
  serial execution.
- Other runs end with **`:valid? :unknown`** due to an Elle assertion error
  (`"No transaction wrote X Y"`). In these runs, Elle still generates
  G2-item artifacts in `store/latest/elle/G2-item/`, but the analysis did
  not complete cleanly — the artifacts were produced before the error, not
  as part of a verified result.

The assertion error is caused by a string storage bug in DuckDB — see
finding #3 below. The clean `:valid? false` runs already demonstrate
non-serializability on their own; the assertion-error runs are inconclusive.

When a clean `:valid? false` run does occur, anomaly details are in
`store/latest/elle/G2-item/` and `store/latest/elle/G2-item.txt`.

## What is being tested

- **Workload**: list-append — transactions append unique integers to lists
  spread across multiple tables, then read them back
- **Upsert strategies**: `INSERT ... ON CONFLICT UPDATE` and `MERGE INTO`
- **Storage**: mix of `INTEGER[]` arrays and text (CSV) columns
- **Access patterns**: by primary key and by unindexed secondary key
- **Checker**: [Elle](https://github.com/jepsen-io/elle) — detects
  non-serializable transaction histories by analyzing dependency graphs

## Architecture

```
+------------------+       HTTP       +------------------+
|  Test harness    | ---------------> |  Local node      |
|  (Jepsen/Elle)   |                  |  (DuckDB JDBC)   |
|  generates txns, |                  |  translates to   |
|  checks results  | <--------------- |  SQL, executes   |
+------------------+                  +------------------+
```

Both run inside a single Docker container. The test harness spawns the local
node as a child process. No cluster, no SSH — everything is local.

## Key findings

1. DuckDB's Strong SI claim holds — no anomalies when tested at that level
2. DuckDB is not serializable — G2-item (write skew) has been observed, though
   some runs hit an Elle analysis error before completing
3. Testing surfaced a string storage bug: when reverting an append due to a
   primary key conflict, the dictionary size was not decremented, causing
   newly inserted strings to contain stale data. The test flags this with
   "Assert failed: No transaction wrote \<id\> \<val\>". Present in 1.5.0
   and 1.5.1; fix merged upstream after 1.5.1 and expected in the next
   bugfix release via
   [duckdb/duckdb#21489](https://github.com/duckdb/duckdb/pull/21489)
4. High conflict rate (~50% txn failures) is normal under the test's contention level

### Reproducing the string storage bug (#3)

The bug triggers when `MERGE INTO` causes a primary key conflict and the
string append is reverted without decrementing the dictionary size. Subsequent
inserts into the same segment read corrupted string data, which Elle detects
as values no transaction ever wrote.

```bash
docker compose run --rm jepsen run test \
  --time-limit 10 \
  --max-writes-per-key 8 \
  --concurrency 5 \
  --rate 1000 \
  --log-sql \
  --upsert merge-into \
  --duckdb-log
```

Key flags: `--upsert merge-into` forces the MERGE INTO code path (where the
bug lives), `--max-writes-per-key 8` keeps lists short so corrupted values
are more likely to surface before keys fill up, and `--concurrency 5` with
`--rate 1000` maximizes contention.

Expected output (on DuckDB < 1.5.2):

```
Assert failed: No transaction wrote 218 31
:valid? :unknown
```

The assertion means Elle found a value in a list that no committed
transaction ever appended — a symptom of the corrupted string dictionary.
DuckDB typically crashes shortly after (visible as `:conn-refused` errors).

## Background

### What is Jepsen?

[Jepsen](https://jepsen.io) is a framework for testing whether databases
actually deliver the safety guarantees they claim. It throws concurrent
transactions at a database, records what happened, and checks whether the
results are consistent with the promised isolation level. Jepsen has found
bugs in virtually every database it has tested — PostgreSQL, MySQL,
CockroachDB, MongoDB, and many others. See
[jepsen.io/analyses](https://jepsen.io/analyses) for past reports.

### What is a consistency model?

Databases promise that concurrent transactions behave "as if" they ran in
some controlled order. The specific promise is called a
[consistency model](https://jepsen.io/consistency). Common levels, from
weakest to strongest:

```
Read Committed  <  Repeatable Read  <  Snapshot Isolation  <  Serializable
```

**Serializable** is the gold standard: transactions behave as if they ran
one at a time. **Snapshot Isolation (SI)** is weaker — each transaction sees
a consistent snapshot of the database, but certain conflicts between
concurrent transactions can slip through.

DuckDB claims **Strong Snapshot Isolation**, which sits between SI and
Serializable. It prevents some anomalies that regular SI allows, but not all.

### What is write skew (G2-item)?

Write skew is a specific anomaly that Snapshot Isolation allows but
Serializable does not. A classic example:

> Two doctors are on call. Each checks "is the other doctor on call?"
> and, seeing yes, takes themselves off call. Both transactions commit.
> Now nobody is on call — an invariant that neither transaction would have
> violated alone.

In Jepsen's terminology this is called **G2-item**: two transactions each
read something the other wrote, creating a dependency cycle that could not
occur under serial execution. For a formal treatment, see Adya et al.'s
*Generalized Isolation Level Definitions* or the
[Elle paper](https://arxiv.org/abs/2003.10554) which describes
how Jepsen detects these anomalies.

### How does this test work?

The test generates random transactions that append unique integers to lists
stored in DuckDB. After the test, [Elle](https://github.com/jepsen-io/elle)
reconstructs the order in which transactions must have executed by examining
which values each transaction read and wrote. If that order contains a cycle,
it means the database allowed an anomaly that should have been prevented.

## Further reading

- [Jepsen: how to read a Jepsen report](https://jepsen.io/analyses)
- [Consistency models explained](https://jepsen.io/consistency)
- [Elle: inferring isolation anomalies from histories](https://arxiv.org/abs/2003.10554)
- [Hermitage: testing transaction isolation levels](https://github.com/ept/hermitage)
