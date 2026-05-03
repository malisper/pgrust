# pgrust SQLancer adapter plan

This note turns "use SQLancer" into a narrower plan for pgrust.

The immediate goal is not to support every PostgreSQL feature SQLancer knows
about. The immediate goal is to get useful differential testing on the most
common PostgreSQL use cases as fast as possible.

## Why this is worth doing now

SQLancer is the best near-term tool for finding wrong-result bugs in normal SQL
workloads. That is a better fit for current single-node pgrust than full Jepsen
because:

- Jepsen's full framework is aimed at distributed systems with multiple nodes,
  injected faults, and history checking across replicas
- pgrust still gets value from Jepsen's mindset, but SQLancer is the tool that
  can pressure ordinary query correctness right now
- the first users of pgrust will care more about `SELECT`/`INSERT`/`UPDATE`/
  `DELETE` behavior matching PostgreSQL than about distributed linearizability

## What the current PostgreSQL adapter expects

From the current `antithesishq/sqlancer` PostgreSQL path, the minimum expected
surface is roughly:

- JDBC connectivity to a server that speaks the PostgreSQL wire protocol
- for stock PostgreSQL, an entry database named `test`; for pgrust smoke runs,
  use the bootstrap database `postgres`
- the ability to `DROP DATABASE IF EXISTS ...` and `CREATE DATABASE ...`
- schema introspection via:
  - `information_schema.tables`
  - `information_schema.columns`
  - `pg_indexes`
  - `pg_proc`
  - `pg_collation`
  - `pg_opclass`
  - `pg_operator`
  - `pg_am`
  - `pg_statistic_ext`

That tells us two useful things:

1. We do not need to "support all of PostgreSQL" before SQLancer is useful.
2. We probably should not start by pointing the stock PostgreSQL adapter at
   pgrust and hoping for the best.

## Recommended adapter strategy

Do a pgrust-specific adapter derived from the PostgreSQL one, but narrower.

Recommended first-pass choices:

- start with one oracle: `WHERE`
  reason: this exercises common filtering correctness without immediately
  dragging in the full `HAVING`/aggregate/query-partitioning surface
- limit generated column types to the common subset we already care about:
  `integer`, `bigint`, `boolean`, and `text`
- keep common PostgreSQL features visible even when pgrust does not support
  them yet, because those failures are useful inputs for parallel agent work
- disable only features that are both uncommon and likely to drown out the
  first useful signal: tablespaces, exotic data types, extension-specific
  behavior, and rarely used catalog branches
- keep the first run single-threaded and short so failures are easy to inspect

The right shape is "pgrust common-app subset" first, not "full Postgres mode."

## Common-use-case focus

The first SQLancer pass should aim at the behavior most ordinary applications
lean on:

- simple table creation
- inserts and updates on scalar columns
- filters with `AND`, `OR`, `NOT`, `IS NULL`, and comparison operators
- joins on ordinary scalar columns
- aggregates that normal dashboards and APIs use
- ordering and limiting

If we catch wrong results here, that is immediately valuable to the largest set
of future users.

## Unsupported-feature policy

Unsupported common features are useful failures. If SQLancer generates a query
that normal PostgreSQL users would reasonably expect to work, and pgrust rejects
it, that should usually become a backlog item rather than disappear from the
generator.

Classify failures into three buckets:

- `wrong-result`: pgrust accepts the query but returns a different result than
  the oracle or PostgreSQL says it should
- `common-unsupported`: pgrust rejects a mainstream PostgreSQL feature that is
  relevant to ordinary application use
- `noise-unsupported`: pgrust rejects a rare or low-priority feature that blocks
  the harness from reaching better signal

Only the third bucket should be aggressively filtered early. The second bucket
is valuable because it gives us concrete, reproducible tasks we can hand to
parallel agents.

## Current smoke milestone

Status as of Apr 24, 2026:

- SQLancer builds locally from `/Users/jasonseibel/dev/2026/your-projects-parent/sqlancer`.
- The sibling checkout has a `pgrust` provider registered with SQLancer.
- `scripts/run_sqlancer_smoke.sh` starts a fresh pgrust server and runs the
  SQLancer `WHERE` oracle through JDBC.
- `scripts/run_sqlancer_triage.sh` runs deterministic seed sets with unique
  ports and per-seed logs so failures become repeatable backlog items.
- A temporary implementation pass proved the harness can drive seeded smoke
  runs through useful failures. Those pgrust source changes are intentionally
  not part of this testing-infra branch; the discovered issues are tracked in
  [SQLancer findings](./sqlancer-findings.md).

Current pgrust-specific SQLancer filters or expected errors:

- setup DDL filters: partitioned tables, unlogged tables, `EXCLUDE`
  constraints, table access methods, table storage options, inheritance, and
  `CREATE TABLE LIKE INCLUDING/EXCLUDING`
- setup DML expected error: `INSERT ... OVERRIDING`
- oracle expected error: generated invalid join-scope references that pgrust
  currently reports as `column tN.cN does not exist`

These filters are not a claim that the features are unimportant. They are there
to keep the first harness lane reaching ordinary query-correctness bugs. Common
unsupported features should still become separate backlog items.

## First smoke milestone

The first milestone is not a polished CI job. It is one deterministic local run
that starts pgrust and gets SQLancer to complete a short session.

Target shape:

1. launch `pgrust_server` on a local port with a fresh data dir
2. run a pgrust-specific SQLancer adapter with:
   - one thread
   - one seed
   - `WHERE` oracle only
   - narrow type generation
3. save the SQLancer log and any minimal repro query

Likely first command shape once the adapter exists:

```bash
scripts/run_sqlancer_smoke.sh
```

## Expected first blockers

These are the most likely reasons the stock PostgreSQL adapter will be noisy
against pgrust:

- unsupported system-catalog queries or incomplete catalog rows
- generator features that rely on rarer PostgreSQL surface
- error-message mismatches that should become expected errors
- unsupported DDL branches that need triage into `common-unsupported` vs.
  `noise-unsupported`

That is normal. The first job is not to hide every unsupported feature. It is
to shape output so each failure clearly says either "logic bug", "common missing
Postgres feature", or "low-priority generator noise."

## Suggested implementation order

1. Clone `antithesishq/sqlancer` into a sibling checkout. Done at
   `/Users/jasonseibel/dev/2026/your-projects-parent/sqlancer`.
2. Add a `pgrust` provider by deriving from the PostgreSQL provider and
   reducing only the obviously noisy actions. Started in
   `src/sqlancer/postgres/PgrustProvider.java`.
3. Compile SQLancer. Done with Maven.
4. Run `scripts/run_sqlancer_smoke.sh`. Done.
5. Make the schema introspection queries work against pgrust's current catalog
   surface. Done for the first schema reader pass.
6. Prove one short local `WHERE`-oracle run. Harness works and now produces
   concrete pgrust findings; keep pgrust fixes separate from this branch.
7. Triage failures into `wrong-result`, `common-unsupported`, and
   `noise-unsupported`. In progress.
8. Only then widen the surface to more types or `QUERY_PARTITIONING`.

## Current skeleton

The initial `pgrust` provider currently:

- registers a separate SQLancer command named `pgrust`
- reuses PostgreSQL's existing global state, schema model, generator, and
  oracles
- connects directly to an existing database from `--connection-url` instead of
  dropping and creating databases
- forces SQLancer's known PostgreSQL type subset
- keeps common DML and a small amount of common DDL visible:
  `INSERT`, `UPDATE`, `DELETE`, `CREATE INDEX`, `ANALYZE`, `TRUNCATE`, and
  `CREATE VIEW`

Next expected blocker is either another generated setup feature that needs
classification or a true wrong-result oracle failure. Keep widening seed count
before widening SQL surface area.
