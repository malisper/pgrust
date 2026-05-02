# pgrust SQLancer findings

This file records SQLancer-discovered pgrust work without mixing production
fixes into the testing-infra branch. Each item should become a small,
parallelizable implementation ticket when we are ready to edit pgrust code.

## Current policy

- Keep this branch focused on harnesses, docs, repros, and external adapter
  work.
- Do not land pgrust source fixes here unless explicitly requested.
- Preserve common unsupported PostgreSQL features as useful backlog when they
  are likely to matter to ordinary applications.
- Filter only low-signal generator noise that blocks better failures.

## Triage runner

Use `scripts/run_sqlancer_triage.sh` to run deterministic seeds and keep
per-seed logs out of the repo by default.

Examples:

```bash
scripts/run_sqlancer_triage.sh 1 2 3
PGRUST_SQLANCER_TRIAGE_SEED_COUNT=10 PGRUST_SQLANCER_QUERIES=50 scripts/run_sqlancer_triage.sh
PGRUST_SQLANCER_TRIAGE_DIR=/tmp/pgrust-sqlancer-triage scripts/run_sqlancer_triage.sh 1
```

The runner writes:

- `summary.tsv`: seed, pass/fail, exit code, extracted blocker, artifact dir
- `seed-N/sqlancer.log`: combined SQLancer command output
- `seed-N/server.log`: pgrust server output
- `seed-N/blocker.txt`: first extracted blocker line
- `seed-N/data/`: fresh pgrust data directory for that seed

Use the summary to decide whether a seed found a new issue or repeated an
existing blocker. Add one finding row per distinct blocker.

## Findings

| ID | Class | Signal | Notes |
|---|---|---|---|
| SQLANCER-001 | fixed-in-branch | schema introspection | pgrust now exposes base-table metadata through `information_schema.tables` and `information_schema.columns`, including the `table_schema`, `table_type`, and `data_type` columns SQLancer needs for schema loading. |
| SQLANCER-002 | fixed-in-branch | scalar compatibility | pgrust now accepts bool-to-integer casts that PostgreSQL accepts, e.g. `CAST((-980570755) NOT IN (-582498801) AS INT)`. Original artifact: `/tmp/pgrust-sqlancer-triage-sqlancer001/seed-1`. |
| SQLANCER-003 | fixed-in-branch | `IN` semantics | All-NULL `IN` lists now bind against the left-hand expression type, so SQLancer's `IN (NULL)` cases no longer fail as ambiguous `ARRAY[]`. Confirmation artifact: `/tmp/pgrust-sqlancer-triage-sqlancer003/seed-1`. |
| SQLANCER-004 | fixed-in-branch | SELECT syntax | pgrust now accepts `SELECT ALL` as PostgreSQL's explicit duplicate-preserving select modifier. Confirmation artifact: `/tmp/pgrust-sqlancer-triage-sqlancer004/seed-1`. |
| SQLANCER-005 | fixed-in-branch | FROM syntax | pgrust now accepts PostgreSQL's legacy `FROM table*` inheritance marker as the same default relation scan behavior pgrust already used for bare table references. Confirmation artifact: `/tmp/pgrust-sqlancer-triage-sqlancer005/seed-1`. |
| SQLANCER-006 | fixed-in-branch | scalar function | pgrust now resolves and executes `upper(text)` through the builtin scalar-function path. Confirmation artifact: `/tmp/pgrust-sqlancer-triage-sqlancer006/seed-1`. |
| SQLANCER-007 | robustness | numeric expression | Seed 2 generated huge negative exponent/cast arithmetic that reset the server connection. Artifact: `/tmp/pgrust-sqlancer-triage-next/seed-2`. |
| SQLANCER-008 | fixed-in-branch | extended protocol | pgrust now describes DML `RETURNING` portals and sends DML command tags for row-returning DML. Confirmation artifact: `/tmp/pgrust-sqlancer-triage-sqlancer008/seed-3`; that seed now advances to `SQLANCER-013`. |
| SQLANCER-009 | robustness | server execution | Deep generated SQL triggered a pgrust client-thread stack overflow. |
| SQLANCER-010 | fixed-in-branch | expression syntax | pgrust now rewrites `BETWEEN SYMMETRIC` into the equivalent pair of asymmetric bounds checks. Confirmation artifact: `/tmp/pgrust-sqlancer-triage-sqlancer010/seed-4`. |
| SQLANCER-011 | common-unsupported / setup-noise | table DDL | Seed 5 generated `CREATE TEMP TABLE ... GENERATED ALWAYS AS ... CHECK ... NO INHERIT`. Split common table features from low-signal generated-column self-reference noise. Artifact: `/tmp/pgrust-sqlancer-triage-next/seed-5`. |
| SQLANCER-012 | harness classification | scalar function typing | Seed 6 generated `to_hex(text)`, currently reported as a pgrust type mismatch during setup. Decide whether to narrow generator typing or add a compatibility ticket. |
| SQLANCER-013 | fixed-in-adapter | JOIN scope error | The pgrust SQLancer adapter now allowlists generated invalid comma/explicit-JOIN scope references when pgrust reports them as qualified missing-column errors. Confirmation artifact: `/tmp/pgrust-sqlancer-triage-sqlancer013/seed-3`. |

## Resume todos

Use these as the next small, parallelizable blockers after the current pushed
milestone.

1. `SQLANCER-007`: minimize the seed 2 numeric expression that reset the server
   connection, then decide whether the fix is overflow/error handling in pgrust
   or an expected arithmetic-domain error in the adapter.
2. `SQLANCER-011`: split the seed 5 table-DDL blocker into common useful
   features (`TEMP TABLE`, `CHECK`, `NO INHERIT`) versus generated-column
   self-reference noise. Keep common Postgres features visible as implementation
   tickets.
3. `SQLANCER-012`: fix or classify `to_hex(text)` generation. PostgreSQL's
   common `to_hex` signatures are integer-shaped, so this is likely SQLancer
   generator typing noise unless a smaller repro shows pgrust mishandles a valid
   call.
4. After one blocker is fixed or classified, rerun:

```bash
PGRUST_SQLANCER_TRIAGE_SEED_COUNT=10 PGRUST_SQLANCER_QUERIES=50 scripts/run_sqlancer_triage.sh
```

## Next triage pass

1. Run `PGRUST_SQLANCER_TRIAGE_SEED_COUNT=10 PGRUST_SQLANCER_QUERIES=50 scripts/run_sqlancer_triage.sh`.
2. Collapse duplicate blockers in `summary.tsv`.
3. Add one finding row per distinct blocker.
4. For each blocker, decide whether it is `wrong-result`, `common-unsupported`,
   or `noise-unsupported`.
5. Only add SQLancer expected errors for `noise-unsupported` or known invalid
   generated SQL. Keep common unsupported PostgreSQL behavior visible.
