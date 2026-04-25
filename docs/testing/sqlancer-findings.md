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
| SQLANCER-001 | common-unsupported | schema introspection | Current seed-1 blocker on unmodified pgrust: `ERROR: column "table_schema" does not exist` while SQLancer reads `information_schema.tables`. SQLancer also needs `information_schema.columns` to read generated schemas. |
| SQLANCER-002 | common-unsupported | scalar compatibility | Generated expressions use bool-to-integer casts that PostgreSQL accepts. |
| SQLANCER-003 | wrong-result / binder edge | `IN` semantics | All-NULL `IN` lists exposed a binding/type edge case. |
| SQLANCER-004 | common-unsupported | SELECT syntax | SQLancer emits `SELECT ALL`, which is standard PostgreSQL-compatible syntax. |
| SQLANCER-005 | common-unsupported | FROM syntax | SQLancer can emit legacy `FROM table*` inheritance syntax. |
| SQLANCER-006 | common-unsupported | scalar function | Generated expressions use `upper(text)`. |
| SQLANCER-007 | robustness | numeric expression | Huge negative numeric exponent generated an arithmetic overflow path. |
| SQLANCER-008 | protocol compatibility | extended protocol | DML `RETURNING` needs row descriptions for JDBC extended-protocol execution. |
| SQLANCER-009 | robustness | server execution | Deep generated SQL triggered a pgrust client-thread stack overflow. |
| SQLANCER-010 | common-unsupported | expression syntax | SQLancer emits `BETWEEN SYMMETRIC`. |
| SQLANCER-011 | common-unsupported / setup-noise | table DDL | Seed 5 generated `CREATE TEMP TABLE ... GENERATED ALWAYS AS ... CHECK ... NO INHERIT`. Split common table features from low-signal generated-column self-reference noise. |
| SQLANCER-012 | harness classification | scalar function typing | Seed 6 generated `to_hex(text)`, currently reported as a pgrust type mismatch during setup. Decide whether to narrow generator typing or add a compatibility ticket. |
| SQLANCER-013 | harness classification | JOIN scope error | Generated invalid comma/explicit-JOIN scope references are currently allowlisted in SQLancer because pgrust reports them as qualified missing-column errors. |

## Next triage pass

1. Run `PGRUST_SQLANCER_TRIAGE_SEED_COUNT=10 PGRUST_SQLANCER_QUERIES=50 scripts/run_sqlancer_triage.sh`.
2. Collapse duplicate blockers in `summary.tsv`.
3. Add one finding row per distinct blocker.
4. For each blocker, decide whether it is `wrong-result`, `common-unsupported`,
   or `noise-unsupported`.
5. Only add SQLancer expected errors for `noise-unsupported` or known invalid
   generated SQL. Keep common unsupported PostgreSQL behavior visible.
