Goal:
Fix the PostgreSQL `stats` regression without expected-output changes.

Key decisions:
Kept the existing partial stats patch and finished the remaining mismatches.
Added PostgreSQL-shaped stats/catalog views and builtins, with compatibility zero/null rows only for missing subsystems.
Fixed real relation/function/session stats semantics for savepoints, rollback/drop cleanup, HOT updates, snapshots, and reset behavior.
Wired enough shared/backend IO and WAL accounting for the `pg_stat_io` and backend-local reset checks.
Closed the SQL gaps hit by `stats`: shared comments, `COMMENT ON DATABASE`, tablespace rewrite compatibility, quoted reloptions, BRIN partial indexes, `RAISE LOG`, `current_schemas`, and `pg_sleep_for(interval)`.
Fixed the last `pg_stat_database` mismatch by correcting the row shape: `checksum_failures` and `checksum_last_failure` must be distinct columns so `sessions` is not shifted.
Added the isolated `stats` helper setup for `check_estimated_rows(text)` and made the regression harness create worker `status` directories.

Files touched:
Stats/catalog/runtime/planner/SQL files across parser, executor, activity stats, system views, catalog rows, session/database command handling, PL/pgSQL, and `scripts/run_regression.sh`.
New catalog file: `src/include/catalog/pg_shdescription.rs`.

Tests run:
`cargo fmt`
`bash -n scripts/run_regression.sh`
`scripts/cargo_isolated.sh check`
`scripts/cargo_isolated.sh test --lib --quiet pg_stat_io_exposes_pg_shaped_rows`
`scripts/run_regression.sh --test stats --port 55457` -> PASS, 479/479 queries matched.

Remaining:
No known remaining `stats` regression mismatches.
