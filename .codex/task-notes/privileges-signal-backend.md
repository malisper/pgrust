Goal:
Fix the `privileges` regression area around backend signaling privileges and `pg_signal_backend`.

Key decisions:
Added `usesysid` to synthetic `pg_stat_activity` rows so the upstream privileges query binds.
Registered `pg_cancel_backend(int4)` and `pg_terminate_backend(int4, int8 default 0)` as builtin scalar functions.
Implemented PostgreSQL-style privilege checks for signaling superuser-owned backends, role-owned backends, and `pg_signal_backend` membership.
Threaded PL/pgSQL `SECURITY DEFINER` owner identity into scalar function execution so the `pg_signal_backend` owner is effective during the wrapper call.
Kept actual termination as a `:HACK:` compatibility shim because pgrust has query-cancel interrupts but no backend termination lifecycle yet.

Files touched:
`src/include/nodes/primnodes.rs`
`src/include/catalog/pg_proc.rs`
`src/backend/parser/analyze/functions.rs`
`src/backend/executor/exec_expr.rs`
`src/backend/utils/cache/system_view_registry.rs`
`src/backend/utils/cache/system_views.rs`
`src/pgrust/database.rs`
`src/pgrust/database_tests.rs`
`src/pl/plpgsql/compile.rs`
`src/pl/plpgsql/exec.rs`

Tests run:
`env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh check` passed.
`env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet pg_signal_backend_role_cannot_terminate_superuser_backend` passed.
`env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet pg_stat_activity_exposes_usesysid_for_registered_backend` passed.
`git diff --check` passed.
`scripts/run_regression.sh --test privileges --jobs 1 --timeout 240 --port 56541 --results-dir /tmp/diffs/privileges-signal-lisbon` timed out at query 612/1295 before reaching the `pg_signal_backend` block; results are in `/tmp/diffs/privileges-signal-lisbon`.
Reran `privileges` twice after the timeout investigation:
`/tmp/diffs/privileges-sample-lisbon` with 300s timeout and `/tmp/diffs/privileges-sample2-lisbon` with 240s timeout both completed as failures, not timeouts, at 1080/1295 matched queries.
Captured a macOS `sample` trace during the security-restricted operations section: `/tmp/pgrust_privileges_sro.sample.txt`.
The sample shows time concentrated in SRO `CREATE INDEX`/`REINDEX` paths, especially `IndexBuildKeyProjector::new` -> `RelationGetIndexExpressions` -> `bind_index_exprs_uncached` -> function lookup/catalog cache loads and physical catalog scans. It did not show time in `CREATE ROLE`, `pg_signal_backend`, or role privilege checks.

Remaining:
The earlier `CREATE ROLE regress_sro_user` timeout did not reproduce. The full `privileges` regression still fails on many expected-output mismatches, including security-restricted operation behavior before the backend-signaling section, so this change is validated by focused unit coverage rather than an end-to-end `privileges` pass.
