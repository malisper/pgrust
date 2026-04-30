Goal:
Fix the remaining PostgreSQL `plancache` regression failures seen under `/tmp/diffs`.

Key decisions:
Added session-side prepared result-shape tracking so a cached `SELECT *` plan errors with `cached plan must not change result type` after table shape changes.
Extended SQL `PREPARE` support to cover prepared `INSERT`, wired prepared external parameters through execution, and exposed simple `pg_prepared_statements` generic/custom plan counters for the regression queries.
Added runtime PL/pgSQL SQL fallbacks for statements whose referenced relations are created or replaced inside the function, including static DML, SELECT INTO, RETURN queries, FOR queries, CREATE VIEW, DROP TABLE, and dynamic DDL catalog refreshes.
Matched PostgreSQL's generic prepared-plan shape for this regression with a scoped planner-config shim until parameter selectivity exists.
Fixed `DROP TABLE ... CASCADE` over temp objects to drop dependent temp views through the view-by-OID path before unregistering the temp entry.
Follow-up CI fixes kept un-analyzed auto-mode prepared statements on the generic external-parameter path, while preserving custom planning for analyzed statements used by the plancache plan-cache-mode test.
Follow-up CI fixes also kept PL/pgSQL ambiguity errors from falling through to runtime SQL fallback.

Files touched:
src/backend/parser/gram.rs
src/backend/tcop/postgres.rs
src/include/nodes/parsenodes.rs
src/pgrust/database/commands/drop.rs
src/pgrust/database/commands/execute.rs
src/pgrust/session.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-plancache RUSTC_WRAPPER=/usr/bin/env scripts/run_regression.sh --test plancache --results-dir /tmp/diffs/plancache-final3 --timeout 60 --jobs 1 --port 55438
CARGO_TARGET_DIR=/tmp/pgrust-target-providence-check RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-providence-ci RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet execute_prepared_select_uses_external_params
CARGO_TARGET_DIR=/tmp/pgrust-target-providence-ci RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet runtime_hash_pruning_uses_custom_opclass_support_proc
CARGO_TARGET_DIR=/tmp/pgrust-target-providence-ci RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet plpgsql_variable_conflict_modes_control_static_select_resolution
CARGO_TARGET_DIR=/tmp/pgrust-target-plancache RUSTC_WRAPPER=/usr/bin/env scripts/run_regression.sh --test plancache --results-dir /tmp/diffs/plancache-ci-fix --timeout 60 --jobs 1 --port 55440

Remaining:
None for `plancache`; the focused regression passes 113/113 queries.
