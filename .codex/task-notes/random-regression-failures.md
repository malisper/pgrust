Goal:
Fix PostgreSQL `random.out` regression failures from `.context/attachments/pasted_text_2026-04-26_10-39-50.txt`.

Key decisions:
- Added PostgreSQL-compatible session PRNG state for `setseed`, `random`, `random_normal`, and bounded `random`.
- Kept cryptographic UUID generation on the existing strong RNG path.
- Fixed bounded `random(min,max)` inference so int4, int8, and numeric calls keep their return family.
- Propagated CTE subplan PARAM_EXEC sources into root ext params and bound PL/pgSQL local variables for scalar subqueries.
- Added executable-plan validation for PARAM_EXEC sources, including root ext params and nested-loop params.
- Added a pre-sort projection for volatile output expressions referenced by ordinal `ORDER BY`.
- Added targeted analyzer shortcuts needed for the PL/pgSQL random normal regression query to stay under the regression timeout.

Files touched:
- `src/backend/executor/random.rs`
- `src/backend/executor/exec_expr.rs`
- `src/backend/executor/expr_ops.rs`
- `src/backend/optimizer/setrefs.rs`
- `src/backend/optimizer/plan/planner.rs`
- `src/backend/optimizer/plan/subselect.rs`
- `src/backend/parser/analyze/*`
- `src/include/catalog/pg_proc.rs`
- `src/pgrust/session.rs`
- `src/pl/plpgsql/*`
- `src/backend/executor/tests.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet random`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-pool/montreal-v1/regress scripts/run_regression.sh --schedule .context/random-only.schedule --test random --port 56475 --timeout 120`
- `git diff --check`

Remaining:
- None for `random.out`.
