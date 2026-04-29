Goal:
Fix PL/pgSQL scalar qualified variable references such as `function_name.arg` and `block_label.var`.

Key decisions:
- Capture an implicit function-name label before compiling the function body so parameters remain addressable after local declarations shadow them.
- Reuse the existing labeled-scope alias mechanism for scalar variables, not only record/composite field references.
- Apply the same implicit label setup to trigger functions.

Files touched:
- `src/pl/plpgsql/compile.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet labeled_scalar`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-labeled-scalars`

Remaining:
- Regression improved to `2177/2271` matched, `1117` diff lines.
- Larger remaining clusters include `WHERE CURRENT OF`, composite return coercion, PG_CONTEXT stack reporting, `SELECT INTO` statement forms, transition tables, and formatting/context details.
