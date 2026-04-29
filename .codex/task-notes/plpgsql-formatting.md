Goal:
Fix PL/pgSQL regression formatting/behavior buckets around line-1 context, strict details, shadow warnings, and cursor syntax.

Key decisions:
Thread session PL/pgSQL GUCs into CREATE FUNCTION validation so shadowed-variable warnings/errors match configured checks.
Infer record shape for literal dynamic EXECUTE INTO targets so later field references compile.
Split strict no-row, strict multi-row, and multi-row hint behavior for static DML/select versus dynamic EXECUTE.
Track NO SCROLL declarations enough to reject backward FETCH with PostgreSQL's hint.

Files touched:
crates/pgrust_plpgsql_grammar/src/gram.pest
src/pl/plpgsql/ast.rs
src/pl/plpgsql/gram.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pl/plpgsql/mod.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/execute.rs
src/pgrust/session.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55485 --results-dir /tmp/diffs/plpgsql-formatting-wip6

Remaining:
plpgsql regression still fails due broader semantic gaps such as RETURN QUERY EXECUTE, cursor direction behavior, string escape warnings, diagnostics, and transition-table behavior.
