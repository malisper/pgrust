Goal:
Diagnose date.out regression differences around EXTRACT(EPOCH FROM DATE) and DATE_TRUNC(..., DATE ...).

Key decisions:
Fixed date-specific runtime semantics, not parser output.
EXTRACT(EPOCH FROM DATE) now renders integer numeric scale.
DATE_TRUNC(..., DATE ...) now converts truncated local midnight through named timezone rules.

Files touched:
.codex/task-notes/date-regression.md
src/backend/executor/expr_date.rs
src/backend/executor/tests.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet select_extract_returns_numeric_with_postgres_scale
scripts/cargo_isolated.sh test --lib --quiet date_trunc_date_uses_local_zone_rules
scripts/cargo_isolated.sh test --lib --quiet date_trunc_handles_bc_boundaries
scripts/run_regression.sh --test date --results-dir /tmp/pgrust_date_regress_fix

Remaining:
None for this diff.
