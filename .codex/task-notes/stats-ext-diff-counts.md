Goal:
Reduce `stats_ext` planner estimated-row mismatches by aligning scalar-array,
dependency, and MCV selectivity with PostgreSQL.

Key decisions:
The original `/tmp/diffs/stats_ext.diff` had 72 changed estimated/actual result
rows. After the selectivity work, the latest run has 21 such rows.

Implemented PostgreSQL-shaped scalar-array selectivity:
- ANY combines as `s = s + s2 - s*s2`.
- ALL combines by multiplication.
- equality ANY keeps the disjoint-sum fast path when the sum stays in range.
- inequality ANY/ALL no longer collapse to one min/max bound.

Implemented dependency selection/application closer to
`dependencies.c::clauselist_apply_dependencies`:
- group compatible clauses by dependency target.
- select widest/strongest dependencies greedily.
- apply selected dependencies in reverse with the conditional probability
  formula using determinant and implied-target selectivities.

Implemented MCV improvements:
- top-level OR clauses are flattened and estimated arm-by-arm with overlap.
- AND lists greedily apply multiple independent MCV stats.
- `const = ANY(array_column)` uses array-column containment-style selectivity.
- MCV payloads keep earliest observed equal-frequency groups first so capped
  payloads retain regression hot values more reliably.

Files touched:
`src/backend/optimizer/path/costsize.rs`
`src/backend/commands/analyze.rs`
`src/backend/statistics/build.rs`
`src/pgrust/database_tests.rs`
`.codex/task-notes/stats-ext-diff-counts.md`

Tests run:
`cargo fmt`
`CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet mcv_tie_break_keeps_earliest_observed_group`
`CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet stats_ext_mcv_or_and_array_selectivity_match_postgres_shapes`
`CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet stats_ext_dependencies_use_postgres_selectivity_formula`
`CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check`
`PGRUST_STATEMENT_TIMEOUT=30 CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test stats_ext --timeout 300 --port 60654 --skip-build`

Remaining:
`stats_ext` still fails overall: latest run matched 755/866 queries with 968
diff lines and no `stats_ext` timeout. Remaining estimated-row mismatches are
mostly histogram/expression-stat residuals plus downstream rows from unrelated
DDL/catalog gaps such as unsupported `ALTER COLUMN TYPE` with dependent indexes
and missing/format-different pg_stats_ext/pg_statistic_ext_data display rows.
