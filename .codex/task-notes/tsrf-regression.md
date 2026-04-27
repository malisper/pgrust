Goal:
Run the PostgreSQL `tsrf` regression against pgrust and diagnose the error.

Key decisions:
Used `scripts/run_regression.sh --test tsrf` with a bounded results directory
under `/tmp/diffs/tsrf`.

Files touched:
`src/backend/parser/analyze/scope.rs`
`src/backend/parser/analyze/mod.rs`
`src/backend/parser/analyze/expr.rs`
`src/backend/parser/analyze/expr/targets.rs`
`src/backend/parser/tests.rs`
`src/backend/tcop/postgres.rs`

Tests run:
`scripts/run_regression.sh --test tsrf --timeout 240 --jobs 1 --port 59448 --results-dir /tmp/pgrust-tsrf-daegu`
`scripts/run_regression.sh --test tsrf --timeout 180 --jobs 1 --port 59761 --results-dir /tmp/diffs/tsrf`
`scripts/cargo_isolated.sh test --lib --quiet generate_series`
`scripts/cargo_isolated.sh test --lib --quiet build_plan_rejects_top_level_values_srf`
`scripts/cargo_isolated.sh test --lib --quiet build_plan_rejects_nested_srf_in_from_function_args`
`scripts/cargo_isolated.sh test --lib --quiet build_plan_for_order_by_only_generate_series_uses_project_set`
`scripts/cargo_isolated.sh test --lib --quiet build_plan_rejects_group_by_srf_before_planning`
`scripts/cargo_isolated.sh test --lib --quiet build_plan_rejects_distinct_on_with_target_srf_before_planning`
`scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_nested_from_function_srf`
`scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_values_srf`
`scripts/cargo_isolated.sh check`
`scripts/run_regression.sh --test tsrf --timeout 180 --jobs 1 --port 59761 --results-dir /tmp/diffs/tsrf-fix5`
`scripts/run_regression.sh --test tsrf --timeout 240 --jobs 1 --port 59449 --results-dir /tmp/pgrust-tsrf-daegu-fixed`

Remaining:
`tsrf` now completes without server errors: 38/74 queries match and 36 differ.
Remaining diffs are unsupported or non-PostgreSQL-compatible tSRF semantics,
not connection-terminating panics.
