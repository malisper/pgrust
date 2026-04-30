Goal:
Fix regression failures for json_encoding, rangetypes, regproc, and stats_import.

Key decisions:
- Match PostgreSQL's regclass missing-name behavior by reporting a missing relation, not a missing schema, for qualified regclass input.
- Suppress caret positions for legacy json operator runtime JSON input errors and pg_input_error_info numeric typmod failures.
- Keep unordered full index-only scans from displacing seq scans when there is no ORDER BY or index qual, preserving expected row order.
- Preserve range element type OIDs in array storage and avoid over-parenthesizing expression-index EXPLAIN keys.
- Treat missing storage forks as empty during ANALYZE and schema-qualify expanded maintenance targets outside public/pg_catalog/temp schemas.
- Route stats import builtins through the stats import runtime in aggregate and FROM-function value execution, preserving argument type hints.

Files touched:
- src/backend/commands/analyze.rs
- src/backend/executor/exec_expr.rs
- src/backend/executor/expr_agg_support.rs
- src/backend/executor/expr_reg.rs
- src/backend/executor/jsonb.rs
- src/backend/executor/nodes.rs
- src/backend/executor/srf.rs
- src/backend/executor/value_io.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/parser/analyze/expr.rs
- src/backend/tcop/postgres.rs
- src/pgrust/database/commands/maintenance.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- cargo check
- cargo test --lib --quiet exec_error_position_omits_legacy_json_runtime_errors
- cargo test --lib --quiet exec_error_position_omits_pg_input_error_info_numeric_typmod
- cargo test --lib --quiet regclass_cast_reports_missing_relation_for_qualified_name
- scripts/run_regression.sh --test json_encoding --jobs 1 --port 62601 --timeout 180 --results-dir /tmp/diffs/kyiv-v5-json_encoding
- scripts/run_regression.sh --test regproc --jobs 1 --port 62603 --timeout 180 --results-dir /tmp/diffs/kyiv-v5-regproc
- scripts/run_regression.sh --test rangetypes --jobs 1 --port 62605 --timeout 180 --results-dir /tmp/diffs/kyiv-v5-rangetypes-2
- scripts/run_regression.sh --test stats_import --jobs 1 --port 62607 --timeout 180 --results-dir /tmp/diffs/kyiv-v5-stats_import-3

Remaining:
- No known remaining mismatches for the four requested regression files.
