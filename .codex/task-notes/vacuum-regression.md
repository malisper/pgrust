Goal:
- Close remaining PostgreSQL `vacuum` regression gaps around VACUUM/ANALYZE semantics, relhassubclass, reloptions, truncation, toast stats, permissions, and error text.

Key decisions:
- Preserve PostgreSQL's stale `relhassubclass` behavior after child drop, then clear it during ANALYZE when no live inheritors remain.
- Treat `CREATE TABLE ... WITH (...)` and table-level `ALTER TABLE ... RESET (...)` reloptions as catalog data so vacuum options can resolve per relation.
- Add minimal SQL-function utility handling for ANALYZE/VACUUM bodies to surface PostgreSQL-compatible nested maintenance errors.
- Add GIN array extraction for build/vacuum coverage; query behavior remains driven by existing GIN scan support.

Files touched:
- `src/backend/commands/analyze.rs`
- `src/backend/access/gin/gin.rs`
- `src/backend/catalog/store/heap.rs`
- `src/backend/parser/gram.pest`
- `src/backend/parser/gram.rs`
- `src/backend/parser/analyze/functions.rs`
- `src/backend/executor/sqlfunc.rs`
- `src/backend/tcop/postgres.rs`
- `src/pgrust/database/commands/maintenance.rs`
- `src/pgrust/database_tests.rs`
- plus earlier parser/catalog/session files for reloptions and table reset plumbing.

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet backend::parser::tests`
- `scripts/cargo_isolated.sh test --lib --quiet analyze_clears_stale_relhassubclass_after_child_drop`
- `scripts/cargo_isolated.sh test --lib --quiet create_gin_array_index_builds_and_vacuums`
- `scripts/cargo_isolated.sh test --lib --quiet create_gin_jsonb_index_uses_bitmap_scan_and_rechecks`
- `scripts/cargo_isolated.sh test --lib --quiet sql_function_from_item_resolves_qualified_utility_function`
- `scripts/cargo_isolated.sh test --lib --quiet analyze_expression_index_reports_nested_sql_function_context`
- `scripts/cargo_isolated.sh check`
- focused `vacuum` regression via `.context/run_regression_timeout30.sh`

Remaining:
- Focused `vacuum` regression is down to `326/328` query matches; the remaining diff is only unsupported `ALTER TABLE ... CLUSTER ON ...` and `CLUSTER table`.
