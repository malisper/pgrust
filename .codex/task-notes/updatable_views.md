Goal:
Fix non-formatting `updatable_views` regression diffs without updating expected output.

Key decisions:
- Keep PostgreSQL `../postgres/src/backend/rewrite/rewriteHandler.c` and regression output as the behavioral reference.
- Auto-updatable view DML now carries view predicates, defaults, check options, local column updatability, security-barrier metadata, and nested-view mappings through INSERT/UPDATE/DELETE/MERGE planning.
- Rule handling is event-specific: unconditional `DO INSTEAD` rules satisfy that event, conditional rules block auto updates, `DO ALSO` rules do not make otherwise auto-updatable views read-only, and MERGE rejects relations with rules.
- `pg_relation_is_updatable` / `pg_column_is_updatable`, ALTER VIEW column defaults, ALTER VIEW check_option reloptions, cascade notices, and dependent function/sequence drops were added or corrected.
- Security-barrier view DML now avoids unsafe subquery pushdown and orders cheap/leakproof predicates before leaky predicates where PostgreSQL does.
- MERGE `old`/`new` pseudo rows now render absent rows as SQL NULL; security-barrier MERGE streams rows so target view predicates and action predicates interleave in PostgreSQL order.
- Full MERGE materialized inputs are ordered target rows before source-only rows, fixing trigger-backed MERGE `RETURNING` order and reducing the auto-view full-MERGE ordering diff.

Files touched:
- `src/backend/commands/tablecmds.rs`
- `src/backend/executor/exec_expr/subquery.rs`
- `src/backend/optimizer/path/allpaths.rs`
- `src/backend/optimizer/path/costsize.rs`
- `src/backend/optimizer/path/mod.rs`
- `src/backend/optimizer/tests.rs`
- `src/backend/parser/analyze/modify.rs`
- `src/backend/parser/analyze/system_views.rs`
- `src/backend/rewrite/mod.rs`
- `src/backend/rewrite/view_dml.rs`
- `src/pgrust/database/commands/drop.rs`
- `src/pgrust/database/commands/rules.rs`
- `src/pgrust/database/commands/sequence.rs`

Tests run:
- `cargo fmt`
- `TMPDIR="/Volumes/OSCOO PSSD/tmp" CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/rust/seoul-v2-target" cargo build --bin pgrust_server --quiet`
- Focused pgrust probes for rule-action EXPLAIN, security-barrier MERGE notices, auto-view MERGE `RETURNING`, and trigger-backed MERGE `RETURNING`.
- `TMPDIR="/Volumes/OSCOO PSSD/tmp" CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/rust/seoul-v2-target" scripts/run_regression.sh --test updatable_views --timeout 120 --port 65510 --results-dir /tmp/pgrust_regress_updatable_views_seoul41` -> `1098/1139`, `771` diff lines.
- `TMPDIR="/Volumes/OSCOO PSSD/tmp" CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/rust/seoul-v2-target" scripts/run_regression.sh --test updatable_views --timeout 120 --port 65515 --results-dir "/Volumes/OSCOO PSSD/tmp/pgrust_regress_updatable_views_seoul42"` -> `1099/1139`, `747` diff lines.
- `CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/rust/seoul-v2-target" scripts/run_regression.sh --test updatable_views --timeout 120 --port 65515 --results-dir /tmp/pgrust_regress_updatable_views_seoul43` -> `1100/1139`, `735` diff lines.

Remaining:
- Latest diff: `/tmp/pgrust_regress_updatable_views_seoul43/diff/updatable_views.diff`.
- Remaining non-formatting output differences are ordering-related:
  - auto-updatable full MERGE `RETURNING` still orders target rows `1,2,5` instead of PostgreSQL's `1,5,2`; this appears tied to PostgreSQL HOT/bitmap heap scan order after prior updates.
  - inherited security-barrier view rows/notices differ within each relation after updating `a=8` to `a=9`; PostgreSQL visits original/new index entries in a different physical order.
- Most other remaining hunks are display/formatting: EXPLAIN plan shape, extra nested `Update on ...` lines, information_schema view_definition parentheses/spacing, unqualified security-barrier filter text, WCO SubPlan rendering, and ON CONFLICT debug expression rendering.
- `/tmp` filled during one run; old generated `/tmp/pgrust_regress_updatable_views_seoul36..42` and probe dirs were removed. Avoid external data dirs for setup unless increasing `PGRUST_STATEMENT_TIMEOUT`, because the 5s statement timeout can trip on slow COPY/VACUUM setup.
