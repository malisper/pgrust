Goal:
Fix the copy regression diff where rows accumulated across `truncate copytest2`.

Key decisions:
The CSV file was not accumulating data; `copytest.csv` ended with only `line1`, `\.`, and `line2`. The stale rows came from TRUNCATE on a temp table rewriting storage without updating the session temp-relation relfilenode map.

Files touched:
- src/pgrust/database/commands/execute.rs
- src/pgrust/session.rs
- src/pgrust/database_tests.rs

Tests run:
- scripts/cargo_isolated.sh test --lib --quiet truncate_temp_table_replaces_visible_storage
- scripts/run_regression.sh --port 55434 --test copy --jobs 1 --timeout 120 --results-dir /tmp/pgrust-copy-after-truncate-fix-rebuilt

Remaining:
None.
