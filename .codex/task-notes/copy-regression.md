Goal:
Investigate copy.out regression mismatches around HEADER option errors and COPY TO materialized views.

Key decisions:
PostgreSQL accepts only true/false/on/off/0/1/match for COPY HEADER; invalid values should produce `header requires a Boolean value or "match"`.
PostgreSQL permits COPY TO from populated materialized views and rejects only unpopulated materialized views with the REFRESH hint.

Files touched:
.codex/task-notes/copy-regression.md

Tests run:
scripts/cargo_isolated.sh test --lib --quiet copy_from_rejects_invalid_header_choice
scripts/cargo_isolated.sh test --lib --quiet copy_to_populated_materialized_view_outputs_rows
scripts/cargo_isolated.sh test --lib --quiet copy_to_unpopulated_materialized_view_uses_copy_error
scripts/run_regression.sh --test copy --timeout 120 --jobs 1

Remaining:
None.
