Goal:
Fix pg_lsn WAL filename/offset helper regression failures in misc_functions.

Key decisions:
Expose wal_segment_size through pg_show_all_settings so psql \gset can bind
segment_size before the helper queries run.
Keep WAL helper math aligned with PostgreSQL's XLByteToSeg face-value boundary
semantics and cover boundary, boundary + 1, and boundary - 1 cases.

Files touched:
src/backend/executor/srf.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet build_plan_resolves_pg_lsn_arithmetic_record_function_in_from
scripts/cargo_isolated.sh test --lib --quiet pg_show_all_settings_includes_wal_segment_size
scripts/cargo_isolated.sh test --lib --quiet pg_lsn_wal_filename_offset_helpers_match_segment_boundaries
scripts/run_regression.sh --test misc_functions --timeout 180 --jobs 1

Remaining:
misc_functions still has unrelated existing failures, but the pg_lsn WAL helper
diff hunk is gone in /tmp/diffs/misc_functions.pg_lsn_wal.diff.
