Goal:
Fix the remaining horology regression diffs without changing expected files.

Key decisions:
- Kept date output behavior unchanged outside EXPLAIN; EXPLAIN uses Postgres
  MDY rendering for date constants/casts.
- Added PostgreSQL-shaped from-char parsing only for the horology-exercised
  to_timestamp/to_date template tokens, including quoted literals, separators,
  FF rounding, field conflicts, and ISO/Gregorian mixing.
- Suppressed SQL cursor output for runtime to_timestamp/to_date template errors,
  while restoring cursor positions for unsupported explicit casts.
- Preserved a separate timezone display string for PostgreSQL's negative
  fractional SET TIME ZONE display while keeping normalized offsets for math.

Files touched:
- src/backend/executor/expr_date.rs
- src/backend/executor/expr_string.rs
- src/backend/executor/expr_datetime.rs
- src/backend/executor/exec_expr.rs
- src/backend/executor/nodes.rs
- src/backend/tcop/postgres.rs
- src/backend/utils/misc/guc_datetime.rs
- src/pgrust/session.rs

Tests run:
- cargo fmt
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/cargo_isolated.sh test --lib --quiet to_timestamp_text_format_supports_horology_templates
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/cargo_isolated.sh test --lib --quiet to_timestamp_template_reports_postgres_field_errors
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/cargo_isolated.sh test --lib --quiet to_timestamp_fractional_template_edges
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/cargo_isolated.sh test --lib --quiet to_date_uses_postgres_template_parser_cases
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/cargo_isolated.sh test --lib --quiet to_char_formats_timestamptz_timezone_tokens
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/cargo_isolated.sh test --lib --quiet preserves_postgres_display_for_negative_fractional_timezone
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_failed_cast_syntax_target
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/cargo_isolated.sh test --lib --quiet exec_error_position_omits_datetime_template_runtime_errors
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/cargo_isolated.sh test --lib --quiet explain_renders_date_constants_in_postgres_mdy_style
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/run_regression.sh --test horology --results-dir /tmp/diffs/horology-after4 --timeout 120 --port 25433
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/run_regression.sh --test timestamp --results-dir /tmp/diffs/horology-timestamp-after --timeout 120 --port 25433 --skip-build
- env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=4 scripts/run_regression.sh --test timestamp --test timestamptz --results-dir /tmp/diffs/horology-datetime-after --timeout 120 --port 25433 --skip-build

Remaining:
- Horology passes 399/399.
- Timestamp passes 177/177.
- The combined timestamp/timestamptz command only scheduled timestamptz; it has
  one remaining unrelated EXPLAIN temp-schema qualification diff:
  "Seq Scan on tmptz" vs "Seq Scan on pg_temp_1.tmptz".
