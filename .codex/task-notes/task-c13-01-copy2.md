Goal:
Fix the bounded COPY option grammar and COPY data-error behavior slice for the `copy2` regression.

Key decisions:
- Fixed the session COPY parser because simple-query COPY FROM STDIN uses `src/pgrust/session.rs`, not the pest grammar path.
- Added legacy and modern COPY option parsing for duplicate detection, `AS` option spelling, ON_ERROR/REJECT_LIMIT/LOG_VERBOSITY, FORCE_* options, DEFAULT, BINARY, and SQL_ASCII handling.
- Made COPY text parsing honor backslash-escaped delimiters before splitting fields, and tightened CSV null/empty-string/end-marker handling.
- Moved COPY WHERE and FORCE_* validation before CopyInResponse so psql does not consume following SQL as COPY data after an invalid COPY command.

Files touched:
- `src/pgrust/session.rs`
- `src/backend/tcop/postgres.rs`

Tests run:
- `cargo fmt`
- `env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/baghdad-copy2-check PGRUST_TARGET_SLOT=0 scripts/cargo_isolated.sh check --lib -q`
- `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/baghdad-v3-target' scripts/run_regression.sh --test copy2 --port 58652 --results-dir /tmp/pgrust-task-c13-01-copy2`

Remaining:
- `copy2` still fails at 184/215 matched, 319 diff lines.
- Remaining mismatches are mostly SQL-visible position lines for COPY WHERE errors, transaction/savepoint COPY FREEZE behavior, CHECK expression return handling, COPY into views with INSTEAD OF triggers, notice CONTEXT output, missing `widget` type setup from `create_type`, and a CSV DEFAULT context delimiter display issue.
