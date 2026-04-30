Goal:
Fix the last json regression diffs caused by extra statement positions on runtime JSON errors.

Key decisions:
Suppress PostgreSQL-incompatible cursor positions only for legacy JSON record conversion input errors and unique JSON object aggregate duplicate-key errors.

Files touched:
src/backend/tcop/postgres.rs

Tests run:
RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet exec_error_position_omits_legacy_json_runtime_errors
RUSTC_WRAPPER=/usr/bin/env scripts/run_regression.sh --test json --results-dir /tmp/pgrust-json-last3-final --ignore-deps
RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh check

Remaining:
None for the json regression file; json passes 470/470.
