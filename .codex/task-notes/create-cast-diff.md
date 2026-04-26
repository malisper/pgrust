Goal:
Implement real catalog-backed CREATE CAST / DROP CAST behavior needed by the create_cast regression.

Key decisions:
Added parse nodes/grammar, a cast command module, pg_cast heap mutations, pg_depend rows, pg_describe_object support, and DROP FUNCTION CASCADE cast cleanup.
Kept user-defined base types text-backed, but stopped treating their OIDs as builtin text-like for function matching and explicit casts.
Lazy/visible catalog lookups now expose dynamic pg_cast rows, so binder, function resolution, CREATE CAST duplicate checks, and DROP CAST share catalog state.
Added parser-derived positions for shell argument type notices and explicit cast target error carets.

Files touched:
src/include/nodes/parsenodes.rs, src/backend/parser/gram.rs, src/pgrust/database/commands/cast.rs, catalog store/cache files, analyzer coercion/function matching, session/command routing, pg_describe_object, drop function cascade, and regression-facing error position code.

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet parse_create_cast && scripts/cargo_isolated.sh test --lib --quiet parse_drop_cast
scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_failed_explicit_cast_target
CARGO_TARGET_DIR=/tmp/pgrust-target-regress-shanghai-createcast bash scripts/run_regression.sh --test create_cast --jobs 1 --timeout 300 --port 57555 --results-dir /tmp/pgrust-create-cast-shanghai-v4

Remaining:
Full PostgreSQL CREATE CAST edge cases remain out of scope: domains, privileges, volatility policy, and complete physical compatibility rules.
