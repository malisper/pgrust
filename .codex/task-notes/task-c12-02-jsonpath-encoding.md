Goal:
- Fix TASK-C12-02 jsonpath_encoding failures for invalid jsonpath Unicode escapes, text-incompatible `\u0000`, and surrogate-pair validation.

Key decisions:
- Match PostgreSQL jsonpath scanner behavior in `src/backend/executor/jsonpath.rs` by separating malformed Unicode escape syntax from valid escapes that cannot be converted to text.
- Return PostgreSQL-shaped detail errors for high/low surrogate ordering and preserve jsonpath parse-error text instead of wrapping it as a generic input syntax error.
- Keep the change scoped to jsonpath input/canonicalization; no SQL/JSON constructor behavior was changed.

Files touched:
- `src/backend/executor/jsonpath.rs`
- `src/backend/executor/expr_json.rs`
- `src/backend/tcop/postgres.rs`
- `src/backend/executor/tests.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet getdatabaseencoding_and_jsonpath_unicode_work`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-c12-02-jsonpath scripts/run_regression.sh --test jsonpath_encoding --port 63806 --results-dir /tmp/pgrust-task-c12-02-jsonpath-encoding`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-c12-02-jsonpath scripts/cargo_isolated.sh check`

Remaining:
- None for `jsonpath_encoding`; the focused regression passed with 32/32 query matches.
