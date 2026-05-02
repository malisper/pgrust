Goal:
Fix SQL/JSON constructor function regressions for JSON(), JSON_SCALAR(), and JSON_SERIALIZE().

Key decisions:
- Added a parser compatibility check so empty JSON(), JSON_SCALAR(), and JSON_SERIALIZE() calls report PostgreSQL-style syntax errors instead of falling through to unsupported SELECT.
- Changed SQL/JSON RETURNING coercion to use text input/assignment-style coercion for text, varchar, and char targets so typmods and domains are enforced.
- Added domain check support for simple VALUE NOT IN (...) constraints needed by sqljson_char2.
- Left broader SQL/JSON deparse, error-position, and aggregate constructor RETURNING diffs for separate work.

Files touched:
- src/backend/parser/gram.rs
- src/backend/parser/tests.rs
- src/backend/executor/expr_json.rs
- src/backend/executor/expr_casts.rs
- src/backend/executor/tests.rs

Tests run:
- cargo fmt
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/puebla-v2/0 scripts/cargo_isolated.sh test --lib --quiet parse_sql_json_special_syntax
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/puebla-v2/0 scripts/cargo_isolated.sh test --lib --quiet sql_json_returning_coerces_typmods_and_domains
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/puebla-v2/0 scripts/run_regression.sh --test sqljson --jobs 1 --timeout 120 --results-dir /tmp/pgrust-sqljson-results

Remaining:
- sqljson regression still fails: 137/221 queries matched, 757 diff lines.
- Remaining diffs include EXPLAIN/deparse rendering for SQL/JSON constructors, missing error positions, JSON aggregate constructor RETURNING typmod coercion, and pre-existing nested aggregate FORMAT JSON errors.
