Goal:
Fix a bounded SQL/JSON constructor slice for TASK-C12-01, focused on WITH/WITHOUT UNIQUE KEYS behavior for JSON(), IS JSON, and JSON_OBJECTAGG.

Key decisions:
- Thread JSON() WITH UNIQUE KEYS through the existing internal SQL/JSON helper instead of adding a new plan node.
- Preserve WITHOUT UNIQUE as the default non-unique path.
- Validate JSON() WITH UNIQUE KEYS only for string-like inputs, matching PostgreSQL's rejection of json/jsonb inputs for this clause.
- Add a recursive textual JSON duplicate-key validator so duplicates are detected before serde_json object normalization discards them.
- Map SQL/JSON JSON_OBJECTAGG WITH UNIQUE to the existing unique aggregate variants and check uniqueness before strict null skipping.

Files touched:
- src/backend/parser/gram.rs
- src/backend/parser/analyze/expr.rs
- src/backend/parser/analyze/functions.rs
- src/backend/executor/expr_json.rs
- src/backend/executor/agg.rs

Tests run:
- cargo fmt
- CARGO_TARGET_DIR=/tmp/pgrust-target-c12 cargo check
- CARGO_TARGET_DIR=/tmp/pgrust-target-c12 scripts/run_regression.sh --test sqljson --port 63068 --results-dir /tmp/pgrust-task-c12-01-sqljson
  - Result: still failing, 129/221 queries matched.
- CARGO_TARGET_DIR=/tmp/pgrust-target-c12 scripts/run_regression.sh --test sqljson_jsontable --port 63062 --results-dir /tmp/pgrust-task-c12-01-sqljson-jsontable
  - Result: still failing, 115/117 queries matched.

Remaining:
- sqljson still has broad deparse/output-format/RETURNING coercion/aggregate-null behavior gaps outside this slice.
- SQL/JSON duplicate-key errors now include key detail and caret text where PostgreSQL expected output sometimes omits them.
- JSON_OBJECTAGG unique variants currently expose internal aggregate column names in some output.
- sqljson_jsontable remaining mismatches are row order for one JSON_TABLE query and view deparse of LATERAL/alias list.
- scripts/cargo_isolated.sh check blocked on an external Cargo build lock in this shared machine; direct cargo check with /tmp/pgrust-target-c12 passed.
