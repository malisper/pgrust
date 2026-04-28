Goal:
Analyze and eliminate jsonb_jsonpath regression failures in the datetime, JSONPath method, jsonb_path_query_tz, error-formatting, keyvalue id, silent-mode, arithmetic, starts-with, and regex buckets.

Key decisions:
Added parser/evaluator support for keyvalue/string JSONPath item methods, registered jsonb_path_query_tz as an SRF, and taught JSONPath datetime methods/comparisons to preserve SQL/JSON temporal rendering and time zone semantics. Used PostgreSQL jsonpath_exec.c as the reference for datetime conversion and timetz comparison behavior.
Follow-up pass preserved JSONPath syntax errors instead of wrapping them, suppressed SQL LINE context for JSONPath datetime runtime errors, derived .keyvalue().id from jsonb binary offsets, propagated variable/vars errors, implemented lax arithmetic unwrapping and strict predicate UNKNOWN behavior, fixed LIKE_REGEX newline/\b semantics, and handled huge-date vs timestamp comparison without timestamp overflow.

Files touched:
.codex/task-notes/jsonb-jsonpath.md
src/backend/executor/jsonpath.rs
src/backend/executor/jsonb.rs
src/backend/executor/expr_json.rs
src/backend/executor/expr_casts.rs
src/backend/access/gin/jsonb_ops.rs
src/include/nodes/primnodes.rs
src/include/catalog/pg_proc.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/expr/targets.rs
src/backend/executor/srf.rs
src/backend/rewrite/views.rs
src/backend/executor/pg_regex.rs
src/backend/tcop/postgres.rs
src/backend/parser/analyze/expr/func.rs

Tests run:
scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet jsonpath_datetime
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test jsonb_jsonpath --timeout 60 --jobs 1 --results-dir /tmp/pgrust_jsonb_jsonpath_results
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test jsonb_jsonpath --timeout 60 --jobs 1 --port 56548 --results-dir /tmp/pgrust_jsonb_jsonpath_results
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib jsonpath_exists -- --nocapture
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet jsonpath

Remaining:
Focused jsonb_jsonpath regression now passes 830/830. CI unit-test fallout was stale jsonb_path_exists expectations: silent JSONPath errors now return NULL, and the non-silent member accessor error text changed.
