Goal:
Fix shared formatting-surface diffs from /tmp/pgrust-regression-diffs-2026-05-01T2044Z without updating expected files: EXPLAIN output shape/options and SQL-visible error message/detail/hint/position/context rendering.

Key decisions:
- Kept compatibility shims local to EXPLAIN/protocol rendering where planner or executor behavior was not changed.
- Added XML/YAML EXPLAIN through JSON-shape conversion as a temporary structured-output bridge.
- Added explicit Cargo tracking for `gram.pest` so grammar edits rebuild `pgrust_sql_grammar`; without this, the parser used stale EXPLAIN syntax during reruns.
- Added EXPLAIN structured-output shims for empty JSON arrays, `track_io_timing` I/O timing fields, and analyze+memory buffer counters.
- Routed more ExecError cases through ExecErrorResponse and added narrow caret/message inference for COPY WHERE and relation lookup contexts.
- PL/pgSQL expression errors now preserve internal QUERY for parse/binding/operator failures, but runtime arithmetic errors use only expression context.
- External SSD env paths disappeared mid-run, so validation used TMPDIR=/tmp and PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/helsinki-v2. Generated /tmp regression artifacts were cleaned after /tmp filled during constraints.

Files touched:
- crates/pgrust_sql_grammar/src/gram.pest
- crates/pgrust_sql_grammar/src/lib.rs
- src/include/nodes/parsenodes.rs
- src/backend/parser/gram.rs
- src/backend/parser/tests.rs
- src/pl/plpgsql/gram.rs
- src/pl/plpgsql/exec.rs
- src/backend/commands/explain.rs
- src/backend/commands/tablecmds.rs
- src/backend/libpq/pqformat.rs
- src/backend/tcop/postgres.rs

Tests run:
- cargo fmt
- TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/pgrust-target PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool scripts/cargo_isolated.sh check
- scripts/cargo_isolated.sh test --lib --quiet parse_insert_update_delete
- scripts/cargo_isolated.sh test --lib --quiet parse_parenthesized_continue_when_stmt
- scripts/cargo_isolated.sh test --lib --quiet structured_explain_json_converts
- scripts/cargo_isolated.sh test --lib --quiet operator_type_mismatch_uses_postgres_message
- scripts/cargo_isolated.sh test --lib --quiet exec_error_response_formats_operator_type_mismatch
- scripts/cargo_isolated.sh test --lib --quiet exec_error_response_formats_copy_where_errors
- scripts/cargo_isolated.sh test --lib --quiet plpgsql_expression_runtime_errors_do_not_add_internal_query
- scripts/cargo_isolated.sh test --lib --quiet plpgsql_expression_parse_errors_keep_internal_query
- scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_relation_lookup_contexts
- scripts/cargo_isolated.sh test --lib --quiet exec_error_response_formats_relation_lookup_message_contexts
- scripts/run_regression.sh --test explain: FAIL, 50/75 matched, 759 diff lines.
- scripts/run_regression.sh --test explain after structured-output fixes: FAIL, 61/75 matched, 393 diff lines. Results: /tmp/pgrust_regress_results.helsinki-v2.ixgy4Z
- scripts/run_regression.sh --skip-build --test join_hash: FAIL, 262/315 matched, 767 diff lines.
- scripts/run_regression.sh --test copy2: improved to FAIL, 180/215 matched, 435 diff lines.
- scripts/run_regression.sh --test plpgsql: improved to FAIL, 2238/2271 matched, 462 diff lines.
- scripts/run_regression.sh --test compression: PASS, 87/87 matched.
- scripts/run_regression.sh --test collate: FAIL, 106/144 matched, 394 diff lines. Results: /tmp/pgrust_regress_results.helsinki-v2.gsjbJa
- scripts/run_regression.sh --skip-build --test privileges: FAIL, 1132/1295 matched, 1270 diff lines.
- scripts/run_regression.sh --skip-build --test constraints: ERROR; /tmp filled before status/diff files could be written, so no usable result.

Remaining:
- formatting fixed: quoted EXPLAIN formats, XML/YAML syntax/render path, EXPLAIN serialize/settings/memory syntax, JSON empty Triggers array layout, EXPLAIN I/O timing fields when track_io_timing is on, analyze+memory structured buffer counters, PL/pgSQL parenthesized CONTINUE/EXIT WHEN, TypeMismatch operator messages, COPY WHERE carets/messages, relation lookup carets/messages in compression, PL/pgSQL runtime expression QUERY suppression, compression regression now passes.
- still formatting: EXPLAIN verbose text qualification/output/query identifier lines; some PL/pgSQL compile-vs-runtime context/LINE/QUERY differences; some collate error caret/detail/hint differences where pgrust reaches the same broad class of error.
- semantic/out of scope: EXPLAIN generic-plan parameter typing, unsupported jsonb_pretty/json path expression in explain, window plan/expression/storage rendering, planner shape and row-count differences in explain/join_hash/privileges, COPY FREEZE and COPY ignore/reject-limit behavior, PL/pgSQL cursor/transition-table/foreach semantics, missing privilege/catalog/information_schema functions and objects, collation propagation/provider/catalog/dependency support, psql meta-command/catalog coverage.
