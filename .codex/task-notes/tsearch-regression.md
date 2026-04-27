Goal:
Execute the first compatibility slices from the tsearch regression debugging plan and measure the remaining tsearch diff.

Key decisions:
Fixed low-risk SQL semantics first: strict NULL behavior for @@, session-aware default_text_search_config for one-argument tsearch functions, parser table SRFs, phrase/quoted operand normalization, tsquery containment, empty-tsquery display, scalar tsearch functions in FROM, text @@ tsquery/text matching, empty-query NOTICEs, direct three-argument ts_rewrite, and a basic ts_headline implementation. Kept parser/analyzer/executor/catalog boundaries intact and used PostgreSQL sources for containment, scalar-FROM, text match, NOTICE, and ts_rewrite semantics.

Files touched:
- .codex/task-notes/tsearch-regression.md
- src/backend/executor/exec_expr.rs
- src/backend/executor/exec_expr/subquery.rs
- src/backend/executor/mod.rs
- src/backend/executor/nodes.rs
- src/backend/executor/srf.rs
- src/backend/executor/tsearch/mod.rs
- src/backend/executor/tsearch/tsquery_op.rs
- src/backend/parser/analyze/expr.rs
- src/backend/parser/analyze/expr/func.rs
- src/backend/parser/analyze/expr/ops.rs
- src/backend/parser/analyze/expr/targets.rs
- src/backend/parser/analyze/functions.rs
- src/backend/parser/analyze/infer.rs
- src/backend/parser/analyze/paths.rs
- src/backend/parser/analyze/scope.rs
- src/backend/tsearch/mod.rs
- src/backend/tsearch/to_tsany.rs
- src/backend/tsearch/ts_utils.rs
- src/include/catalog/pg_operator.rs
- src/include/catalog/pg_proc.rs
- src/include/nodes/primnodes.rs
- src/include/nodes/tsearch.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet phrase_queries_count_removed_stop_words
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet to_tsquery_tokenizes_quoted_operands_as_phrases
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet websearch_ignores_tsquery_syntax_and_weights
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet tsquery_containment_uses_query_lexeme_sets
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet tsquery_containment_operators_use_lexeme_sets
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet tsearch_match_operator_is_null_strict
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet one_arg_tsearch_functions_use_default_text_search_config
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet tsearch_parser_table_functions_return_rows
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet scalar_tsearch_function_can_be_used_in_from
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet tsearch
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet ts_rewrite_replaces_tsquery_subtrees
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet ts_headline_handles_empty_and_basic_queries
- PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh check (passes with pre-existing query_repl.rs unreachable-pattern warning)
- scripts/run_regression.sh --test tsearch --jobs 1 --timeout 300 --port 55434 --results-dir /tmp/diffs/tsearch-copenhagen-v3-7
  Result: FAIL, 357/464 queries matched, 1266 diff lines. Earlier rebuilt baseline in this session was 333/464 and 1498 diff lines.
- scripts/run_regression.sh --test tsearch --jobs 1 --timeout 300 --port 55436 --results-dir /tmp/diffs/tsearch-copenhagen-v3-8
  Result after scalar-FROM: FAIL, 357/464 queries matched, 1248 diff lines.
- scripts/run_regression.sh --test tsearch --jobs 1 --timeout 300 --port 55438 --results-dir /tmp/diffs/tsearch-copenhagen-v3-10
  Result after text @@ tsquery, empty-query NOTICEs, and direct ts_rewrite: FAIL, 367/464 queries matched, 1171 diff lines.
- scripts/run_regression.sh --test tsearch --jobs 1 --timeout 300 --port 55439 --results-dir /tmp/diffs/tsearch-copenhagen-v3-11
  Result after basic ts_headline: FAIL, 369/464 queries matched, 1221 diff lines. Diff lines rose because ts_headline now returns rows for many cases whose exact fragments still differ.

Remaining:
Major remaining buckets are tsearch GIN/GiST opclass/index support, ts_stat, two-argument SQL-driven ts_rewrite, full ts_headline fragment/options fidelity, tsvector_update_trigger, full default parser fidelity for ts_parse/ts_debug/to_tsvector, and GIN tsvector index insertion/query integration.

Plan:
1. Reproduce and split the current tsearch regression diff into independent buckets: core @@ count mismatches, parser/tokenizer output, missing SRFs/scalars, one-argument default-config behavior, tsquery operators/comparison/rewrite, trigger behavior, and GIN/GiST index behavior.
2. Fix low-risk executor correctness first: make @@ strict/null-propagating, align empty-tsquery rendering/notices, and add focused tests for tsquery comparison/order operators so later unique-index and btree failures are not cascading.
3. Thread default_text_search_config through one-argument tsearch entry points. Replace resolve_config(None) = simple with session/GUC-aware resolution at execution time, while keeping explicit config calls immutable.
4. Replace tokenize_document with a PostgreSQL-default-parser-compatible token stream, including token ids/aliases needed by ts_token_type, ts_parse, ts_debug, to_tsvector, plainto_tsquery, phraseto_tsquery, and websearch_to_tsquery. Use ../postgres/src/backend/tsearch/wparser_def.c as the behavioral reference.
5. Fix query construction and normalization: preserve token positions through lexization, account for removed stop words in phrase distances, keep weights/prefix flags through normalization, and match PostgreSQL output formatting for NOT/phrase expressions.
6. Rework @@ evaluation around PostgreSQL-style extents: weights, prefix lexemes, stripped positions, NOT under phrase, negative phrase operands, and phrase distance matches. Validate against the repeated count sections before touching indexes.
7. Implement required table/scalar tsearch functions in dependency order: ts_token_type, ts_parse, ts_debug, ts_stat, ts_rewrite, then ts_headline. Add scalar-function-in-FROM support for to_tsquery(...) AS query if the planner still treats it as a relation.
8. Add tsvector_update_trigger compatibility, at least for the regression-covered argument forms, with a :HACK: comment if implemented as a temporary built-in trigger shim instead of full fmgr trigger support.
9. Add tsearch opclass/index support in two passes: first accept and validate tsvector_ops/tsquery_ops options and build catalog/index metadata so DDL/DROP/describe output is correct; then implement enough GIN/GiST scan integration for @@, @>, <@, and phrase/prefix queries to produce the expected plans and results.
10. Run focused validation after each slice: targeted Rust tests for the edited module, then the tsearch regression file; copy useful .diff files to /tmp/diffs and keep this note updated.
