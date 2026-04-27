Goal:
Bring pgrust tsearch behavior closer to PostgreSQL tsearch regression parity.

Key decisions:
Added a shared PostgreSQL-style default parser/token stream for tsearch callers, explicit SRF execution for ts_token_type/ts_parse/ts_debug/ts_stat, English/simple config catalog rows, runtime default_text_search_config lookup, tsvector_update_trigger support, scalar tsearch functions in FROM, tsquery containment/btree comparison/catalog rows, and lossy GIN/GiST tsearch opclass support with heap recheck.

Files touched:
src/backend/tsearch/parser.rs, src/backend/tsearch/*, src/backend/executor/{exec_expr.rs,srf.rs,tsearch/*}, src/backend/parser/analyze/*, src/include/catalog/pg_{proc,operator,opclass,opfamily,amop,amproc,ts_*}.rs, src/backend/access/{gin,gist}/**, src/backend/commands/trigger.rs, src/pgrust/database/commands/index.rs, src/pgrust/session.rs, plus supporting relcache/catalog/planner/parser files.

Tests run:
scripts/cargo_isolated.sh test --lib --quiet tsearch
scripts/cargo_isolated.sh test --lib --quiet parse_create_index_with_opclass_options
scripts/cargo_isolated.sh test --lib --quiet default_text_search_config_guc_drives_one_arg_tsearch
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test tsearch --timeout 120 --jobs 1 --results-dir /tmp/pgrust-tsearch-regress --port 56444

Remaining:
Latest targeted regression is 400/464 queries matched with 840 diff lines. Remaining clusters are exact ts_parse blank-token grouping, phrase stop-word distance off by one, ts_rank_cd stripped-position behavior, ts_headline fragment/context/stemming precision, real ts_rewrite substitution semantics, duplicate-key behavior for the btree tsquery fixture, trigger/index count mismatches, and EXPLAIN plan shape for scalar to_tsquery in FROM. Current artifacts are copied to /tmp/diffs/tsearch.{diff,out,summary.json}.
