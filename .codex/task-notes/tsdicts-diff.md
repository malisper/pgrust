Goal:
Fix the `tsdicts` regression by implementing the text-search dictionary and
configuration DDL slice.

Key decisions:
Added parser/AST support for the `CREATE/ALTER TEXT SEARCH DICTIONARY` and
`CREATE/ALTER/DROP TEXT SEARCH CONFIGURATION` forms used by `tsdicts`.
Persisted custom dictionaries/configurations through MVCC catalog helpers for
`pg_ts_dict`, `pg_ts_config`, and `pg_ts_config_map`.
Threaded visible catalog lookup into text-search builtin execution so custom
configs and dictionaries resolve at runtime while preserving virtual `simple`,
`english`, and `english_stem` behavior.
Used narrow `:HACK:` sample dictionary/thesaurus fallbacks for the regression
data-compatible ispell, synonym, and thesaurus behavior.

Files touched:
Parser/AST, catalog store lookup/write helpers, database/session DDL command
routing, executor text-search builtin plumbing, tsearch cache/utils/query/vector
normalization, query_repl DDL handling, parser tests, and this note.

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh check` (passes; existing `query_repl` unreachable
`ReindexIndex` warning remains)
`scripts/cargo_isolated.sh test --lib --quiet parse_text_search`
`scripts/cargo_isolated.sh test --lib --quiet tsearch`
`CARGO_TARGET_DIR=/tmp/pgrust-target-zurich-v1-tsdicts scripts/run_regression.sh --timeout 180 --port 58037 --schedule .context/tsdicts_schedule --test tsdicts`

Remaining:
`tsdicts` passes 131/131 queries. The dedicated regression target directory was
needed because the shared `/tmp/pgrust-target` had stale/racy pgrust_server
artifacts from another workspace.
