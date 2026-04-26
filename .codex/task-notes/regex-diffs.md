Goal:
Diagnose regex.sql regression diffs where anchored regex predicates on pg_proc.proname plan as Seq Scan instead of Index Scan.

Key decisions:
PostgreSQL extracts fixed regex prefixes via like_support.c / regex regprefix and synthesizes btree quals. pgrust currently treats ~ as a normal RegexMatch operator filter and only indexifies existing comparison/function quals.
Implemented a conservative pgrust-side fixed-prefix extractor for the regex.sql cases. It only returns a prefix when it can prove a literal prefix safely; ambiguous cases remain seq scans.

Files touched:
.codex/task-notes/regex-diffs.md
src/backend/optimizer/mod.rs
src/backend/optimizer/path/mod.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/path/regex_prefix.rs
src/pgrust/database_tests.rs

Tests run:
CARGO_TARGET_DIR=.context/cargo-target cargo test --lib --quiet regex_prefix
CARGO_TARGET_DIR=.context/cargo-target cargo test --lib --quiet explain_bootstrap_anchored_regex_uses_proname_index_range
CARGO_TARGET_DIR=.context/cargo-target cargo test --lib --quiet explain_bootstrap_exact_regex_uses_proname_index_equality
CARGO_TARGET_DIR=.context/cargo-target scripts/run_regression.sh --test regex --timeout 60 --port 55453 --results-dir /tmp/diffs/regex-prefix-fix-3
CARGO_TARGET_DIR=.context/cargo-target cargo check --quiet

Remaining:
cargo check still reports an existing unreachable-pattern warning in src/bin/query_repl.rs:1026.
