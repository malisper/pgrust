Goal:
Fix the remaining PostgreSQL brin.sql regression failures.

Key decisions:
Use PostgreSQL catalogs as the compatibility reference and keep BRIN fixes split between catalog rows, planner opfamily matching, executor range/network semantics, and BRIN runtime maintenance.
CIDR assignment now masks host bits when cast through cidr. BRIN has enough minmax/inclusion catalog metadata for the regression types, and inclusion/bloom families are treated as lossy include-all at execution.
BRIN summarize/desummarize SQL functions are wired through pg_proc, analyzer inference, primnodes, and exec_expr.
BRIN build/summarize paths materialize toasted heap values when a heap toast relation is available.
Range &< / &> now reject empty ranges, matching PostgreSQL. BRIN planner matching now recognizes network, timestamp/timestamptz, and bit/varbit cross-type families and maps PostgreSQL proc aliases such as range_overleft/range_overright and box_contain_pt.
CI follow-up: keep name/text btree planning conservative so catalog comment joins do not use an unsafe fallback strategy; keep tid heap reads on the existing text storage path for query output, but cast tid values during index build materialization and compare text-stored tid values against tid literals in executor predicates.

Files touched:
src/backend/access/brin/brin.rs
src/backend/access/brin/mod.rs
src/backend/access/index/buildkeys.rs
src/backend/catalog/indexing.rs
src/backend/commands/tablecmds.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_casts.rs
src/backend/executor/expr_ops.rs
src/backend/executor/expr_range.rs
src/backend/executor/value_io.rs
src/backend/optimizer/path/costsize.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/infer.rs
src/backend/utils/cache/lsyscache.rs
src/backend/utils/cache/relcache.rs
src/include/access/amapi.rs
src/include/catalog/pg_amop.rs
src/include/catalog/pg_amproc.rs
src/include/catalog/pg_opclass.rs
src/include/catalog/pg_opfamily.rs
src/include/catalog/pg_proc.rs
src/include/nodes/primnodes.rs
src/pgrust/database/commands/cluster.rs
src/pgrust/database/commands/index.rs

Tests run:
RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet range_over_left_and_right_reject_empty_ranges
TMPDIR=/tmp RUSTC_WRAPPER=/usr/bin/env CARGO_TARGET_DIR=/tmp/pgrust-target-brin-fix2 scripts/run_regression.sh --test brin --jobs 1 --timeout 300 --port 55433 --results-dir /tmp/diffs/brin-fix6
RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh check
TMPDIR=/tmp RUSTC_WRAPPER=/usr/bin/env CARGO_TARGET_DIR=/tmp/pgrust-target-brin-pr scripts/run_regression.sh --test brin --jobs 1 --timeout 300 --port 55433 --results-dir /tmp/diffs/brin-pr
RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet comment_on_function_uses_pg_proc_description_rows
RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet comment_on_operator_uses_pg_operator_description_rows
RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet comment_on_aggregate_uses_pg_proc_description_rows
RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet alter_table_add_column_supports_tid_xid_and_interval
TMPDIR=/tmp RUSTC_WRAPPER=/usr/bin/env CARGO_TARGET_DIR=/tmp/pgrust-target-brin-ci scripts/run_regression.sh --test brin --jobs 1 --timeout 300 --port 55433 --results-dir /tmp/diffs/brin-ci-fix2

Remaining:
brin.sql passes locally: 125/125 queries matched. cargo check passes with existing unreachable-pattern warnings.
