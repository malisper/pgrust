Goal:
Fix regression diffs for alter_operator, functional_deps, inet, tid, and xml.

Key decisions:
- Preserve quoted ALTER OPERATOR option names and expose pg_operator procs with PostgreSQL regproc display.
- Store functional grouping constraint dependencies from grouped output binding so view rewrite dependencies protect PK-backed GROUP BY views.
- Convert inet subnet/supernet btree quals into PostgreSQL-style lower/upper range keys and prefer plain btree index scans over bitmap/full index fallbacks for those probes.
- Implement currtid2 compatibility for heap-like relations, views, sequences, and error cases; check heap block count before fetching invalid TIDs.
- Carry XML parsing/casting config through prepared params/XMLCONCAT and deparse XMLTABLE/XMLSERIALIZE views closer to PostgreSQL.

Files touched:
- src/backend/catalog/pg_depend.rs
- src/backend/commands/explain.rs
- src/backend/executor/exec_expr.rs
- src/backend/executor/expr_casts.rs
- src/backend/executor/expr_xml.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/optimizer/path/subquery_prune.rs
- src/backend/optimizer/setrefs.rs
- src/backend/optimizer/tests.rs
- src/backend/parser/analyze/functions.rs
- src/backend/parser/analyze/mod.rs
- src/backend/parser/gram.rs
- src/backend/rewrite/views.rs
- src/include/catalog/pg_operator.rs
- src/include/catalog/pg_proc.rs
- src/include/nodes/primnodes.rs
- src/pgrust/database/commands/create.rs
- src/pgrust/database/commands/execute.rs
- src/pgrust/database/commands/operator.rs

Tests run:
- CARGO_BUILD_RUSTC_WRAPPER= SCCACHE_DISABLE=1 CARGO_TARGET_DIR=/tmp/pgrust-target-sarajevo-v3 cargo test --lib --quiet planner_renders_inet_btree_subnet_range_keys
- CARGO_BUILD_RUSTC_WRAPPER= SCCACHE_DISABLE=1 CARGO_TARGET_DIR=/tmp/pgrust-target-sarajevo-v3 scripts/run_regression.sh --test alter_operator --jobs 1 --timeout 180 --port 57693 --results-dir /tmp/diffs/requested-five-final-alter_operator3
- CARGO_BUILD_RUSTC_WRAPPER= SCCACHE_DISABLE=1 CARGO_TARGET_DIR=/tmp/pgrust-target-sarajevo-v3 scripts/run_regression.sh --test functional_deps --jobs 1 --timeout 180 --port 57693 --results-dir /tmp/diffs/requested-five-final-functional_deps3
- CARGO_BUILD_RUSTC_WRAPPER= SCCACHE_DISABLE=1 CARGO_TARGET_DIR=/tmp/pgrust-target-sarajevo-v3 scripts/run_regression.sh --test inet --jobs 1 --timeout 180 --port 57693 --results-dir /tmp/diffs/requested-five-final-inet3
- CARGO_BUILD_RUSTC_WRAPPER= SCCACHE_DISABLE=1 CARGO_TARGET_DIR=/tmp/pgrust-target-sarajevo-v3 scripts/run_regression.sh --test tid --jobs 1 --timeout 180 --port 57693 --results-dir /tmp/diffs/requested-five-final-tid2
- CARGO_BUILD_RUSTC_WRAPPER= SCCACHE_DISABLE=1 CARGO_TARGET_DIR=/tmp/pgrust-target-sarajevo-v3 scripts/run_regression.sh --test xml --jobs 1 --timeout 180 --port 57693 --results-dir /tmp/diffs/requested-five-final-xml
- CARGO_BUILD_RUSTC_WRAPPER= SCCACHE_DISABLE=1 CARGO_TARGET_DIR=/tmp/pgrust-target-sarajevo-v3 cargo check

Remaining:
None.
