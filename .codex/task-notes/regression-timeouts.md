Goal:
Investigate and fix statement timeouts reported in join.sql, memoize.sql,
subselect.sql, and tidscan.sql.

Key decisions:
Use PostgreSQL planner behavior as the reference: pull up IN sublinks before
planning derived queries, use merge/hash-joinable equality where PostgreSQL
does, and prefer index/hash shapes over repeated unique parameter probes.
Use sample on a live regression run when join.sql still timed out after the
planner fixes. The active profile showed the remaining time was not executor
join work: pgrust was eagerly formatting every pg_proc signature while preparing
row output for ordinary SELECT responses. PostgreSQL does not do that per row
description, so pgrust now builds wire catalog name/signature maps only when the
result column types can need them.

Files touched:
scripts/run_regression.sh
src/backend/optimizer/bestpath.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/path/mod.rs
src/backend/optimizer/setrefs.rs
src/backend/tcop/postgres.rs
src/pgrust/session.rs

Tests run:
scripts/cargo_isolated.sh build --bin pgrust_server
scripts/run_regression.sh --test join --timeout 240 --jobs 1 --port 55532 --results-dir /tmp/pgrust-timeout-join-samplefix --data-dir /tmp/pgrust-timeout-join-samplefix-data --skip-build
scripts/run_regression.sh --test subselect --timeout 240 --jobs 1 --port 55533 --results-dir /tmp/pgrust-timeout-subselect-samplefix --data-dir /tmp/pgrust-timeout-subselect-samplefix-data --skip-build
scripts/run_regression.sh --test tidscan --timeout 240 --jobs 1 --port 55534 --results-dir /tmp/pgrust-timeout-tidscan-samplefix --data-dir /tmp/pgrust-timeout-tidscan-samplefix-data --skip-build
scripts/run_regression.sh --test memoize --timeout 240 --jobs 1 --port 55535 --results-dir /tmp/pgrust-timeout-memoize-samplefix --data-dir /tmp/pgrust-timeout-memoize-samplefix-data --skip-build
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet typed_data_row_renders_regprocedure_with_proc_name

Remaining:
Focused regression files still have output mismatches unrelated to statement
timeouts. subselect.sql reaches the existing executor panic at
src/backend/executor/nodes.rs:9332 after the previously timed-out IN queries
complete.
