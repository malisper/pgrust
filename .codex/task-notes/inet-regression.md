Goal:
Fix inet.sql regression failures from pasted diff.

Key decisions:
Failures are in GiST/SP-GiST inet index scans after enable_seqscan=off.
The catalog advertises PostgreSQL network strategy numbers: &&=3, <<=24,
<<==25, >>=26, >>=27, equality/comparisons=18..23.
pgrust GiST/SP-GiST network support currently interprets strategies as a compact
local 1..6 set, so && strategy 3 is treated as strict supernet and returns no
rows, while 24..27 hit unsupported strategy errors.
PostgreSQL network_gist.c and network_spgist.c use RT*StrategyNumber values
directly, matching pg_amop.dat.
Fixed by teaching GiST and SP-GiST network leaf checks to use the catalog
strategy numbers directly and by making GiST internal network checks broad
recheck matches to avoid false negatives for ordering strategies.

Files touched:
.codex/task-notes/inet-regression.md
src/backend/access/gist/support/network_ops.rs
src/backend/access/spgist/support.rs

Tests run:
CARGO_TARGET_DIR=/tmp/pgrust-missoula-inet-target cargo check --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/pgrust-missoula-inet-target cargo test --lib --quiet uses_catalog_strategy_numbers
Attempted targeted inet regression:
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/pgrust-missoula-inet-target scripts/run_regression.sh --test inet --timeout 120 --results-dir /tmp/pgrust_regress_inet_fix
The server build succeeded, but the harness failed during shared setup bootstrap
before running inet.sql.

Remaining:
Investigate the regression harness setup bootstrap failure if full inet.sql
validation is required.
