Goal:
Implement PostgreSQL-compatible TABLESAMPLE for built-in BERNOULLI and SYSTEM, matching the upstream tablesample regression file.

Key decisions:
TABLESAMPLE now stays on relation RTEs and SeqScan plans instead of lowering to a security qual.
Built-in methods are bound/coerced in analysis, sampled in SeqScan before normal quals, and rendered in EXPLAIN/view deparse.
Sampled relations use scan paths only; inheritance/partition child scans preserve the clause.
Heap inserts honor relation fillfactor so regression page layout matches PostgreSQL expectations.

Files touched:
Grammar/parser/analyzer, planner path/cost/setrefs/constfold, SeqScan executor/runtime state, heap insert/scan helpers, EXPLAIN/view deparse, view DML updatability, error positioning, and tests.

Tests run:
cargo fmt
env -u CARGO_TARGET_DIR TMPDIR='/Volumes/OSCOO PSSD/tmp' TEMP='/Volumes/OSCOO PSSD/tmp' TMP='/Volumes/OSCOO PSSD/tmp' SCCACHE_DIR='/Volumes/OSCOO PSSD/sccache' PGRUST_TARGET_POOL_DIR='/Volumes/OSCOO PSSD/pgrust-target-pool/yokohama-v3' scripts/cargo_isolated.sh check
env -u CARGO_TARGET_DIR TMPDIR='/Volumes/OSCOO PSSD/tmp' TEMP='/Volumes/OSCOO PSSD/tmp' TMP='/Volumes/OSCOO PSSD/tmp' SCCACHE_DIR='/Volumes/OSCOO PSSD/sccache' PGRUST_TARGET_POOL_DIR='/Volumes/OSCOO PSSD/pgrust-target-pool/yokohama-v3' scripts/cargo_isolated.sh test --lib --quiet tablesample
env -u CARGO_TARGET_DIR TMPDIR='/Volumes/OSCOO PSSD/tmp' TEMP='/Volumes/OSCOO PSSD/tmp' TMP='/Volumes/OSCOO PSSD/tmp' SCCACHE_DIR='/Volumes/OSCOO PSSD/sccache' PGRUST_TARGET_POOL_DIR='/Volumes/OSCOO PSSD/pgrust-target-pool/yokohama-v3' scripts/run_regression.sh --test tablesample --timeout 60 --jobs 1 --port 65400

Remaining:
None for the requested tablesample acceptance path. Existing unrelated unreachable-pattern warnings remain.
