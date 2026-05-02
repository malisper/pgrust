Goal:
Fix privileges regression diffs for expression-index security-restricted operation behavior.

Key decisions:
Run index expression and predicate evaluation as the heap relation owner during CREATE INDEX, REINDEX, and CLUSTER-related rebuild/order paths. Reuse expression projection for BRIN build/summarize instead of treating expression keys as raw heap columns.

Files touched:
src/backend/access/brin/brin.rs
src/backend/catalog/indexing.rs
src/backend/commands/tablecmds.rs
src/backend/executor/exec_expr.rs
src/include/access/amapi.rs
src/pgrust/database/commands/cluster.rs
src/pgrust/database/commands/index.rs

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-priv-sro scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-priv-sro scripts/run_regression.sh --test privileges --jobs 1 --timeout 120 --port 16401 --skip-build --results-dir /tmp/pgrust-privileges-sro-results-16401

Remaining:
The expression-index SRO hunk is clean. The broader privileges regression still fails on unrelated pre-existing privilege diffs and remaining materialized-view security-restricted-operation error wording/side-effect behavior.
