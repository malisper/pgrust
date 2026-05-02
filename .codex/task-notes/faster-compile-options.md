Goal:
Assess and start options for making pgrust compile faster.

Key decisions:
Measurements point at root-crate frontend/test compilation, not linker or one
third-party dependency, as the primary bottleneck. Keep Cranelift disabled for
now because .cargo/config.toml documents current correctness bugs.

Implemented first migration slice: added a workspace, added pgrust_core, moved
CompactString there, and kept src/pgrust/compact_string.rs as a compatibility
re-export. Stopped before pgrust_nodes because datum/parsenodes still reference
catalog/access/parser/runtime types that need deliberate API cleanup first.

Files touched:
Cargo.toml, Cargo.lock, crates/pgrust_core/*, src/pgrust/compact_string.rs.
Also added cache-workspace-crates: true to merge-queue and regression workflow
cache steps. Branch was renamed to malisper/faster-compile.

Tests run:
scripts/cargo_isolated.sh check --timings
scripts/cargo_isolated.sh test --lib --no-run --timings
CARGO_PROFILE_TEST_DEBUG=0 scripts/cargo_isolated.sh test --lib --no-run --timings
cargo fmt --all -- --check
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet compact_string

Remaining:
Choose between test-target splitting, generated catalog crate extraction,
parser/nodes crate extraction, and cache/workflow cleanup. For pgrust_nodes,
first move or abstract ItemPointerData, RangeCanonicalization, PolicyCommand,
RelFileLocator, AttributeStorage/Compression, and parser partition structs so
node files do not depend back on root runtime modules.
