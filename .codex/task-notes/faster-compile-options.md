Goal:
Assess and start options for making pgrust compile faster.

Key decisions:
Measurements point at root-crate frontend/test compilation, not linker or one
third-party dependency, as the primary bottleneck. Keep Cranelift disabled for
now because .cargo/config.toml documents current correctness bugs.

Implemented first migration slices:
- Added a workspace, added pgrust_core, moved CompactString there, and kept
  src/pgrust/compact_string.rs as a compatibility re-export.
- Moved shared primitive node blockers into pgrust_core: RelFileLocator,
  OffsetNumber, ItemPointerData, AttributeAlign, AttributeStorage,
  AttributeCompression, AttributeDesc, PolicyCommand, and
  RangeCanonicalization. Kept the old storage/access/catalog paths as
  compatibility re-exports.
- Added pgrust_nodes and moved portable non-runtime node modules there:
  datetime, tsearch, datum, parsenodes, primnodes, plannodes, and pathnodes.
  Old src/include/nodes paths are now compatibility re-export shims.
- Moved node-support data into pgrust_nodes: CommandType, partition data
  structs, scan key/direction/index option data, and IndexRelCacheEntry shape.
  Root keeps executor/runtime, parser lowering, relcache builders, and access
  method behavior through free functions or extension traits.
- Added small node-local builtin proc/type lookup helpers in pgrust_nodes and
  moved PgInheritsRow plus stable catalog constants into pgrust_core.

Files touched:
Cargo.toml, Cargo.lock, crates/pgrust_core/*, src/pgrust/compact_string.rs.
Also added cache-workspace-crates: true to merge-queue and regression workflow
cache steps. Latest slice touched smgr/bufpage, access itemptr/tupdesc, catalog
policy/range re-export shims, node imports, crates/pgrust_nodes/*, optimizer
Path/PlannerInfo helper ownership, parser partition re-exports, and relcache
index helper call sites. Branch was renamed to malisper/faster-compile.

Tests run:
scripts/cargo_isolated.sh check --timings
scripts/cargo_isolated.sh test --lib --no-run --timings
CARGO_PROFILE_TEST_DEBUG=0 scripts/cargo_isolated.sh test --lib --no-run --timings
cargo fmt --all -- --check
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet compact_string
cargo fmt --all -- --check
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test -p pgrust_core
scripts/cargo_isolated.sh test --lib --quiet parser
cargo fmt --all -- --check
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test -p pgrust_nodes
scripts/cargo_isolated.sh test --lib --quiet parser
scripts/cargo_isolated.sh test --lib --quiet optimizer
rg "crate::backend::|crate::include::|crate::pgrust::|crate::RelFileLocator" crates/pgrust_nodes/src

Remaining:
Choose between test-target splitting, generated catalog crate extraction,
parser/nodes crate extraction, and cache/workflow cleanup. For pgrust_nodes,
the portable node crate now exists and compiles independently. Remaining crate
split work is mostly higher-level: decide whether parser analysis/planner
helpers, generated catalog data, or executor runtime state should get their own
crates next.
