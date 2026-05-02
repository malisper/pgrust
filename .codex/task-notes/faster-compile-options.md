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
- Added pgrust_catalog_ids for builtin function identity enums plus scalar,
  window, aggregate, hypothetical aggregate, and ordered-set aggregate proc-OID
  lookup helpers. pgrust_nodes now re-exports those enums from primnodes and no
  longer owns the big builtin match table.
- Added pgrust_catalog_data for generated catalog rows/constants/index
  descriptors/range specs. Root src/include/catalog modules are compatibility
  shims that re-export pgrust_catalog_data modules.
- Kept root runtime catalog/cache/storage behavior in root. Pure descriptor
  helpers needed to construct generated catalog rows moved with the generated
  data so pgrust_catalog_data has no root dependency.

Files touched:
Cargo.toml, Cargo.lock, crates/pgrust_core/*, src/pgrust/compact_string.rs.
Also added cache-workspace-crates: true to merge-queue and regression workflow
cache steps. Latest slice touched smgr/bufpage, access itemptr/tupdesc, catalog
policy/range re-export shims, node imports, crates/pgrust_nodes/*, optimizer
Path/PlannerInfo helper ownership, parser partition re-exports, and relcache
index helper call sites. Catalog split touched crates/pgrust_catalog_ids/*,
crates/pgrust_catalog_data/*, crates/pgrust_nodes builtins/primnodes/Cargo.toml,
Cargo.toml/Cargo.lock, and src/include/catalog compatibility shims. Branch was
renamed to malisper/faster-compile.

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
cargo fmt --all -- --check
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test -p pgrust_catalog_ids
scripts/cargo_isolated.sh test -p pgrust_catalog_data
scripts/cargo_isolated.sh test -p pgrust_nodes
scripts/cargo_isolated.sh test --lib --quiet catalog
scripts/cargo_isolated.sh test --lib --quiet parser
scripts/cargo_isolated.sh test --lib --quiet optimizer
rg "crate::backend::|crate::include::|crate::pgrust::" crates/pgrust_catalog_ids/src crates/pgrust_catalog_data/src
rg "pub enum (BuiltinScalarFunction|BuiltinWindowFunction|AggFunc|HypotheticalAggFunc|OrderedSetAggFunc|HashFunctionKind)" crates/pgrust_nodes/src/primnodes.rs
rg "BuiltinScalarFunction::" crates/pgrust_nodes/src/builtins.rs

Remaining:
Generated catalog data and builtin catalog IDs are split out. Remaining crate
split work is mostly higher-level: parser/analyzer/planner logic can move into
smaller crates next, while executor runtime state should stay root-owned until a
separate executor-runtime crate is worth the extra boundary.
