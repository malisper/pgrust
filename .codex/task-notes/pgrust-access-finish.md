Goal:
Finish the `pgrust_access` split by moving access runtime behind explicit service boundaries while keeping root as orchestration/shim code.

Key decisions:
- Start with scalar-dependent AM support before heap/transam moves.
- Added `AccessError`, `AccessResult`, and service traits in `pgrust_access`.
- Root adapter is `RootAccessServices`; it maps scalar comparisons/network helpers/JSONB comparison back to existing executor code.
- Kept `pgrust_access` independent of root, parser/analyze/optimizer, executor, PL/pgSQL, and `pgrust_expr`.
- Did not move GIN JSONB support yet because it needs a broader JSONB visitor/extraction service to avoid a `pgrust_expr` dependency.

Files touched:
- `crates/pgrust_access/src/error.rs`
- `crates/pgrust_access/src/services.rs`
- `crates/pgrust_access/src/nbtree/{mod.rs,nbtcompare.rs,nbtpreprocesskeys.rs}`
- `crates/pgrust_access/src/brin/{mod.rs,minmax.rs}`
- `crates/pgrust_access/src/gist/{mod.rs,support/*}`
- `src/backend/access/services.rs`
- Root compatibility shims under `src/backend/access/{nbtree,brin,gist}/...`

Tests run:
- `cargo fmt --all`
- `scripts/cargo_isolated.sh check --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_access --quiet`
- `scripts/cargo_isolated.sh test --lib --quiet btree`
- `scripts/cargo_isolated.sh test --lib --quiet brin`
- `scripts/cargo_isolated.sh test --lib --quiet gist`
- Boundary checks for `crates/pgrust_access/src` root imports and `crates/pgrust_storage/src` access imports.

Remaining:
- Design JSONB extraction/visitor methods for `AccessScalarServices`, then move GIN `jsonb_ops`.
- Move remaining GiST/SP-GiST scalar support modules after adding range/network/geometry/multirange service hooks.
- Move index runtime only after expression/partial index projection is represented by `AccessIndexServices`.
- Move lock/transam/WAL/checkpoint and heap/table runtime in separate slices; those need storage/runtime traits and careful recovery byte-preservation checks.
