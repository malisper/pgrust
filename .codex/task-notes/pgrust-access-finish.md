Goal:
Finish the `pgrust_access` split by moving access runtime behind explicit service boundaries while keeping root as orchestration/shim code.

Key decisions:
- Start with scalar-dependent AM support before heap/transam moves.
- Added `AccessError`, `AccessResult`, and service traits in `pgrust_access`.
- Root adapter is `RootAccessServices`; it maps scalar comparisons/range/multirange/network helpers, GIN JSONB extraction, and geometry helpers back to existing executor code.
- Kept `pgrust_access` independent of root, parser/analyze/optimizer, executor, PL/pgSQL, and `pgrust_expr`.
- GIN JSONB and GiST support modules now live in `pgrust_access` behind scalar service hooks.

Files touched:
- `crates/pgrust_access/src/error.rs`
- `crates/pgrust_access/src/services.rs`
- `crates/pgrust_access/src/nbtree/{mod.rs,nbtcompare.rs,nbtpreprocesskeys.rs}`
- `crates/pgrust_access/src/brin/{mod.rs,minmax.rs,tuple.rs}`
- `crates/pgrust_access/src/gin/{mod.rs,jsonb_ops.rs}`
- `crates/pgrust_access/src/gist/{mod.rs,support/*,tuple.rs}`
- `crates/pgrust_access/src/spgist/{mod.rs,support.rs,quad_box.rs,tuple.rs}`
- `src/backend/access/services.rs`
- Root compatibility shims under `src/backend/access/{nbtree,brin,gin,gist}/...`

Tests run:
- `cargo fmt --all`
- `scripts/cargo_isolated.sh check --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_access --quiet`
- `scripts/cargo_isolated.sh test -p pgrust_storage --quiet`
- `scripts/cargo_isolated.sh check --features lz4 --message-format short`
- `scripts/cargo_isolated.sh test --lib --quiet btree`
- `scripts/cargo_isolated.sh test --lib --quiet brin`
- `scripts/cargo_isolated.sh test --lib --quiet gist`
- `scripts/cargo_isolated.sh test --lib --quiet spgist`
- `scripts/cargo_isolated.sh test --lib --quiet jsonb_ops_extracts_object_keys_and_array_strings_as_keys`
- `scripts/cargo_isolated.sh test --lib --quiet jsonb_ops_empty_container_emits_empty_item`
- Boundary checks for `crates/pgrust_access/src` root imports and `crates/pgrust_storage/src` access imports.

Remaining:
- Move remaining btree tuple/key payload helpers.
- Wire `AccessIndexServices`/`AccessToastServices` into runtime index build paths.
- Move index runtime only after expression/partial index projection is represented by `AccessIndexServices`.
- Move lock/transam/WAL/checkpoint and heap/table runtime in separate slices; those need storage/runtime traits and careful recovery byte-preservation checks.
