Goal:
Extract logical optimizer/planner code from root `pgrust` into a portable `pgrust_optimizer` crate to reduce root crate compile coupling.

Key decisions:
- Added `crates/pgrust_optimizer` and moved the existing optimizer modules there.
- Kept `src/backend/optimizer/mod.rs` as a `:HACK:` compatibility shim that re-exports old root paths and installs root runtime services.
- Added `OptimizerServices` in `pgrust_optimizer::runtime` for executor casts/comparisons, rewrite hooks, explain rendering, privilege collection, date parsing, hashing, index AM metadata, and statistics value keys.
- Moved reusable statistics JSON payload structs into `pgrust_catalog_data::statistics_payload`; root statistics code now re-exports those data helpers.
- Added small portable storage/access constants to `pgrust_core`.

Files touched:
- `Cargo.toml`, `Cargo.lock`
- `crates/pgrust_optimizer/**`
- `src/backend/optimizer/mod.rs`
- `src/backend/optimizer/{bestpath,constfold,groupby_rewrite,grouping_sets,inherit,joininfo,partition_cache,partition_prune,partitionwise,path,pathnodes,plan,rewrite,root,setrefs,sublink_pullup,upperrels,util}` moved out of root
- `crates/pgrust_catalog_data/src/statistics_payload.rs`
- `crates/pgrust_catalog_data/src/lib.rs`
- `crates/pgrust_core/src/{access,storage}.rs`
- `src/backend/parser/analyze/mod.rs`
- `src/backend/statistics/types.rs`

Tests run:
- `cargo fmt --all -- --check`
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test -p pgrust_optimizer`
- `scripts/cargo_isolated.sh test --lib --quiet optimizer`
- `scripts/cargo_isolated.sh test --lib --quiet parser`
- `scripts/cargo_isolated.sh test --lib --quiet catalog`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `rg "crate::backend::|crate::include::|crate::pgrust::|crate::pl::" crates/pgrust_optimizer/src` returned no matches

Remaining:
- Benchmark clean/no-op/touch rebuild impact if the next turn asks for compile timing numbers.
- Root shim can shrink once executor/rewrite/catalog service boundaries are split further.
