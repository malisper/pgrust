Goal:
Extract semantic parse analysis into `crates/pgrust_analyze` while preserving the root `crate::backend::parser::*` compatibility surface.

Key decisions:
- Kept raw parsing in `pgrust_parser` and planning/rewrite/executor behavior in root.
- Added `AnalyzeServices` so portable analyzer code can call root-only casts, constant folding, planning, rewrite, RLS, view, trigger, timestamp, and notice behavior through root shims.
- Kept system-view row builders in root and exposed only analyzer-facing metadata through `pgrust_analyze`.
- Preserved old imports with `src/backend/parser/analyze/mod.rs` and `src/backend/utils/cache/system_view_registry.rs` shim modules.

Files touched:
- Added `crates/pgrust_analyze` and moved analyzer modules from `src/backend/parser/analyze`.
- Updated root parser, rewrite, relcache/lsyscache/visible catalog, record, catalog-data, core, and nodes compatibility code.
- Added portable record descriptor support in `crates/pgrust_nodes/src/record.rs`.

Tests run:
- `cargo fmt --all -- --check`
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test -p pgrust_analyze`
- `scripts/cargo_isolated.sh test --lib --quiet parser`
- `scripts/cargo_isolated.sh test --lib --quiet optimizer`
- `scripts/cargo_isolated.sh test --lib --quiet catalog`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `rg "crate::backend::|crate::include::|crate::pgrust::|crate::pl::" crates/pgrust_analyze/src`

Remaining:
- Existing warnings remain, mostly dead-code/private-interface fallout from moving analyzer internals into a standalone crate.
- Broader cleanup could split root compatibility shims further once downstream imports move to `pgrust_analyze` directly.
