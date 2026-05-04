Goal:
Extract raw SQL parser lowering from root `pgrust` into a portable `pgrust_parser` crate.

Key decisions:
- Kept `pgrust_sql_grammar` as the Pest grammar-only crate.
- Moved `gram.rs` and `comments.rs` into `crates/pgrust_parser`.
- Kept root `src/backend/parser::{gram,comments}` as compatibility shims.
- Moved reusable stack-depth tracking into `pgrust_core::stack_depth`; root keeps error adapters.
- Added parser-local notices and root replay shims after parse calls.

Files touched:
- `Cargo.toml`, `Cargo.lock`
- `crates/pgrust_parser/*`
- `crates/pgrust_core/src/stack_depth.rs`
- `src/backend/parser/{gram.rs,comments.rs}`
- `src/backend/utils/misc/stack_depth.rs`

Tests run:
- `cargo fmt --all -- --check`
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test -p pgrust_core`
- `scripts/cargo_isolated.sh test -p pgrust_parser`
- `scripts/cargo_isolated.sh test --lib --quiet parser`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `rg "crate::backend::|crate::include::|crate::pgrust::" crates/pgrust_parser/src`

Remaining:
- No known parser split blockers.
- Existing unreachable-pattern warnings remain outside this migration.
