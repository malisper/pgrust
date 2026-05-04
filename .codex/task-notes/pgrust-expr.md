Goal:
Extract portable scalar SQL expression semantics into `crates/pgrust_expr` while keeping root runtime execution and old root paths working.

Key decisions:
- Added `pgrust_expr` as a non-root workspace crate for scalar casts, operators, value I/O, JSON/JSONPath/XML, regex, tsearch helpers, date/time helpers, CRC/hash support, and scalar config/error/service types.
- Kept plan-node execution, tuple scans, storage, TOAST, session orchestration, and table-producing runtime in root.
- Used root compatibility shims for low-risk shared support paths: time helpers, datetime/XML GUC config, CRC32C, scalar hash support, `ByteaOutputFormat`, `FloatFormatOptions`, and `ExprError` to `ExecError`.
- Did not replace all root executor scalar modules with direct re-exports yet; that needs wrapper signatures around root `ExecError`, root `CatalogLookup`, and executor context.
- Fixed the root `CatalogLookup for Catalog` shim to expose seeded auth/database/tablespace rows so `pg_get_userbyid` sees the bootstrap `postgres` role.

Files touched:
- `crates/pgrust_expr/**`
- `Cargo.toml`, `Cargo.lock`
- `crates/pgrust_analyze/Cargo.toml`, `crates/pgrust_analyze/src/lib.rs`
- `crates/pgrust_optimizer/Cargo.toml`
- Root shims under `src/backend/utils`, `src/backend/access/hash`, `src/backend/libpq`, `src/pgrust/session.rs`, and `src/backend/executor/mod.rs`
- `src/backend/parser/analyze/mod.rs`

Tests run:
- `cargo fmt --all -- --check`
- `scripts/cargo_isolated.sh check --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_expr --lib --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_analyze --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_optimizer --message-format short`
- `scripts/cargo_isolated.sh test --lib --quiet executor`
- `scripts/cargo_isolated.sh test --lib --quiet parser`
- `scripts/cargo_isolated.sh test --lib --quiet optimizer`
- `scripts/cargo_isolated.sh test --lib --quiet catalog`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `rg "crate::backend::|crate::include::|crate::pgrust::|crate::pl::" crates/pgrust_expr/src`

Remaining:
- Next slice should add root executor wrapper modules that call `pgrust_expr` with root adapters, then remove duplicate scalar implementation bodies from root executor modules.
- After that, move analyzer/optimizer scalar-service calls from root bridges to direct `pgrust_expr` services where signatures are fully portable.
