Goal:
Move remaining BRIN index runtime into pgrust_access and remove the root BRIN fallback from generic index dispatch.

Key decisions:
- Added pgrust_access::brin::runtime and wired BRIN into pgrust_access::index::indexam.
- Kept root BRIN file as a :HACK: compatibility shim that adapts root services.
- BRIN build/vacuum uses AccessHeapServices/AccessIndexServices/AccessScalarServices/AccessWalServices.

Files touched:
- crates/pgrust_access/src/brin/runtime.rs
- crates/pgrust_access/src/brin/mod.rs
- crates/pgrust_access/src/index/indexam.rs
- src/backend/access/brin/brin.rs
- src/backend/access/index/indexam.rs

Tests run:
- cargo fmt --all -- --check
- scripts/cargo_isolated.sh check -p pgrust_access --message-format short
- scripts/cargo_isolated.sh check --message-format short
- scripts/cargo_isolated.sh check --features lz4 --message-format short
- scripts/cargo_isolated.sh test -p pgrust_access --quiet
- scripts/cargo_isolated.sh test -p pgrust_storage --quiet
- scripts/cargo_isolated.sh test --lib --quiet brin
- scripts/cargo_isolated.sh test --lib --quiet vacuum
- scripts/cargo_isolated.sh test --lib --quiet catalog
- scripts/cargo_isolated.sh test --lib --quiet concurrent_indexed_updates_and_deletes_keep_index_results_correct

Remaining:
- Broad index filter timed out once in concurrent_indexed_updates_and_deletes_keep_index_results_correct; exact rerun passed.
- Continue with broader root cleanup or the next crate extraction slice.
