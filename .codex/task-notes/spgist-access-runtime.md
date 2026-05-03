Goal:
Move SP-GiST index runtime from root pgrust into pgrust_access while preserving old root AM callback paths and leaving redo in root.

Key decisions:
- Moved SP-GiST build, insert, page, scan, state, and vacuum runtime under pgrust_access::spgist.
- Kept root spgist_am_handler and root build/insert/scan/vacuum shims as compatibility adapters.
- Routed scalar tuple encoding, opclass support, and WAL logging through existing access service traits.
- Left BRIN paused and generic index dispatch for a later slice.

Files touched:
- crates/pgrust_access/src/spgist/*
- src/backend/access/spgist/*

Tests run:
- cargo fmt --all
- cargo fmt --all -- --check
- scripts/cargo_isolated.sh check -p pgrust_access --message-format short
- scripts/cargo_isolated.sh check --message-format short
- scripts/cargo_isolated.sh check --features lz4 --message-format short
- scripts/cargo_isolated.sh test -p pgrust_access --quiet
- scripts/cargo_isolated.sh test -p pgrust_storage --quiet
- scripts/cargo_isolated.sh test --lib --quiet spgist
- scripts/cargo_isolated.sh test --lib --quiet index
- scripts/cargo_isolated.sh test --lib --quiet vacuum
- scripts/cargo_isolated.sh test --lib --quiet simple_select_uses_keyed_catalog_without_broad_catcache
- scripts/cargo_isolated.sh test --lib --quiet catalog -- --test-threads=1
- rg "crate::backend::|crate::include::|crate::pgrust::|crate::pl::" crates/pgrust_access/src
- rg "pgrust_access" crates/pgrust_storage/src

Remaining:
- Replace process-global broad-catalog test counters with database-scoped counters so catalog can run in parallel again.
- Move BRIN when unpaused, then generic index dispatch, heap/table/TOAST, and transam/WAL/checkpoint.
