Goal:
Move btree index runtime from root pgrust into pgrust_access while preserving old root module paths and leaving nbtxlog redo in root.

Key decisions:
- Kept root btree_am_handler and build/index projection shims in root; moved runtime callbacks, page/search/insert/sort/vacuum helpers, and payload codecs under pgrust_access::nbtree.
- Routed uniqueness, WAL logging, scalar formatting/comparison, interrupt checks, and transaction state through existing access service traits.
- Added AccessError variants for IO and unique-violation compatibility and preserved root error mapping.

Files touched:
- crates/pgrust_access/src/nbtree/*
- src/backend/access/nbtree/*
- crates/pgrust_access/src/{error,services}.rs
- src/backend/access/{services,index/buildkeys}.rs
- small service/error compatibility updates in AM shim modules.

Tests run:
- cargo fmt --all
- cargo fmt --all -- --check
- scripts/cargo_isolated.sh check -p pgrust_access --message-format short
- scripts/cargo_isolated.sh check --message-format short
- scripts/cargo_isolated.sh check --features lz4 --message-format short
- scripts/cargo_isolated.sh test -p pgrust_access --quiet
- scripts/cargo_isolated.sh test -p pgrust_storage --quiet
- scripts/cargo_isolated.sh test --lib --quiet btree
- scripts/cargo_isolated.sh test --lib --quiet index
- scripts/cargo_isolated.sh test --lib --quiet unique
- scripts/cargo_isolated.sh test --lib --quiet vacuum
- scripts/cargo_isolated.sh test --lib --quiet catalog
- rg "crate::backend::|crate::include::|crate::pgrust::|crate::pl::" crates/pgrust_access/src
- rg "pgrust_access" crates/pgrust_storage/src

Remaining:
- Move remaining AM runtime slices (SP-GiST, then BRIN when unpaused), generic index dispatch, heap/table/TOAST runtime, and transam/WAL/checkpoint.
- Keep nbtxlog redo in root until the transam/xlog types move.
