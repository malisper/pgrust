Goal:
Move GiST runtime into `pgrust_access` while preserving root AM callback paths.

Key decisions:
- Root keeps heap scan/materialization and expression/partial index projection through a `GistBuildRowSource` adapter.
- GiST runtime receives `AccessScalarServices` and `AccessWalServices`; redo remains root-owned in `src/backend/access/gist/wal.rs`.
- Root `src/backend/access/gist/{build,insert,scan,vacuum}.rs` are compatibility shims.

Files touched:
- `crates/pgrust_access/src/gist/{build,build_buffers,insert,page,scan,state,support,vacuum,mod}.rs`
- `src/backend/access/gist/{build,insert,mod,scan,support/mod,vacuum}.rs`

Tests run:
- `cargo fmt --all -- --check`
- `scripts/cargo_isolated.sh check --message-format short`
- `scripts/cargo_isolated.sh check --features lz4 --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_access --quiet`
- `scripts/cargo_isolated.sh test -p pgrust_storage --quiet`
- `scripts/cargo_isolated.sh test --lib --quiet gist`
- `scripts/cargo_isolated.sh test --lib --quiet index`
- Boundary checks for root imports in `pgrust_access` and access imports in `pgrust_storage`

Remaining:
- GiST WAL redo still belongs to the future transam/WAL move.
- Other AM runtimes still to move: SP-GiST, btree, and BRIN when unpaused.
