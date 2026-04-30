Goal:
Investigate local sccache failures reported across sessions.

Key decisions:
The failure reproduced as `sccache: error: Operation not supported (os error 45)` while starting the sccache server. Direct rustc worked. The local `TMPDIR` points at `/Volumes/OSCOO PSSD/pgrust/tmp/`, an ExFAT external volume. Setting `TMPDIR=/tmp` lets sccache start.

Files touched:
scripts/rustc_sccache_wrapper.sh
scripts/cargo_isolated.sh

Tests run:
scripts/cargo_isolated.sh check -p pgrust_sql_grammar --quiet
cargo check -p pgrust_sql_grammar --quiet

Remaining:
Global `CARGO_TARGET_DIR=/Volumes/OSCOO PSSD/rust/cargo-target` still puts Cargo incremental cache on ExFAT, producing hard-link fallback warnings and slower builds.
