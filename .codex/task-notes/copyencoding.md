Goal:
Fix COPY ENCODING handling for the copyencoding regression.
Key decisions:
Store ENCODING on the session-layer CopyOptions used by parse_copy_command, then apply it before file COPY FROM parsing and after COPY TO rendering.
Files touched:
src/pgrust/session.rs
Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-copyencoding-target cargo check
CARGO_TARGET_DIR=/tmp/pgrust-copyencoding-target cargo test --lib --quiet parse_copy
CARGO_TARGET_DIR=/tmp/pgrust-copyencoding-regress-target scripts/run_regression.sh --test copyencoding --jobs 1 --timeout 60
Remaining:
None.
