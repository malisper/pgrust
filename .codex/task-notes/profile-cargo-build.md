Goal:
Profile a fresh `cargo build` for pgrust using Cargo's build timing report.

Key decisions:
Used stable `cargo build --timings` with a fresh target dir under `/tmp`:
`/tmp/pgrust-cargo-build-profile-20260427083038`.
This follows the repo rule to keep build artifacts out of the workspace.

Files touched:
`Cargo.toml`
`scripts/check_syscalls.sh`
`scripts/run_bench.py`
`.codex/task-notes/profile-cargo-build.md`

Tests run:
`/usr/bin/time -lp cargo build --timings --target-dir /tmp/pgrust-cargo-build-profile-20260427083038`
`cargo clean -p pgrust --target-dir /tmp/pgrust-cargo-build-profile-20260427083038`
`RUSTC_BOOTSTRAP=1 /usr/bin/time -lp cargo rustc --lib --target-dir /tmp/pgrust-cargo-build-profile-20260427083038 -- -Z time-passes -Z time-passes-format=json`
`/usr/bin/time -lp cargo build --lib --timings --target-dir /tmp/pgrust-cargo-build-lib-profile-20260427083720`
`scripts/cargo_isolated.sh check --bin pgrust_server`
`cargo build --bin bench_insert`
`/usr/bin/time -lp cargo build --timings --target-dir /tmp/pgrust-cargo-autobins-profile-20260427084144`
`cargo build --features tools --bin bench_insert --target-dir /tmp/pgrust-tools-bin-check`
`/usr/bin/time -lp cargo build --timings --target-dir /tmp/pgrust-cargo-debug-lines-profile-20260427084600`
`scripts/cargo_isolated.sh check --bin pgrust_server`
`/usr/bin/time -lp cargo build --timings --target-dir /tmp/pgrust-cargo-cgu256-profile-20260427084913`
`scripts/cargo_isolated.sh check --bin pgrust_server`
`cargo install cargo-llvm-lines`
`/usr/bin/time -lp cargo llvm-lines --lib --target-dir /tmp/pgrust-llvm-lines-target-20260427085247`
`rustup toolchain install nightly --profile minimal`
`rustup +nightly component add rustc-codegen-cranelift`
`/usr/bin/time -lp cargo +nightly build --timings --target-dir /tmp/pgrust-nightly-llvm-profile-20260427090115`
`/usr/bin/time -lp env RUSTFLAGS="-Zcodegen-backend=cranelift" cargo +nightly build --timings --target-dir /tmp/pgrust-nightly-cranelift-profile-20260427090251`
`/usr/bin/time -lp cargo build --timings --target-dir /tmp/pgrust-default-cranelift-profile-20260427090624`
`cargo check --bin pgrust_server`
`cargo check --bin pgrust_server`
`cargo check --features tools --bin bench_insert`
`RUSTFLAGS="-Zub-checks=no" cargo test --lib backend::parser::gram::tests --quiet`
`RUSTFLAGS="-Zub-checks=no" CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm cargo test --lib backend::parser --quiet`
`/usr/bin/time -lp cargo build --timings --target-dir /tmp/pgrust-parser-split-profile-20260427093619`
`/usr/bin/time -lp env CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm cargo llvm-lines --lib --target-dir /tmp/pgrust-parser-split-llvm-lines-20260427093719`
`cargo check --bin pgrust_server`
`cargo check --features tools --bin bench_insert`
`RUSTFLAGS="-Zub-checks=no" cargo test --lib pl::plpgsql::gram --quiet`
`RUSTFLAGS="-Zub-checks=no" CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm cargo test --lib backend::parser --quiet`
`/usr/bin/time -lp cargo build --timings --target-dir /tmp/pgrust-plpgsql-split-profile-20260427094736`
`/usr/bin/time -lp env CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm cargo llvm-lines --lib --target-dir /tmp/pgrust-plpgsql-split-llvm-lines-20260427094836`

Remaining:
No code changes. Timing report:
`/tmp/pgrust-cargo-build-profile-20260427083038/cargo-timings/cargo-timing-20260427T153038.5575Z.html`
Detailed rustc pass log:
`/tmp/pgrust-rustc-time-passes.log`
Fresh lib-only timing report:
`/tmp/pgrust-cargo-build-lib-profile-20260427083720/cargo-timings/cargo-timing-20260427T153720.971788Z.html`
Fresh default build after `autobins = false`:
`/tmp/pgrust-cargo-autobins-profile-20260427084144/cargo-timings/cargo-timing-20260427T154144.726902Z.html`
Fresh default build after `debug = "line-tables-only"`:
`/tmp/pgrust-cargo-debug-lines-profile-20260427084600/cargo-timings/cargo-timing-20260427T154601.013752Z.html`
Fresh default build with `codegen-units = 256`:
`/tmp/pgrust-cargo-cgu256-profile-20260427084913/cargo-timings/cargo-timing-20260427T154914.045672Z.html`
LLVM lines report:
`/tmp/pgrust-llvm-lines-lib.log`
Nightly LLVM timing report:
`/tmp/pgrust-nightly-llvm-profile-20260427090115/cargo-timings/cargo-timing-20260427T160115829Z-d509a240a2493975.html`
Nightly Cranelift timing report:
`/tmp/pgrust-nightly-cranelift-profile-20260427090251/cargo-timings/cargo-timing-20260427T160251029Z-d509a240a2493975.html`
Default dev Cranelift timing report:
`/tmp/pgrust-default-cranelift-profile-20260427090624/cargo-timings/cargo-timing-20260427T160624658Z-d509a240a2493975.html`
Parser split timing report:
`/tmp/pgrust-parser-split-profile-20260427093619/cargo-timings/cargo-timing-20260427T163619155Z-d509a240a2493975.html`
Parser split llvm-lines report:
`/tmp/pgrust-parser-split-llvm-lines.log`
PL/pgSQL split timing report:
`/tmp/pgrust-plpgsql-split-profile-20260427094736/cargo-timings/cargo-timing-20260427T164736619Z-d509a240a2493975.html`
PL/pgSQL split llvm-lines report:
`/tmp/pgrust-plpgsql-split-llvm-lines.log`

Top pgrust lib rustc passes:
- total: 45.93s
- codegen_crate: 15.60s
- codegen_to_LLVM_IR: 15.51s
- LLVM_passes: 14.59s
- MIR_borrow_checking: 9.64s
- generate_crate_metadata: 7.46s
- type_check_crate: 6.90s
- monomorphization_collector_graph_walk: 5.85s
- macro_expand_crate: 1.41s
- link: 1.38s

Fresh default build vs lib-only:
- `cargo build`: 60.11s real, pgrust lib 45.7s, plus 25 auto-discovered bins.
- `cargo build --lib`: 52.43s real, pgrust lib 48.9s.
- after disabling autobins: `cargo build`: 53.39s real, pgrust lib 47.0s, pgrust_server 2.5s.
- after dev line-table debuginfo: `cargo build`: 45.71s real, pgrust lib 39.6s, codegen 11.0s.
- with `codegen-units = 256`: `cargo build`: 46.23s real, pgrust lib 39.7s, frontend 27.3s, codegen 12.4s. Reverted because it was slower.
- `cargo llvm-lines --lib`: 7,423,240 LLVM IR lines, 179,342 copies. Top overall: `pest::parser_state::ParserState<R>::rule` at 830,453 lines / 1,359 copies. Top pgrust item: `pgrust::include::catalog::pg_proc::build_bootstrap_pg_proc_rows` at 120,025 lines / 1 copy.
- Aggregate namespace split from llvm-lines log: `pgrust::` 2,354,741 lines, `pest::` 1,543,785 lines, std/core/alloc 3,375,042 lines.
- Nightly LLVM: `cargo +nightly build`: 42.81s real, pgrust lib 37.2s, frontend 27.1s, codegen 10.1s, max RSS 4.51GB.
- Nightly Cranelift: `RUSTFLAGS="-Zcodegen-backend=cranelift" cargo +nightly build`: 46.00s real, pgrust lib 39.6s, frontend 28.5s, codegen 11.1s, max RSS 3.73GB. Slower wall-clock than nightly LLVM, but lower CPU and memory.
- Default dev Cranelift config: switched `rust-toolchain.toml` to nightly with `rustc-codegen-cranelift`, enabled `[unstable] codegen-backend = true` and `[profile.dev] codegen-backend = "cranelift"` in `.cargo/config.toml`. Plain `cargo build`: 42.66s real, 44.00s user CPU, max RSS 3.73GB, pgrust lib 36.7s, frontend 28.3s, codegen 8.5s.
- After SQL grammar crate split: plain `cargo build`: 35.43s real, 43.20s user CPU, max RSS 3.25GB. `pgrust` lib 29.9s (frontend 23.8s, codegen 6.1s); `pgrust_sql_grammar` 4.6s.
- After SQL grammar crate split: `cargo llvm-lines --lib` main report dropped from 7,423,240 to 5,875,200 LLVM IR lines. `pest::*` dropped from 1,543,785 to 212,352 lines.
- Full parser-filtered tests passed with LLVM backend override and UB runtime checks disabled: 737 passed.
- After PL/pgSQL grammar crate split: plain `cargo build`: 36.15s real, 44.89s user CPU, max RSS 3.25GB. `pgrust` lib 30.6s, `pgrust_sql_grammar` 4.9s, `pgrust_plpgsql_grammar` 0.6s.
- After PL/pgSQL grammar crate split: `cargo llvm-lines --lib` main report dropped from 5,875,200 to 5,707,746 LLVM IR lines. `pest::*` dropped from 212,352 to 47,293 lines.
