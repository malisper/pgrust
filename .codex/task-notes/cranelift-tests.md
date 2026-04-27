Goal:
Investigate why the focused database test aborts with the default Cranelift dev backend.

Key decisions:
Removed the external crc32c crate because it emits llvm.aarch64.crc32cx, which rustc_codegen_cranelift does not support on aarch64 macOS.
Kept dev builds on Cranelift, but pinned the Cargo test profile to LLVM because the focused database test still aborts later in std Vec::IntoIter under Cranelift.

Files touched:
.cargo/config.toml
Cargo.toml
Cargo.lock
src/backend/utils/crc32c.rs
src/backend/utils/mod.rs
src/backend/access/transam/controlfile.rs
src/backend/access/transam/xlog.rs
src/backend/executor/expr_string.rs
src/backend/executor/tests.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet pgbench_style_accounts_workload_completes
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet pgbench_style_accounts_workload_completes
scripts/cargo_isolated.sh test --lib --quiet backend::utils::crc32c::tests::matches_standard_vectors
scripts/cargo_isolated.sh check --lib

Remaining:
Forced-Cranelift test runs reduce to `include::catalog::pg_amop::tests::spgist_box_ordering_row_matches_postgres_shape`, which calls `bootstrap_pg_amop_rows()` and then `bootstrap_pg_operator_rows()`/generated catalog data materialization. `rg` finds no local unsafe in `src/include/catalog/pg_amop.rs` or `src/include/catalog/pg_operator.rs`; LLVM passes the reduced test; Miri passes the reduced test. Rewriting the nearby `pg_amop`/`pg_operator` vector paths changed the failure from checked UB aborts to SIGSEGV, so the remaining issue looks like a Cranelift codegen/runtime bug rather than a narrow pgrust semantic bug. Keep tests on LLVM unless Cranelift itself is fixed or catalog bootstrap generation is redesigned.
