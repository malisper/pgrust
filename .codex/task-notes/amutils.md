Goal:
Run `amutils`, diagnose the regression errors, and fix the blockers.

Key decisions:
- The original run was a regression mismatch, not a harness error.
- `fooindex` needed PostgreSQL's implicit `DESC => NULLS FIRST` default.
- `brinidx` failed during dependency setup because pgrust lacked several BRIN
  default opclass/catalog rows used by PostgreSQL's `brin.sql`.
- BRIN runtime currently supports minmax summaries only, so unsupported
  catalog-visible BRIN opclasses are left all-null during build/insert.

Files touched:
- `src/backend/access/brin/brin.rs`
- `src/backend/commands/tablecmds.rs`
- `src/backend/parser/tests.rs`
- `src/backend/utils/cache/lsyscache.rs`
- `src/include/catalog/pg_amop.rs`
- `src/include/catalog/pg_amproc.rs`
- `src/include/catalog/pg_opclass.rs`
- `src/include/catalog/pg_operator.rs`
- `src/include/catalog/pg_opfamily.rs`
- `src/pgrust/database/commands/index.rs`

Tests run:
- `cargo fmt`
- `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust-target-amutils-khartoum' cargo test --lib --quiet validate_brin_bootstrap_rows`
- `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust-target-amutils-khartoum' cargo test --lib --quiet parse_create_index_desc_leaves_nulls_default_unspecified`
- `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust-target-amutils-khartoum' scripts/run_regression.sh --test amutils --results-dir /tmp/pgrust-amutils-results11 --timeout 120 --jobs 1 --port 63137`
- `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust-target-amutils-khartoum' cargo check --quiet`

Remaining:
- `amutils` now passes: 10/10 queries matched.
- The dependency `brin.sql` still reports the pre-existing strict CIDR input
  error for `10.2.37.96/24`, but that setup output is not compared by
  `amutils` and no longer prevents `brinidx` from being created.
