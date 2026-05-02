Goal:
Fix regression diffs in brin_multi related to BRIN minmax-multi operator classes.

Key decisions:
Registered PostgreSQL-compatible BRIN minmax-multi opfamilies/opclasses/amop/amproc rows for the supported scalar types.
Validated minmax-multi column opclass option values_per_range with PostgreSQL-style 8..=256 bounds in both CREATE INDEX paths.
Fixed BRIN tuple allocation to size from the aligned data offset, preventing many-column BRIN tuples with null bitmaps from panicking.
Reinitialized rewritten indexes after database-level TRUNCATE and made BRIN build-empty install its metapage through the buffer pool.
Added pgrust-native minmax-multi interval summary behavior on top of BRIN's existing tuple storage so sparse interval ranges can be eliminated.
Aligned Bitmap Index Scan EXPLAIN output with PostgreSQL for Index Searches and datetime/interval scan-key rendering under the regression DateStyle/IntervalStyle.

Files touched:
src/include/catalog/pg_opfamily.rs
src/include/catalog/pg_opclass.rs
src/include/catalog/pg_amop.rs
src/include/catalog/pg_amproc.rs
src/backend/commands/tablecmds.rs
src/pgrust/database/commands/index.rs
src/backend/access/brin/tuple.rs
src/backend/access/brin/brin.rs
src/backend/access/brin/minmax.rs
src/backend/executor/nodes.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
git diff --check
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/cargo-target-pool/6" RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh check
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/cargo-target-pool/6" RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet forms_tuple_with_aligned_bitmap_padding
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/cargo-target-pool/6" RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet brinbuildempty_writes_postgres_shaped_metapage
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/cargo-target-pool/6" RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet brin_truncate_reinitializes_metapage
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/cargo-target-pool/6" RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet minmax_multi_eliminates_values_between_disjoint_points
CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust/cargo-target-pool/6" RUSTC_WRAPPER=/usr/bin/env scripts/run_regression.sh --test brin_multi --jobs 1 --timeout 300 --port 56644 --results-dir /tmp/pgrust-brin-multi-final5-port56644

Remaining:
brin_multi passes locally: 220/220 queries matched.
