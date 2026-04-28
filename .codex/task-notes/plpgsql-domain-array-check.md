Goal:
Close the PL/pgSQL orderedarray domain regression cluster.

Key decisions:
Kept explicit runtime casts for dynamic/domain type OIDs so catalog-free constant folding cannot bypass domain constraints.
Indexed domain lookups by both domain OID and domain array OID for runtime catalogs.
Added cast-only domain check support for array element comparisons such as value[1] < value[2].

Files touched:
src/backend/executor/expr_casts.rs
src/backend/optimizer/constfold.rs
src/backend/parser/analyze/expr.rs
src/pgrust/database.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55433 --results-dir /tmp/diffs/plpgsql-domain-array-check

Remaining:
plpgsql regression now reports 2151/2271 matched and 1431 diff lines.
The next visible cluster after orderedarray is mostly planner output for set-returning functions and later PL/pgSQL diagnostics/context gaps.
