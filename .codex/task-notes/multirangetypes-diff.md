Goal:
Diagnose and fix the multirangetypes regression diff around manual multirange constructor names and type ACL display.

Key decisions:
Function-style casts now skip invalid catalog-backed casts so unresolved function calls can produce PostgreSQL-style 42883 errors.
Dynamic catalog type names are used in missing-function signatures.
The psql \dT+ compatibility path renders stored type ACLs for range types instead of deriving them from stale usage booleans.

Files touched:
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/functions.rs
src/backend/tcop/postgres.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
cargo test --lib --quiet multirange
scripts/run_regression.sh --test multirangetypes --timeout 20 --jobs 1 was attempted but terminated while blocked on a cargo artifact lock from other workspaces.

Remaining:
Run scripts/run_regression.sh --test multirangetypes --timeout 20 --jobs 1 once the shared cargo target lock is free.
