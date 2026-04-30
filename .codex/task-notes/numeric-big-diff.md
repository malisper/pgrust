Goal:
Fix the upstream `numeric_big` regression.

Key decisions:
- Teach the regression harness that `numeric_big` depends on `numeric`, matching
  the upstream schedule assumption.
- Resolve typmodded numeric arguments against typmod-free function signatures
  before numeric widening, so calls like `sqrt(numeric(1000,800))` use numeric
  overloads rather than float-style fallbacks.
- Add PostgreSQL-compatible `pow(float8,float8)` and `pow(numeric,numeric)`
  catalog aliases.
- Keep high-scale finite numeric values out of the direct `BigInt -> f64` path
  when that path produces `inf / inf = NaN`; use the digit-based approximation
  for exp/power scale planning instead.
- Preserve numeric add/sub display scale from the max input dscale, which fixes
  zero diffs like `0.00000000` vs `0`.

Files touched:
- `scripts/run_regression.sh`
- `src/backend/executor/expr_numeric.rs`
- `src/backend/executor/expr_ops.rs`
- `src/backend/executor/tests.rs`
- `src/backend/parser/analyze/functions.rs`
- `src/include/catalog/pg_proc.rs`

Tests run:
- `bash -n scripts/run_regression.sh`
- `RUSTC_WRAPPER=/usr/bin/env CARGO_BUILD_RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet high_scale_sqrt_keeps_numeric_precision`
- `RUSTC_WRAPPER=/usr/bin/env CARGO_BUILD_RUSTC_WRAPPER=/usr/bin/env PGRUST_TARGET_POOL_DIR="/Volumes/OSCOO PSSD/rust/pgrust-target-pool" scripts/cargo_isolated.sh test --lib --quiet high_scale_power_keeps_near_overflow_precision`
- `RUSTC_WRAPPER=/usr/bin/env CARGO_BUILD_RUSTC_WRAPPER=/usr/bin/env PGRUST_TARGET_POOL_DIR="/Volumes/OSCOO PSSD/rust/pgrust-target-pool" scripts/cargo_isolated.sh test --lib --quiet numeric_addition_preserves_display_scale`
- `RUSTC_WRAPPER=/usr/bin/env CARGO_BUILD_RUSTC_WRAPPER=/usr/bin/env PGRUST_TARGET_POOL_DIR="/Volumes/OSCOO PSSD/rust/pgrust-target-pool" scripts/cargo_isolated.sh test --lib --quiet numeric_transcendentals`
- `RUSTC_WRAPPER=/usr/bin/env CARGO_BUILD_RUSTC_WRAPPER=/usr/bin/env CARGO_TARGET_DIR=/tmp/pgrust-target scripts/run_regression.sh --test numeric_big --results-dir /tmp/diffs/numeric_big_fixed3 --port 55465 --jobs 1 --timeout 300`
  Result: PASS, 552/552 queries matched.

Remaining:
None for `numeric_big`.
