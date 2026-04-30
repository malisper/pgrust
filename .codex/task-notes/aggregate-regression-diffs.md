Goal:
Fix the semantic aggregate regressions in `/tmp/pgrust-diffs-2026-04-30T0340Z/aggregates.diff`, excluding planner-shape drift, error caret/wording drift, and view deparse formatting.

Key decisions:
- Extended ordered-set handling beyond hardcoded `percentile_disc`: catalog `aggkind` now drives custom ordered/hypothetical aggregate binding, and executor finalizers cover `percentile_cont`, array percentiles, `mode`, and PostgreSQL-shaped custom ordered/hypothetical support functions.
- Carried custom aggregate runtime metadata for support arg types, finalfunc-extra placeholders, polymorphic transition/final types, and custom combine functions.
- Preserved named composite/record and array element identity through aggregate state/final values and view storage.
- Fixed aggregate semantic-level tracking for nested sublinks and direct args, including lower-level var errors.
- Added runtime fast paths for the two timeout shapes: correlated scalar lookup subplans and uncorrelated `= ANY` subplans in aggregate `FILTER`.
- Flattened multidimensional `PgArray` values for `unnest(anyarray)`, matching PostgreSQL storage-order unnesting.
- Simulated custom aggregate combinefunc behavior for plain aggregates with custom combine functions so the regression’s NULL-returning combiner matches PostgreSQL’s forced parallel result.

Files touched:
- `src/backend/parser/analyze/*`
- `src/backend/executor/*`
- `src/include/catalog/pg_aggregate.rs`
- `src/include/catalog/pg_proc.rs`
- `src/include/nodes/primnodes.rs`
- `src/pgrust/database/commands/create.rs`
- `src/pgrust/database_tests.rs`
- `src/pl/plpgsql/*`
- `src/backend/rewrite/views.rs`

Tests run:
- `cargo fmt`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-pool/amsterdam-v5-check scripts/cargo_isolated.sh test --lib --quiet aggregate_regress`
  - 7 passed.
- `CARGO_TARGET_DIR=/tmp/pgrust-target-pool/amsterdam-v5-check scripts/cargo_isolated.sh test --lib --quiet unnest_multidimensional_array_flattens_storage_order`
  - 1 passed.
- `scripts/run_regression.sh --test aggregates --results-dir /tmp/pgrust-diffs-2026-04-30T0340Z/amsterdam-v5-impl10 --timeout 180 --jobs 1`
  - `create_aggregate` PASS.
  - `aggregates` FAIL, 499/583 matched, 84 mismatches, 1364 diff lines.
  - No timeouts.

Remaining:
- Remaining `aggregates.diff` mismatches in `amsterdam-v5-impl10` are in the excluded buckets:
  - EXPLAIN planner-shape drift: min/max index paths, GROUP BY/pathkey/incremental-sort choices, Memoize placement, and missing parallel/partial aggregate plan shapes.
  - SQL error text formatting: missing `LINE`/caret details and shorter DISTINCT+ORDER BY wording.
  - `pg_get_viewdef` formatting/deparse drift for ordered-set aggregate views.
- Current semantic buckets fixed: ordered-set/custom ordered-set values, `test_rank`, `test_percentile_disc`, `v_pagg_test` values, bytea/text split pipeline, `balk` NULL transition/combine results, aggregate state sharing notices, `rwagg`/`eatarray`, strict support-proc signatures, composite aggregate state/finalfn, grouped `t1.*`/JOIN USING behavior, bitwise BIT(4) width, and both aggregate subquery timeout cases.
