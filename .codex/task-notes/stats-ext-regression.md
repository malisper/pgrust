Goal:
Run the PostgreSQL `stats_ext` regression against pgrust.

Key decisions:
`scripts/run_regression.sh --test stats_ext` builds a debug server and aborted
during `test_setup` startup with a huge allocation request. Reran via a
temporary one-line schedule to keep the release server path while limiting the
run to `stats_ext`.

Files touched:
`.context/stats_ext_schedule` was created as a temporary local schedule.

Tests run:
`scripts/run_regression.sh --test stats_ext` failed before running the test.
`scripts/run_regression.sh --schedule .context/stats_ext_schedule --jobs 1`
completed and failed `stats_ext`: 538/866 queries matched, 328 mismatched,
2556 diff lines.
After rebasing `malisper/stats-ext-reg` onto
`origin/malisper/foreign-key-regression-2`,
`scripts/run_regression.sh --schedule .context/stats_ext_schedule --jobs 1 --results-dir /tmp/diffs/stats_ext_after_fk_rebase.A3sO9U`
completed and failed `stats_ext`: 538/866 queries matched, 328 mismatched,
2556 diff lines.

Remaining:
Diff copied to `/tmp/diffs/stats_ext.diff`. Main result dir:
`/var/folders/tc/1psz8_jd0hnfmgyyr0n2wtzh0000gn/T//pgrust_regress_results.belo-horizonte.nwuqwJ`.
Post-rebase artifacts are under
`/tmp/diffs/stats_ext_after_fk_rebase.A3sO9U`.

Goal:
Implement extended-statistics build/storage/planner use for MCV,
dependencies, and ndistinct/group-count estimates.

Key decisions:
Keep the existing JSON payload envelope for `pg_ndistinct`,
`pg_dependencies`, and `pg_mcv_list`.
Build `pg_statistic_ext_data` during `ANALYZE`, including expression stats in
`stxdexpr`, and skip replacement when `stxstattarget = 0`.
Planner loads decoded extended stats into `RelationStats`; filter estimates use
MCV first and dependency adjustments after, and aggregate rows use multivariate
ndistinct when a matching stats object is available.

Files touched:
`src/backend/statistics/build.rs`
`src/backend/statistics/types.rs`
`src/backend/commands/analyze.rs`
`src/pgrust/database/commands/maintenance.rs`
`src/backend/optimizer/mod.rs`
`src/backend/optimizer/path/costsize.rs`

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh check`
`scripts/cargo_isolated.sh test --lib --quiet statistics`
`scripts/run_regression.sh --port 55440 --schedule .context/stats_ext_schedule --jobs 1 --results-dir /tmp/diffs/stats_ext_after_dependency_any`

Remaining:
Latest `stats_ext` run still fails overall: 608/866 queries matched, 258
mismatched. Remaining classified row-estimate mismatches in the requested
buckets: MCV 59, functional dependencies 43, ndistinct/group-count 66.
The largest known gaps are inherited/append group estimates, PostgreSQL-specific
group-estimate heuristics for expressions not exactly covered by ndistinct
items, and MCV/SAOP inequality formula coverage.

Goal:
Continue reducing the extended-statistics row-estimate buckets.

Key decisions:
Use same-relation damping and single-column expression equivalence in group
estimates. Combine compatible ndistinct components greedily. Load inherited
extended stats for append group estimates where the parent stats object can be
identified. Prevent reciprocal functional dependencies from both applying.
Treat `= ANY` as disjoint equality selectivity. Add scalar-array inequality
matching for MCV items and normalize UUID stats keys with UUID text rendering.

Files touched:
`src/backend/statistics/types.rs`
`src/backend/optimizer/path/costsize.rs`

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh check`
`scripts/cargo_isolated.sh test --lib --quiet statistics`
`scripts/run_regression.sh --port 55443 --schedule .context/stats_ext_schedule --jobs 1 --results-dir /tmp/diffs/stats_ext_after_scalar_bound_fix`

Remaining:
Latest `stats_ext` run still fails overall: 705/866 queries matched, 161
mismatched. Row-estimate mismatches against PostgreSQL expected output are now:
MCV 51, functional dependencies 20, ndistinct/group-count 1. The remaining
ndistinct mismatch is the unrelated `grouping_unique` left-join aggregate
estimate. The remaining FD/MCV misses are mostly scalar-array inequality
selectivity, exact MCV list coverage/rounding, null handling, and boolean
pre-MCV per-column estimates.
