Goal:
Finish extended-statistics estimate and display behavior for the `stats_ext` regression.

Key decisions:
Use PostgreSQL-style MCV minimum-count gating, keep SQL three-valued logic when matching MCV items, recognize casted constants in planner selectivity, and avoid functional-dependency correction only for bitmap OR branches synthesized from OR arms that reference different extended-statistics targets.

Files touched:
`src/backend/statistics/build.rs`
`src/backend/optimizer/mod.rs`
`src/backend/optimizer/path/allpaths.rs`
`src/backend/optimizer/path/costsize.rs`

Tests run:
`TMPDIR='/Volumes/OSCOO PSSD/tmp' scripts/cargo_isolated.sh test --lib --quiet mcv_`
`TMPDIR='/Volumes/OSCOO PSSD/tmp' scripts/cargo_isolated.sh test --lib --quiet column_const_pair_recognizes_casted_constants`
`TMPDIR='/Volumes/OSCOO PSSD/tmp' scripts/run_regression.sh --test stats_ext --port 59668 --results-dir /tmp/pgrust-task-c14-02-stats-ext`
`TMPDIR='/Volumes/OSCOO PSSD/tmp' scripts/cargo_isolated.sh check`

Remaining:
`stats_ext` is down to the out-of-scope `DROP SCHEMA tststats CASCADE` detail ordering mismatch.
