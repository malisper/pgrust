Goal:
Investigate the remaining regression crash categories from the Conductor list and fix reproducible crash paths.

Key decisions:
Matched pgrust behavior against PostgreSQL for rewrite/planner/setrefs details:
- View `UPDATE ... FROM` must rewrite view RTEs before planning and must evaluate view assignments against the joined target/source row.
- Rule-backed `DELETE ... USING` must use the original joined delete input events, not a target-only rescan.
- Ordered aggregate inputs include aggregate `ORDER BY` expressions below aggregation.
- Parameterized inner paths must recurse through array subscripts when turning outer Vars into exec params.
- Base scan setrefs should be able to match semantic relation Vars to scan tuple slots.

Files touched:
src/backend/parser/analyze/modify.rs
src/backend/optimizer/root.rs
src/backend/optimizer/setrefs.rs
src/backend/optimizer/path/costsize.rs
src/pgrust/database/commands/rules.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet view_update_from_routes_joined_rows_through_instead_rules
scripts/cargo_isolated.sh test --lib --quiet view_update_from_routes_joined_rows_through_instead_of_triggers
scripts/cargo_isolated.sh test --lib --quiet delete_using_rules_use_joined_input_rows
scripts/cargo_isolated.sh test --lib --quiet ordered_aggregate_keeps_order_inputs_available_after_join_grouping
scripts/cargo_isolated.sh test --lib --quiet not_null_constraint_describe_query_lowers_conkey_subscript_join_qual
scripts/cargo_isolated.sh test --lib --quiet partition_update_routing_remaps_dropped_column_layouts
scripts/run_regression.sh --test rules --timeout 240 --jobs 1 --port 59601 --results-dir /tmp/pgrust-rest-rules --data-dir /tmp/pgrust-rest-rules-data
scripts/run_regression.sh --test subselect --timeout 240 --jobs 1 --port 59602 --results-dir /tmp/pgrust-rest-subselect --data-dir /tmp/pgrust-rest-subselect-data --skip-build
scripts/run_regression.sh --test triggers --timeout 300 --jobs 1 --port 59603 --results-dir /tmp/pgrust-rest-triggers --data-dir /tmp/pgrust-rest-triggers-data --skip-build
scripts/run_regression.sh --test aggregates --timeout 240 --jobs 1 --port 59604 --results-dir /tmp/pgrust-rest-aggregates --data-dir /tmp/pgrust-rest-aggregates-data --skip-build
scripts/run_regression.sh --test update --timeout 240 --jobs 1 --port 59605 --results-dir /tmp/pgrust-rest-update --data-dir /tmp/pgrust-rest-update-data --skip-build
scripts/run_regression.sh --test foreign_key --timeout 240 --jobs 1 --port 59606 --results-dir /tmp/pgrust-rest-foreign-key --data-dir /tmp/pgrust-rest-foreign-key-data --skip-build
scripts/run_regression.sh --test merge --timeout 300 --jobs 1 --port 59607 --results-dir /tmp/pgrust-rest-merge --data-dir /tmp/pgrust-rest-merge-data --skip-build

Remaining:
The checked regression files now complete without server errors or statement timeouts. They still fail on ordinary expected-output diffs.
