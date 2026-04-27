Goal:
Diagnose json regression failures from .context/attachments/pasted_text_2026-04-26_16-47-30.txt.

Key decisions:
The diff contains multiple independent gaps, not one root cause. First mismatch is recursive json input reporting serde syntax errors instead of PostgreSQL stack-depth errors. Major actionable clusters are JSON pretty formatting, timestamptz JSON rendering, json/jsonb error mapping, json_object array coercion, record/domain handling in json_populate_record, and missing json variants for tsvector/headline functions.

Files touched:
src/backend/executor/expr_json.rs
src/backend/executor/agg.rs
src/backend/executor/expr_datetime.rs
src/backend/utils/misc/guc_datetime.rs
src/backend/executor/tests.rs
src/pgrust/database_tests.rs
src/backend/commands/tablecmds.rs
src/backend/utils/cache/visible_catalog.rs
src/backend/catalog/loader.rs
scripts/run_regression.sh
this task note

Tests run:
scripts/cargo_isolated.sh test --lib --quiet json
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test json (blocked in setup bootstrap at 60s)
scripts/run_regression.sh --test json --skip-build --timeout 180 (blocked before json by post_create_index base startup: Catalog(Io("WrongValueCount { expected: 28, actual: 29 }")))
sample 32328 10 -file /tmp/pgrust_bootstrap_sample.txt
sample 88281 5 -file /tmp/pgrust_post_create_index_sample.txt
sample 62636 5 -file /tmp/pgrust_bootstrap_second_sample.txt
sample 61493 5 -file /tmp/pgrust_post_create_index_after_arc_sample.txt
CARGO_TARGET_DIR=/tmp/pgrust-target-bucharest-json scripts/run_regression.sh --test json --timeout 180 --results-dir /tmp/pgrust-json-isolated --port 55439 (port race with another workspace)
CARGO_TARGET_DIR=/tmp/pgrust-target-bucharest-json scripts/run_regression.sh --test json --skip-build --timeout 180 --results-dir /tmp/pgrust-json-isolated2 --port 60439 (ran json: FAIL, 345/470 queries matched)
bash -n scripts/run_regression.sh
git diff --check

Remaining:
Remaining json regression gaps are mostly larger feature areas: recursion stack-depth compatibility, json/jsonb record/domain population semantics, anonymous record FROM column-definition support, json_to_tsvector/json ts_headline support, to_tsvector(json) filtering, and SQL-visible error text polish in deeper json_populate_record paths.
Profiling found the setup slowness was dominated by per-row catalog cloning during COPY bootstrap: execute_insert_values -> execute_insert_rows -> enforce_partition_constraint_after_before_insert -> VisibleCatalog::clone/RelCache::clone. tablecmds.rs now borrows the catalog for user-defined base coercion and skips the expensive clone unless the target relation is actually a partition. VisibleCatalog now stores RelCache/CatCache behind Arc so the remaining partition-path catalog snapshot clone is cheap. After that, the sampled hot path moved to post_create_index CTAS planning/catalog loading.

The WrongValueCount/panic seen from scripts/run_regression.sh was traced to shared Cargo target contamination: /tmp/pgrust-target/debug/pgrust_server had been rebuilt from another Conductor workspace. Running the regression with a workspace-specific CARGO_TARGET_DIR reached the json test without startup catalog errors. scripts/run_regression.sh now defaults CARGO_TARGET_DIR through scripts/cargo_isolated.sh when CARGO_TARGET_DIR is unset, so future runs use a binary from this checkout. loader.rs also includes catalog relation context in heap-scan/decode errors.
