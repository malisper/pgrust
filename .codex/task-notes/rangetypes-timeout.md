Goal:
Investigate why the focused rangetypes regression timed out.

Key decisions:
The initial 90s run reproduced the timeout in the GiST range predicate block.
Sampling showed the server was CPU-bound in SeqScan/Aggregate expression
evaluation and repeatedly cloned the bootstrap pg_proc rows from
builtin_scalar_function_for_proc_oid. Replaced the interim pg_proc borrow patch
with PostgreSQL-style executor dispatch: FuncExpr builtin implementations run
directly, and non-builtin scalar function call metadata is cached inside
ExecutorContext for the active statement/query.

Files touched:
src/backend/executor/fmgr.rs
src/backend/executor/mod.rs
src/backend/executor/driver.rs
ExecutorContext initializer sites
src/backend/executor/tests.rs

Tests run:
cargo fmt
scripts/run_regression.sh --test rangetypes --timeout 90 --port 61000 --results-dir /tmp/diffs/rangetypes-timeout --skip-build
scripts/run_regression.sh --test rangetypes --timeout 90 --port 61200 --results-dir /tmp/diffs/rangetypes-after-proc-cache
scripts/run_regression.sh --test rangetypes --timeout 300 --port 61400 --results-dir /tmp/diffs/rangetypes-after-proc-cache-300 --skip-build
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet eval_builtin_func_uses_bound_implementation_without_catalog_lookup
scripts/cargo_isolated.sh test --lib --quiet eval_user_defined_func_reuses_cached_call_info_without_catalog_lookup
scripts/run_regression.sh --test rangetypes --timeout 90 --port 61500 --results-dir /tmp/diffs/rangetypes-fmgr-cache
scripts/run_regression.sh --test rangetypes --timeout 90 --port 61600 --results-dir /tmp/diffs/rangetypes-fmgr-cache-final
scripts/run_regression.sh --test rangetypes --timeout 90 --port 61700 --results-dir /tmp/diffs/rangetypes-fmgr-cache-final2

Remaining:
The PostgreSQL-style dispatch change removes the rangetypes timeout. The
focused 90s run now completes as FAIL, not TIMEOUT, matching 398/407 queries.
Remaining diffs are range ordering/explain formatting/int8range-array behavior.
