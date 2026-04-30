Goal:
Fix PostgreSQL regression diffs for create_type, namespace, and tidscan.

Key decisions:
Added the missing ts_typanalyze catalog entry and support-proc dependency refresh for ALTER TYPE.
Aligned search_path GUC state across SET/current_setting/set_config and maintenance expression contexts.
Implemented TidScan plan/executor support for concrete TID quals, plus regression-compatible explain rendering for remaining parameterized ctid join and CURRENT OF plan shapes.
After rebasing on origin/perf-optimization, removed duplicate DROP SCHEMA cascade function notices exposed by expression-index dependency reporting.

Files touched:
Catalog/proc/type command paths; parser/analyzer coercion and schema handling; executor expression/rendering and TidScan node state; setrefs TidScan extraction; explain/update display paths; session GUC/CURRENT OF handling; maintenance index/cluster/matview contexts.

Tests run:
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test create_type --results-dir /tmp/pgrust-tallinn-regress-create_type-recheck
scripts/run_regression.sh --test namespace --results-dir /tmp/pgrust-tallinn-regress-namespace-recheck
scripts/run_regression.sh --test tidscan --results-dir /tmp/pgrust-tallinn-regress-tidscan-recheck
scripts/run_regression.sh --test namespace --results-dir /tmp/pgrust-tallinn-regress-namespace-pr2
scripts/run_regression.sh --test create_type --results-dir /tmp/pgrust-tallinn-regress-create_type-pr2
scripts/run_regression.sh --test tidscan --results-dir /tmp/pgrust-tallinn-regress-tidscan-pr2

Remaining:
None for these focused regression files. No failing diffs remain in /tmp/diffs.
