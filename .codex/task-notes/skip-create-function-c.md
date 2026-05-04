Goal:
Skip PostgreSQL's create_function_c regression file in pgrust regression runs.

Key decisions:
Added an explicit regression-test skip helper and applied it during scheduled and unscheduled test collection. A direct --test create_function_c invocation exits early with a clear SKIP message.

Files touched:
scripts/run_regression.sh

Tests run:
bash -n scripts/run_regression.sh
scripts/run_regression.sh --test create_function_c

Remaining:
None.
