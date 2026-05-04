Goal:
Ignore PostgreSQL regression access-method DDL that pgrust does not plan to support.

Key decisions:
Filter prepared regression SQL and expected output together, after existing per-test fixture transforms.
Treat create_am as a no-op regression because the file is entirely access-method behavior.
Drop known dependent AM display/opclass blocks in psql, create_table, and select_parallel.

Files touched:
scripts/run_regression.sh

Tests run:
bash -n scripts/run_regression.sh
scripts/cargo_isolated.sh build --bin pgrust_server
scripts/run_regression.sh --skip-build --test create_am --timeout 30
git diff --check
scripts/run_regression.sh --skip-build --test create_table --timeout 90 (completed; known unrelated diffs remain)
scripts/run_regression.sh --skip-build --test select_parallel --timeout 90 (completed; known unrelated diffs remain)
scripts/run_regression.sh --skip-build --test psql --timeout 60 (filtered create_am dependency completed; later unrelated timeout)
rg checks over prepared fixtures for create_table, select_parallel, and psql found no filtered AM markers

Remaining:
None.
