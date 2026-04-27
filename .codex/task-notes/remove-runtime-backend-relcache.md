Goal:
Remove runtime backend_relcache use and keep trigger/rangetypes regressions from timing out.

Key decisions:
Use PostgreSQL-style targeted syscache/relation descriptor lookup instead of full relation enumeration.
Keep relation descriptor cache statement/backend local with conservative catalog invalidation.
Replace hot DDL ownership checks with targeted AUTHOID/AuthMemMemRole syscache probes.
Batch internal FK trigger creation into one catalog write/effect.

Files touched:
Executor catalog plumbing, syscache/lsyscache, catalog store/persistence, DDL/drop/trigger/index paths, and focused tests.

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet lookup_any_relation_uses_targeted_relation_cache_without_catcache
scripts/cargo_isolated.sh test --lib --quiet relation_descriptor_cache_survives_command_id_changes_and_invalidates
scripts/cargo_isolated.sh test --lib --quiet backend::utils::cache::syscache
scripts/cargo_isolated.sh test --lib --quiet trigger
scripts/run_regression.sh --test triggers --timeout 90 --port 62361 --results-dir /tmp/diffs/triggers-no-backend-relcache
scripts/run_regression.sh --test rangetypes --timeout 90 --port 62381 --results-dir /tmp/diffs/rangetypes-fmgr-cache

Remaining:
triggers and rangetypes still have semantic diffs, but both complete as FAIL rather than TIMEOUT.
