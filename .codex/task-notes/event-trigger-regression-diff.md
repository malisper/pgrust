Goal:
Fix the five remaining event_trigger regression mismatches found in /tmp/diffs.

Key decisions:
The failures came from five separate compatibility gaps: unsupported-statement
execution returned before generic event-trigger firing, DROP SCHEMA CASCADE
notices were grouped per schema instead of per statement, PL/pgSQL dynamic SQL
added duplicate SQL-statement context, OID-alias casts did not match
PostgreSQL's implicit reg* <-> oid catalog casts, and event trigger object
addresses did not round-trip through pg_identify_object_as_address /
pg_get_object_address.

Files touched:
- src/pgrust/session.rs
- src/pgrust/database/commands/drop.rs
- src/pl/plpgsql/exec.rs
- src/include/catalog/pg_cast.rs
- src/backend/parser/analyze/mod.rs
- src/backend/parser/tests.rs
- src/backend/catalog/object_address.rs
- .codex/task-notes/event-trigger-regression-diff.md

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet analyze_lateral_scalar_record_out_function_can_feed_next_lateral_item
- scripts/cargo_isolated.sh test --lib --quiet analyze_alias_for_scalar_record_out_function_exposes_out_columns
- scripts/cargo_isolated.sh test --lib --quiet bootstrap_pg_cast_rows_preserve_core_oid_and_reg_casts
- scripts/cargo_isolated.sh test --lib --quiet event_trigger_object_address_round_trips
- RUST_BACKTRACE=1 scripts/run_regression.sh --test event_trigger --results-dir /tmp/diffs/event_trigger_fix3 --timeout 180 --jobs 1 --port 55436

Remaining:
event_trigger passes locally: 281/281 queries matched.
