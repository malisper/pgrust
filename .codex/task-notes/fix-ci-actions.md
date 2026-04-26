Goal:
Fix CI failures reported in attached cargo test logs.

Key decisions:
Restore INSERT CTE body lowering in the parser.
Do not re-apply OVERRIDING USER identity defaults for VALUES rows already normalized by binding.
Return SQL NULL for unavailable tableoid/ctid on null-extended rows while preserving slot metadata fallback.

Files touched:
src/backend/parser/gram.rs
src/backend/commands/tablecmds.rs
src/backend/executor/exec_expr.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet writable_cte
scripts/cargo_isolated.sh test --lib --quiet parse_select_with_writable_insert_cte_returning_tableoid_and_star
scripts/cargo_isolated.sh test --lib --quiet parse_insert_with_writable_insert_cte
scripts/cargo_isolated.sh test --lib --quiet alter_identity_and_overriding_enforce_generated_always
scripts/cargo_isolated.sh test --lib --quiet outer_join_null_extended_ctid_is_null
scripts/cargo_isolated.sh check

Remaining:
query_repl.rs still has the existing unreachable-pattern warning during check.
