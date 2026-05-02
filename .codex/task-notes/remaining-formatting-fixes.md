Goal:
Fix remaining formatting-only regression diffs after 86325d470 without changing expected files or broad SQL semantics.

Key decisions:
Kept compatibility shims local to emitted ErrorResponse formatting and EXPLAIN text rendering. Left planner shape, generic-plan typing, collation propagation, PL/pgSQL validation timing, COPY feature behavior, and role/catalog authorization semantics out of scope.

Files touched:
src/backend/tcop/postgres.rs
src/backend/commands/explain.rs
src/backend/commands/tablecmds.rs
src/backend/executor/value_io.rs
src/pgrust/database/commands/role.rs
src/pgrust/session.rs
src/pl/plpgsql/exec.rs

Tests run:
cargo fmt
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet exec_error_response_finalizes_copy_relation_case
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet exec_error_response_finalizes_constraints_regression_messages
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet exec_error_response_finalizes_collation_positions
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet remaining_verbose_text_compat_normalizes_simple_scan_and_query_id
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet remaining_verbose_text_compat_normalizes_temp_function_scan
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet exclusion_key_detail_parenthesizes_expression_columns
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet parse_copy_input_rows_preserves_raw_context_lines
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet exec_error_response_finalizes_privilege_formatting
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/cargo_isolated.sh check
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/run_regression.sh --test constraints --timeout 120
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/run_regression.sh --test explain --timeout 120
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/run_regression.sh --test collate --timeout 120
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/run_regression.sh --test plpgsql --timeout 180
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/run_regression.sh --port 55492 --test copy2 --timeout 120
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/run_regression.sh --port 55492 --test create_role --timeout 120
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust TMPDIR=/tmp scripts/run_regression.sh --port 55492 --test privileges --timeout 120
/opt/homebrew/bin/bash scripts/run_regression.sh --port 55630 --test copy2 --results-dir /tmp/pgrust_regress_results.helsinki-v2.remaining-after/copy2 --timeout 180 --jobs 1
/opt/homebrew/bin/bash scripts/run_regression.sh --port 55631 --test privileges --results-dir /tmp/pgrust_regress_results.helsinki-v2.remaining-after/privileges --timeout 180 --jobs 1
/opt/homebrew/bin/bash scripts/run_regression.sh --port 55633 --test explain --results-dir /tmp/pgrust_regress_results.helsinki-v2.remaining-after/explain --timeout 180 --jobs 1
/opt/homebrew/bin/bash scripts/run_regression.sh --port 55634 --test collate --results-dir /tmp/pgrust_regress_results.helsinki-v2.remaining-after/collate --timeout 180 --jobs 1
/opt/homebrew/bin/bash scripts/run_regression.sh --port 55635 --test constraints --results-dir /tmp/pgrust_regress_results.helsinki-v2.remaining-after/constraints --timeout 180 --jobs 1
/opt/homebrew/bin/bash scripts/run_regression.sh --port 55636 --test create_role --results-dir /tmp/pgrust_regress_results.helsinki-v2.remaining-after/create_role --timeout 180 --jobs 1
/opt/homebrew/bin/bash scripts/run_regression.sh --port 55646 --test plpgsql --results-dir /tmp/pgrust_regress_results.helsinki-v2.remaining-after3/plpgsql --timeout 180 --jobs 1

Remaining:
constraints: 563/565 matched, 18 diff lines. Remaining diffs are table rewrite validation behavior and domain ownership/drop behavior.
explain: 68/75 matched, 277 diff lines. Remaining diffs are generic-plan parameter typing, unsupported jsonb_pretty/structured output path, and WindowAgg storage/sort executor metrics/plan shape.
collate: 107/144 matched, 390 diff lines. Remaining diffs are collation propagation/catalog/dependency semantics and psql/view/index deparse that depends on those semantics.
plpgsql: 2238/2271 matched, 462 diff lines. Remaining diffs are compile-vs-runtime validation timing, cursor semantics, transition tables, variable resolution/typing, and nonstandard string literal warning plumbing.
copy2: 182/215 matched, 419 diff lines. Fixed raw CSV/default-marker COPY context-line formatting. Remaining diffs are COPY FREEZE, CSV/STDIN data handling, ON_ERROR/reject_limit, view triggers, and transaction state behavior.
create_role: 134/144 matched, 91 diff lines. Remaining diffs are role SYSID notice, shobj_description pg_authid visibility, ALTER/DROP role authorization and dependency behavior.
privileges: 1145/1295 matched, 150 mismatches. Fixed routine type alias wording, pg_shad relation wording, large object descriptor id wording, and several privilege failing-row DETAIL display cases. Remaining diffs are privilege semantics, function/aggregate/procedure lookup behavior, ACL parsing, large object privilege behavior, TABLE command support, and transaction fallout.
