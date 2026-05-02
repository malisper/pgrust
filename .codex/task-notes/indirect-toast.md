Goal:
Bring indirect_toast support up to PostgreSQL-style runtime indirect varlena behavior.

Key decisions:
Use `Value::IndirectVarlena` with `Arc<[u8]>` complete varlena bytes as a runtime-only representation; never serialize literal indirect pointer bytes into heap storage. Preserve raw varlena sidecars on whole-row records so compressed/on-disk source datums can be wrapped accurately. Implement `make_tuple_indirect(record)` as a real record transformer and replace the PL/pgSQL special case with general `NEW`/`OLD` whole-row assignment.

Files touched:
src/backend/access/common/heaptuple.rs
src/backend/executor/agg.rs
src/include/nodes/primnodes.rs
src/include/nodes/datum.rs
src/include/varatt.rs
src/include/catalog/pg_proc.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/infer.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_casts.rs
src/backend/executor/expr_json.rs
src/backend/executor/expr_string.rs
src/backend/executor/foreign_keys.rs
src/backend/executor/jsonb.rs
src/backend/executor/value_io.rs
src/backend/executor/value_io/array.rs
src/backend/libpq/pqformat.rs
src/backend/rewrite/views.rs
src/pgrust/session.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-belgrade-check scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-belgrade-check scripts/cargo_isolated.sh test --lib --quiet indirect_varlena_dereferences_for_record_text_and_tuple_encoding
CARGO_TARGET_DIR=/tmp/pgrust-target-belgrade-check scripts/cargo_isolated.sh test --lib --quiet stored_literal_indirect_toast_pointer_is_rejected
CARGO_TARGET_DIR=/tmp/pgrust-target-belgrade-regression scripts/run_regression.sh --test indirect_toast

Remaining:
No known remaining issues for indirect_toast.
