//! Seam installation for `backend-utils-adt-jsonpath-exec`.
//!
//! This unit owns no INWARD seam: its public surface (the `jsonb_path_*` SQL
//! entrypoints, the `JsonPathExists`/`Query`/`Value` path evaluators, and the
//! `JsonTable*` JSON_TABLE callbacks) is consumed by units that depend on this
//! crate directly (no cycle), so there is no inward contract to install here.
//!
//! The unit's OUTWARD dependencies fall into three groups:
//!
//!  * Operations called directly against the owning leaf adt unit (no seam):
//!    the recursion / interrupt guards (`check_stack_depth` via
//!    `backend-utils-misc-stack-depth-seams`, `CHECK_FOR_INTERRUPTS` via
//!    `backend-tcop-postgres-seams`) and the `DirectInputFunctionCallSafe`
//!    soft-parse helpers (`int4in`/`int8in`/`numeric_in`/`float8in_internal`/
//!    `parse_bool` → their owning int/int8/numeric/float/bool units).
//!
//!  * Genuine cross-subsystem externals declared in
//!    `backend-utils-adt-jsonpath-exec-seams` and installed by their OWNING
//!    unit's `init_seams()` (regexp.c for `re_compile_and_execute`; formatting.c
//!    for `parse_datetime`; json.c for `json_encode_datetime`; `format_type_be`;
//!    mbutils for `server_to_utf8` / `get_database_encoding`).
//!
//!  * The remaining jsonpath_exec.c-private datetime / `Datum`->`JsonbValue`
//!    coercions (`compare_datetime` / `datetime_method_cast` /
//!    `json_item_from_datum`) and the JSON_TABLE executor/`ExprState` boundary
//!    (`init_table_func` / `eval_column`), which still bottom out on the
//!    date/time-`Datum` fmgr substrate and the `TableFunc`/`JsonExpr`
//!    `ExecEvalExpr` executor substrate respectively — a call panics loudly
//!    until those land, which is correct.
//!
//! This unit owns no INWARD seam, so `init_seams()` is empty.
pub fn init_seams() {}
