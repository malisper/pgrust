//! Seam installation for `backend-utils-adt-jsonpath-exec`.
//!
//! This unit owns no INWARD seam: its public surface (the `jsonb_path_*` SQL
//! entrypoints, the `JsonPathExists`/`Query`/`Value` path evaluators, and the
//! `JsonTable*` JSON_TABLE callbacks) is consumed by units that depend on this
//! crate directly (no cycle), so there is no inward contract to install here.
//!
//! Every OUTWARD dependency the executor reaches is a genuine cross-subsystem
//! operation declared in `backend-utils-adt-jsonpath-exec-seams` and installed
//! by its OWNING unit's `init_seams()` (regexp.c for `re_compile_and_execute` /
//! `convert_regex_flags`; json.c for `parse_datetime` / `json_encode_datetime`;
//! the date/time fmgr layer for `compare_datetime` / `datetime_method_cast`; the
//! type-input functions; `format_type_be`; mbutils for `server_to_utf8` /
//! `get_database_encoding`; the stack/interrupt guards; and the
//! executor/`ExprState` boundary for `init_table_func` / `eval_column`). None of
//! those owners are this crate, so installing them is their job — until then a
//! call panics loudly, which is correct.
pub fn init_seams() {}
