//! Seam installation for `backend-utils-adt-jsonpath`.
//!
//! This unit owns no INWARD seam: every public function of `jsonpath.c`
//! (`jsonpath_in`/`_out`/`_recv`/`_send`, `jspInit`, `jspIsMutable`,
//! `jspConvertRegexFlags`, the `jsp*` reader API) is consumed only by units
//! that are not yet ported (`jsonpath_exec.c`, `jsonfuncs.c`, the optimizer's
//! `clauses.c`). When the first cross-cycle consumer lands it will create a
//! companion `-seams` crate; until then there is no inward contract to install
//! and consumers depend on this crate directly.
//!
//! The crate's OUTWARD dependencies are reached through their owners' seam
//! crates (`parse` via `backend-utils-adt-jsonpath-gram-seams`,
//! `escape_json_with_len` via `backend-utils-adt-json-seams`, `expr_type` via
//! `backend-nodes-nodeFuncs-seams`) or directly (`numeric_out`,
//! `datetime_format_has_tz`); installing those is the owners' job.
//!
//! This crate DOES own its fmgr `pg_proc` entries (`jsonpath_in`/`_out`): they
//! are registered into the `fmgr_builtins[]` table here so the internal-lookup
//! resolution by `prosrc` name finds them, mirroring how every type's I/O
//! family is published by its owner's `init_seams()`.
pub fn init_seams() {
    crate::register_jsonpath_builtins();

    // clauses.c's contain_mutable_functions JsonExpr arm crosses here for
    // jspIsMutable (jsonpath.c); this unit owns it.
    clauses_seams::jsp_is_mutable::set(crate::jsp_is_mutable_seam);
}
