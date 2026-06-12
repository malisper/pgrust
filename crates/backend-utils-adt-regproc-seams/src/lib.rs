//! Seam declarations for the `backend-utils-adt-regproc` unit
//! (`utils/adt/regproc.c` printable-name helpers).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `format_procedure(procedure_oid)` (regproc.c): the function's printable
    /// name (possibly qualified) for diagnostics. Never NULL in C; `Err`
    /// carries the (shouldn't-happen) catalog-lookup `elog(ERROR)`.
    pub fn format_procedure(procedure_oid: Oid) -> PgResult<String>
);

seam_core::seam!(
    /// `format_operator(operator_oid)` (regproc.c): the operator's printable
    /// name for diagnostics.
    pub fn format_operator(operator_oid: Oid) -> PgResult<String>
);

seam_core::seam!(
    /// `stringToQualifiedNameList(string, escontext)` (regproc.c): parse a
    /// possibly-qualified SQL identifier into its name parts, allocated in
    /// `mcx`. With `soft = false` (C: `escontext == NULL`) bad syntax raises
    /// `ERRCODE_INVALID_NAME` (`Err`); with `soft = true` (C: an
    /// `ErrorSaveContext`) it is `Ok(None)` (C: NIL). `Err` includes OOM.
    pub fn string_to_qualified_name_list<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        string: &str,
        soft: bool,
    ) -> PgResult<Option<mcx::PgVec<'mcx, mcx::PgString<'mcx>>>>
);
