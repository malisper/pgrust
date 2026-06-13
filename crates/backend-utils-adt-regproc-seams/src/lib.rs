//! Seam declarations for the `backend-utils-adt-regproc` unit
//! (`utils/adt/regproc.c` printable-name helpers).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `format_procedure(procedure_oid)` (regproc.c): the function's printable
    /// name (possibly qualified) for diagnostics, palloc'd in the caller's
    /// current context (`mcx`). Never NULL in C; `Err` carries the
    /// (shouldn't-happen) catalog-lookup `elog(ERROR)` and OOM.
    pub fn format_procedure<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        procedure_oid: Oid,
    ) -> PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `format_operator(operator_oid)` (regproc.c): the operator's printable
    /// name for diagnostics, palloc'd in the caller's current context (`mcx`).
    pub fn format_operator<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        operator_oid: Oid,
    ) -> PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `regprocedurein(signature)` (regproc.c) via the
    /// `DirectFunctionCall1(regprocedurein, CStringGetDatum(...))` shape:
    /// parse a function signature (e.g. `int4pl(int4,int4)`) to its
    /// `pg_proc` OID. A bad/ambiguous/unknown signature raises (`Err`);
    /// an unmatched-but-syntactically-valid signature yields `InvalidOid`.
    pub fn regprocedurein(signature: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// `regtypein(typename)` (regproc.c) via the
    /// `DirectFunctionCall1(regtypein, CStringGetDatum(...))` shape: parse a
    /// type name to its `pg_type` OID. Bad syntax raises (`Err`); an
    /// unmatched-but-valid name yields `InvalidOid`.
    pub fn regtypein(typename: &str) -> PgResult<Oid>
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
