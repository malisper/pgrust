//! Seam declarations for the `backend-utils-fmgr-fmgr` unit
//! (`utils/fmgr/fmgr.c`), the function-call manager.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! C's `FmgrInfo` embeds the resolved C function pointer and so cannot cross
//! a seam; callers keep the function's OID and dispatch by OID. The eager
//! lookup-failure surface of `fmgr_info` is preserved by [`fmgr_info_check`].

use types_cache::DefElemString;
use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;

seam_core::seam!(
    /// `fmgr_info(functionId, &finfo)` (fmgr.c), lookup half only: resolve
    /// the function and fail exactly where C would (`elog(ERROR, "cache
    /// lookup failed for function %u")`, unsupported language, etc.). The
    /// owned model re-resolves at call time, so no handle is returned.
    pub fn fmgr_info_check(function_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `OidFunctionCall1(functionId,
    /// PointerGetDatum(deserialize_deflist(...)))` — the dictionary-init
    /// invocation shape (ts_cache.c): the argument is a `List` of
    /// string-valued `DefElem`s, crossing as typed rows the owner re-forms
    /// into the node list. The returned `Datum` is the dictionary's private
    /// `dictData` pointer word — genuinely heterogeneous per-template state
    /// (the C `void *`), kept opaque. `Err` carries whatever the called
    /// function raises plus the C `elog(ERROR, "function %u returned NULL")`
    /// strict-null check.
    pub fn oid_function_call_1_deflist(
        function_id: Oid,
        options: &[DefElemString<'_>],
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `ri_CompareWithCast(eq_opr, typeid, collid, lhs, rhs)` (ri_triggers.c
    /// caller; fmgr owns the cached `ri_compare_cache` `FmgrInfo`s): apply the
    /// equality (or contained-by) operator after the optional input cast,
    /// returning the boolean result. The cache + `fmgr_info` lookups +
    /// `FunctionCall2Coll` all belong to the fmgr/coercion layer, so the whole
    /// computation crosses as one seam. Can `ereport(ERROR)`.
    pub fn ri_compare_with_cast(
        eq_opr: Oid,
        typeid: Oid,
        collid: Oid,
        lhs: Datum,
        rhs: Datum,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// Render the given (1-based, relation) `attnums` of a violator
    /// `TupleTableSlot` into printable [`ResultColumn`]s for
    /// `ri_ReportViolation` (`getTypeOutputInfo` + `OidOutputFunctionCall`;
    /// NULL → C's `"null"`). Allocated into `mcx`. Can `ereport(ERROR)`.
    pub fn render_slot_columns<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        slot: types_ri_triggers::TupleTableSlotRef,
        attnums: &[i16],
    ) -> PgResult<mcx::PgVec<'mcx, types_ri_triggers::ResultColumn<'mcx>>>
);
