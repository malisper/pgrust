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
    /// `OutputFunctionCall(flinfo, val)` (fmgr.c): invoke a type's text output
    /// function through an already-resolved `FmgrInfo`. The owned `FmgrInfo`
    /// carries only the resolved function's OID (the lookup key), so the owner
    /// re-resolves and calls. The argument crosses as the owned per-attribute
    /// value model (`TupleValue`, as the deformed-slot readers produce). The C
    /// `char *` result crosses as its NUL-excluded bytes allocated in `mcx`.
    /// `Err` carries the strict-null `elog` and whatever the output function
    /// raises.
    pub fn output_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        flinfo: &types_core::fmgr::FmgrInfo,
        val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `SendFunctionCall(flinfo, val)` (fmgr.c): invoke a type's binary send
    /// function through an already-resolved `FmgrInfo`. The argument crosses as
    /// the owned per-attribute value model. The C `bytea *` result crosses as
    /// its payload bytes with the varlena header already stripped (`VARDATA`,
    /// `VARSIZE - VARHDRSZ` bytes) allocated in `mcx`. `Err` carries the
    /// strict-null `elog` and whatever the send function raises.
    pub fn send_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        flinfo: &types_core::fmgr::FmgrInfo,
        val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `OidSendFunctionCall(functionId, val)` (fmgr.c): one-shot lookup +
    /// call of a type's binary send function. The C argument `Datum` crosses
    /// as the owned per-attribute value model
    /// ([`types_tuple::backend_access_common_heaptuple::TupleValue`]); the C
    /// `bytea *` result crosses as its payload bytes with the varlena header
    /// already stripped (`VARDATA`, `VARSIZE - VARHDRSZ` bytes), allocated in
    /// `mcx`. `Err` carries the lookup failure, the strict-null `elog`, and
    /// whatever the send function raises.
    pub fn oid_send_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `OidOutputFunctionCall(functionId, val)` (fmgr.c): one-shot lookup +
    /// call of a type's text output function. The C `char *` result crosses
    /// as its NUL-excluded bytes allocated in `mcx`. `Err` carries the lookup
    /// failure, the strict-null `elog`, and whatever the output function
    /// raises.
    pub fn oid_output_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `OidInputFunctionCall(functionId, str, typioparam, typmod)` (fmgr.c) as
    /// used by bootstrap's `InsertOneValue`: one-shot lookup + call of a type's
    /// text input function on the NUL-terminated C string `str_` (`typmod` is
    /// `-1` at bootstrap). Returns the resulting `Datum`. `Err` carries invalid
    /// input syntax, cache-lookup failure, and OOM.
    pub fn oid_input_function_call(
        function_id: Oid,
        str_: &str,
        typioparam: Oid,
        typmod: i32,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `FunctionCall3(flinfo, arg1, arg2, arg3)` (fmgr.c): call the function
    /// identified by `function_id` (the caller's cached `FmgrInfo`, which
    /// cannot cross the seam, so we re-resolve by OID) with three
    /// non-collation arguments under the default (invalid) collation, returning
    /// its `Datum` result. Used by `ri_CompareWithCast` to apply a cast
    /// function `(value, typmod=-1, implicit=false)`. The C path asserts the
    /// result is non-null. Can `ereport(ERROR)`.
    pub fn function_call3(
        function_id: Oid,
        arg1: Datum,
        arg2: Datum,
        arg3: Datum,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `OidOutputFunctionCall(functionId, val)` (fmgr.c), raw-`Datum` form used
    /// by bootstrap's `InsertOneValue` DEBUG4 trace: one-shot lookup + call of
    /// a type's text output function on the bare `Datum` it just built (the
    /// typed `TupleValue` form is unavailable there). The C `char *` result
    /// crosses as its NUL-excluded bytes in `mcx`. `Err` carries the lookup
    /// failure, the strict-null `elog`, and whatever the output function raises.
    pub fn oid_output_function_call_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        function_id: Oid,
        val: Datum,
    ) -> PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `FunctionCall2Coll(flinfo, collation, arg1, arg2)` (fmgr.c): call the
    /// function identified by `function_id` (the caller's cached `FmgrInfo`,
    /// re-resolved by OID) with two arguments under the given input
    /// `collation`, returning its `Datum` result. Used by `ri_CompareWithCast`
    /// to apply the equality/contained-by operator. The C path asserts the
    /// result is non-null. Can `ereport(ERROR)`.
    pub fn function_call2_coll(
        function_id: Oid,
        collation: Oid,
        arg1: Datum,
        arg2: Datum,
    ) -> PgResult<Datum>
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
