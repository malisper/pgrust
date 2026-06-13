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
use types_tuple::backend_access_common_heaptuple::TupleValue;

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
    /// rowtypes.c `record_in` per-column conversion: `getTypeInputInfo(coltype,
    /// &typiofunc, &typioparam)` + `fmgr_info_cxt` + `InputFunctionCallSafe(...,
    /// column_data, typioparam, atttypmod, escontext, &result)`. `column_data`
    /// is `None` for a SQL NULL field (C passes a NULL cstring). The soft-error
    /// path is modelled by the `Option` result: `Ok(None)` means the input
    /// function reported a soft error via the `escontext` (C's
    /// `InputFunctionCallSafe` returned `false`), so the caller bails the same
    /// way C does at its `goto fail`. `Ok(Some(v))` is the converted column
    /// value (allocated in `mcx`). `Err` carries hard `ereport(ERROR)`s
    /// (catalog lookups, OOM).
    pub fn record_column_input<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        coltype: Oid,
        column_data: Option<&str>,
        atttypmod: i32,
        escontext: Option<&mut types_error::SoftErrorContext>,
    ) -> PgResult<Option<TupleValue<'mcx>>>
);

seam_core::seam!(
    /// rowtypes.c `record_recv` per-column conversion:
    /// `getTypeBinaryInputInfo(coltype, &typiofunc, &typioparam)` +
    /// `fmgr_info_cxt` + `ReceiveFunctionCall(..., buf, typioparam, atttypmod)`.
    /// `item` is the column's binary payload bytes, or `None` for a -1-length
    /// NULL field (C passes a NULL `StringInfo`). The owner verifies the
    /// receive function consumed the whole item buffer, raising
    /// `errcode(ERRCODE_INVALID_BINARY_REPRESENTATION)` ("improper binary
    /// format in record column %d") with the 1-based `colno` otherwise. Result
    /// is allocated in `mcx`. `Err` carries the catalog lookups and the receive
    /// function's `ereport(ERROR)`s.
    pub fn record_column_receive<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        coltype: Oid,
        item: Option<&[u8]>,
        atttypmod: i32,
        colno: i32,
    ) -> PgResult<TupleValue<'mcx>>
);

seam_core::seam!(
    /// rowtypes.c `record_out` per-column conversion: `getTypeOutputInfo(coltype,
    /// &typiofunc, &typisvarlena)` + `fmgr_info_cxt` +
    /// `OutputFunctionCall(&proc, attr)`. The C `char *` result crosses as its
    /// NUL-excluded bytes allocated in `mcx`. `val` is the non-null column
    /// value. `Err` carries the catalog lookups and the output function's
    /// `ereport(ERROR)`s.
    pub fn record_column_output<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        coltype: Oid,
        val: &TupleValue<'_>,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// rowtypes.c `record_send` per-column conversion:
    /// `getTypeBinaryOutputInfo(coltype, &typiofunc, &typisvarlena)` +
    /// `fmgr_info_cxt` + `SendFunctionCall(&proc, attr)`. The C `bytea *`
    /// result crosses as its payload bytes with the varlena header already
    /// stripped (`VARDATA`, `VARSIZE - VARHDRSZ` bytes), allocated in `mcx`.
    /// `val` is the non-null column value. `Err` carries the catalog lookups
    /// and the send function's `ereport(ERROR)`s.
    pub fn record_column_send<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        coltype: Oid,
        val: &TupleValue<'_>,
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
