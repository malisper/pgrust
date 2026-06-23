//! Seam declarations for the `backend-utils-fmgr-funcapi` unit
//! (`utils/fmgr/funcapi.c`): the materialized set-returning-function
//! plumbing, over the owned `FunctionCallInfoBaseData` / `ReturnSetInfo`
//! shapes in `nodes`.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_namespace::FuncArgInfo;
// The canonical unified value type (Datum-unification keystone). The seam
// signatures below take/return it (`ByVal`/`ByRef`) with the call frame's
// `'mcx` lifetime; the bare-word `datum::Datum` shim is retained
// elsewhere until cleanup.
use ::types_tuple::Datum;

seam_core::seam!(
    /// `InitMaterializedSRF(fcinfo, flags)` (funcapi.c) — set up the calling
    /// function's materialize-mode tuplestore and descriptor in the
    /// `ReturnSetInfo` at `fcinfo->resultinfo` (`setResult`/`setDesc`). Can
    /// `ereport(ERROR)` (allocation, tupledesc lookup), carried on `Err`.
    pub fn InitMaterializedSRF<'mcx>(
        fcinfo: &mut nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
        flags: u32,
    ) -> ::types_error::PgResult<()>
);

seam_core::seam!(
    /// The materialize-mode `ReturnSetInfo` setup that `populate_recordset_worker`
    /// (jsonfuncs.c) performs by hand — the same rsinfo checks and
    /// `tuplestore_begin_heap` / `returnMode = SFRM_Materialize` /
    /// `setResult` / `setDesc` block as `InitMaterializedSRF`, but with the
    /// result descriptor supplied by the caller instead of being resolved via
    /// `get_call_result_type`. jsonfuncs needs this because it determines the
    /// result row type from the input record / column-definition list
    /// (`get_record_type_from_argument` / `get_record_type_from_query`), for
    /// which `get_call_result_type` would (correctly, in C) bail out with
    /// "return type must be a row type". Can `ereport(ERROR)` (rsinfo context,
    /// tuplestore allocation), carried on `Err`.
    pub fn init_materialized_srf_with_desc<'mcx>(
        fcinfo: &mut nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
        setdesc: ::types_tuple::heaptuple::TupleDesc<'mcx>,
    ) -> ::types_error::PgResult<()>
);

seam_core::seam!(
    /// The C call `tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
    /// values, nulls)` against an `InitMaterializedSRF`-prepared
    /// `ReturnSetInfo`: resolving `setResult`/`setDesc` is funcapi-owned; the
    /// append delegates to tuplestore. `values`/`nulls` mirror the C stack
    /// arrays (borrowed, no allocation at the call site). Can
    /// `ereport(ERROR)` (palloc inside tuple forming/append), carried on
    /// `Err`.
    pub fn materialized_srf_putvalues<'mcx>(
        rsinfo: &mut nodes::funcapi::ReturnSetInfo<'mcx>,
        values: &[Datum<'mcx>],
        nulls: &[bool],
    ) -> ::types_error::PgResult<()>
);

seam_core::seam!(
    /// `get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes)`
    /// (funcapi.c) for the pg_proc row of `func_oid` (the C caller holds the
    /// tuple; the owned marshal re-fetches it by OID). The arrays are
    /// allocated in `mcx` (C: palloc in the current context). `Err` carries
    /// cache-lookup / deform `elog(ERROR)`s and OOM from the copies.
    pub fn get_func_arg_info<'mcx>(
        mcx: Mcx<'mcx>,
        func_oid: Oid,
    ) -> PgResult<FuncArgInfo<'mcx>>
);

seam_core::seam!(
    /// Build an anonymous record `Datum` from a row of `values`/`nulls` whose
    /// columns have the given type OIDs (`coltypes[i]` is column `i+1`): the C
    /// `CreateTemplateTupleDesc(n)` + per-column `TupleDescInitEntry(..., typ,
    /// -1, 0)` + `BlessTupleDesc` + `heap_form_tuple` + `HeapTupleGetDatum`
    /// idiom used by record-returning builtins (e.g. `pg_stat_file`). The
    /// tupledesc / tuple machinery is funcapi/heaptuple/tupdesc-owned; the
    /// result is allocated in `mcx`. `Err` carries OOM from forming the tuple.
    pub fn record_from_values<'mcx>(
        mcx: Mcx<'mcx>,
        coltypes: &[Oid],
        values: &[Datum<'mcx>],
        nulls: &[bool],
    ) -> PgResult<Datum<'mcx>>
);

seam_core::seam!(
    /// `init_MultiFuncCall(fcinfo)` (funcapi.c) = `SRF_FIRSTCALL_INIT()`: the
    /// first-call setup of a value-per-call SRF. Verifies the `ReturnSetInfo`
    /// context, creates the long-lived multi-call memory context under the
    /// frame's `fn_mcxt` channel, allocates and zeroes a [`FuncCallContext`],
    /// and stashes it in the frame's `fn_extra` channel (C's
    /// `flinfo->fn_extra`). Returns a `&mut` to the stashed context (C returns
    /// the `FuncCallContext *` that aliases `fn_extra`). `Err` on the wrong
    /// calling context (`ERRCODE_FEATURE_NOT_SUPPORTED`) or a second call
    /// ("init_MultiFuncCall cannot be called more than once"). The caller must
    /// set `fcinfo.fn_mcxt` (per-query context) first.
    pub fn init_MultiFuncCall<'a, 'mcx>(
        fcinfo: &'a mut nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
    ) -> ::types_error::PgResult<&'a mut nodes::funcapi::FuncCallContext<'mcx>>
);

seam_core::seam!(
    /// `per_MultiFuncCall(fcinfo)` (funcapi.c) = `SRF_PERCALL_SETUP()`: return
    /// (a `&mut` to) the cross-call [`FuncCallContext`] saved in the frame's
    /// `fn_extra` channel for the current per-call step. `Err` if called before
    /// `init_MultiFuncCall` (the C contract violation, `fn_extra == NULL`).
    pub fn per_MultiFuncCall<'a, 'mcx>(
        fcinfo: &'a mut nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
    ) -> ::types_error::PgResult<&'a mut nodes::funcapi::FuncCallContext<'mcx>>
);

seam_core::seam!(
    /// `end_MultiFuncCall(fcinfo, funcctx)` (funcapi.c) — the teardown
    /// `SRF_RETURN_DONE` drives: tear down the multi-call context and unbind the
    /// frame's `fn_extra` channel. (The C shutdown-callback deregistration is
    /// subsumed by ownership in the owned model — see the funcapi body — so the
    /// `funcctx` argument C threads through is not needed; the context is taken
    /// straight out of `fcinfo.fn_extra`.) Can carry an `Err` from the teardown.
    pub fn end_MultiFuncCall<'mcx>(
        fcinfo: &mut nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
    ) -> ::types_error::PgResult<()>
);

seam_core::seam!(
    /// The value-per-call set-returning-function machinery
    /// (`SRF_IS_FIRSTCALL`/`SRF_FIRSTCALL_INIT`/`SRF_PERCALL_SETUP`/
    /// `SRF_RETURN_NEXT`/`SRF_RETURN_DONE` over a `FuncCallContext` with a
    /// `multi_call_memory_ctx` and `user_fctx`) is funcapi-owned and not yet
    /// modeled (only the materialize-mode tuplestore path is). The
    /// `pg_partition_tree` / `pg_partition_ancestors` / `pg_lock_status`
    /// value-SRFs cross here until that machinery lands. funcapi (the owner)
    /// INSTALLS this seam in its `init_seams()` with an EXPLICIT honest
    /// seam-and-panic body (mirror-pg-and-panic): the call panics loudly,
    /// owner-rooted, naming the missing value-per-call protocol instead of
    /// silently degrading the SRF or aborting on an uninstalled seam. Replace
    /// the body with the real per-call `FuncCallContext` machinery when ported.
    pub fn value_srf_unported() -> ()
);

seam_core::seam!(
    /// `PG_ARGISNULL(0) ? None : Some(PG_GETARG_OID(0))` against the call
    /// frame: read the optional leading `Oid` argument of a SQL-callable
    /// function (used by `pg_stat_get_subscription`'s subid filter). fmgr owns
    /// the trimmed `args`/`isnull` arrays, so the read is seamed.
    pub fn srf_arg0_oid<'mcx>(
        fcinfo: &nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
    ) -> Option<Oid>
);

seam_core::seam!(
    /// `PG_GETARG_INT64(n)` (`DatumGetInt64(fcinfo->args[n].value)`) against
    /// the call frame: read the `int8` argument at position `n`. Used by
    /// `pg_wal_summary_contents`'s timeline argument. fmgr owns the trimmed
    /// `args` array, so the read is seamed.
    pub fn srf_arg_int64<'mcx>(
        fcinfo: &nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
        n: usize,
    ) -> i64
);

seam_core::seam!(
    /// `PG_GETARG_LSN(n)` (`DatumGetLSN(fcinfo->args[n].value)`, `utils/pg_lsn.h`)
    /// against the call frame: read the `pg_lsn` (`XLogRecPtr`) argument at
    /// position `n`. Used by `pg_wal_summary_contents`'s start/end LSN
    /// arguments. fmgr owns the trimmed `args` array, so the read is seamed.
    pub fn srf_arg_lsn<'mcx>(
        fcinfo: &nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
        n: usize,
    ) -> ::types_core::XLogRecPtr
);

seam_core::seam!(
    /// `CStringGetTextDatum(s)` (builtins.h / varlena): build a `text *`
    /// Datum from a C string, allocated in `mcx` (C: the current context). Used
    /// by `pg_stat_get_subscription` for the worker-type text column. `Err`
    /// carries OOM.
    pub fn cstring_get_text_datum<'mcx>(
        mcx: Mcx<'mcx>,
        s: &str,
    ) -> PgResult<Datum<'mcx>>
);

seam_core::seam!(
    /// Read a varlena (`bytea`/`text`/`json`/`jsonb`) argument at position `n`
    /// as its detoasted, FULL on-disk varlena image (the 4-byte length word
    /// included): the C `PG_GETARG_*_PP(n)` / `PG_DETOAST` of a by-reference
    /// argument off the call frame's by-reference lane. The fmgr owns the
    /// trimmed `args` array AND the bare-word -> varlena detoast boundary, so
    /// the read is seamed. Used by `json[b]_object_keys` /
    /// `json[b]_array_elements[_text]` / `json[b]_each[_text]` /
    /// `json[b]_populate_record[set]` to obtain the input document bytes. The
    /// bytes are always header-ful (matching the by-reference lane's
    /// header-for-header round-trip): the `jsonb` callers read the container at
    /// `&image[VARHDRSZ..]`; the `text`/`json` callers read `VARDATA` (skip the
    /// 4-byte header). `Err` carries detoast OOM.
    pub fn srf_arg_varlena_bytes<'mcx>(
        mcx: Mcx<'mcx>,
        fcinfo: &nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
        n: usize,
    ) -> PgResult<::mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `PG_GETARG_HEAPTUPLEHEADER(n)` against the call frame: read a composite
    /// (row-type) argument at position `n` as a [`FormedTuple`] (owned header +
    /// user-data area). The fmgr owns the trimmed `args` array AND the
    /// bare-word -> composite-Datum detoast boundary, so the read is seamed.
    /// Used by `json[b]_populate_record` to obtain the optional record argument
    /// whose existing field values seed the result tuple. Returns the detoasted
    /// composite as a `FormedTuple`; `Err` carries detoast OOM. The caller must
    /// have already checked `PG_ARGISNULL(n)` is false (C reads the header only
    /// when the arg is non-null).
    pub fn srf_arg_record<'mcx>(
        mcx: Mcx<'mcx>,
        fcinfo: &nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
        n: usize,
    ) -> PgResult<::types_tuple::heaptuple::FormedTuple<'mcx>>
);

seam_core::seam!(
    /// `build_function_result_tupdesc_t(procedureTuple)` (funcapi.c:1683): build
    /// the result `TupleDesc` describing the OUT/INOUT columns of the procedure
    /// whose `pg_proc` OID is `proc_oid`. Returns `None` when the procedure is
    /// not declared to return a composite (no OUT params / not RECORD-returning).
    /// The descriptor is owned in `mcx`. Used by `CallStmtResultDesc`
    /// (functioncmds.c). Fallible on the cache-lookup `ereport(ERROR)`.
    pub fn build_function_result_tupdesc_t<'mcx>(
        mcx: Mcx<'mcx>,
        proc_oid: Oid,
    ) -> PgResult<::types_tuple::heaptuple::TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `get_expr_result_tupdesc(expr, noError)` (funcapi.c): get the tuple
    /// descriptor describing the result of an expression of composite type. Used
    /// by `ParseComplexProjection` (parse_func.c). With `no_error = true` an
    /// unresolvable result type yields `Ok(None)` (the C `NULL` return); with
    /// `no_error = false` it raises. The descriptor is owned in `mcx`.
    pub fn get_expr_result_tupdesc<'mcx>(
        mcx: Mcx<'mcx>,
        expr: Option<&nodes::nodes::Node<'mcx>>,
        no_error: bool,
    ) -> PgResult<::types_tuple::heaptuple::TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `get_expr_result_type(expr, &resultTypeId, &resultTupleDesc)`
    /// (funcapi.c:262): classify an expression's result type, returning the
    /// `TypeFuncClass` plus (for a composite) the result type OID and tuple
    /// descriptor. Used by `ExecInitFunctionScan` (nodeFunctionscan.c) to build
    /// the scan's tuple descriptor from the SRF's result type. Owner:
    /// `backend-utils-fmgr-funcapi`.
    pub fn get_expr_result_type<'mcx>(
        mcx: Mcx<'mcx>,
        expr: Option<&nodes::nodes::Node<'mcx>>,
    ) -> PgResult<nodes::funcapi::ResolvedResultType<'mcx>>
);
