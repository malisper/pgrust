//! Set Returning Function (SRF) plumbing — `funcapi.c` lines 58–256.
//!
//! `InitMaterializedSRF` (materialize-mode tuplestore + descriptor setup) and
//! the `*_MultiFuncCall` cross-call helpers that thread a [`FuncCallContext`]
//! across fmgr calls via `flinfo->fn_extra` and register an `ExprContext`
//! shutdown callback. Also hosts the two thin fmgr-call-frame seam readers the
//! inward seam crate exposes (`srf_arg0_oid`, `cstring_get_text_datum`).

use backend_utils_error::ereport;
use mcx::Mcx;
use types_core::Oid;
// The canonical unified value (Datum-unification keystone): the public funcapi
// seams (`materialized_srf_putvalues`, `cstring_get_text_datum`) carry it.
// `materialized_srf_putvalues` threads it straight through to the
// `tuplestore_putvalues` seam, which is itself canonical (Datum-completion
// Wave 7). `cstring_get_text_datum` bridges the still-bare-word `cstring_to_text`
// varlena seam at that audited ABI edge: its returned pointer word is carried in
// the canonical by-value arm here, with no shim type held by funcapi.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_error::error::ERRCODE_FEATURE_NOT_SUPPORTED;
use types_error::{PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::{
    FuncCallContext, ReturnSetInfo, SetFunctionReturnMode, TypeFuncClass, MAT_SRF_BLESS,
    MAT_SRF_USE_EXPECTED_DESC, SFRM_Materialize, SFRM_Materialize_Random,
};
use types_tuple::heaptuple::RECORDOID;

use crate::result_type::internal_get_result_type;

/// `InitMaterializedSRF(fcinfo, flags)` (funcapi.c:76) — sanity-check the
/// `ReturnSetInfo` at `fcinfo->resultinfo`, create the materialize-mode
/// Tuplestore and result `TupleDesc` in the per-query context, and store them
/// back (`returnMode`/`setResult`/`setDesc`). `MAT_SRF_USE_EXPECTED_DESC` uses
/// `rsinfo->expectedDesc`; `MAT_SRF_BLESS` blesses a transient RECORD desc.
pub fn InitMaterializedSRF<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    flags: u32,
) -> PgResult<()> {
    // C funcapi.c:84-90:
    //   ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    //   if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
    //       ereport(ERROR,
    //               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
    //                errmsg("set-valued function called in context that "
    //                       "cannot accept a set")));
    //
    // The owned `resultinfo` carries the `ReturnSetInfo` node inline; `None`
    // is the C `rsinfo == NULL` (the placeholder is always the right tag, so
    // the `!IsA` arm collapses into the `None` check).
    let rsinfo = match fcinfo.resultinfo.as_ref() {
        Some(r) => r,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("set-valued function called in context that cannot accept a set")
                .into_error());
        }
    };

    // C funcapi.c:91-94:
    //   if (!(rsinfo->allowedModes & SFRM_Materialize) ||
    //       ((flags & MAT_SRF_USE_EXPECTED_DESC) != 0 && rsinfo->expectedDesc == NULL))
    //       ereport(ERROR,
    //               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
    //                errmsg("materialize mode required, but it is not allowed "
    //                       "in this context")));
    if (rsinfo.allowedModes & SFRM_Materialize) == 0
        || ((flags & MAT_SRF_USE_EXPECTED_DESC) != 0 && rsinfo.expectedDesc.is_none())
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("materialize mode required, but it is not allowed in this context")
            .into_error());
    }
    // Captured before the call frame is re-borrowed for the result-type reads
    // and the final mutable store (C reads `rsinfo->allowedModes` again at the
    // `random_access` line).
    let rsinfo_allowed_modes = rsinfo.allowedModes;

    // C funcapi.c:96-122 (the descriptor + tuplestore build):
    //   per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    //   old_context = MemoryContextSwitchTo(per_query_ctx);
    //   if (flags & MAT_SRF_USE_EXPECTED_DESC)
    //       stored_tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
    //   else if (get_call_result_type(fcinfo, NULL, &stored_tupdesc) != TYPEFUNC_COMPOSITE)
    //       elog(ERROR, "return type must be a row type");
    //   if (flags & MAT_SRF_BLESS) BlessTupleDesc(stored_tupdesc);
    //   random_access = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
    //   tupstore = tuplestore_begin_heap(random_access, false, work_mem);
    //   rsinfo->returnMode = SFRM_Materialize;
    //   rsinfo->setResult = tupstore;
    //   rsinfo->setDesc = stored_tupdesc;
    //   MemoryContextSwitchTo(old_context);
    //
    // Every allocation here (the result descriptor and the tuplestore) must
    // outlive the SRF call frame. C reaches that context through
    // `rsinfo->econtext->ecxt_per_query_memory` and switches the global
    // `CurrentMemoryContext` into it before allocating; the trimmed
    // `ReturnSetInfo` carries no `econtext`. In the owned model the
    // tuplestore/tupledesc builders take an explicit `Mcx` instead of relying
    // on a switched global context, and the call's memory context is reached
    // through the fmgr owner's call frame (the trimmed `FunctionCallInfoBaseData`
    // has no `flinfo`/`context` either, so both the context and the
    // `fn_oid`/`fn_expr` `get_call_result_type` needs come through the fmgr
    // seams). This is the `switch_to_per_query_context` of src-idiomatic
    // expressed as a context argument: `pg_call_mcx` is C's
    // `CurrentMemoryContext` at fmgr dispatch — the same context the consumers
    // (`pg_get_shmem_allocations`, …) build their row datums in.
    let mcx: Mcx<'mcx> = backend_utils_fmgr_fmgr_seams::pg_call_mcx::call(fcinfo);

    // `get_call_result_type` (funcapi.c) pulls `fn_oid`/`fn_expr` off
    // `fcinfo->flinfo`; the `fn_oid_and_expr` seam reads them from the
    // fmgr-widened frame. Its contract borrows the frame for `'mcx` (the
    // `fn_expr` node it can hand back lives in the call's arena), so a `'mcx`
    // shared view of the frame is needed.
    //
    // SAFETY: `fcinfo_ref` aliases `*fcinfo` as a shared `'mcx` view used ONLY
    // for the read-only result-type resolution below; it is dead before the
    // single `&mut fcinfo.resultinfo` store at the end of this function (the
    // resolution produces owned `TupleDesc`/`Tuplestorestate` values that do
    // not borrow the frame), so the shared and exclusive accesses never
    // overlap in time. This is the trimmed call-frame boundary: the seam
    // contract is `&'mcx`, the owning seam here receives `&mut`, and the frame
    // is the SRF call frame both views describe.
    let fcinfo_ref: &'mcx FunctionCallInfoBaseData<'mcx> = unsafe { &*(fcinfo as *const _) };
    // The call expression `get_call_result_type` resolves a polymorphic result
    // type from is `fcinfo->flinfo->fn_expr` (the owned `Expr` `fmgr_info_set_expr`
    // stamped), recovered as the erased carrier and wrapped in `CallExpr`.
    let (fn_oid, fn_expr_erased) =
        backend_utils_fmgr_fmgr_seams::fn_oid_and_fn_expr_erased::call(fcinfo_ref);
    let call_expr = fn_expr_erased.map(crate::polymorphic::CallExpr::from_erased);

    // Build a tuple descriptor for our result type.
    //
    // C: if (flags & MAT_SRF_USE_EXPECTED_DESC)
    //        stored_tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
    let mut stored_tupdesc: types_tuple::heaptuple::TupleDesc<'mcx> =
        if (flags & MAT_SRF_USE_EXPECTED_DESC) != 0 {
            let expected = fcinfo_ref
                .resultinfo
                .as_ref()
                .and_then(|r| r.expectedDesc.as_deref())
                .expect(
                    "InitMaterializedSRF: MAT_SRF_USE_EXPECTED_DESC checked rsinfo->expectedDesc != NULL above",
                );
            let copy = backend_access_common_tupdesc_seams::create_tuple_desc_copy::call(mcx, expected)?;
            Some(mcx::alloc_in(mcx, copy)?)
        } else {
            // C: if (get_call_result_type(fcinfo, NULL, &stored_tupdesc)
            //         != TYPEFUNC_COMPOSITE)
            //        elog(ERROR, "return type must be a row type");
            //
            // get_call_result_type funnels into internal_get_result_type with
            // the function OID + call expression off the fmgr frame and the
            // ReturnSetInfo for the RECORD-via-expectedDesc arm.
            let rsinfo_for_resolve = fcinfo_ref.resultinfo.as_ref();
            let resolved =
                internal_get_result_type(mcx, fn_oid, call_expr.as_ref(), rsinfo_for_resolve)?;
            if resolved.class != Some(TypeFuncClass::Composite) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INTERNAL_ERROR)
                    .errmsg("return type must be a row type")
                    .into_error());
            }
            resolved.result_tuple_desc
        };

    // C: if (flags & MAT_SRF_BLESS) BlessTupleDesc(stored_tupdesc);
    //
    // BlessTupleDesc (execTuples.c) assigns a transient typmod to an anonymous
    // RECORD descriptor via the typcache; mirrored inline (the only owned step
    // is the RECORDOID/typmod<0 guard around assign_record_type_typmod).
    if (flags & MAT_SRF_BLESS) != 0 {
        if let Some(td) = stored_tupdesc.as_deref_mut() {
            if td.tdtypeid == RECORDOID && td.tdtypmod < 0 {
                backend_utils_cache_typcache_seams::assign_record_type_typmod::call(td)?;
            }
        }
    }

    // C: random_access = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
    let random_access = (rsinfo_allowed_modes & SFRM_Materialize_Random) != 0;

    // C: tupstore = tuplestore_begin_heap(random_access, false, work_mem);
    //    (the store is allocated in the per-query/current context via `mcx`.)
    let tupstore = backend_utils_sort_storage_seams::tuplestore_begin_heap::call(
        mcx,
        random_access,
        false,
        backend_utils_init_small_seams::work_mem::call(),
    )?;

    // C: rsinfo->returnMode = SFRM_Materialize;
    //    rsinfo->setResult = tupstore;
    //    rsinfo->setDesc = stored_tupdesc;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF: resultinfo present (checked at entry)");
    rsinfo.returnMode = SetFunctionReturnMode::Materialize;
    // The storage seam hands back the carrier inside an `mcx`-allocated box (its
    // `PgBox<Tuplestorestate>` MEMCTX-RETYPE); `setResult` is the carrier by
    // value, so move it out of the box.
    rsinfo.setResult = allocator_api2::boxed::Box::into_inner(tupstore);
    rsinfo.setDesc = stored_tupdesc;

    // C funcapi.c:122: MemoryContextSwitchTo(old_context) — no global context
    // is switched in the owned model (the builders allocated into `mcx`
    // directly), so there is nothing to restore.
    Ok(())
}

/// The `tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls)`
/// append against an `InitMaterializedSRF`-prepared `ReturnSetInfo`. funcapi
/// resolves `setResult`/`setDesc`; the append delegates to the tuplestore unit.
pub fn materialized_srf_putvalues<'mcx>(
    rsinfo: &mut ReturnSetInfo<'mcx>,
    values: &[DatumV<'mcx>],
    nulls: &[bool],
) -> PgResult<()> {
    // The C SRF callers do, after InitMaterializedSRF(fcinfo, ...):
    //   tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    // funcapi resolves the (setResult, setDesc) pair the init filled; the
    // MinimalTuple build + append belongs to the tuplestore unit's seam.
    let setDesc = rsinfo
        .setDesc
        .as_deref()
        .expect("materialized_srf_putvalues: rsinfo->setDesc is NULL (InitMaterializedSRF not run)");
    // The `tuplestore_putvalues` seam now carries the canonical unified
    // `Datum<'mcx>` (Datum-completion Wave 7), so the by-attribute values flow
    // straight through with no lowering to a bare scalar word.
    backend_utils_sort_storage_seams::tuplestore_putvalues::call(
        &mut rsinfo.setResult,
        setDesc,
        values,
        nulls,
    )
}

/// `init_MultiFuncCall(PG_FUNCTION_ARGS)` (funcapi.c:133) — first-call setup:
/// verify the `ReturnSetInfo` context, create the long-lived multi-call memory
/// context, allocate and zero a [`FuncCallContext`], stash it in
/// `flinfo->fn_extra`, and register [`shutdown_MultiFuncCall`] on the
/// `ExprContext`. `elog(ERROR)` on a second call.
pub fn init_MultiFuncCall<'a, 'mcx>(
    fcinfo: &'a mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<&'a mut FuncCallContext<'mcx>> {
    // C funcapi.c:140-146:
    //   if (fcinfo->resultinfo == NULL || !IsA(fcinfo->resultinfo, ReturnSetInfo))
    //       ereport(ERROR,
    //               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
    //                errmsg("set-valued function called in context that "
    //                       "cannot accept a set")));
    if fcinfo.resultinfo.is_none() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("set-valued function called in context that cannot accept a set")
            .into_error());
    }

    // C funcapi.c:148-196: the first/second-call dispatch and the cross-call
    // state setup:
    //   if (fcinfo->flinfo->fn_extra == NULL) {
    //       rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    //       multi_call_ctx = AllocSetContextCreate(fcinfo->flinfo->fn_mcxt,
    //                            "SRF multi-call context", ALLOCSET_SMALL_SIZES);
    //       retval = MemoryContextAllocZero(multi_call_ctx, sizeof(FuncCallContext));
    //       retval->call_cntr = 0; retval->max_calls = 0;
    //       retval->user_fctx = NULL; retval->attinmeta = NULL;
    //       retval->tuple_desc = NULL;
    //       retval->multi_call_memory_ctx = multi_call_ctx;
    //       fcinfo->flinfo->fn_extra = retval;
    //       RegisterExprContextCallback(rsi->econtext, shutdown_MultiFuncCall,
    //                                   PointerGetDatum(fcinfo->flinfo));
    //   } else
    //       elog(ERROR, "init_MultiFuncCall cannot be called more than once");
    //
    // OWNED MODEL (the value-per-call SRF channel, #349):
    //   * C's `flinfo->fn_extra` (the once-only guard + the cross-call
    //     FuncCallContext slot) is the `fn_extra` channel on the owned call
    //     frame (the `'mcx`-bound place the `FuncCallContext` can live — the
    //     std/lifetime-free `flinfo` cannot name it, #327). `SRF_IS_FIRSTCALL()`
    //     (`flinfo->fn_extra == NULL`) is `fcinfo.fn_extra.is_none()`.
    //   * C's `flinfo->fn_mcxt` (the parent of the new multi-call context) is
    //     the `fn_mcxt` channel on the frame.
    //   * The `RegisterExprContextCallback(rsi->econtext, shutdown_MultiFuncCall,
    //     ...)` early-reset cleanup hook is SUBSUMED BY OWNERSHIP here: the
    //     multi-call context is the owned `MemoryContext` stored in
    //     `funcctx.multi_call_memory_ctx`, owned (transitively) by
    //     `SetExprState.fcinfo`, so a dropped/reset ExprContext (or an aborted
    //     query) frees it deterministically via RAII — exactly the leak the C
    //     callback exists to prevent. `end_MultiFuncCall` still performs the
    //     explicit teardown C drives through the callback. No hollow callback is
    //     registered (the bare-`fn` ExprContext callback cannot reach the
    //     frame's `fn_extra`, and RAII already guarantees the cleanup).
    if fcinfo.fn_extra.is_some() {
        // C: elog(ERROR, "init_MultiFuncCall cannot be called more than once");
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg("init_MultiFuncCall cannot be called more than once")
            .into_error());
    }

    // C: multi_call_ctx = AllocSetContextCreate(fcinfo->flinfo->fn_mcxt,
    //                         "SRF multi-call context", ALLOCSET_SMALL_SIZES);
    // The owned multi-call context is a child of the per-query `fn_mcxt` channel.
    let fn_mcxt = fcinfo.fn_mcxt.expect(
        "init_MultiFuncCall: fcinfo->fn_mcxt (the per-query context the multi-call \
         context is created under) must be set by the SRF caller before the call",
    );
    let multi_call_ctx = fn_mcxt.context().new_child("SRF multi-call context");

    // C: retval = MemoryContextAllocZero(multi_call_ctx, sizeof(FuncCallContext));
    //    retval->call_cntr = 0; ...; retval->multi_call_memory_ctx = multi_call_ctx;
    //    fcinfo->flinfo->fn_extra = retval;
    fcinfo.fn_extra = Some(FuncCallContext {
        call_cntr: 0,
        max_calls: 0,
        user_fctx: None,
        attinmeta: None,
        multi_call_memory_ctx: Some(multi_call_ctx),
        tuple_desc: Default::default(),
    });

    Ok(fcinfo
        .fn_extra
        .as_mut()
        .expect("init_MultiFuncCall: fn_extra just assigned"))
}

/// `per_MultiFuncCall(PG_FUNCTION_ARGS)` (funcapi.c:208) — return the
/// `FuncCallContext` saved in `flinfo->fn_extra` for the current per-call step.
pub fn per_MultiFuncCall<'a, 'mcx>(
    fcinfo: &'a mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<&'a mut FuncCallContext<'mcx>> {
    // C funcapi.c:208-214:
    //   FuncCallContext *retval = (FuncCallContext *) fcinfo->flinfo->fn_extra;
    //   return retval;
    //
    // The cross-call FuncCallContext lives in the frame's `fn_extra` channel
    // (#349). C never NULL-checks `fn_extra` here (the SRF body only calls
    // SRF_PERCALL_SETUP after SRF_FIRSTCALL_INIT has set it); the owned model
    // surfaces the contract violation loudly instead of returning a bogus value.
    fcinfo.fn_extra.as_mut().ok_or_else(|| {
        ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg("per_MultiFuncCall called before init_MultiFuncCall (fn_extra is NULL)")
            .into_error()
    })
}

/// `end_MultiFuncCall(PG_FUNCTION_ARGS, funcctx)` (funcapi.c:220) — deregister
/// the shutdown callback and run [`shutdown_MultiFuncCall`] to tear down the
/// multi-call context.
pub fn end_MultiFuncCall<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<()> {
    // C funcapi.c:220-235:
    //   ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    //   /* Deregister the shutdown callback */
    //   UnregisterExprContextCallback(rsi->econtext, shutdown_MultiFuncCall,
    //                                 PointerGetDatum(fcinfo->flinfo));
    //   /* But use it to do the real work */
    //   shutdown_MultiFuncCall(PointerGetDatum(fcinfo->flinfo));
    //
    // OWNED MODEL: no ExprContext callback was registered (init_MultiFuncCall
    // documents why — the cleanup is RAII-owned), so there is nothing to
    // deregister. The teardown C drives through `shutdown_MultiFuncCall` is run
    // directly: take the cross-call state out of the frame's `fn_extra` channel
    // (the C `flinfo->fn_extra = NULL` unbind) and delete its multi-call context
    // (the C `MemoryContextDelete`), which `shutdown_MultiFuncCall` does and
    // dropping the taken-out `FuncCallContext` completes.
    if let Some(mut funcctx) = fcinfo.fn_extra.take() {
        shutdown_MultiFuncCall(&mut funcctx)?;
        // Dropping `funcctx` here frees the FuncCallContext itself (C: it lived
        // inside the now-deleted multi_call_memory_ctx).
    }
    Ok(())
}

/// `shutdown_MultiFuncCall(Datum arg)` (funcapi.c:238) — the `ExprContext`
/// shutdown callback: unbind `flinfo->fn_extra` and delete the multi-call
/// memory context (freeing the `FuncCallContext` itself).
pub fn shutdown_MultiFuncCall<'mcx>(funcctx: &mut FuncCallContext<'mcx>) -> PgResult<()> {
    // C funcapi.c:238-249:
    //   FmgrInfo *flinfo = (FmgrInfo *) DatumGetPointer(arg);
    //   FuncCallContext *funcctx = (FuncCallContext *) flinfo->fn_extra;
    //   /* unbind from flinfo */
    //   flinfo->fn_extra = NULL;
    //   /* deletion of context and shutdown callbacks will free the
    //    * FuncCallContext */
    //   MemoryContextDelete(funcctx->multi_call_memory_ctx);
    //
    // OWNED MODEL: the C callback recovers `funcctx` from `flinfo->fn_extra` via
    // the `PointerGetDatum(flinfo)` arg; the owned signature is handed the
    // `funcctx` directly (the `flinfo->fn_extra = NULL` unbind is the caller's
    // `fcinfo.fn_extra.take()` in `end_MultiFuncCall`). The
    // `MemoryContextDelete(funcctx->multi_call_memory_ctx)` is dropping the
    // owned multi-call arena ([`FuncCallContext::shutdown`]).
    funcctx.shutdown();
    Ok(())
}

/// `PG_ARGISNULL(0) ? None : Some(PG_GETARG_OID(0))` — read the optional
/// leading `Oid` argument of a SQL-callable function (the SRF-filter helper the
/// inward seam exposes). fmgr owns the `args`/`isnull` arrays.
pub fn srf_arg0_oid<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Option<Oid> {
    // C: PG_ARGISNULL(0) ? (no filter) : PG_GETARG_OID(0)
    //   #define PG_ARGISNULL(n)  (fcinfo->args[n].isnull)
    //   #define PG_GETARG_OID(n) DatumGetObjectId(fcinfo->args[n].value)
    //
    // The call frame now carries `args` (`Vec<NullableDatum>`); read
    // `args[0]` directly: a NULL argument means "no filter" (`None`),
    // otherwise read the Oid out of the by-value Datum word.
    let arg0 = &fcinfo.args[0];
    if arg0.isnull {
        None
    } else {
        Some(arg0.value.as_oid())
    }
}

/// `PG_GETARG_INT64(n)` — read the `int8` argument at position `n` out of the
/// call frame. fmgr owns the `args` array.
pub fn srf_arg_int64<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>, n: usize) -> i64 {
    // C: #define PG_GETARG_INT64(n) DatumGetInt64(PG_GETARG_DATUM(n))
    fcinfo.args[n].value.as_i64()
}

/// `PG_GETARG_LSN(n)` (`utils/pg_lsn.h`) — read the `pg_lsn` (`XLogRecPtr`)
/// argument at position `n` out of the call frame. fmgr owns the `args` array.
pub fn srf_arg_lsn<'mcx>(
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
    n: usize,
) -> types_core::XLogRecPtr {
    // C: #define PG_GETARG_LSN(n) DatumGetLSN(PG_GETARG_DATUM(n))
    //    #define DatumGetLSN(X) ((XLogRecPtr) GET_8_BYTES(X))
    fcinfo.args[n].value.as_u64()
}

/// Read a varlena (`bytea`/`text`/`json`/`jsonb`) argument at position `n` off
/// the call frame's by-reference lane as its FULL on-disk varlena image (the
/// 4-byte length word included).
///
/// In C the SRF readers are `PG_GETARG_TEXT_PP(n)` (a detoasted `text *`, read
/// via `VARDATA_ANY`) and `PG_GETARG_JSONB_P(n)` (a detoasted `Jsonb *`, read
/// via its container header at `&jb->root`); both hand back a header-ful
/// (`struct varlena *`) pointer and the caller peels the header itself. The
/// owned by-reference lane carries that same header-ful image: `ExecEvalFuncArgs`
/// (execSRF.c) stores the compiled argument's `Datum::ByRef` bytes verbatim into
/// `ref_args[n]` as a [`FmgrArgRef::Varlena`], and a by-reference value
/// round-trips header-for-header. So this returns the image unchanged; the
/// `json`/`text` callers read its `VARDATA` (skip the 4-byte header) and the
/// `jsonb` callers read its container at `&image[VARHDRSZ..]`. `Err` carries
/// alloc OOM. A NULL or non-varlena argument is a contract violation (the SRF
/// callers check `PG_ARGISNULL`/strictness first) and panics.
pub fn srf_arg_varlena_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
    n: usize,
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    use types_nodes::fmgr::FmgrArgRef;

    // C: PG_GETARG_TEXT_PP / PG_GETARG_JSONB_P detoast the by-reference arg via
    // `pg_detoast_datum_packed`. Unlike the regular function-call dispatch
    // (which applies `detoast_ref_arg_if_toasted` before invoking the callee),
    // the SRF call frame in `ExecEvalFuncArgs` marshals the compiled argument's
    // by-reference image verbatim — so a value stored compressed inline (a wide
    // jsonb/text column past the toast threshold) or out-of-line external
    // reaches here still toasted. Apply the `PG_GETARG_*_PP` detoast here, the
    // SRF analog of those macros, so the worker reads a flat container.
    let image: &[u8] = match fcinfo.ref_arg(n) {
        Some(FmgrArgRef::Varlena(bytes)) => bytes.as_slice(),
        _ => panic!(
            "srf_arg_varlena_bytes: argument {n} is absent from the by-reference \
             lane or is not a varlena (the SRF caller must check PG_ARGISNULL / \
             strictness before reading a varlena argument)"
        ),
    };

    // `pg_detoast_datum_packed` returns a plain (uncompressed, inline) value
    // verbatim and only fetches/decompresses a compressed or external one — the
    // exact `PG_DETOAST_DATUM_PACKED` contract. An SRF varlena arg is always a
    // genuine varlena type (jsonb/text/array), never a fixed-length by-reference
    // value, so this is unconditionally safe to apply.
    backend_access_common_detoast_seams::pg_detoast_datum_packed::call(mcx, image)
}

/// `CStringGetTextDatum(s)` — build a `text *` Datum from a string in `mcx`
/// (the SRF text-column helper the inward seam exposes).
pub fn cstring_get_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<DatumV<'mcx>> {
    // C builtins.h:
    //   #define CStringGetTextDatum(s) PointerGetDatum(cstring_to_text(s))
    // `cstring_to_text` (varlena.c) builds the `text` varlena in the current
    // context and is owned by the varlena unit; route through its by-reference
    // `_v` seam, which yields a `Datum::ByRef` over the freshly built varlena
    // bytes (the canonical form for a pass-by-reference `text` value).
    backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `PG_GETARG_HEAPTUPLEHEADER(n)` (fmgr.h / postgres.h) — read a composite
/// (row-type) argument at position `n` as a [`FormedTuple`].
///
/// C:
/// ```c
/// #define PG_GETARG_HEAPTUPLEHEADER(n) DatumGetHeapTupleHeader(PG_GETARG_DATUM(n))
/// #define DatumGetHeapTupleHeader(d) ((HeapTupleHeader) PG_DETOAST_DATUM(d))
/// ```
/// i.e. the by-reference composite Datum is (possibly) detoasted to a flat
/// `HeapTupleHeaderData`-prefixed varlena image (the `DatumTupleFields` choice
/// carrying `datum_len_`/`datum_typmod`/`datum_typeid`, then the on-disk header
/// fields and the user-data area). The executor call frame already carries that
/// header-ful image on the by-reference side channel: `ExecEvalFuncArgs`
/// (execSRF.c) serializes a `Composite` Datum via `FormedTuple::to_datum_image()`
/// into `ref_args[n] = FmgrArgRef::Varlena(image)` — the same convention
/// `srf_arg_varlena_bytes` reads for a `text`/`json` argument (the trimmed frame's
/// `FmgrArgRef` has no `Composite` arm; a composite is varlena-shaped, so it
/// collapses into `Varlena`). So the detoast boundary here is reading that image
/// and decoding it back into the owned [`FormedTuple`] via `from_datum_image`,
/// the exact inverse of `to_datum_image`.
///
/// The caller must have checked `PG_ARGISNULL(n)` is false first (C reads the
/// header only for a non-null arg); a NULL or non-varlena slot is a contract
/// violation and panics.
pub fn srf_arg_record<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
    n: usize,
) -> PgResult<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>> {
    use types_nodes::fmgr::FmgrArgRef;
    use types_tuple::backend_access_common_heaptuple::FormedTuple;

    // C: rec = PG_GETARG_HEAPTUPLEHEADER(n) — detoast the by-reference composite
    // argument. The owned lane carries the detoasted, header-ful composite-Datum
    // image in `ref_args[n]` (a `Varlena` arm — a composite Datum is
    // varlena-shaped, a pointer to a `HeapTupleHeaderData` whose first word is the
    // varlena length header).
    let image: &[u8] = match fcinfo.ref_arg(n) {
        Some(FmgrArgRef::Varlena(bytes)) => bytes.as_slice(),
        _ => panic!(
            "srf_arg_record: argument {n} is absent from the by-reference lane or \
             is not a composite varlena image (the SRF caller must check \
             PG_ARGISNULL before reading a record argument)"
        ),
    };

    // Decode the `HeapTupleHeader` varlena image back into the owned
    // `FormedTuple` (header — incl. the `DatumTupleFields` typeid/typmod the
    // callers read via `HeapTupleHeaderGetTypeId`/`HeapTupleHeaderGetTypMod` — plus
    // the user-data area). C aliases the detoasted pointer in place; the owned
    // model materializes an `mcx`-allocated tuple.
    FormedTuple::from_datum_image(mcx, image)
}

/// LEGACY stub for value-per-call SRF call sites not yet rewired onto the now-
/// ported multi-call protocol (#349).
///
/// The value-per-call machinery (`SRF_FIRSTCALL_INIT`/`SRF_PERCALL_SETUP`/the
/// `SRF_RETURN_DONE` teardown over a [`FuncCallContext`] carrying a
/// `multi_call_memory_ctx`/`user_fctx`) is now implemented here as
/// [`init_MultiFuncCall`] / [`per_MultiFuncCall`] / [`end_MultiFuncCall`] /
/// [`shutdown_MultiFuncCall`], threaded through the `fn_extra`/`fn_mcxt` call-
/// frame channels with `ReturnSetInfo.{econtext,isDone}`. New SRFs use that.
///
/// A handful of legacy consumers (`pg_partition_tree` / `pg_partition_ancestors`
/// / `pg_lock_status`) still route here because their *owners* are not yet
/// ported through `ExecMakeFunctionResultSet` (execSRF.c) — the consumer that
/// builds the frame and drives the per-call series. They get a loud, owner-
/// rooted failure naming the missing executor leg rather than an implicit
/// "uninstalled seam" abort, exactly as before. Rewire each onto
/// `init/per/end_MultiFuncCall` as execSRF lands.
pub fn value_srf_unported() {
    panic!(
        "value-per-call SRF call site not yet rewired onto the ported \
         init/per/end_MultiFuncCall protocol (#349). The funcapi multi-call \
         FuncCallContext machinery IS ported; this caller's owner still needs \
         the executor leg (ExecMakeFunctionResultSet / execSRF.c) that builds \
         the call frame and drives the row series. Port that, then call \
         init_MultiFuncCall / per_MultiFuncCall / end_MultiFuncCall here \
         (pg_partition_tree/pg_partition_ancestors/pg_lock_status)"
    )
}

// `SetFunctionReturnMode::Materialize` is the `rsinfo->returnMode = SFRM_Materialize`
// store InitMaterializedSRF performs once the per-query context lands; named here
// so the enum stays referenced from the SRF module that sets it.
const _: SetFunctionReturnMode = SetFunctionReturnMode::Materialize;

#[cfg(test)]
mod srf_protocol_tests {
    //! Proof-of-life for the value-per-call SRF protocol keystone (#349): drive
    //! the full `SRF_FIRSTCALL_INIT` / `SRF_PERCALL_SETUP` / `SRF_RETURN_NEXT` /
    //! `SRF_RETURN_DONE` cycle through `init_MultiFuncCall` / `per_MultiFuncCall`
    //! / `end_MultiFuncCall` over a real `types_nodes` call frame, faithfully
    //! mirroring `int.c`'s `generate_series_step_int4` — the canonical
    //! value-per-call SRF. This is exactly the body
    //! `ExecMakeFunctionResultSet` (execSRF.c) will run a value-SRF `PGFunction`
    //! through once that executor leg lands.

    use super::*;
    use core::any::Any;
    use mcx::{MemoryContext, PgBox};
    use types_datum::NullableDatum;
    use types_nodes::execexpr::ExprDoneCond;
    use types_nodes::funcapi::{ReturnSetInfo, SFRM_ValuePerCall};

    /// `generate_series_fctx` (int.c:47) — the cross-call state. A plain
    /// `'static` struct, exactly as the C SRF's `user_fctx` payload (`void *`).
    #[derive(Debug)]
    struct GenerateSeriesFctx {
        current: i32,
        finish: i32,
        step: i32,
    }

    /// Erase a `'static` user-state value into the `FuncCallContext.user_fctx`
    /// carrier (`PgBox<'mcx, dyn Any>`) — the C `funcctx->user_fctx = palloc(...)`
    /// stash. Same unsize-through-raw-pointer pattern the rest of the repo uses
    /// (no `CoerceUnsized` on stable).
    fn erase_user_fctx<'mcx, T: Any>(
        mcx: Mcx<'mcx>,
        v: T,
    ) -> PgResult<PgBox<'mcx, dyn Any>> {
        let boxed = mcx::alloc_in(mcx, v)?;
        let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
        // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast
        // only attaches the `dyn Any` vtable.
        Ok(unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) })
    }

    /// One invocation of the value-per-call SRF, mirroring
    /// `generate_series_step_int4(PG_FUNCTION_ARGS)`. Returns
    /// `(result_i32_or_None, isDone)` — `None` is `SRF_RETURN_DONE`'s NULL.
    fn generate_series_step_int4<'mcx>(
        mcx: Mcx<'mcx>,
        fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    ) -> PgResult<(Option<i32>, ExprDoneCond)> {
        // C: if (SRF_IS_FIRSTCALL()) { ... funcctx = SRF_FIRSTCALL_INIT(); ... }
        if fcinfo.fn_extra.is_none() {
            let start = fcinfo.args[0].value.as_i32();
            let finish = fcinfo.args[1].value.as_i32();
            // C: step = 1; if (PG_NARGS() == 3) step = PG_GETARG_INT32(2);
            let step = if fcinfo.nargs == 3 {
                fcinfo.args[2].value.as_i32()
            } else {
                1
            };
            // C: if (step == 0) ereport(ERROR, "step size cannot equal zero");
            assert!(step != 0, "test uses non-zero step");

            // funcctx = SRF_FIRSTCALL_INIT();  (allocates the multi-call context
            // + FuncCallContext, stashes it in the fn_extra channel.)
            init_MultiFuncCall(fcinfo)?;
            // C: oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
            //    fctx = palloc(...); fctx->current = start; ...
            //    funcctx->user_fctx = fctx;
            let fctx = erase_user_fctx(
                mcx,
                GenerateSeriesFctx {
                    current: start,
                    finish,
                    step,
                },
            )?;
            // Reborrow the stashed context to set user_fctx.
            let funcctx = per_MultiFuncCall(fcinfo)?;
            funcctx.user_fctx = Some(fctx);
        }

        // C: funcctx = SRF_PERCALL_SETUP();
        let funcctx = per_MultiFuncCall(fcinfo)?;
        // C: fctx = funcctx->user_fctx; result = fctx->current;
        let fctx: &mut GenerateSeriesFctx = funcctx
            .user_fctx
            .as_mut()
            .expect("user_fctx set on first call")
            .downcast_mut::<GenerateSeriesFctx>()
            .expect("user_fctx is a GenerateSeriesFctx");
        let result = fctx.current;

        let in_range = (fctx.step > 0 && fctx.current <= fctx.finish)
            || (fctx.step < 0 && fctx.current >= fctx.finish);

        if in_range {
            // C: if (pg_add_s32_overflow(...)) fctx->step = 0; else current=next.
            match fctx.current.checked_add(fctx.step) {
                Some(next) => fctx.current = next,
                None => fctx.step = 0,
            }
            // C: SRF_RETURN_NEXT(funcctx, Int32GetDatum(result)):
            //    funcctx->call_cntr++; rsi->isDone = ExprMultipleResult;
            funcctx.call_cntr += 1;
            let rsi = fcinfo
                .resultinfo
                .as_mut()
                .expect("resultinfo present for an SRF call");
            rsi.isDone = ExprDoneCond::ExprMultipleResult;
            Ok((Some(result), ExprDoneCond::ExprMultipleResult))
        } else {
            // C: SRF_RETURN_DONE(funcctx):
            //    end_MultiFuncCall(fcinfo, funcctx); rsi->isDone = ExprEndResult;
            end_MultiFuncCall(fcinfo)?;
            let rsi = fcinfo
                .resultinfo
                .as_mut()
                .expect("resultinfo present for an SRF call");
            rsi.isDone = ExprDoneCond::ExprEndResult;
            Ok((None, ExprDoneCond::ExprEndResult))
        }
    }

    /// Build a value-per-call SRF call frame the way `ExecMakeFunctionResultSet`
    /// would: a `ReturnSetInfo` at `resultinfo` (ValuePerCall, isDone pre-set to
    /// ExprSingleResult by the caller) and the per-query context on the
    /// `fn_mcxt` channel.
    fn make_srf_frame<'mcx>(mcx: Mcx<'mcx>, args: &[i32]) -> FunctionCallInfoBaseData<'mcx> {
        FunctionCallInfoBaseData {
            resultinfo: Some(ReturnSetInfo {
                allowedModes: SFRM_ValuePerCall,
                isDone: ExprDoneCond::ExprSingleResult,
                ..Default::default()
            }),
            nargs: args.len() as i16,
            args: args
                .iter()
                .map(|&v| NullableDatum::value(types_datum::Datum::from_i32(v)))
                .collect(),
            fn_mcxt: Some(mcx),
            ..Default::default()
        }
    }

    #[test]
    fn value_per_call_series_runs_to_completion() {
        let ctx = MemoryContext::new("srf-proof per-query");
        let mcx = ctx.mcx();
        let mut fcinfo = make_srf_frame(mcx, &[1, 3]); // generate_series(1, 3)

        // First call: SRF_IS_FIRSTCALL true → init + stash; produces 1.
        let mut produced = Vec::new();
        loop {
            let (result, done) = generate_series_step_int4(mcx, &mut fcinfo).unwrap();
            match done {
                ExprDoneCond::ExprMultipleResult => {
                    produced.push(result.expect("a value on a producing call"));
                }
                ExprDoneCond::ExprEndResult => {
                    assert!(result.is_none(), "SRF_RETURN_DONE returns NULL");
                    break;
                }
                ExprDoneCond::ExprSingleResult => panic!("set SRF never returns single"),
            }
            assert!(produced.len() < 100, "series must terminate");
        }

        // generate_series(1, 3) = {1, 2, 3}.
        assert_eq!(produced, vec![1, 2, 3]);

        // After SRF_RETURN_DONE the fn_extra channel is unbound (C:
        // flinfo->fn_extra = NULL), so a fresh series could start again.
        assert!(fcinfo.fn_extra.is_none(), "fn_extra unbound after RETURN_DONE");
        // The caller-visible isDone reached ExprEndResult.
        assert_eq!(
            fcinfo.resultinfo.as_ref().unwrap().isDone,
            ExprDoneCond::ExprEndResult
        );
    }

    #[test]
    fn first_call_is_detected_via_fn_extra_channel() {
        let ctx = MemoryContext::new("srf-firstcall");
        let mcx = ctx.mcx();
        let mut fcinfo = make_srf_frame(mcx, &[5, 5]);

        // SRF_IS_FIRSTCALL() == (fn_extra == NULL).
        assert!(fcinfo.fn_extra.is_none());
        init_MultiFuncCall(&mut fcinfo).unwrap();
        assert!(fcinfo.fn_extra.is_some(), "init binds fn_extra");
        // The multi-call context is owned by the FuncCallContext.
        assert!(fcinfo
            .fn_extra
            .as_ref()
            .unwrap()
            .multi_call_memory_ctx
            .is_some());

        // A second init is the C "cannot be called more than once" error.
        let err = init_MultiFuncCall(&mut fcinfo).unwrap_err();
        assert_eq!(err.message(), "init_MultiFuncCall cannot be called more than once");
    }

    #[test]
    fn end_multifunccall_tears_down_the_context() {
        let ctx = MemoryContext::new("srf-end");
        let mcx = ctx.mcx();
        let mut fcinfo = make_srf_frame(mcx, &[1, 1]);

        init_MultiFuncCall(&mut fcinfo).unwrap();
        assert!(fcinfo.fn_extra.is_some());
        // SRF_RETURN_DONE's end_MultiFuncCall unbinds fn_extra and deletes the
        // multi-call context (drops the owned arena).
        end_MultiFuncCall(&mut fcinfo).unwrap();
        assert!(fcinfo.fn_extra.is_none(), "end unbinds fn_extra");
    }

    #[test]
    fn no_resultinfo_is_feature_not_supported() {
        let ctx = MemoryContext::new("srf-norsi");
        let mcx = ctx.mcx();
        let mut fcinfo = FunctionCallInfoBaseData {
            fn_mcxt: Some(mcx),
            ..Default::default()
        };
        // C: rsinfo == NULL → ERRCODE_FEATURE_NOT_SUPPORTED.
        let err = init_MultiFuncCall(&mut fcinfo).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    }
}
