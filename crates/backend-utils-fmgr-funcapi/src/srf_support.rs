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
    let (fn_oid, call_expr) = backend_utils_fmgr_fmgr_seams::fn_oid_and_expr::call(fcinfo_ref);

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
            let resolved = internal_get_result_type(mcx, fn_oid, call_expr, rsinfo_for_resolve)?;
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
pub fn init_MultiFuncCall<'mcx>(
    _mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<FuncCallContext<'mcx>> {
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
    // This whole body lives on `fcinfo->flinfo`: the once-only guard reads/writes
    // `flinfo->fn_extra`, the multi-call context is created under
    // `flinfo->fn_mcxt`, and the shutdown callback is registered against
    // `rsi->econtext` keyed by `PointerGetDatum(flinfo)`. The trimmed call frame
    // carries no `flinfo` and the trimmed `ReturnSetInfo` no `econtext`, so none
    // of it is reachable from the ported shape. Mirror PG and panic at that
    // boundary (it lands when the fmgr call frame + ExprContext widen here).
    panic!(
        "init_MultiFuncCall: fn_extra / fn_mcxt (fcinfo->flinfo) and the \
         ExprContext (rsinfo->econtext) for the multi-call context and shutdown \
         callback are not reachable from the trimmed call frame; widen the fmgr \
         call frame (flinfo) and ReturnSetInfo (econtext) here"
    )
}

/// `per_MultiFuncCall(PG_FUNCTION_ARGS)` (funcapi.c:208) — return the
/// `FuncCallContext` saved in `flinfo->fn_extra` for the current per-call step.
pub fn per_MultiFuncCall<'mcx>(
    _fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<FuncCallContext<'mcx>> {
    // C funcapi.c:208-214:
    //   FuncCallContext *retval = (FuncCallContext *) fcinfo->flinfo->fn_extra;
    //   return retval;
    //
    // The cross-call state lives in `fcinfo->flinfo->fn_extra`, which the
    // trimmed call frame does not carry. Mirror PG and panic (lands with the
    // fmgr call frame widening here).
    panic!(
        "per_MultiFuncCall: the FuncCallContext in fcinfo->flinfo->fn_extra is \
         not reachable from the trimmed call frame; widen the fmgr call frame \
         (flinfo) here"
    )
}

/// `end_MultiFuncCall(PG_FUNCTION_ARGS, funcctx)` (funcapi.c:220) — deregister
/// the shutdown callback and run [`shutdown_MultiFuncCall`] to tear down the
/// multi-call context.
pub fn end_MultiFuncCall<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    funcctx: &mut FuncCallContext<'mcx>,
) -> PgResult<()> {
    // C funcapi.c:220-235:
    //   ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    //   /* Deregister the shutdown callback */
    //   UnregisterExprContextCallback(rsi->econtext, shutdown_MultiFuncCall,
    //                                 PointerGetDatum(fcinfo->flinfo));
    //   /* But use it to delete the context we are about to lose */
    //   shutdown_MultiFuncCall(PointerGetDatum(fcinfo->flinfo));
    //
    // Both the callback deregistration (against `rsi->econtext`, keyed by
    // `PointerGetDatum(fcinfo->flinfo)`) and the teardown reach `fcinfo->flinfo`
    // and `rsi->econtext`, neither carried by the trimmed shapes. Mirror PG and
    // panic at the boundary.
    let _ = (fcinfo, funcctx);
    panic!(
        "end_MultiFuncCall: the ExprContext (rsinfo->econtext) and flinfo \
         (fcinfo->flinfo) needed to unregister the shutdown callback and delete \
         the multi-call context are not reachable from the trimmed call frame; \
         widen the fmgr call frame (flinfo) and ReturnSetInfo (econtext) here"
    )
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
    // The C callback receives `PointerGetDatum(flinfo)` and recovers the
    // `FuncCallContext` from `flinfo->fn_extra`; the owned signature hands the
    // `FuncCallContext` directly. Both the `flinfo->fn_extra = NULL` unbind and
    // the `MemoryContextDelete(funcctx->multi_call_memory_ctx)` reach state the
    // trimmed shapes do not carry (`flinfo`, and `FuncCallContext` carries no
    // `multi_call_memory_ctx` — the scaffold notes it is owned by the SRF
    // plumbing seam). Mirror PG and panic at that boundary.
    let _ = funcctx;
    panic!(
        "shutdown_MultiFuncCall: the flinfo unbind (flinfo->fn_extra = NULL) and \
         the multi-call memory context deletion \
         (funcctx->multi_call_memory_ctx) are not reachable from the trimmed \
         shapes; widen the fmgr call frame (flinfo) and FuncCallContext \
         (multi_call_memory_ctx) here"
    )
}

/// `PG_ARGISNULL(0) ? None : Some(PG_GETARG_OID(0))` — read the optional
/// leading `Oid` argument of a SQL-callable function (the SRF-filter helper the
/// inward seam exposes). fmgr owns the `args`/`isnull` arrays.
pub fn srf_arg0_oid<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Option<Oid> {
    // C: PG_ARGISNULL(0) ? (no filter) : PG_GETARG_OID(0)
    //   #define PG_ARGISNULL(n)  (fcinfo->args[n].isnull)
    //   #define PG_GETARG_OID(n) DatumGetObjectId(fcinfo->args[n].value)
    //
    // The read needs `fcinfo->args[0]` (value + isnull), which the trimmed call
    // frame does not carry (it holds only `resultinfo`). Mirror PG and panic at
    // the call-frame boundary (lands with the fmgr call frame's `args`/`isnull`
    // widening here).
    let _ = fcinfo;
    panic!(
        "srf_arg0_oid: fcinfo->args[0] (value/isnull) is not reachable from the \
         trimmed call frame; widen the fmgr call frame (args/isnull) here"
    )
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

// `SetFunctionReturnMode::Materialize` is the `rsinfo->returnMode = SFRM_Materialize`
// store InitMaterializedSRF performs once the per-query context lands; named here
// so the enum stays referenced from the SRF module that sets it.
const _: SetFunctionReturnMode = SetFunctionReturnMode::Materialize;
