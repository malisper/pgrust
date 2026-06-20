//! The executor-frame set-returning-function dispatch table.
//!
//! C's `FunctionCallInvoke(fcinfo)` is `fcinfo->flinfo->fn_addr(fcinfo)`: the
//! same `PGFunction` callable receives ordinary AND set-returning calls, and
//! the `resultinfo` field carries the live `ReturnSetInfo` for the latter. The
//! owned model has two `FunctionCallInfoBaseData` homes (WONTFIX dual-home,
//! DESIGN_DEBT): the by-OID builtin registry (`backend_utils_fmgr_core`) holds
//! `types_fmgr::PGFunction`s whose frame's `resultinfo` is a tag-only carrier.
//! An SRF dispatched through it can never see a LIVE `ReturnSetInfo`.
//!
//! This table is the executor-frame counterpart of `fmgr_builtins[]`: it maps a
//! function OID to a [`types_nodes::execexpr::PGFunction`] (`for<'mcx> fn(&mut
//! FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx>`), the frame that DOES carry
//! the live `ReturnSetInfo`. `ExecMakeTableFunctionResult` /
//! `ExecMakeFunctionResultSet` dispatch through it — exactly C's `fn_addr` over
//! the executor frame. SRFs register their executor-frame core here from their
//! own `init_seams` (e.g. `generate_series_int4/int8`).
//!
//! Process-global, like the fmgr builtin registry (`thread_local` to avoid a
//! `static mut`; the single-user backend has one thread).

extern crate alloc;

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use backend_utils_error::ereport;
use types_core::Oid;
use types_error::error::ERRCODE_UNDEFINED_FUNCTION;
use types_error::{PgResult, ERROR};
use types_nodes::execexpr::SrfFunction;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

/// Process-global, matching the seam registry's `OnceLock` model (NOT
/// thread-local): the single-user backend dispatches on one thread, and the
/// registry must be visible to whatever thread runs the dispatch. The stored
/// `PGFunction` is a plain `fn` pointer (`Send + Sync`).
fn table() -> &'static Mutex<HashMap<Oid, SrfFunction>> {
    static SRF_TABLE: OnceLock<Mutex<HashMap<Oid, SrfFunction>>> = OnceLock::new();
    SRF_TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register an executor-frame set-returning function under its `pg_proc` OID
/// (the executor-frame counterpart of adding a `fmgr_builtins[]` row). Returns
/// the previous registration if the OID was already present.
pub fn register_srf(foid: Oid, func: SrfFunction) -> Option<SrfFunction> {
    table().lock().expect("SRF table lock").insert(foid, func)
}

/// Whether an OID has an executor-frame SRF registered.
pub fn srf_is_registered(foid: Oid) -> bool {
    table().lock().expect("SRF table lock").contains_key(&foid)
}

/// The outcome of dispatching one SRF call frame: either a builtin
/// executor-frame `PGFunction` produced a per-call (or in-frame materialize)
/// `Datum` result, or a USER (plpgsql/SQL) function ran the SFRM_Materialize
/// protocol through the fmgr path and filled a [`types_fmgr::mat_srf::MatSrfSink`]
/// with the whole tuplestore.
pub enum SrfDispatch<'mcx> {
    /// A builtin executor-frame SRF ran over the live frame (its `resultinfo`
    /// carries the `ReturnSetInfo` it read/wrote). The `Datum` is its result
    /// word (value-per-call) or the materialize-mode sentinel.
    Builtin(Datum<'mcx>),
    /// A non-builtin (USER plpgsql/SQL) SETOF function ran in materialize mode
    /// through the fmgr dispatch (`function_call_invoke_datum` ->
    /// fmgr_sql / plpgsql_call_handler) and filled the materialize sink with the
    /// complete row set + column-type descriptor.
    Materialized(types_fmgr::mat_srf::MatSrfSink),
}

/// `FunctionCallInvoke(fcinfo)` for a set-returning function (execSRF.c) —
/// resolve `foid` in the executor-frame SRF table and dispatch the callable
/// over the LIVE call frame (whose `resultinfo` carries the `ReturnSetInfo` the
/// callee reads/writes). For an OID that has no executor-frame SRF registered
/// (a USER plpgsql/SQL function — the C `fmgr_isbuiltin` miss for this ABI),
/// fall through to the fmgr `FunctionCallInvoke` path with a live materialize
/// sink, exactly as C's `ExecMakeTableFunctionResult` points
/// `fcinfo->resultinfo` at a `ReturnSetInfo` and lets `fmgr_sql` /
/// `plpgsql_call_handler` fill `setResult`/`setDesc`.
pub fn srf_invoke_by_oid<'mcx>(
    foid: Oid,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<SrfDispatch<'mcx>> {
    let func = table().lock().expect("SRF table lock").get(&foid).copied();
    match func {
        Some(f) => Ok(SrfDispatch::Builtin(f(fcinfo)?)),
        None => dispatch_user_setof(foid, fcinfo),
    }
}

/// Dispatch a USER (plpgsql / SQL-language) SETOF function through the fmgr
/// by-OID path, threading the live materialize sink. The executor frame already
/// holds the evaluated args (`ExecEvalFuncArgs`); reconstruct the canonical
/// `Datum` arg vector C's `FunctionCallInvoke` would pass, push the sink with
/// the caller's `allowedModes`, resolve+invoke the function (which reaches
/// `fmgr_sql`/`plpgsql_call_handler`, sees the active sink, and materializes),
/// then take the filled sink back.
fn dispatch_user_setof<'mcx>(
    foid: Oid,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<SrfDispatch<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;

    // The per-call memory context the resolved fmgr call charges its scratch to
    // (and the arena a by-reference canonical argument image is cloned into).
    let mcx = fcinfo
        .fn_mcxt
        .expect("dispatch_user_setof: fn_mcxt set by ExecMakeTableFunctionResult");
    let collation = fcinfo.fncollation;

    // Reconstruct the canonical argument vector from the executor call frame:
    // a by-value arg is its bare word; a by-reference arg's owned image lives in
    // the `ref_args[i]` side channel (the same split `ExecEvalFuncArgs` wrote).
    let nargs = fcinfo.args.len();
    let mut args: alloc::vec::Vec<CanonDatum<'mcx>> = alloc::vec::Vec::with_capacity(nargs);
    let mut nulls: alloc::vec::Vec<bool> = alloc::vec::Vec::with_capacity(nargs);
    for i in 0..nargs {
        let isnull = fcinfo.args[i].isnull;
        nulls.push(isnull);
        if isnull {
            args.push(CanonDatum::null());
            continue;
        }
        match fcinfo.ref_arg(i) {
            Some(types_nodes::fmgr::FmgrArgRef::Varlena(b)) => {
                args.push(CanonDatum::ByRef(mcx::slice_in(mcx, b.as_slice())?));
            }
            Some(types_nodes::fmgr::FmgrArgRef::Cstring(s)) => {
                args.push(CanonDatum::Cstring(s.clone()));
            }
            None => {
                args.push(CanonDatum::ByVal(fcinfo.args[i].value.as_usize()));
            }
        }
    }

    // The allowed return modes the caller (ExecMakeTableFunctionResult) set on
    // its live ReturnSetInfo — the materialize sink mirrors them so the callee
    // can verify `allowedModes & SFRM_Materialize`.
    let allowed_modes = fcinfo
        .resultinfo
        .as_ref()
        .map(|r| r.allowedModes)
        .unwrap_or(0);

    // Push the live materialize sink (C: point fcinfo->resultinfo at the
    // ReturnSetInfo) for the duration of the call; the RAII guard pops it even
    // if the callee `ereport(ERROR)`s (unwinds).
    let guard = types_fmgr::mat_srf::push(allowed_modes);

    // FunctionCallInvoke over the fmgr home: resolves the OID (plpgsql/SQL ->
    // fmgr_sql / plpgsql_call_handler) and runs the body. For a materialize SETOF
    // function the rows arrive via the sink and the scalar word is the NULL
    // sentinel; for a NON-set function in the FROM clause (a single-row table
    // function — C still drives it through ExecMakeTableFunctionResult, with the
    // ValuePerCall path delivering one row) the scalar word IS the single result.
    let invoke = backend_utils_fmgr_fmgr_seams::function_call_invoke_datum::call(
        mcx, foid, collation, &args, &nulls, None,
    );

    let sink = guard.take();
    let (result, result_isnull) = invoke?;

    if sink.materialized {
        // A SETOF function delivered its whole result set into the sink.
        return Ok(SrfDispatch::Materialized(sink));
    }

    // The function did NOT materialize: it is a non-set function reached through
    // the table-function path (RETURNS <scalar|composite>, one row). Hand the
    // single scalar/composite Datum back through the ValuePerCall branch — the
    // caller sees `returnMode == SFRM_ValuePerCall` / `isDone == ExprSingleResult`
    // (the live ReturnSetInfo's defaults, untouched) and stores exactly one row.
    fcinfo.isnull = result_isnull;
    Ok(SrfDispatch::Builtin(result))
}
