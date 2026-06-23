//! The executor-frame set-returning-function dispatch table.
//!
//! C's `FunctionCallInvoke(fcinfo)` is `fcinfo->flinfo->fn_addr(fcinfo)`: the
//! same `PGFunction` callable receives ordinary AND set-returning calls, and
//! the `resultinfo` field carries the live `ReturnSetInfo` for the latter. The
//! owned model has two `FunctionCallInfoBaseData` homes (WONTFIX dual-home,
//! DESIGN_DEBT): the by-OID builtin registry (`fmgr_core`) holds
//! `fmgr::PGFunction`s whose frame's `resultinfo` is a tag-only carrier.
//! An SRF dispatched through it can never see a LIVE `ReturnSetInfo`.
//!
//! This table is the executor-frame counterpart of `fmgr_builtins[]`: it maps a
//! function OID to a [`nodes::execexpr::PGFunction`] (`for<'mcx> fn(&mut
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

use utils_error::ereport;
use types_core::Oid;
use types_error::error::ERRCODE_UNDEFINED_FUNCTION;
use types_error::{PgResult, ERROR};
use nodes::execexpr::SrfFunction;
use nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

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

/// Resolve a built-in SRF's `prosrc` name (e.g. `"generate_series_int4"`) to its
/// executor-frame `PGFunction` core, mirroring C's `fmgr_lookupByName` over the
/// `fmgr_builtins[]` table — except that in C the SAME `PGFunction` serves both
/// scalar and set-returning calls, while here the executor-frame SRF core lives
/// in this OID-keyed table, NOT in the scalar `fmgr-core` by-name registry.
///
/// The bridge is the canonical `(oid, name, …, retset)` table (the runtime
/// counterpart of `Gen_fmgrtab.pl`'s `fmgr_builtins[]`): we map the `prosrc`
/// name to its canonical OID and then look that OID up in this registry. A name
/// that is not a registered executor-frame SRF returns `None` (the caller raises
/// C's `there is no built-in function named "%s"`).
///
/// This is the by-NAME half of the dual-home bridge: it lets `CREATE FUNCTION
/// ... LANGUAGE internal AS $$generate_series_int4$$` validate (the prosrc names
/// a real built-in SRF) and lets a call through a USER pg_proc OID re-dispatch
/// to the underlying built-in SRF core (see [`srf_resolve_by_oid_or_name`]).
pub fn srf_lookup_by_name(proname: &str) -> Option<SrfFunction> {
    let foid = srf_oid_for_name(proname)?;
    table().lock().expect("SRF table lock").get(&foid).copied()
}

/// The canonical OID a built-in SRF `prosrc` name maps to, iff that name is a
/// set-returning built-in (`retset == true`) in the canonical `fmgr_builtins[]`
/// counterpart. `None` for a non-set built-in name or an unknown name.
fn srf_oid_for_name(proname: &str) -> Option<Oid> {
    fmgr_core::builtin_canonical::CANONICAL
        .iter()
        .find(|&&(_oid, name, _nargs, _strict, retset)| retset && name == proname)
        .map(|&(oid, ..)| oid)
}

/// Whether `proname` names a registered built-in set-returning function — the
/// executor-frame counterpart of `OidIsValid(fmgr_internal_function(prosrc))`
/// for the SRF case. The CREATE FUNCTION `LANGUAGE internal` validator
/// (`fmgr_internal_validator`) consults this so an SRF prosrc passes the
/// `there is no built-in function named "%s"` gate.
pub fn srf_name_is_builtin(proname: &str) -> bool {
    srf_lookup_by_name(proname).is_some()
}

/// The outcome of dispatching one SRF call frame: either a builtin
/// executor-frame `PGFunction` produced a per-call (or in-frame materialize)
/// `Datum` result, or a USER (plpgsql/SQL) function ran the SFRM_Materialize
/// protocol through the fmgr path and filled a [`fmgr::mat_srf::MatSrfSink`]
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
    Materialized(fmgr::mat_srf::MatSrfSink),
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
        // An OID with no executor-frame SRF registered is one of:
        //   (a) a USER `LANGUAGE internal` function whose `prosrc` names a
        //       built-in SRF (e.g. `CREATE FUNCTION my_gen_series(...) ...
        //       AS $$generate_series_int4$$`). C's `fmgr_info` resolves the
        //       prosrc to the built-in's address; here we re-dispatch through
        //       the by-NAME bridge to the underlying built-in SRF core, running
        //       it over the SAME live frame (its `resultinfo` carries the
        //       `ReturnSetInfo`).
        //   (b) a USER plpgsql/SQL SETOF function (the C `fmgr_isbuiltin` miss),
        //       which materializes through the fmgr path.
        None => match resolve_internal_srf_core(foid) {
            Some(f) => Ok(SrfDispatch::Builtin(f(fcinfo)?)),
            None => dispatch_user_setof(foid, fcinfo),
        },
    }
}

/// For an OID absent from the executor-frame SRF table, read its `pg_proc`
/// language + `prosrc`; if it is a `LANGUAGE internal` function whose `prosrc`
/// names a registered built-in SRF, return that built-in's executor-frame core.
/// `None` for any other case (plpgsql/SQL/C function, or a prosrc that is not a
/// known built-in SRF). This is the call-time counterpart of C's `fmgr_info`
/// setting `finfo->fn_addr` to the built-in's address for a `LANGUAGE internal`
/// pg_proc row — the dual-home bridge resolving the USER OID to the shared core.
fn resolve_internal_srf_core(foid: Oid) -> Option<SrfFunction> {
    use fmgr::resolution::ProcLanguage;

    let scratch = mcx::MemoryContext::new("resolve_internal_srf_core");
    let proc =
        syscache_seams::lookup_proc::call(scratch.mcx(), foid).ok()??;
    if !matches!(proc.language, ProcLanguage::Internal) {
        return None;
    }
    let prosrc = proc.prosrc.as_ref().map(|s| s.as_str())?;
    srf_lookup_by_name(prosrc)
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
    use types_tuple::heaptuple::Datum as CanonDatum;

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
            Some(nodes::fmgr::FmgrArgRef::Varlena(b)) => {
                args.push(CanonDatum::ByRef(mcx::slice_in(mcx, b.as_slice())?));
            }
            Some(nodes::fmgr::FmgrArgRef::Cstring(s)) => {
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
    let guard = fmgr::mat_srf::push(allowed_modes);

    // Carry the caller's `expectedDesc` (the FunctionScan coldeflist) columns
    // into the sink so a `RETURNS [SETOF] record` SQL function can resolve its
    // target rowtype via `get_call_result_type` (C's `internal_get_result_type`
    // reads `rsinfo->expectedDesc` for the TYPEFUNC_RECORD case). Without this
    // the body's output (e.g. int columns) is never coerced to the declared
    // coldeflist (e.g. numeric(4,2)) and the materialized row mismatches the
    // descriptor at deform time.
    if let Some(rsi) = fcinfo.resultinfo.as_ref() {
        if let Some(exp) = rsi.expectedDesc.as_deref() {
            let cols: alloc::vec::Vec<fmgr::mat_srf::MatDescCol> = (0..exp
                .natts
                .max(0) as usize)
                .map(|i| {
                    let a = &exp.attrs[i];
                    fmgr::mat_srf::MatDescCol {
                        name: alloc::string::String::from_utf8_lossy(a.attname.name_str())
                            .into_owned(),
                        typid: a.atttypid,
                        typmod: a.atttypmod,
                        collation: a.attcollation,
                    }
                })
                .collect();
            fmgr::mat_srf::with_top(|sink| {
                if let Some(sink) = sink {
                    sink.expected_desc_cols = cols;
                }
            });
        }
    }

    // The call-expression node `ExecMakeTableFunctionResult` stamped onto the
    // frame's `flinfo->fn_expr` (`fmgr_info_set_expr`). The by-OID re-dispatch
    // below re-resolves the `FmgrInfo` and would otherwise drop it; thread it
    // through so a polymorphic SQL/plpgsql table function can resolve its actual
    // argument types (`prepare_sql_fn_parse_info` / `get_fn_expr_argtype`) — C's
    // `init_sql_fcache` reads `finfo->fn_expr`. Without this a polymorphic SQL
    // function over `anymultirange`/`anyrange` args fails its body type-check.
    let fn_expr = fcinfo.flinfo.as_ref().and_then(|f| f.fn_expr.clone());

    // FunctionCallInvoke over the fmgr home: resolves the OID (plpgsql/SQL ->
    // fmgr_sql / plpgsql_call_handler) and runs the body. For a materialize SETOF
    // function the rows arrive via the sink and the scalar word is the NULL
    // sentinel; for a NON-set function in the FROM clause (a single-row table
    // function — C still drives it through ExecMakeTableFunctionResult, with the
    // ValuePerCall path delivering one row) the scalar word IS the single result.
    let invoke = fmgr_seams::function_call_invoke_datum::call(
        mcx, foid, collation, &args, &nulls, fn_expr,
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
