//! `backend-utils-fmgr-core` — the PostgreSQL function manager
//! calling-convention core (`src/backend/utils/fmgr/fmgr.c`).
//!
//! Prepares and invokes functions by Oid in owned-value Rust: no raw pointers,
//! no `extern "C"`. The giant generated `fmgrtab.c` built-in table is replaced
//! by a per-backend REGISTRY the embedder populates as built-ins get ported; an
//! unregistered Oid behaves exactly as a `fmgr_builtin_oid_index` miss. The
//! catalog/syscache, dynamic-library loading, GUC, userid, ACL, and node-expr
//! introspection externals are reached through per-owner seam crates.
//!
//! Errors surface as [`types_error::PgResult`] / [`PgError`] mirroring the C
//! `ereport`/`elog` text and SQLSTATE instead of `longjmp`; allocation goes
//! through the caller-provided [`Mcx`] (OOM is `Err`).
//!
//! `pg_detoast_datum` / `_copy` / `_slice` / `_packed` in `fmgr.c` are
//! one-liners delegating to TOAST (`detoast_attr*`); the faithful port belongs
//! to the varlena/Detoast subsystem and is intentionally not reproduced here.

#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use std::cell::RefCell;
use std::collections::HashMap;

use mcx::{Mcx, MemoryContext, PgString, PgVec};

use types_acl::{AclMode, AclResult, ACL_EXECUTE, ACL_USAGE};
use types_core::init::SECURITY_LOCAL_USERID_CHANGE;
use types_core::{InvalidOid, Oid, TransactionId};
use types_datum::{Datum, NullableDatum};
use types_tuple::heaptuple::ItemPointerData;
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_FUNCTION,
};
use types_fmgr::boundary::{FmgrArg, FmgrOut, RefPayload};
use types_fmgr::{
    AclObjectType, BuiltinFunction, FmgrHookEventType, FmgrInfo, FmgrResolution, FnExpr,
    FunctionCallInfoBaseData, LangInfo, LoadedCFunc, PGFunction, ProcInfo, ProcLanguage,
    ResolvedFmgrInfo, TRACK_FUNC_ALL, TRACK_FUNC_OFF, TRACK_FUNC_PL,
};
use types_guc::GucContext;
use types_nodes::parsenodes::ObjectType;

pub mod datum_ref_registry;

/// C: `BYTEAOID` (`pg_type.h`) — the `consttype` of the opclass-options `Const`.
pub const BYTEAOID: Oid = 17;
/// C: `ProcedureRelationId` (`pg_proc` relation Oid).
pub const PROCEDURE_RELATION_ID: Oid = 1255;
/// C: `LanguageRelationId` (`pg_language` relation Oid).
pub const LANGUAGE_RELATION_ID: Oid = 2612;
/// C: `InvalidOidBuiltinMapping`, the `(uint16) -1` sentinel for a non-builtin
/// Oid in `fmgr_builtin_oid_index[]`. Kept for documentation parity; the
/// registry expresses the same idea with a map miss.
pub const INVALID_OID_BUILTIN_MAPPING: u16 = u16::MAX;

// ===========================================================================
// Built-in function REGISTRY (C: fmgr_builtins[] / fmgr_builtin_oid_index[]).
//
// Per-backend (thread_local, never a shared static): the registered built-in
// set is backend-private state, populated at backend init.
// ===========================================================================

#[derive(Default)]
struct BuiltinRegistry {
    by_oid: HashMap<Oid, BuiltinFunction>,
    by_name: HashMap<String, Oid>,
    /// C: `fmgr_last_builtin_oid`.
    last_builtin_oid: Oid,
}

thread_local! {
    static REGISTRY: RefCell<BuiltinRegistry> = RefCell::new(BuiltinRegistry::default());
}

/// Register a single built-in function. Re-registering an Oid replaces the prior
/// row; returns the previous registration, if any.
pub fn register_builtin(entry: BuiltinFunction) -> Option<BuiltinFunction> {
    REGISTRY.with(|r| {
        let mut reg = r.borrow_mut();
        reg.by_name.insert(entry.name.clone(), entry.foid);
        if entry.foid > reg.last_builtin_oid {
            reg.last_builtin_oid = entry.foid;
        }
        reg.by_oid.insert(entry.foid, entry)
    })
}

/// Bulk-register built-ins.
pub fn register_builtins(entries: impl IntoIterator<Item = BuiltinFunction>) {
    for e in entries {
        register_builtin(e);
    }
}

/// Clear the registry (test/re-init support; no C analogue).
pub fn clear_builtins() {
    REGISTRY.with(|r| *r.borrow_mut() = BuiltinRegistry::default());
}

/// C: `fmgr_last_builtin_oid`.
pub fn fmgr_last_builtin_oid() -> Oid {
    REGISTRY.with(|r| r.borrow().last_builtin_oid)
}

/// C: `fmgr_nbuiltins`.
pub fn fmgr_nbuiltins() -> usize {
    REGISTRY.with(|r| r.borrow().by_oid.len())
}

/// Port of `fmgr_isbuiltin`.
///
/// C: `if (id > fmgr_last_builtin_oid) return NULL; index =
/// fmgr_builtin_oid_index[id]; if (index == InvalidOidBuiltinMapping) return
/// NULL; return &fmgr_builtins[index];`. The `id > last` early-out and the
/// index-miss both map to `None`.
pub fn fmgr_isbuiltin(id: Oid) -> Option<BuiltinFunction> {
    REGISTRY.with(|r| {
        let reg = r.borrow();
        if id > reg.last_builtin_oid {
            return None;
        }
        reg.by_oid.get(&id).cloned()
    })
}

/// Port of `fmgr_lookupByName` (a linear `strcmp` scan in C).
pub fn fmgr_lookup_by_name(name: &str) -> Option<BuiltinFunction> {
    REGISTRY.with(|r| {
        let reg = r.borrow();
        let oid = *reg.by_name.get(name)?;
        reg.by_oid.get(&oid).cloned()
    })
}

/// Port of `fmgr_internal_function`.
///
/// C: `fbp = fmgr_lookupByName(proname); if (fbp == NULL) return InvalidOid;
/// return fbp->foid;`
pub fn fmgr_internal_function(proname: &str) -> Oid {
    match fmgr_lookup_by_name(proname) {
        Some(fbp) => fbp.foid,
        None => InvalidOid,
    }
}

// ===========================================================================
// fcinfo helpers (C: InitFunctionCallInfoData + the args[] flexible array).
// ===========================================================================

/// C: `InitFunctionCallInfoData(*fcinfo, flinfo, nargs, collation, NULL, NULL)`
/// then filling `args[]`. `flinfo` is the optional caller lookup-info frame
/// (`None` is C's NULL, as in `DirectFunctionCall*`).
fn init_fcinfo(
    flinfo: Option<FmgrInfo>,
    collation: Oid,
    args: Vec<NullableDatum>,
) -> FunctionCallInfoBaseData {
    let nargs = args.len() as i16;
    let mut fcinfo =
        FunctionCallInfoBaseData::new(flinfo.map(Box::new), nargs, collation, None, None);
    fcinfo.args = args;
    fcinfo
}

/// C: `PG_GETARG_DATUM(i)` — the raw datum word of arg `i` (or `0` past nargs).
/// Meaningful only for a pass-by-value argument (invariant (2)).
pub fn arg_value(fcinfo: &FunctionCallInfoBaseData, index: usize) -> Datum {
    fcinfo.args.get(index).map_or(Datum::null(), |d| d.value)
}

// ===========================================================================
// FunctionCallInvoke dispatch (C: the FunctionCallInvoke macro).
// ===========================================================================

/// Port of `FunctionCallInvoke(fcinfo)`: `(*fcinfo->flinfo->fn_addr)(fcinfo)`.
/// The callable is carried by the [`FmgrResolution`] produced during
/// `fmgr_info`. A security-definer resolution dispatches to
/// [`fmgr_security_definer`], threading `fn_expr` (fmgr.c:658) and the caller's
/// `Mcx` for the cache.
pub fn function_call_invoke_with_expr(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    fcinfo: &mut FunctionCallInfoBaseData,
    fn_expr: Option<Box<FnExpr>>,
) -> PgResult<Datum> {
    match res {
        FmgrResolution::Builtin(b)
        | FmgrResolution::InternalByName(b)
        | FmgrResolution::CLanguage(b) => invoke_pgfunction(&b.func, fcinfo),
        FmgrResolution::SecurityDefiner { fn_oid } => {
            fmgr_security_definer(mcx, *fn_oid, fn_expr, fcinfo)
        }
    }
}

/// Equivalent to [`function_call_invoke_with_expr`] with a NULL `fn_expr`.
pub fn function_call_invoke(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    function_call_invoke_with_expr(mcx, res, fcinfo, None)
}

// ===========================================================================
// CURRENT-FCINFO tracking (thread_local owned snapshot, RAII pop).
// ===========================================================================

/// An owned SNAPSHOT of the fields a called function reads *through* `fcinfo`
/// (`fcinfo->context`, `fcinfo->flinfo->fn_oid`, `fcinfo->nargs`,
/// `fcinfo->fncollation`). A `thread_local` raw pointer would alias the
/// exclusive `&mut fcinfo` the dispatch hands the callee, so the snapshot is
/// pushed for the duration of the call instead.
#[derive(Clone, Debug)]
pub struct CurrentFcinfo {
    /// C: `fcinfo->flinfo->fn_oid` (`InvalidOid` when `flinfo` is NULL).
    pub fn_oid: Oid,
    /// C: whether `fcinfo->flinfo->fn_expr` is non-NULL.
    pub has_fn_expr: bool,
    /// C: `nodeTag(fcinfo->context)` a context-demuxing callee switches on.
    pub context_tag: Option<u32>,
    /// C: `fcinfo->nargs`.
    pub nargs: i16,
    /// C: `fcinfo->fncollation` (`PG_GET_COLLATION()`).
    pub collation: Oid,
}

thread_local! {
    /// The per-thread stack of in-flight fmgr calls (nested via recursive
    /// dispatch). Innermost call last.
    static CURRENT_FCINFO: RefCell<Vec<CurrentFcinfo>> = const { RefCell::new(Vec::new()) };
}

/// RAII pop for [`CURRENT_FCINFO`] — panic-safe (a panicking `PGFunction`
/// unwinds through this guard, so the stack never leaks a dead frame).
struct CurrentFcinfoGuard;

impl Drop for CurrentFcinfoGuard {
    fn drop(&mut self) {
        CURRENT_FCINFO.with(|s| {
            s.borrow_mut().pop();
        });
    }
}

fn push_current_fcinfo(fcinfo: &FunctionCallInfoBaseData) -> CurrentFcinfoGuard {
    let snapshot = CurrentFcinfo {
        fn_oid: fcinfo.flinfo.as_ref().map_or(InvalidOid, |f| f.fn_oid),
        has_fn_expr: fcinfo.flinfo.as_ref().is_some_and(|f| f.fn_expr.is_some()),
        context_tag: fcinfo.context.as_ref().map(|n| n.tag),
        nargs: fcinfo.nargs,
        collation: fcinfo.fncollation,
    };
    CURRENT_FCINFO.with(|s| s.borrow_mut().push(snapshot));
    CurrentFcinfoGuard
}

/// Run `f` against the innermost in-flight fmgr call's snapshot (`None` when no
/// fmgr dispatch is on this thread's stack). The snapshot is cloned out before
/// `f` runs, so `f` may itself dispatch fmgr calls.
pub fn with_current_fcinfo<R>(f: impl FnOnce(Option<&CurrentFcinfo>) -> R) -> R {
    let snapshot = CURRENT_FCINFO.with(|s| s.borrow().last().cloned());
    f(snapshot.as_ref())
}

/// Invoke a resolved safe `PGFunction` (C: `(*fn_addr)(fcinfo)`). A `None`
/// callable is the safe-port stand-in for a NULL `fn_addr`, which C never
/// dispatches.
///
/// A `PGFunction` returns a bare `Datum` (no error channel), so a `*_v1`
/// wrapper's hard error arrives as a message panic; catch it here — the one
/// dispatch point every builtin crosses — and surface it as the structured
/// `PgResult` error C's `ereport` longjmp delivers. A non-string payload (a
/// genuine programming-error panic) is re-raised unchanged.
fn invoke_pgfunction(func: &PGFunction, fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    match func {
        Some(f) => {
            let _current = push_current_fcinfo(fcinfo);
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(fcinfo))) {
                Ok(d) => Ok(d),
                Err(payload) => {
                    let msg = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()));
                    match msg {
                        // A structured *_v1 hard error: "PGRUST-SQLSTATE:XXXXX:<msg>".
                        Some(m) => match m.strip_prefix("PGRUST-SQLSTATE:") {
                            Some(rest) if rest.len() > 6 && rest.as_bytes()[5] == b':' => {
                                let (code, msg) = rest.split_at(5);
                                let mut chars = [0u8; 5];
                                chars.copy_from_slice(code.as_bytes());
                                Err(PgError::error(msg[1..].to_string())
                                    .with_sqlstate(types_error::make_sqlstate(chars)))
                            }
                            _ => Err(PgError::error(m)),
                        },
                        None => std::panic::resume_unwind(payload),
                    }
                }
            }
        }
        None => Err(PgError::error("function pointer is NULL")),
    }
}

/// The post-call NULL check shared by the `*FunctionCallNColl` family. C uses a
/// plain `elog(ERROR, "function %u returned NULL")` (default internal SQLSTATE).
fn null_check(
    fcinfo: &FunctionCallInfoBaseData,
    result: Datum,
    name_for_error: &str,
) -> PgResult<Datum> {
    if fcinfo.result_is_null() {
        return Err(PgError::error(format!(
            "function {name_for_error} returned NULL"
        )));
    }
    Ok(result)
}

/// Run a built-in directly (the `DirectFunctionCallNColl` invoke + NULL check).
/// C names the error with `%p` (the function pointer); the safe port uses
/// `<direct>` since it holds no pointer.
fn invoke_direct(func: &PGFunction, fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let result = invoke_pgfunction(func, fcinfo)?;
    null_check(fcinfo, result, "<direct>")
}

/// Run a resolved function through `flinfo` (`FunctionCallNColl` invoke + NULL
/// check). C names the error with `%u` (`flinfo->fn_oid`).
fn invoke_flinfo(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    fcinfo: &mut FunctionCallInfoBaseData,
    oid: Oid,
    fn_expr: Option<Box<FnExpr>>,
) -> PgResult<Datum> {
    let result = function_call_invoke_with_expr(mcx, res, fcinfo, fn_expr)?;
    null_check(fcinfo, result, &oid.to_string())
}

// ===========================================================================
// DirectFunctionCall{1..9}Coll family.
// ===========================================================================

macro_rules! direct_function_call {
    ($name:ident, $n:expr, $($arg:ident),*) => {
        #[doc = concat!("Port of `DirectFunctionCall", stringify!($n), "Coll`.")]
        pub fn $name(func: &PGFunction, collation: Oid, $($arg: Datum),*) -> PgResult<Datum> {
            let mut fcinfo = init_fcinfo(None, collation, vec![$(NullableDatum::value($arg)),*]);
            invoke_direct(func, &mut fcinfo)
        }
    };
}

direct_function_call!(direct_function_call1_coll, 1, arg1);
direct_function_call!(direct_function_call2_coll, 2, arg1, arg2);
direct_function_call!(direct_function_call3_coll, 3, arg1, arg2, arg3);
direct_function_call!(direct_function_call4_coll, 4, arg1, arg2, arg3, arg4);
direct_function_call!(direct_function_call5_coll, 5, arg1, arg2, arg3, arg4, arg5);
direct_function_call!(direct_function_call6_coll, 6, arg1, arg2, arg3, arg4, arg5, arg6);
direct_function_call!(direct_function_call7_coll, 7, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
direct_function_call!(direct_function_call8_coll, 8, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
direct_function_call!(
    direct_function_call9_coll, 9, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9
);

/// Port of `CallerFInfoFunctionCall1`. Threads the caller's `flinfo` through
/// `fcinfo->flinfo` (so the callee may read `fn_extra`/`fn_mcxt`). C names the
/// NULL-result error with `%p`; the safe port uses `<direct>`.
pub fn caller_finfo_function_call1(
    func: &PGFunction,
    flinfo: FmgrInfo,
    collation: Oid,
    arg1: Datum,
) -> PgResult<Datum> {
    let mut fcinfo = init_fcinfo(Some(flinfo), collation, vec![NullableDatum::value(arg1)]);
    invoke_direct(func, &mut fcinfo)
}

/// Port of `CallerFInfoFunctionCall2`.
pub fn caller_finfo_function_call2(
    func: &PGFunction,
    flinfo: FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
) -> PgResult<Datum> {
    let mut fcinfo = init_fcinfo(
        Some(flinfo),
        collation,
        vec![NullableDatum::value(arg1), NullableDatum::value(arg2)],
    );
    invoke_direct(func, &mut fcinfo)
}

// ===========================================================================
// FunctionCall{0..9}Coll family.
// ===========================================================================

/// Port of `FunctionCall0Coll`.
pub fn function_call0_coll(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    collation: Oid,
) -> PgResult<Datum> {
    let oid = flinfo.fn_oid;
    // C: fcache->flinfo.fn_expr = fcinfo->flinfo->fn_expr (fmgr.c:658) — thread
    // the caller's fn_expr before it is moved into fcinfo.
    let fn_expr = flinfo.fn_expr.clone();
    let mut fcinfo = init_fcinfo(Some(flinfo), collation, vec![]);
    invoke_flinfo(mcx, res, &mut fcinfo, oid, fn_expr)
}

macro_rules! function_call {
    ($name:ident, $n:expr, $($arg:ident),*) => {
        #[doc = concat!("Port of `FunctionCall", stringify!($n), "Coll`.")]
        pub fn $name(
            mcx: Mcx<'_>,
            res: &FmgrResolution,
            flinfo: FmgrInfo,
            collation: Oid,
            $($arg: Datum),*
        ) -> PgResult<Datum> {
            let oid = flinfo.fn_oid;
            let fn_expr = flinfo.fn_expr.clone();
            let mut fcinfo =
                init_fcinfo(Some(flinfo), collation, vec![$(NullableDatum::value($arg)),*]);
            invoke_flinfo(mcx, res, &mut fcinfo, oid, fn_expr)
        }
    };
}

function_call!(function_call1_coll, 1, arg1);
function_call!(function_call2_coll, 2, arg1, arg2);
function_call!(function_call3_coll, 3, arg1, arg2, arg3);
function_call!(function_call4_coll, 4, arg1, arg2, arg3, arg4);
function_call!(function_call5_coll, 5, arg1, arg2, arg3, arg4, arg5);
function_call!(function_call6_coll, 6, arg1, arg2, arg3, arg4, arg5, arg6);
function_call!(function_call7_coll, 7, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
function_call!(function_call8_coll, 8, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
function_call!(function_call9_coll, 9, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);

/// `FunctionCallNColl` generalized over the Option-4 by-reference argument side
/// channel: `args[i]` is the by-value word lane (a placeholder for a by-ref
/// arg), `ref_args[i]` is the owned referent for the non-NULL by-ref args.
/// `args.len() == ref_args.len()`.
pub fn function_call_coll_ref_args(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    collation: Oid,
    args: Vec<NullableDatum>,
    ref_args: Vec<Option<RefPayload>>,
) -> PgResult<Datum> {
    Ok(function_call_coll_ref_args_out(mcx, res, flinfo, collation, args, ref_args)?.0)
}

/// [`function_call_coll_ref_args`] for a callee whose RESULT may be by-reference:
/// returns the raw result `Datum` word plus the callee's `ref_result` payload.
pub fn function_call_coll_ref_args_out(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    collation: Oid,
    args: Vec<NullableDatum>,
    ref_args: Vec<Option<RefPayload>>,
) -> PgResult<(Datum, Option<RefPayload>)> {
    debug_assert_eq!(args.len(), ref_args.len());
    let oid = flinfo.fn_oid;
    let fn_expr = flinfo.fn_expr.clone();
    let mut fcinfo = init_fcinfo(Some(flinfo), collation, args);
    fcinfo.ref_args = ref_args;
    fcinfo.debug_assert_ref_null_consistency();
    let d = invoke_flinfo(mcx, res, &mut fcinfo, oid, fn_expr)?;
    Ok((d, fcinfo.take_ref_result()))
}

// ===========================================================================
// OidFunctionCall{0..9}Coll family.
// ===========================================================================

/// Port of `OidFunctionCall0Coll` (C: `fmgr_info(functionId, &flinfo);
/// FunctionCall0Coll(&flinfo, collation)`). `mcx` is the safe-Rust equivalent of
/// the `CurrentMemoryContext` global the C `fmgr_info` reads.
pub fn oid_function_call0_coll(
    mcx: Mcx<'_>,
    function_id: Oid,
    collation: Oid,
) -> PgResult<Datum> {
    let resolved = fmgr_info(mcx, function_id)?;
    function_call0_coll(mcx, &resolved.resolution, resolved.finfo, collation)
}

macro_rules! oid_function_call {
    ($name:ident, $n:expr, $call:ident, $($arg:ident),*) => {
        #[doc = concat!("Port of `OidFunctionCall", stringify!($n), "Coll`.")]
        pub fn $name(
            mcx: Mcx<'_>,
            function_id: Oid,
            collation: Oid,
            $($arg: Datum),*
        ) -> PgResult<Datum> {
            let resolved = fmgr_info(mcx, function_id)?;
            $call(mcx, &resolved.resolution, resolved.finfo, collation, $($arg),*)
        }
    };
}

oid_function_call!(oid_function_call1_coll, 1, function_call1_coll, arg1);
oid_function_call!(oid_function_call2_coll, 2, function_call2_coll, arg1, arg2);
oid_function_call!(oid_function_call3_coll, 3, function_call3_coll, arg1, arg2, arg3);
oid_function_call!(oid_function_call4_coll, 4, function_call4_coll, arg1, arg2, arg3, arg4);
oid_function_call!(oid_function_call5_coll, 5, function_call5_coll, arg1, arg2, arg3, arg4, arg5);
oid_function_call!(
    oid_function_call6_coll, 6, function_call6_coll, arg1, arg2, arg3, arg4, arg5, arg6
);
oid_function_call!(
    oid_function_call7_coll, 7, function_call7_coll, arg1, arg2, arg3, arg4, arg5, arg6, arg7
);
oid_function_call!(
    oid_function_call8_coll, 8, function_call8_coll, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8
);
oid_function_call!(
    oid_function_call9_coll, 9, function_call9_coll,
    arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9
);

// ===========================================================================
// fmgr_info family (resolve an FmgrInfo for a function Oid).
// ===========================================================================

/// Reset the constant `FmgrInfo` fields every `fmgr_info_cxt_security` entry
/// initializes, before resolution (C: `fn_oid = InvalidOid; fn_extra = NULL;
/// fn_mcxt = mcxt; fn_expr = NULL;`).
fn init_finfo() -> FmgrInfo {
    let mut finfo = FmgrInfo::empty();
    finfo.fn_oid = InvalidOid;
    finfo.fn_extra = None;
    finfo.fn_expr = None;
    finfo
}

/// Port of `fmgr_info` (C: `fmgr_info_cxt_security(functionId, finfo,
/// CurrentMemoryContext, false)`). The caller supplies the current memory
/// context as `mcx`.
pub fn fmgr_info(mcx: Mcx<'_>, function_id: Oid) -> PgResult<ResolvedFmgrInfo> {
    fmgr_info_cxt_security(mcx, function_id, false)
}

/// Port of `fmgr_info_cxt`.
pub fn fmgr_info_cxt(mcx: Mcx<'_>, function_id: Oid) -> PgResult<ResolvedFmgrInfo> {
    fmgr_info_cxt_security(mcx, function_id, false)
}

/// Port of the static `fmgr_info_cxt_security`: zero the constant `FmgrInfo`
/// fields, take the `fmgr_isbuiltin` fast path, otherwise resolve via the
/// `pg_proc` catalog seam and branch on `prosecdef`/`proconfig`/`needs_fmgr_hook`
/// then on `prolang`. `ignore_security = true` avoids recursion (the
/// `fmgr_security_definer` / `fmgr_info_other_lang` re-entry).
pub fn fmgr_info_cxt_security(
    mcx: Mcx<'_>,
    function_id: Oid,
    ignore_security: bool,
) -> PgResult<ResolvedFmgrInfo> {
    let mut finfo = init_finfo();

    // --- built-in fast path (C: fmgr_isbuiltin) ---
    if let Some(fbp) = fmgr_isbuiltin(function_id) {
        finfo.fn_nargs = fbp.nargs;
        finfo.fn_strict = fbp.strict;
        finfo.fn_retset = fbp.retset;
        finfo.fn_stats = TRACK_FUNC_ALL;
        finfo.fn_addr = fbp.func.clone();
        finfo.fn_oid = function_id;
        return Ok(ResolvedFmgrInfo {
            finfo,
            resolution: FmgrResolution::Builtin(fbp),
        });
    }

    // --- catalog path (C: SearchSysCache1(PROCOID, ...)) ---
    let proc = backend_utils_cache_syscache_seams::lookup_proc::call(mcx, function_id)?
        // C: if (!HeapTupleIsValid) elog(ERROR, "cache lookup failed for function %u")
        .ok_or_else(|| PgError::error(format!("cache lookup failed for function {function_id}")))?;

    finfo.fn_nargs = proc.nargs;
    finfo.fn_strict = proc.strict;
    finfo.fn_retset = proc.retset;

    // C: if (!ignore_security && (prosecdef || !proconfig-is-null ||
    //        FmgrHookIsNeeded(functionId)))
    //    { fn_addr = fmgr_security_definer; fn_stats = TRACK_FUNC_ALL; fn_oid; return; }
    if !ignore_security && (proc.security_definer || fmgr_hook_is_needed(function_id)) {
        finfo.fn_stats = TRACK_FUNC_ALL;
        finfo.fn_oid = function_id;
        return Ok(ResolvedFmgrInfo {
            resolution: FmgrResolution::SecurityDefiner { fn_oid: function_id },
            finfo,
        });
    }

    // C: switch (procedureStruct->prolang) { ... }
    let resolution = match proc.language {
        ProcLanguage::Internal => {
            // C: prosrc -> fmgr_lookupByName -> fbp; error if not found.
            let prosrc = proc.prosrc.as_ref().map(|s| s.as_str()).unwrap_or("");
            let fbp = fmgr_lookup_by_name(prosrc).ok_or_else(|| {
                PgError::error(format!(
                    "internal function \"{prosrc}\" is not in internal lookup table"
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION)
            })?;
            finfo.fn_stats = TRACK_FUNC_ALL;
            finfo.fn_addr = fbp.func.clone();
            FmgrResolution::InternalByName(fbp)
        }
        ProcLanguage::C => {
            // C: fmgr_info_C_lang(functionId, finfo, procedureTuple);
            //    finfo->fn_stats = TRACK_FUNC_PL;
            finfo.fn_stats = TRACK_FUNC_PL;
            let loaded = fmgr_info_c_lang(function_id, &proc)?;
            finfo.fn_addr = loaded.func.func.clone();
            FmgrResolution::CLanguage(loaded.func)
        }
        ProcLanguage::Sql => {
            // C: fn_addr = fmgr_sql; fn_stats = TRACK_FUNC_PL. The SQL-function
            // body lives in executor/functions.c, not fmgr.c; only this
            // assignment is here. The SQL leg reports unsupported until that
            // owner lands.
            finfo.fn_stats = TRACK_FUNC_PL;
            return Err(unsupported(
                "SQL-language function (fmgr_sql) not supported",
                function_id,
            ));
        }
        ProcLanguage::Other => {
            // C: fmgr_info_other_lang(functionId, finfo, procedureTuple);
            //    finfo->fn_stats = TRACK_FUNC_OFF;
            finfo.fn_stats = TRACK_FUNC_OFF;
            let (res, fn_addr) = fmgr_info_other_lang(mcx, &mut finfo, &proc)?;
            finfo.fn_addr = fn_addr;
            res
        }
    };

    finfo.fn_oid = function_id;
    Ok(ResolvedFmgrInfo { finfo, resolution })
}

// ---------------------------------------------------------------------------
// CFuncHash — the function manager's cache of looked-up external C functions
// (C: the `CFuncHash` HTAB + `lookup_C_func` / `record_C_func`). Per-backend
// (thread_local). The slow dfmgr routines run at most once per external
// function per session; the cache key is the pg_proc OID, with the tuple's
// (xmin, tid) as the up-to-dateness check.
// ---------------------------------------------------------------------------

/// C: `struct CFuncHashTabEntry` (fmgr.c). `fn_oid` is the hash key.
#[derive(Clone)]
struct CFuncHashTabEntry {
    /// C: `TransactionId fn_xmin` — for checking up-to-dateness.
    fn_xmin: TransactionId,
    /// C: `ItemPointerData fn_tid`.
    fn_tid: ItemPointerData,
    /// C: `PGFunction user_fn` — the function's address.
    user_fn: PGFunction,
    /// C: `const Pg_finfo_record *inforec` — its info record (`api_version`).
    api_version: i32,
}

thread_local! {
    /// C: `static HTAB *CFuncHash` — the per-backend external-C-function cache.
    static C_FUNC_HASH: RefCell<HashMap<Oid, CFuncHashTabEntry>> =
        RefCell::new(HashMap::new());
}

/// Port of the static `lookup_C_func`: return the cached entry iff it exists and
/// is up to date (xmin + tid match the pg_proc tuple), else `None`. `fn_oid` is
/// the hash key (C: `((Form_pg_proc) GETSTRUCT(procedureTuple))->oid`).
fn lookup_c_func(fn_oid: Oid, proc: &ProcInfo) -> Option<CFuncHashTabEntry> {
    C_FUNC_HASH.with(|h| {
        let map = h.borrow();
        let entry = map.get(&fn_oid)?;
        // C: if (entry->fn_xmin == HeapTupleHeaderGetRawXmin(...) &&
        //        ItemPointerEquals(&entry->fn_tid, &procedureTuple->t_self))
        //        return entry; else return NULL; (out of date)
        if entry.fn_xmin == proc.xmin && entry.fn_tid == proc.tid {
            Some(entry.clone())
        } else {
            None
        }
    })
}

/// Port of the static `record_C_func`: enter (or update) the cache entry for a
/// C function. C creates the HTAB lazily; the thread_local map is always present.
fn record_c_func(proc: &ProcInfo, fn_oid: Oid, user_fn: PGFunction, api_version: i32) {
    C_FUNC_HASH.with(|h| {
        h.borrow_mut().insert(
            fn_oid,
            CFuncHashTabEntry {
                fn_xmin: proc.xmin,
                fn_tid: proc.tid,
                user_fn,
                api_version,
            },
        );
    });
}

/// Port of the static `fmgr_info_C_lang`. Checks the [`CFuncHash`] cache; on a
/// miss, enforces non-null `prosrc`/`probin` (`SysCacheGetAttrNotNull`), loads
/// the symbol + info record via the
/// [`load_external_function`](backend_utils_fmgr_dfmgr_seams::load_external_function)
/// dfmgr seam, caches the result (`record_C_func`), then validates
/// `inforec->api_version` (C's `case 1:` / `default: elog`). The `CFuncHash`
/// caching and the `api_version` switch are the function manager's own logic;
/// only `load_external_function`/`fetch_finfo_record` are the dfmgr external.
fn fmgr_info_c_lang(function_id: Oid, proc: &ProcInfo) -> PgResult<LoadedCFunc> {
    // C: hashentry = lookup_C_func(procedureTuple); if (hashentry) { user_fn =
    //    hashentry->user_fn; inforec = hashentry->inforec; } else { ...load... }
    let (user_fn, api_version) = if let Some(entry) = lookup_c_func(function_id, proc) {
        (entry.user_fn, entry.api_version)
    } else {
        // C: prosrcstring = TextDatumGetCString(SysCacheGetAttrNotNull(.., prosrc));
        let prosrc = proc
            .prosrc
            .as_ref()
            .map(|s| s.as_str())
            .ok_or_else(|| PgError::error(format!("null prosrc for function {function_id}")))?;
        // C: probinstring = TextDatumGetCString(SysCacheGetAttrNotNull(.., probin));
        let probin = proc
            .probin
            .as_ref()
            .map(|s| s.as_str())
            .ok_or_else(|| PgError::error(format!("null probin for function {function_id}")))?;

        // C: user_fn = load_external_function(...); inforec = fetch_finfo_record(...);
        let loaded =
            backend_utils_fmgr_dfmgr_seams::load_external_function::call(probin, prosrc, function_id)?;

        // C: record_C_func(procedureTuple, user_fn, inforec);
        record_c_func(proc, function_id, loaded.user_fn, loaded.api_version);
        (loaded.user_fn, loaded.api_version)
    };

    // C: switch (inforec->api_version) { case 1: finfo->fn_addr = user_fn; break;
    //        default: elog(ERROR, "unrecognized function API version: %d"); }
    match api_version {
        1 => Ok(LoadedCFunc {
            func: BuiltinFunction {
                foid: function_id,
                name: proc
                    .prosrc
                    .as_ref()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default(),
                nargs: proc.nargs,
                strict: proc.strict,
                retset: proc.retset,
                func: user_fn,
            },
        }),
        v => Err(PgError::error(format!(
            "unrecognized function API version: {v}"
        ))),
    }
}

fn unsupported(what: &str, function_id: Oid) -> PgError {
    PgError::error(what.to_string())
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_detail(format!("function {function_id}"))
}

/// Port of the static `fmgr_info_other_lang`: read `lanplcallfoid` from the
/// `pg_language` cache (the [`lookup_language`](backend_utils_cache_syscache_seams::lookup_language)
/// seam), then recurse into [`fmgr_info_cxt_security`] with `ignore_security =
/// true` (the *bare* call-handler resolution) and copy `plfinfo.fn_addr`. C uses
/// `CurrentMemoryContext` for the inner lookup; here it is the same `mcx`.
fn fmgr_info_other_lang(
    mcx: Mcx<'_>,
    _finfo: &mut FmgrInfo,
    proc: &ProcInfo,
) -> PgResult<(FmgrResolution, PGFunction)> {
    // C: Oid language = procedureStruct->prolang;
    let language = proc.prolang;

    // C: languageTuple = SearchSysCache1(LANGOID, ...); if (!valid) elog(ERROR,
    //    "cache lookup failed for language %u"); ... lanplcallfoid.
    let lang = backend_utils_cache_syscache_seams::lookup_language::call(mcx, language)?
        .ok_or_else(|| {
            PgError::error(format!("cache lookup failed for language {language}"))
        })?;

    // C: fmgr_info_cxt_security(languageStruct->lanplcallfoid, &plfinfo,
    //                           CurrentMemoryContext, true);
    //    finfo->fn_addr = plfinfo.fn_addr;
    let plfinfo = fmgr_info_cxt_security(mcx, lang.lanplcallfoid, true)?;
    Ok((plfinfo.resolution, plfinfo.finfo.fn_addr))
}

/// Port of `fmgr_symbol`. Reads `prosecdef`/`proconfig`/`FmgrHookIsNeeded` then
/// switches on `prolang`. Returns `(mod, fn)`: `(None, None)` no C symbol (PL
/// default); `(None, Some)` main binary (INTERNAL / SQL / secdef); `(Some, Some)`
/// extension shared object (C-language). The returned strings are owned `String`
/// (C `pstrdup`s into the current context); the projected `prosrc`/`probin`
/// already live in `mcx` so they are copied out by value.
pub fn fmgr_symbol(mcx: Mcx<'_>, function_id: Oid) -> PgResult<(Option<String>, Option<String>)> {
    let proc = backend_utils_cache_syscache_seams::lookup_proc::call(mcx, function_id)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for function {function_id}")))?;

    // C: if (prosecdef || !proconfig-is-null || FmgrHookIsNeeded(functionId))
    //        { *mod = NULL; *fn = pstrdup("fmgr_security_definer"); return; }
    if proc.security_definer || fmgr_hook_is_needed(function_id) {
        return Ok((None, Some("fmgr_security_definer".to_string())));
    }

    match proc.language {
        ProcLanguage::Internal => {
            // C: *mod = NULL; *fn = TextDatumGetCString(prosrc);
            let prosrc = proc
                .prosrc
                .as_ref()
                .map(|s| s.as_str().to_string())
                .ok_or_else(|| PgError::error(format!("null prosrc for function {function_id}")))?;
            Ok((None, Some(prosrc)))
        }
        ProcLanguage::C => {
            // C: *mod = TextDatumGetCString(probin); *fn = TextDatumGetCString(prosrc);
            let probin = proc
                .probin
                .as_ref()
                .map(|s| s.as_str().to_string())
                .ok_or_else(|| PgError::error(format!("null probin for function {function_id}")))?;
            let prosrc = proc
                .prosrc
                .as_ref()
                .map(|s| s.as_str().to_string())
                .ok_or_else(|| PgError::error(format!("null prosrc for function {function_id}")))?;
            Ok((Some(probin), Some(prosrc)))
        }
        ProcLanguage::Sql => {
            // C: *mod = NULL; *fn = pstrdup("fmgr_sql");
            Ok((None, Some("fmgr_sql".to_string())))
        }
        ProcLanguage::Other => {
            // C: *mod = NULL; *fn = NULL;  (unknown)
            Ok((None, None))
        }
    }
}

/// Port of `fmgr_info_copy` (C: `memcpy(dst, src, sizeof(FmgrInfo)); dst->fn_mcxt
/// = destcxt; dst->fn_extra = NULL;`). The field-for-field clone replaces
/// `memcpy`; `fn_mcxt` is dormant here, so only `fn_extra` is reset.
pub fn fmgr_info_copy(dstinfo: &mut FmgrInfo, srcinfo: &FmgrInfo) {
    *dstinfo = srcinfo.clone();
    dstinfo.fn_extra = None;
}

// ===========================================================================
// fmgr-hook globals (C: needs_fmgr_hook / fmgr_hook + FmgrHookIsNeeded).
//
// These are plugin globals C declares as PGDLLIMPORT function pointers,
// default NULL. They are per-backend state (a loaded plugin sets them), so
// they live in thread_local slots, not shared statics.
// ===========================================================================

type NeedsFmgrHook = fn(Oid) -> bool;
type FmgrHook = fn(FmgrHookEventType, Oid, Datum) -> Datum;

thread_local! {
    static NEEDS_FMGR_HOOK: RefCell<Option<NeedsFmgrHook>> = const { RefCell::new(None) };
    static FMGR_HOOK: RefCell<Option<FmgrHook>> = const { RefCell::new(None) };
}

/// Install the `needs_fmgr_hook` plugin (C: `needs_fmgr_hook = f`). `None`
/// restores the default (no hook).
pub fn set_needs_fmgr_hook(f: Option<NeedsFmgrHook>) {
    NEEDS_FMGR_HOOK.with(|s| *s.borrow_mut() = f);
}

/// Install the `fmgr_hook` plugin (C: `fmgr_hook = f`).
pub fn set_fmgr_hook(f: Option<FmgrHook>) {
    FMGR_HOOK.with(|s| *s.borrow_mut() = f);
}

/// Port of `FmgrHookIsNeeded(fn_oid)` (`fmgr.h`):
/// `!needs_fmgr_hook ? false : (*needs_fmgr_hook)(fn_oid)`.
pub fn fmgr_hook_is_needed(fn_oid: Oid) -> bool {
    NEEDS_FMGR_HOOK.with(|s| match *s.borrow() {
        Some(f) => f(fn_oid),
        None => false,
    })
}

/// Invoke the `fmgr_hook` plugin (C: `if (fmgr_hook) (*fmgr_hook)(event,
/// &fcache->flinfo, &fcache->arg)`). The plugin's passthrough `private` word is
/// read and updated; with no plugin (the C `NULL` default) it is returned
/// unchanged.
fn call_fmgr_hook(event: FmgrHookEventType, fn_oid: Oid, private: Datum) -> Datum {
    FMGR_HOOK.with(|s| match *s.borrow() {
        Some(f) => f(event, fn_oid, private),
        None => private,
    })
}

// ===========================================================================
// fmgr_security_definer (the call handler for secdef/proconfig/hooked funcs).
// ===========================================================================

/// C: `struct fmgr_security_definer_cache` — the per-call cache C stashes in
/// `fcinfo->flinfo->fn_extra`. Here it is owned by the handler invocation; the
/// charged GUC-name/value buffers live in the per-call [`MemoryContext`] the
/// handler builds.
struct SecurityDefinerCache<'mcx> {
    /// C: `FmgrInfo flinfo` — the inner target's lookup info.
    flinfo: FmgrInfo,
    /// C: the inner target's resolution (the stand-in for `flinfo.fn_addr`).
    inner: FmgrResolution,
    /// C: `Oid userid` — the userid to switch to, or `InvalidOid`.
    userid: Oid,
    /// C: `List *configNames` (already `TransformGUCArray`'d), charged to the
    /// per-call context.
    config_names: PgVec<'mcx, PgString<'mcx>>,
    /// C: `List *configValues`, charged like `config_names`.
    config_values: PgVec<'mcx, PgString<'mcx>>,
    /// C: `Datum arg` — the plugin passthrough for `fmgr_hook`.
    arg: Datum,
}

/// Build the security-definer cache (C: the `if (!fcinfo->flinfo->fn_extra)
/// {...}` block). `fn_oid` / `fn_expr` come from the outer `fcinfo->flinfo`; the
/// cache's GUC lists are charged to `mcx` (the per-call context).
fn build_cache<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    fn_expr: Option<Box<FnExpr>>,
) -> PgResult<SecurityDefinerCache<'mcx>> {
    // C: fmgr_info_cxt_security(fcinfo->flinfo->fn_oid, &fcache->flinfo,
    //                           fcinfo->flinfo->fn_mcxt, true);
    let resolved = fmgr_info_cxt_security(mcx, fn_oid, true)?;
    let mut flinfo = resolved.finfo;
    let inner = resolved.resolution;
    // C: fcache->flinfo.fn_expr = fcinfo->flinfo->fn_expr;
    flinfo.fn_expr = fn_expr;

    // C: tuple = SearchSysCache1(PROCOID, fn_oid); read prosecdef/proowner/proconfig.
    let proc = backend_utils_cache_syscache_seams::lookup_proc::call(mcx, fn_oid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for function {fn_oid}")))?;

    // C: if (procedureStruct->prosecdef) fcache->userid = procedureStruct->proowner;
    //                                     (fmgr.c:667)
    // The userid switch is gated on `prosecdef` ALONE, not the folded routing
    // predicate `security_definer` (which also covers proconfig/FmgrHookIsNeeded).
    let userid = if proc.prosecdef {
        proc.proowner
    } else {
        InvalidOid
    };

    // C: datum = SysCacheGetAttr(.., proconfig, &isnull); if (!isnull)
    //        TransformGUCArray(array, &configNames, &configValues);
    // (Already transformed; get_config_handle is folded into set_config_with_handle.)
    // The crate's own working copy of the two GUC lists is charged to `mcx`.
    let config_names = copy_charged(mcx, &proc.proconfig_names)?;
    let config_values = copy_charged(mcx, &proc.proconfig_values)?;

    Ok(SecurityDefinerCache {
        flinfo,
        inner,
        userid,
        config_names,
        config_values,
        // C: fcache->arg starts zeroed (MemoryContextAllocZero).
        arg: Datum::null(),
    })
}

/// Copy a `&[PgString]` into a fresh charged `PgVec<PgString>` against `mcx` (the
/// `TransformGUCArray`-into-`fn_mcxt` analog). Fallible: every step is the mcx
/// fallible API, so OOM surfaces as `mcx.oom(..)` and `?` drops the partial copy
/// (uncharging it) before returning.
fn copy_charged<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[PgString<'_>],
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    let mut out = mcx::vec_with_capacity_in::<PgString>(mcx, src.len())?;
    for s in src {
        let ps = PgString::from_str_in(s.as_str(), mcx)?;
        // The spine was reserved above, so this never reallocates.
        out.push(ps);
    }
    Ok(out)
}

/// Port of `fmgr_security_definer`. `outer` is the outer `FunctionCallInfo`
/// carrying the actual arguments. C reads the target's `fn_oid` / `fn_mcxt` /
/// `fn_expr` from `fcinfo->flinfo`; this port receives `fn_oid` from the
/// [`FmgrResolution::SecurityDefiner`] resolution and `fn_expr` from the call
/// layer. The handler builds the cache, switches userid + GUC nest level around
/// the inner invoke, and restores everything — 1:1 with the C body. The
/// `pgstat_*_function_usage` stat instrumentation is a faithful no-op (C
/// produces the identical Datum without it).
pub fn fmgr_security_definer(
    parent_mcx: Mcx<'_>,
    fn_oid: Oid,
    fn_expr: Option<Box<FnExpr>>,
    outer: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // The crate OWNS a per-call accounting context (C's `fn_mcxt`, where the
    // cache is `MemoryContextAllocZero`'d). A child of the caller's context so
    // it is reclaimed when this stack frame ends; the cache's charged buffers
    // (the two GUC lists) live and die with it.
    let ctx: MemoryContext = parent_mcx.context().new_child("fmgr_security_definer");
    let mcx = ctx.mcx();

    // C: if (!fcinfo->flinfo->fn_extra) { ...build fcache... } else fcache = fn_extra;
    let mut fcache = build_cache(mcx, fn_oid, fn_expr)?;
    fmgr_security_definer_body(parent_mcx, &mut fcache, outer)
}

/// The body of [`fmgr_security_definer`], borrowing `fcache` (whose charged
/// buffers are freed when the per-call context drops in the caller). `mcx` is
/// the caller's context (the inner target's allocation target).
fn fmgr_security_definer_body(
    mcx: Mcx<'_>,
    fcache: &mut SecurityDefinerCache,
    outer: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // C: GetUserIdAndSecContext(&save_userid, &save_sec_context);
    let (save_userid, save_sec_context) =
        backend_utils_init_miscinit_seams::get_user_id_and_sec_context::call();

    // C: if (fcache->configNames != NIL) save_nestlevel = NewGUCNestLevel(); else 0;
    let has_config = !fcache.config_names.is_empty();
    let save_nestlevel = if has_config {
        backend_utils_misc_guc_file_seams::new_guc_nest_level::call()
    } else {
        0
    };

    // C: if (OidIsValid(fcache->userid))
    //        SetUserIdAndSecContext(fcache->userid,
    //                               save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
    if oid_is_valid(fcache.userid) {
        backend_utils_init_miscinit_seams::set_user_id_and_sec_context::call(
            fcache.userid,
            save_sec_context | SECURITY_LOCAL_USERID_CHANGE,
        );
    }

    // C: forthree(name, handle, value in configNames, configHandles, configValues) {
    //        context = superuser() ? PGC_SUSET : PGC_USERSET;
    //        set_config_with_handle(name, handle, value, context, PGC_S_SESSION,
    //                               GetUserId(), GUC_ACTION_SAVE, true, 0, false); }
    if has_config {
        for (name, value) in fcache.config_names.iter().zip(fcache.config_values.iter()) {
            // C: GucContext context = superuser() ? PGC_SUSET : PGC_USERSET;
            let context = if backend_utils_init_miscinit_seams::superuser::call(mcx)? {
                GucContext::PGC_SUSET
            } else {
                GucContext::PGC_USERSET
            };
            // C: GetUserId() — the srole argument.
            let srole = backend_utils_init_miscinit_seams::get_user_id::call();
            backend_utils_misc_guc_file_seams::set_config_with_handle::call(
                name.as_str(),
                value.as_str(),
                context,
                srole,
            )?;
        }
    }

    // C: if (fmgr_hook) (*fmgr_hook)(FHET_START, &fcache->flinfo, &fcache->arg);
    fcache.arg = call_fmgr_hook(FmgrHookEventType::Start, fcache.flinfo.fn_oid, fcache.arg);

    // C: save_flinfo = fcinfo->flinfo; PG_TRY { fcinfo->flinfo = &fcache->flinfo;
    //        result = FunctionCallInvoke(fcinfo); } PG_CATCH { restore + FHET_ABORT; rethrow }
    //
    // The flinfo swap is realized by installing the cached flinfo on `outer`
    // around the inner-resolution invoke; the PG_TRY/PG_CATCH is a match on the
    // Result.
    let saved_flinfo = outer.flinfo.take();
    outer.flinfo = Some(Box::new(fcache.flinfo.clone()));
    // C: the inner target runs with fn_expr copied from the outer flinfo (fmgr.c:658).
    let inner_fn_expr = fcache.flinfo.fn_expr.clone();
    let call_result = function_call_invoke_with_expr(mcx, &fcache.inner, outer, inner_fn_expr);
    // C: fcinfo->flinfo = save_flinfo; (done in BOTH the try and catch arms)
    outer.flinfo = saved_flinfo;

    let result = match call_result {
        Ok(r) => r,
        Err(e) => {
            // C PG_CATCH: fcinfo->flinfo = save_flinfo (done above);
            //             if (fmgr_hook) (*fmgr_hook)(FHET_ABORT, ...); PG_RE_THROW();
            fcache.arg = call_fmgr_hook(FmgrHookEventType::Abort, fcache.flinfo.fn_oid, fcache.arg);
            return Err(e);
        }
    };

    // C: if (fcache->configNames != NIL) AtEOXact_GUC(true, save_nestlevel);
    if has_config {
        backend_utils_misc_guc_file_seams::at_eoxact_guc::call(true, save_nestlevel)?;
    }
    // C: if (OidIsValid(fcache->userid)) SetUserIdAndSecContext(save_userid, save_sec_context);
    if oid_is_valid(fcache.userid) {
        backend_utils_init_miscinit_seams::set_user_id_and_sec_context::call(
            save_userid,
            save_sec_context,
        );
    }
    // C: if (fmgr_hook) (*fmgr_hook)(FHET_END, &fcache->flinfo, &fcache->arg);
    fcache.arg = call_fmgr_hook(FmgrHookEventType::End, fcache.flinfo.fn_oid, fcache.arg);
    let _ = fcache.arg; // C keeps the updated arg in the (cached) fcache.

    Ok(result)
}

/// C: `OidIsValid(oid)` == `oid != InvalidOid`.
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

// ===========================================================================
// Datatype I/O-function convenience wrappers.
// ===========================================================================

/// C: `ObjectIdGetDatum(typioparam)`.
#[inline]
fn objectid_get_datum(oid: Oid) -> Datum {
    Datum::from_u32(oid)
}

/// C: `Int32GetDatum(typmod)`.
#[inline]
fn int32_get_datum(typmod: i32) -> Datum {
    Datum::from_i32(typmod)
}

/// Shared 3-argument invoke core for `InputFunctionCall` / `ReceiveFunctionCall`.
/// Collation is `InvalidOid` for I/O functions; the throwing path has no
/// escontext. Returns the result and the `fcinfo->isnull` flag.
fn invoke_io3(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    arg0: Datum,
    typioparam: Datum,
    typmod: Datum,
) -> PgResult<(Datum, bool)> {
    let mut fcinfo = init_fcinfo(
        Some(flinfo),
        InvalidOid,
        vec![
            NullableDatum::value(arg0),
            NullableDatum::value(typioparam),
            NullableDatum::value(typmod),
        ],
    );
    let result = function_call_invoke(mcx, res, &mut fcinfo)?;
    Ok((result, fcinfo.result_is_null()))
}

/// Same as [`invoke_io3`] but threads a soft-error context. A soft error fills
/// the context (returning `(0, true, true)`); with no context the error is
/// rethrown. C builds `fcinfo` with `escontext` as the context node; here the
/// `PGFunction` has no escontext channel, so the callee signals a soft error by
/// returning `Err`, absorbed into the context at this boundary.
fn invoke_io3_soft(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    arg0: Datum,
    typioparam: Datum,
    typmod: Datum,
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<(Datum, bool, bool)> {
    let mut fcinfo = init_fcinfo(
        Some(flinfo),
        InvalidOid,
        vec![
            NullableDatum::value(arg0),
            NullableDatum::value(typioparam),
            NullableDatum::value(typmod),
        ],
    );
    match function_call_invoke(mcx, res, &mut fcinfo) {
        Ok(result) => Ok((result, fcinfo.result_is_null(), false)),
        Err(e) => match escontext {
            Some(ctx) => {
                ctx.save(e);
                Ok((Datum::null(), true, true))
            }
            None => Err(e),
        },
    }
}

/// Port of `InputFunctionCall`. `str` is `Option<Datum>`: `None` is C's `str ==
/// NULL`, `Some(d)` is `CStringGetDatum(str)`. C: `if (str == NULL &&
/// fn_strict) return 0;` then a 3-arg invoke with the str-NULL / non-NULL result
/// checks.
pub fn input_function_call(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    str: Option<Datum>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<Datum> {
    if str.is_none() && flinfo.fn_strict {
        return Ok(Datum::null());
    }
    let fn_oid = flinfo.fn_oid;
    let arg0 = str.unwrap_or(Datum::null());
    let (result, isnull) = invoke_io3(
        mcx,
        res,
        flinfo,
        arg0,
        objectid_get_datum(typioparam),
        int32_get_datum(typmod),
    )?;
    // C: should get null result iff str is NULL.
    if str.is_none() {
        if !isnull {
            return Err(PgError::error(format!(
                "input function {fn_oid} returned non-NULL"
            )));
        }
    } else if isnull {
        return Err(PgError::error(format!(
            "input function {fn_oid} returned NULL"
        )));
    }
    Ok(result)
}

/// Port of `InputFunctionCallSafe`. A soft error fills `escontext` and returns
/// `Ok((false, _))` (C returns `false`); success returns `Ok((true, result))`.
pub fn input_function_call_safe(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    str: Option<Datum>,
    typioparam: Oid,
    typmod: i32,
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<(bool, Datum)> {
    // C: if (str == NULL && fn_strict) { *result = 0; return true; }
    if str.is_none() && flinfo.fn_strict {
        return Ok((true, Datum::null()));
    }
    let fn_oid = flinfo.fn_oid;
    let arg0 = str.unwrap_or(Datum::null());
    let (result, isnull, soft_error) = invoke_io3_soft(
        mcx,
        res,
        flinfo,
        arg0,
        objectid_get_datum(typioparam),
        int32_get_datum(typmod),
        escontext,
    )?;
    // C: if (SOFT_ERROR_OCCURRED(escontext)) return false;
    if soft_error {
        return Ok((false, Datum::null()));
    }
    if str.is_none() {
        if !isnull {
            return Err(PgError::error(format!(
                "input function {fn_oid} returned non-NULL"
            )));
        }
    } else if isnull {
        return Err(PgError::error(format!(
            "input function {fn_oid} returned NULL"
        )));
    }
    Ok((true, result))
}

/// Port of `DirectInputFunctionCallSafe`: a direct callable (assumed strict,
/// cannot read `FmgrInfo`). `str == NULL` always yields a NULL result; a soft
/// error fills `escontext` and returns `false`.
pub fn direct_input_function_call_safe(
    func: &PGFunction,
    str: Option<Datum>,
    typioparam: Oid,
    typmod: i32,
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<(bool, Datum)> {
    // C: if (str == NULL) { *result = 0; return true; }  (assumed strict)
    if str.is_none() {
        return Ok((true, Datum::null()));
    }
    let arg0 = str.unwrap_or(Datum::null());
    // C: InitFunctionCallInfoData(*fcinfo, NULL, 3, InvalidOid, escontext, NULL);
    let mut fcinfo = init_fcinfo(
        None,
        InvalidOid,
        vec![
            NullableDatum::value(arg0),
            NullableDatum::value(objectid_get_datum(typioparam)),
            NullableDatum::value(int32_get_datum(typmod)),
        ],
    );
    // C: *result = (*func)(fcinfo);
    match invoke_pgfunction(func, &mut fcinfo) {
        Ok(result) => {
            // C: if (fcinfo->isnull) elog(ERROR, "input function %p returned NULL", func);
            if fcinfo.result_is_null() {
                return Err(PgError::error(
                    "input function <direct> returned NULL".to_string(),
                ));
            }
            Ok((true, result))
        }
        // C: if (SOFT_ERROR_OCCURRED(escontext)) return false; (soft) else rethrow.
        Err(e) => match escontext {
            Some(ctx) => {
                ctx.save(e);
                Ok((false, Datum::null()))
            }
            None => Err(e),
        },
    }
}

/// Port of `OutputFunctionCall` (C: `DatumGetCString(FunctionCall1(flinfo,
/// val))`). The result `Datum` carries the output function's `cstring` return.
pub fn output_function_call(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    val: Datum,
) -> PgResult<Datum> {
    function_call1_coll(mcx, res, flinfo, InvalidOid, val)
}

/// Port of `ReceiveFunctionCall`. `buf` is `Option<Datum>` (`None` == C's `buf ==
/// NULL`); C: `PointerGetDatum(buf)` and "receive function %u returned
/// NULL/non-NULL".
pub fn receive_function_call(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    buf: Option<Datum>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<Datum> {
    if buf.is_none() && flinfo.fn_strict {
        return Ok(Datum::null());
    }
    let fn_oid = flinfo.fn_oid;
    let arg0 = buf.unwrap_or(Datum::null());
    let (result, isnull) = invoke_io3(
        mcx,
        res,
        flinfo,
        arg0,
        objectid_get_datum(typioparam),
        int32_get_datum(typmod),
    )?;
    if buf.is_none() {
        if !isnull {
            return Err(PgError::error(format!(
                "receive function {fn_oid} returned non-NULL"
            )));
        }
    } else if isnull {
        return Err(PgError::error(format!(
            "receive function {fn_oid} returned NULL"
        )));
    }
    Ok(result)
}

/// Port of `SendFunctionCall` (C: `DatumGetByteaP(FunctionCall1(flinfo, val))`
/// where `DatumGetByteaP(X)` expands to `PG_DETOAST_DATUM(X)`). This
/// calling-convention layer returns the raw `Datum` from `FunctionCall1`; the
/// detoast is a varlena operation applied by the varlena-aware caller.
pub fn send_function_call(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    val: Datum,
) -> PgResult<Datum> {
    function_call1_coll(mcx, res, flinfo, InvalidOid, val)
}

/// Port of `OidInputFunctionCall` (C: `fmgr_info` + `InputFunctionCall`).
pub fn oid_input_function_call(
    mcx: Mcx<'_>,
    function_id: Oid,
    str: Option<Datum>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<Datum> {
    let resolved = fmgr_info(mcx, function_id)?;
    input_function_call(mcx, &resolved.resolution, resolved.finfo, str, typioparam, typmod)
}

/// Port of `OidOutputFunctionCall`.
pub fn oid_output_function_call(mcx: Mcx<'_>, function_id: Oid, val: Datum) -> PgResult<Datum> {
    let resolved = fmgr_info(mcx, function_id)?;
    output_function_call(mcx, &resolved.resolution, resolved.finfo, val)
}

/// Port of `OidReceiveFunctionCall`.
pub fn oid_receive_function_call(
    mcx: Mcx<'_>,
    function_id: Oid,
    buf: Option<Datum>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<Datum> {
    let resolved = fmgr_info(mcx, function_id)?;
    receive_function_call(mcx, &resolved.resolution, resolved.finfo, buf, typioparam, typmod)
}

/// Port of `OidSendFunctionCall`.
pub fn oid_send_function_call(mcx: Mcx<'_>, function_id: Oid, val: Datum) -> PgResult<Datum> {
    let resolved = fmgr_info(mcx, function_id)?;
    send_function_call(mcx, &resolved.resolution, resolved.finfo, val)
}

// ===========================================================================
// Option-4 typed I/O boundary (cstring = &str in / String out; varlena =
// &[u8] in / Vec<u8> out). C: the same I/O entry points, carrying the
// pass-by-reference payload as an owned `RefPayload` side-channel instead of a
// pointer-`Datum`.
// ===========================================================================

/// Build the 3-arg I/O `fcinfo` whose `args[0]` is a by-reference value (C:
/// `CStringGetDatum(str)` / `PointerGetDatum(buf)`), carrying the owned referent
/// in `ref_args[0]`. `args[1..2]` are the by-value `typioparam`/`typmod`.
fn init_io3_ref(
    flinfo: FmgrInfo,
    arg0_ref: RefPayload,
    typioparam: Oid,
    typmod: i32,
) -> FunctionCallInfoBaseData {
    let mut fcinfo = init_fcinfo(
        Some(flinfo),
        InvalidOid,
        vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(objectid_get_datum(typioparam)),
            NullableDatum::value(int32_get_datum(typmod)),
        ],
    );
    fcinfo.ref_args = vec![Some(arg0_ref), None, None];
    fcinfo.debug_assert_ref_null_consistency();
    fcinfo
}

/// Read the dispatched I/O function's result as an [`FmgrOut`].
fn io_result_out(result: Datum, fcinfo: &mut FunctionCallInfoBaseData) -> FmgrOut {
    match fcinfo.take_ref_result() {
        Some(payload) => FmgrOut::Ref(payload),
        None => FmgrOut::ByVal(result),
    }
}

/// Option-4 port of `InputFunctionCall` (`cstring` in). C's `char *str` is
/// `input: &str`; a NULL input is `None`. The result is an [`FmgrOut`].
pub fn input_function_call_typed(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    input: Option<&str>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<FmgrOut> {
    if input.is_none() && flinfo.fn_strict {
        return Ok(FmgrOut::ByVal(Datum::null()));
    }
    let fn_oid = flinfo.fn_oid;
    let arg_ref = RefPayload::Cstring(input.unwrap_or("").to_string());
    let mut fcinfo = init_io3_ref(flinfo, arg_ref, typioparam, typmod);
    let result = function_call_invoke(mcx, res, &mut fcinfo)?;
    let isnull = fcinfo.result_is_null();
    if input.is_none() {
        if !isnull {
            return Err(PgError::error(format!(
                "input function {fn_oid} returned non-NULL"
            )));
        }
    } else if isnull {
        return Err(PgError::error(format!(
            "input function {fn_oid} returned NULL"
        )));
    }
    Ok(io_result_out(result, &mut fcinfo))
}

/// Option-4 port of `InputFunctionCallSafe` (`cstring` in, soft-error capable).
/// `Ok(None)` means a soft error was saved into `escontext`.
pub fn input_function_call_safe_typed(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    input: Option<&str>,
    typioparam: Oid,
    typmod: i32,
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<Option<FmgrOut>> {
    if input.is_none() && flinfo.fn_strict {
        return Ok(Some(FmgrOut::ByVal(Datum::null())));
    }
    let fn_oid = flinfo.fn_oid;
    let arg_ref = RefPayload::Cstring(input.unwrap_or("").to_string());
    let mut fcinfo = init_io3_ref(flinfo, arg_ref, typioparam, typmod);
    let result = match function_call_invoke(mcx, res, &mut fcinfo) {
        Ok(result) => result,
        Err(e) => match escontext {
            Some(ctx) => {
                ctx.save(e);
                return Ok(None);
            }
            None => return Err(e),
        },
    };
    let isnull = fcinfo.result_is_null();
    if input.is_none() {
        if !isnull {
            return Err(PgError::error(format!(
                "input function {fn_oid} returned non-NULL"
            )));
        }
    } else if isnull {
        return Err(PgError::error(format!(
            "input function {fn_oid} returned NULL"
        )));
    }
    Ok(Some(io_result_out(result, &mut fcinfo)))
}

/// Option-4 port of `OutputFunctionCall` (`cstring` out). C never calls this on
/// a NULL datum. The result is the owned `String` the output function produced.
pub fn output_function_call_typed(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    value: FmgrArg,
) -> PgResult<String> {
    let fn_oid = flinfo.fn_oid;
    let mut fcinfo = init_output_fcinfo(flinfo, value);
    let result = function_call_invoke(mcx, res, &mut fcinfo)?;
    if fcinfo.result_is_null() {
        return Err(PgError::error(format!("function {fn_oid} returned NULL")));
    }
    out_cstring(result, &mut fcinfo, fn_oid)
}

/// Option-4 port of `ReceiveFunctionCall` (binary `bytea` buffer in). C's
/// `StringInfo buf` is `buf: &[u8]`; a NULL buffer is `None`.
pub fn receive_function_call_typed(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    buf: Option<&[u8]>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<FmgrOut> {
    if buf.is_none() && flinfo.fn_strict {
        return Ok(FmgrOut::ByVal(Datum::null()));
    }
    let fn_oid = flinfo.fn_oid;
    let arg_ref = RefPayload::Varlena(buf.unwrap_or(&[]).to_vec());
    let mut fcinfo = init_io3_ref(flinfo, arg_ref, typioparam, typmod);
    let result = function_call_invoke(mcx, res, &mut fcinfo)?;
    let isnull = fcinfo.result_is_null();
    if buf.is_none() {
        if !isnull {
            return Err(PgError::error(format!(
                "receive function {fn_oid} returned non-NULL"
            )));
        }
    } else if isnull {
        return Err(PgError::error(format!(
            "receive function {fn_oid} returned NULL"
        )));
    }
    Ok(io_result_out(result, &mut fcinfo))
}

/// Option-4 port of `SendFunctionCall` (binary `bytea` out). C never calls this
/// on a NULL datum. The result is the owned `Vec<u8>` the send function produced.
pub fn send_function_call_typed(
    mcx: Mcx<'_>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    value: FmgrArg,
) -> PgResult<Vec<u8>> {
    let fn_oid = flinfo.fn_oid;
    let mut fcinfo = init_output_fcinfo(flinfo, value);
    let result = function_call_invoke(mcx, res, &mut fcinfo)?;
    if fcinfo.result_is_null() {
        return Err(PgError::error(format!("function {fn_oid} returned NULL")));
    }
    out_varlena(result, &mut fcinfo, fn_oid)
}

/// Build the 1-arg `fcinfo` for an output/send function (C: `FunctionCall1`).
fn init_output_fcinfo(flinfo: FmgrInfo, value: FmgrArg) -> FunctionCallInfoBaseData {
    match value {
        FmgrArg::ByVal(d) => init_fcinfo(Some(flinfo), InvalidOid, vec![NullableDatum::value(d)]),
        FmgrArg::Ref(payload) => {
            let mut fcinfo = init_fcinfo(
                Some(flinfo),
                InvalidOid,
                vec![NullableDatum::value(Datum::null())],
            );
            fcinfo.ref_args = vec![Some(payload.clone_flat())];
            fcinfo.debug_assert_ref_null_consistency();
            fcinfo
        }
    }
}

/// Read an output function's `cstring` result back as an owned `String` (C:
/// `DatumGetCString`). A missing / wrong-kind payload is a hard error.
fn out_cstring(
    _result: Datum,
    fcinfo: &mut FunctionCallInfoBaseData,
    fn_oid: Oid,
) -> PgResult<String> {
    match fcinfo.take_ref_result() {
        Some(RefPayload::Cstring(s)) => Ok(s),
        _ => Err(PgError::error(format!(
            "output function {fn_oid} did not return a cstring"
        ))),
    }
}

/// Read a send function's varlena result back as owned bytes (C:
/// `DatumGetByteaP`).
fn out_varlena(
    _result: Datum,
    fcinfo: &mut FunctionCallInfoBaseData,
    fn_oid: Oid,
) -> PgResult<Vec<u8>> {
    match fcinfo.take_ref_result() {
        Some(RefPayload::Varlena(b)) => Ok(b),
        _ => Err(PgError::error(format!(
            "send function {fn_oid} did not return a varlena"
        ))),
    }
}

/// `OidInputFunctionCall` over the Option-4 typed boundary, returning the value
/// as a bare `Datum` word (a by-reference result is minted into the per-backend
/// [`datum_ref_registry`] — the owned-model `PointerGetDatum(palloc'd result)`).
pub fn oid_input_function_call_typed(
    mcx: Mcx<'_>,
    function_id: Oid,
    input: Option<&str>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<Datum> {
    let resolved = fmgr_info(mcx, function_id)?;
    let out =
        input_function_call_typed(mcx, &resolved.resolution, resolved.finfo, input, typioparam, typmod)?;
    Ok(match out {
        FmgrOut::ByVal(d) => d,
        FmgrOut::Ref(payload) => datum_ref_registry::register(payload),
    })
}

/// `OidOutputFunctionCall` over the Option-4 typed boundary, returning the
/// rendered text as an owned `String`. `arg_byval` is the value type's
/// `pg_type.typbyval` (the fact C encodes statically in its calling convention).
pub fn oid_output_function_call_typed(
    mcx: Mcx<'_>,
    function_id: Oid,
    val: Datum,
    arg_byval: bool,
) -> PgResult<String> {
    let resolved = fmgr_info(mcx, function_id)?;
    if arg_byval {
        output_function_call_typed(mcx, &resolved.resolution, resolved.finfo, FmgrArg::ByVal(val))
    } else {
        let payload = datum_ref_registry::fetch(val)?;
        output_function_call_typed(mcx, &resolved.resolution, resolved.finfo, FmgrArg::Ref(&payload))
    }
}

/// `InputFunctionCallSafe` over the Option-4 typed boundary with one-shot
/// `fmgr_info`. `Ok(None)` means a soft error was saved into `escontext`.
pub fn oid_input_function_call_safe_typed(
    mcx: Mcx<'_>,
    function_id: Oid,
    input: Option<&str>,
    typioparam: Oid,
    typmod: i32,
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<Option<Datum>> {
    let resolved = fmgr_info(mcx, function_id)?;
    let out = input_function_call_safe_typed(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        input,
        typioparam,
        typmod,
        escontext,
    )?;
    Ok(out.map(|out| match out {
        FmgrOut::ByVal(d) => d,
        FmgrOut::Ref(payload) => datum_ref_registry::register(payload),
    }))
}

/// `OidReceiveFunctionCall` over the Option-4 typed boundary.
pub fn oid_receive_function_call_typed(
    mcx: Mcx<'_>,
    function_id: Oid,
    buf: Option<&[u8]>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<Datum> {
    let resolved = fmgr_info(mcx, function_id)?;
    let out = receive_function_call_typed(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        buf,
        typioparam,
        typmod,
    )?;
    Ok(match out {
        FmgrOut::ByVal(d) => d,
        FmgrOut::Ref(payload) => datum_ref_registry::register(payload),
    })
}

/// `OidSendFunctionCall` over the Option-4 typed boundary, returning the full
/// flat varlena byte image.
pub fn oid_send_function_call_typed(
    mcx: Mcx<'_>,
    function_id: Oid,
    val: Datum,
    arg_byval: bool,
) -> PgResult<Vec<u8>> {
    let resolved = fmgr_info(mcx, function_id)?;
    if arg_byval {
        send_function_call_typed(mcx, &resolved.resolution, resolved.finfo, FmgrArg::ByVal(val))
    } else {
        let payload = datum_ref_registry::fetch(val)?;
        send_function_call_typed(mcx, &resolved.resolution, resolved.finfo, FmgrArg::Ref(&payload))
    }
}

// ===========================================================================
// fn_expr parse-tree extraction (get_fn_expr_* / get_call_expr_*).
// ===========================================================================

/// Port of `get_fn_expr_rettype` (C: `if (!flinfo || !flinfo->fn_expr) return
/// InvalidOid; return exprType(flinfo->fn_expr);`). The opclass-options
/// `ByteaConst` fn_expr has result type `BYTEAOID`; an external node's result
/// type comes from the `exprType` seam (owned by nodeFuncs).
pub fn get_fn_expr_rettype(flinfo: Option<&FmgrInfo>) -> Oid {
    match flinfo {
        Some(f) => match &f.fn_expr {
            None => InvalidOid,
            Some(node) => match node.as_ref() {
                FnExpr::ByteaConst(_) => BYTEAOID,
                FnExpr::External(ext) => {
                    backend_nodes_nodeFuncs_seams::expr_type::call(ext.clone())
                }
            },
        },
        None => InvalidOid,
    }
}

/// Port of `get_fn_expr_argtype` (C: `if (!flinfo || !flinfo->fn_expr) return
/// InvalidOid; return get_call_expr_argtype(flinfo->fn_expr, argnum);`).
pub fn get_fn_expr_argtype(flinfo: Option<&FmgrInfo>, argnum: i32) -> Oid {
    match flinfo {
        Some(f) => get_call_expr_argtype(f.fn_expr.as_deref(), argnum),
        None => InvalidOid,
    }
}

/// Port of `get_call_expr_argtype`. The `IsA`-dispatch over the call-expression
/// node kinds (and the `ScalarArrayOpExpr` element-type hack) is owned by the
/// [`call_expr_argtype`](backend_nodes_nodeFuncs_seams::call_expr_argtype) seam.
/// A NULL `expr` yields `InvalidOid` (C: `if (expr == NULL) return InvalidOid;`).
/// The opclass-options `ByteaConst` is not a call expression, so it has no
/// argument types — `InvalidOid`.
pub fn get_call_expr_argtype(expr: Option<&FnExpr>, argnum: i32) -> Oid {
    match expr {
        None => InvalidOid,
        Some(FnExpr::ByteaConst(_)) => InvalidOid,
        Some(FnExpr::External(ext)) => {
            backend_nodes_nodeFuncs_seams::call_expr_argtype::call(ext.clone(), argnum)
        }
    }
}

/// Port of `get_fn_expr_arg_stable` (C: `if (!flinfo || !flinfo->fn_expr) return
/// false; return get_call_expr_arg_stable(flinfo->fn_expr, argnum);`).
pub fn get_fn_expr_arg_stable(flinfo: Option<&FmgrInfo>, argnum: i32) -> bool {
    match flinfo {
        Some(f) => get_call_expr_arg_stable(f.fn_expr.as_deref(), argnum),
        None => false,
    }
}

/// Port of `get_call_expr_arg_stable`. A NULL `expr` yields `false`. The
/// `ByteaConst` is not a call expression — `false`.
pub fn get_call_expr_arg_stable(expr: Option<&FnExpr>, argnum: i32) -> bool {
    match expr {
        None => false,
        Some(FnExpr::ByteaConst(_)) => false,
        Some(FnExpr::External(ext)) => {
            backend_nodes_nodeFuncs_seams::call_expr_arg_stable::call(ext.clone(), argnum)
        }
    }
}

/// Port of `get_fn_expr_variadic` (C: `if (!flinfo || !flinfo->fn_expr) return
/// false; if (IsA(expr, FuncExpr)) return funcvariadic; else return false;`).
pub fn get_fn_expr_variadic(flinfo: Option<&FmgrInfo>) -> bool {
    match flinfo {
        Some(f) => match &f.fn_expr {
            None => false,
            // C: a `Const` is not a `FuncExpr`, so `false`.
            Some(node) => match node.as_ref() {
                FnExpr::ByteaConst(_) => false,
                FnExpr::External(ext) => {
                    backend_nodes_nodeFuncs_seams::expr_variadic::call(ext.clone())
                }
            },
        },
        None => false,
    }
}

// ===========================================================================
// Opclass-options support (set/has/get_fn_opclass_options).
// ===========================================================================

/// Port of `set_fn_opclass_options` (C: `flinfo->fn_expr = (Node *)
/// makeConst(BYTEAOID, -1, InvalidOid, -1, PointerGetDatum(options), options ==
/// NULL, false);`). The bytea `Const` is the [`FnExpr::ByteaConst`] carrier;
/// `options` is the owned bytea bytes (`None` is `options == NULL`).
pub fn set_fn_opclass_options(flinfo: &mut FmgrInfo, options: Option<Vec<u8>>) {
    flinfo.fn_expr = Some(Box::new(FnExpr::ByteaConst(options)));
}

/// Port of `has_fn_opclass_options` (C: true iff `flinfo->fn_expr` is a non-null
/// `Const` of type `BYTEAOID`).
pub fn has_fn_opclass_options(flinfo: Option<&FmgrInfo>) -> bool {
    let Some(f) = flinfo else { return false };
    match &f.fn_expr {
        // C: IsA(fn_expr, Const) && consttype == BYTEAOID -> return !constisnull.
        Some(node) => match node.as_ref() {
            FnExpr::ByteaConst(options) => options.is_some(),
            FnExpr::External(_) => false,
        },
        _ => false,
    }
}

/// Port of `get_fn_opclass_options`. Returns `Ok(None)` when the `Const` is null,
/// `Ok(Some(bytes))` for the present options, or the C error
/// (`ERRCODE_INVALID_PARAMETER_VALUE`, "operator class options info is absent in
/// function call context") when no opclass `Const` is present.
pub fn get_fn_opclass_options(flinfo: Option<&FmgrInfo>) -> PgResult<Option<Vec<u8>>> {
    if let Some(f) = flinfo {
        if let Some(node) = &f.fn_expr {
            if let FnExpr::ByteaConst(options) = node.as_ref() {
                // C: return expr->constisnull ? NULL : DatumGetByteaP(constvalue);
                return Ok(options.clone());
            }
        }
    }
    Err(
        PgError::error("operator class options info is absent in function call context")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

// ===========================================================================
// CheckFunctionValidatorAccess (PL validator support).
// ===========================================================================

/// Port of `CheckFunctionValidatorAccess`. Looks up the function's `pg_proc`
/// row and its language's `pg_language` row, checks `lanvalidator ==
/// validatorOid`, then `object_aclcheck`s `ACL_USAGE` on the language and
/// `ACL_EXECUTE` on the function. `Ok(true)` when access is granted (C's only
/// non-error return); each failure is the faithful C error.
pub fn check_function_validator_access(
    mcx: Mcx<'_>,
    validator_oid: Oid,
    function_oid: Oid,
) -> PgResult<bool> {
    // C: procTup = SearchSysCache1(PROCOID, functionOid);
    //    if (!HeapTupleIsValid) ereport(ERROR, ERRCODE_UNDEFINED_FUNCTION,
    //        "function with OID %u does not exist");
    let proc = backend_utils_cache_syscache_seams::lookup_proc::call(mcx, function_oid)?
        .ok_or_else(|| {
            PgError::error(format!("function with OID {function_oid} does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION)
        })?;

    // C: langTup = SearchSysCache1(LANGOID, procStruct->prolang);
    //    if (!valid) elog(ERROR, "cache lookup failed for language %u");
    let lang: LangInfo = backend_utils_cache_syscache_seams::lookup_language::call(mcx, proc.prolang)?
        .ok_or_else(|| {
            PgError::error(format!("cache lookup failed for language {}", proc.prolang))
        })?;

    // C: if (langStruct->lanvalidator != validatorOid)
    //        ereport(ERROR, ERRCODE_INSUFFICIENT_PRIVILEGE,
    //            "language validation function %u called for language %u instead of %u");
    if lang.lanvalidator != validator_oid {
        return Err(PgError::error(format!(
            "language validation function {validator_oid} called for language {} instead of {}",
            proc.prolang, lang.lanvalidator
        ))
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
    }

    // C: aclresult = object_aclcheck(LanguageRelationId, prolang, GetUserId(), ACL_USAGE);
    //    if (aclresult != ACLCHECK_OK)
    //        aclcheck_error(aclresult, OBJECT_LANGUAGE, NameStr(lanname));
    let userid = backend_utils_init_miscinit_seams::get_user_id::call();
    let aclresult = backend_catalog_aclchk_seams::object_aclcheck::call(
        LANGUAGE_RELATION_ID,
        proc.prolang,
        userid,
        ACL_USAGE as AclMode,
    )?;
    if aclresult != AclResult::AclcheckOk {
        backend_catalog_aclchk_seams::aclcheck_error::call(
            aclresult,
            acl_object_type_to_object_type(AclObjectType::Language),
            Some(lang.lanname.as_str().to_string()),
        )?;
    }

    // C: aclresult = object_aclcheck(ProcedureRelationId, functionOid, GetUserId(), ACL_EXECUTE);
    //    if (aclresult != ACLCHECK_OK)
    //        aclcheck_error(aclresult, OBJECT_FUNCTION, NameStr(proname));
    let aclresult = backend_catalog_aclchk_seams::object_aclcheck::call(
        PROCEDURE_RELATION_ID,
        function_oid,
        userid,
        ACL_EXECUTE as AclMode,
    )?;
    if aclresult != AclResult::AclcheckOk {
        let proname = proc.proname.as_ref().map(|s| s.as_str().to_string());
        backend_catalog_aclchk_seams::aclcheck_error::call(
            aclresult,
            acl_object_type_to_object_type(AclObjectType::Function),
            proname,
        )?;
    }

    // C: ReleaseSysCache(procTup); ReleaseSysCache(langTup); return true;
    Ok(true)
}

/// C: the `OBJECT_LANGUAGE` / `OBJECT_FUNCTION` arg `CheckFunctionValidatorAccess`
/// passes to `aclcheck_error`.
fn acl_object_type_to_object_type(t: AclObjectType) -> ObjectType {
    match t {
        AclObjectType::Language => ObjectType::Language,
        AclObjectType::Function => ObjectType::Function,
    }
}

// ===========================================================================
// Seam installation (this unit owns `backend-utils-fmgr-fmgr-seams`).
// ===========================================================================

/// `fmgr_info(functionId, &finfo)` lookup half only (the `fmgr_info_check` seam):
/// resolve the function and fail exactly where C would. The owned model
/// re-resolves at call time, so no handle is returned. The resolution allocates
/// projections in a transient per-call context (dropped on return).
fn fmgr_info_check(function_id: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("fmgr_info_check");
    fmgr_info(ctx.mcx(), function_id)?;
    Ok(())
}

/// `OidFunctionCall1(functionId, PointerGetDatum(deserialize_deflist(options)))`
/// (the `oid_function_call_1_deflist` seam): the dictionary-init invocation
/// (ts_cache.c). The `List` of string-`DefElem`s crosses as typed rows; here it
/// is re-formed into the owned `internal`-pointer argument carried in the fmgr
/// internal lane (C passes `PointerGetDatum(List *)`). The returned `Datum` is
/// the dictionary's private `dictData` pointer word (genuinely heterogeneous
/// per-template state, kept opaque). Includes the C strict-null check.
fn oid_function_call_1_deflist(
    function_id: Oid,
    options: &[types_cache::DefElemString<'_>],
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("oid_function_call_1_deflist");
    let mcx = ctx.mcx();
    let resolved = fmgr_info(mcx, function_id)?;

    // C: PointerGetDatum(deserialize_deflist(...)) — a single by-reference
    // `List *` argument. The owned model carries the re-formed DefElem list in
    // the internal lane (an owned `Box<dyn Any>`); the bare `args[0]` word is a
    // placeholder. The dict-template's `dictinit` PGFunction reads it back.
    let deflist: Vec<(String, String)> = options
        .iter()
        .map(|de| (de.defname.as_str().to_string(), de.arg.as_str().to_string()))
        .collect();

    let mut fcinfo = init_fcinfo(
        Some(resolved.finfo),
        InvalidOid,
        vec![NullableDatum::value(Datum::null())],
    );
    fcinfo.set_internal_arg(0, Box::new(deflist));

    let oid = function_id;
    let result = function_call_invoke(mcx, &resolved.resolution, &mut fcinfo)?;
    // C: OidFunctionCall1 -> FunctionCall1Coll -> "function %u returned NULL".
    null_check(&fcinfo, result, &oid.to_string())
}

/// Marshal a tuple-attribute [`TupleValue`] into the boundary [`FmgrArg`] an
/// output/send function expects: a by-value scalar stays a `Datum` word; a
/// by-reference attribute's owned byte image is its `Varlena` referent (the
/// already-detoasted `struct varlena *` C would have passed).
fn tuple_value_to_arg(
    val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
) -> (Datum, Option<RefPayload>) {
    use types_tuple::backend_access_common_heaptuple::TupleValue;
    match val {
        TupleValue::ByVal(d) => (*d, None),
        TupleValue::ByRef(b) => (
            Datum::null(),
            Some(RefPayload::Varlena(b.as_slice().to_vec())),
        ),
    }
}

/// Copy a `&[u8]` into a fresh `mcx`-charged `PgVec<u8>` (the seam returns its
/// result allocated in the caller's context, as C `pstrdup`/`palloc` would).
fn bytes_into<'mcx>(mcx: Mcx<'mcx>, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = mcx::vec_with_capacity_in::<u8>(mcx, src.len())?;
    for &byte in src {
        out.push(byte);
    }
    Ok(out)
}

/// `OidSendFunctionCall(functionId, val)` seam installer (C: `fmgr_info` +
/// `SendFunctionCall`, returning a `bytea *`). Marshals the attribute value into
/// the typed boundary, runs the one-shot lookup + send call, and returns the
/// `bytea` PAYLOAD bytes (`VARSIZE - VARHDRSZ`, header stripped) charged to `mcx`.
fn oid_send_function_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let (datum, ref_arg) = tuple_value_to_arg(val);
    let resolved = fmgr_info(mcx, function_id)?;
    let arg = match &ref_arg {
        Some(p) => FmgrArg::Ref(p),
        None => FmgrArg::ByVal(datum),
    };
    // C: SendFunctionCall -> DatumGetByteaP(FunctionCall1(...)), a full bytea.
    let image = send_function_call_typed(mcx, &resolved.resolution, resolved.finfo, arg)?;
    // The seam contract strips the 4-byte varlena header to the payload the wire
    // protocol carries (proto.c reads VARSIZE - VARHDRSZ / VARDATA).
    let payload = image.get(types_datum::varlena::VARHDRSZ..).unwrap_or(&[]);
    bytes_into(mcx, payload)
}

/// `OidOutputFunctionCall(functionId, val)` seam installer (C: `fmgr_info` +
/// `OutputFunctionCall`, returning a `char *`). Marshals the attribute value into
/// the typed boundary and returns the output cstring's bytes (no terminating NUL)
/// charged to `mcx`.
fn oid_output_function_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let (datum, ref_arg) = tuple_value_to_arg(val);
    let resolved = fmgr_info(mcx, function_id)?;
    let arg = match &ref_arg {
        Some(p) => FmgrArg::Ref(p),
        None => FmgrArg::ByVal(datum),
    };
    let s = output_function_call_typed(mcx, &resolved.resolution, resolved.finfo, arg)?;
    bytes_into(mcx, s.as_bytes())
}

/// `FunctionCall1Coll(flinfo, collation, arg1)` seam: the caller's resolved
/// `FmgrInfo` cannot cross, so re-resolve by `function_id` (C: `fmgr_info`) and
/// invoke under `collation`. The transient resolution context is dropped on
/// return; the result `Datum` is by-value or a per-backend registry token.
fn function_call1_coll_seam(function_id: Oid, collation: Oid, arg1: Datum) -> PgResult<Datum> {
    let ctx = MemoryContext::new("function_call1_coll");
    oid_function_call1_coll(ctx.mcx(), function_id, collation, arg1)
}

/// `FunctionCall2Coll(flinfo, collation, arg1, arg2)` seam (re-resolve by OID).
fn function_call2_coll_seam(
    function_id: Oid,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("function_call2_coll");
    oid_function_call2_coll(ctx.mcx(), function_id, collation, arg1, arg2)
}

/// `FunctionCall3(flinfo, arg1, arg2, arg3)` seam: three non-collation arguments
/// under the default (invalid) collation (C: `FunctionCall3Coll(flinfo,
/// InvalidOid, ...)`), re-resolved by OID.
fn function_call3_seam(
    function_id: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("function_call3");
    oid_function_call3_coll(ctx.mcx(), function_id, InvalidOid, arg1, arg2, arg3)
}

/// `OutputFunctionCall(flinfo, val)` seam: the resolved `FmgrInfo` carries only
/// the lookup `fn_oid`, so re-resolve and invoke the type's text output function
/// on the per-attribute value, returning the cstring's bytes (no terminating
/// NUL) charged to `mcx`.
fn output_function_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: &types_core::fmgr::FmgrInfo,
    val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    oid_output_function_call_seam(mcx, flinfo.fn_oid, val)
}

/// `SendFunctionCall(flinfo, val)` seam: re-resolve by the `FmgrInfo`'s lookup
/// `fn_oid` and invoke the type's binary send function, returning the `bytea`
/// PAYLOAD bytes (varlena header stripped) charged to `mcx`.
fn send_function_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: &types_core::fmgr::FmgrInfo,
    val: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    oid_send_function_call_seam(mcx, flinfo.fn_oid, val)
}

/// `OidInputFunctionCall(functionId, str, typioparam, typmod)` seam used by
/// bootstrap's `InsertOneValue`: one-shot lookup + call of a type's text input
/// function on `str_`. A by-reference result is minted into the per-backend
/// [`datum_ref_registry`] (the owned `PointerGetDatum(palloc'd result)`); the
/// returned `Datum` is that token (or the by-value word).
fn oid_input_function_call_seam(
    function_id: Oid,
    str_: &str,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("oid_input_function_call");
    oid_input_function_call_typed(ctx.mcx(), function_id, Some(str_), typioparam, typmod)
}

/// `InputFunctionCall(&flinfo, str, typioparam, typmod)` seam over a
/// caller-cached `FmgrInfo` (`BuildTupleFromCStrings`), returning the result
/// classified as a [`TupleValue`] for `heap_form_tuple`. The owned `FmgrInfo`
/// carries only `fn_oid`, so this is the `Option<&str>` (NULL-allowing) form of
/// the one-shot lookup + call. By-value results travel as `ByVal`; by-reference
/// results have their registry payload materialized into `mcx`-owned bytes.
fn input_function_call_for_heap_form_seam<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    str_: Option<&str>,
    typioparam: Oid,
    typmod: i32,
    attbyval: bool,
) -> PgResult<types_tuple::backend_access_common_heaptuple::TupleValue<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::TupleValue;
    let datum = oid_input_function_call_typed(mcx, fn_oid, str_, typioparam, typmod)?;
    if attbyval {
        return Ok(TupleValue::ByVal(datum));
    }
    // By-reference: materialize the registry payload's verbatim bytes.
    let bytes: Vec<u8> = datum_ref_registry::fetch(datum)?.flatten();
    Ok(TupleValue::ByRef(mcx::slice_in(mcx, &bytes)?))
}

/// `OidOutputFunctionCall(functionId, val)` seam over a bare `Datum` (bootstrap's
/// `InsertOneValue` DEBUG4 trace): one-shot lookup + call of a type's text output
/// function on the `Datum` just built. A by-reference `Datum` is a
/// [`datum_ref_registry`] token (probed via the registry); a by-value `Datum` is
/// the literal word. Returns the rendered cstring (no NUL) as a `PgString` in
/// `mcx`.
fn oid_output_function_call_datum_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    val: Datum,
) -> PgResult<PgString<'mcx>> {
    let resolved = fmgr_info(mcx, function_id)?;
    // A by-reference `Datum` is a token into the per-backend payload table; a
    // by-value `Datum` is the literal word. Probe the table to decide which
    // boundary arm the output function expects.
    let s = match datum_ref_registry::fetch(val) {
        Ok(payload) => output_function_call_typed(
            mcx,
            &resolved.resolution,
            resolved.finfo,
            FmgrArg::Ref(&payload),
        )?,
        Err(_) => output_function_call_typed(
            mcx,
            &resolved.resolution,
            resolved.finfo,
            FmgrArg::ByVal(val),
        )?,
    };
    PgString::from_str_in(&s, mcx)
}

/// Install every seam in `backend-utils-fmgr-fmgr-seams` whose implementation is
/// `fmgr.c`'s own logic.
///
/// `render_slot_columns` (`ri_triggers.c`'s violator-column rendering) and
/// `call_bgworker_entrypoint` (the bgworker library/function dispatch) are
/// declared in this seam crate but are NOT `fmgr.c` logic; they are installed by
/// their real owners (`backend-utils-adt-ri-triggers` / loader) and panic until
/// those land, which is the correct frontier state.
pub fn init_seams() {
    backend_utils_fmgr_fmgr_seams::fmgr_info_check::set(fmgr_info_check);
    backend_utils_fmgr_fmgr_seams::oid_function_call_1_deflist::set(oid_function_call_1_deflist);
    backend_utils_fmgr_fmgr_seams::oid_send_function_call::set(oid_send_function_call_seam);
    backend_utils_fmgr_fmgr_seams::oid_output_function_call::set(oid_output_function_call_seam);
    backend_utils_fmgr_fmgr_seams::function_call1_coll::set(function_call1_coll_seam);
    backend_utils_fmgr_fmgr_seams::function_call2_coll::set(function_call2_coll_seam);
    backend_utils_fmgr_fmgr_seams::function_call3::set(function_call3_seam);
    backend_utils_fmgr_fmgr_seams::output_function_call::set(output_function_call_seam);
    backend_utils_fmgr_fmgr_seams::send_function_call::set(send_function_call_seam);
    backend_utils_fmgr_fmgr_seams::oid_input_function_call::set(oid_input_function_call_seam);
    backend_utils_fmgr_fmgr_seams::input_function_call_for_heap_form::set(
        input_function_call_for_heap_form_seam,
    );
    backend_utils_fmgr_fmgr_seams::oid_output_function_call_datum::set(
        oid_output_function_call_datum_seam,
    );
}
