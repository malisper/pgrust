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
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE,
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

pub mod builtin_canonical;

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
// Built-in REGISTRY completeness guard (no C analogue — C's compile-time
// `Gen_fmgrtab.pl` makes `fmgr_builtins[]` complete by construction; the
// per-crate runtime registry has no such guarantee, so we assert it).
// ===========================================================================

/// A canonical built-in that is absent from (or mismatched in) the runtime
/// registry. Reported by [`missing_builtins`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuiltinGap {
    /// C: `FmgrBuiltin.foid` — the canonical OID.
    pub foid: Oid,
    /// C: `FmgrBuiltin.funcName` — the canonical `prosrc` name.
    pub name: &'static str,
    /// What is wrong with this OID in the runtime registry.
    pub kind: BuiltinGapKind,
}

/// Why a canonical built-in is reported as a gap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BuiltinGapKind {
    /// `fmgr_isbuiltin(foid)` returns `None`: the OID is not registered at all
    /// (its owner crate is unported or its `init_seams()` is unwired). This is
    /// the silent-miss / boot-recursion failure mode the guard exists to catch.
    NotRegistered,
    /// The OID is registered but a metadata field diverges from `pg_proc.dat`.
    Mismatch {
        /// The mismatching field, e.g. `"nargs"`, `"strict"`, `"retset"`,
        /// `"name"`.
        field: &'static str,
    },
}

/// Compare the runtime built-in registry against the canonical set
/// [`builtin_canonical::CANONICAL`] and return every gap.
///
/// MUST be called AFTER all `init_seams()` / `register_builtins` have run
/// (i.e. on a fully-initialized backend), since the registry is per-backend
/// `thread_local` state populated at init. An empty result means the registry
/// is complete — every built-in `pg_proc.dat` declares is reachable through
/// `fmgr_isbuiltin`, exactly as C's `fmgr_builtins[]` guarantees.
///
/// This is the runtime half of the completeness guard; the
/// `seams-init` integration test calls `init_all()` then asserts this is empty.
pub fn missing_builtins() -> Vec<BuiltinGap> {
    let mut gaps = Vec::new();
    for &(foid, name, nargs, strict, retset) in builtin_canonical::CANONICAL {
        match fmgr_isbuiltin(foid) {
            None => gaps.push(BuiltinGap {
                foid,
                name,
                kind: BuiltinGapKind::NotRegistered,
            }),
            Some(reg) => {
                // The registry is keyed by OID; verify the metadata C's
                // `fmgr_builtins[]` row would carry actually matches. A
                // divergence means a crate registered the right OID with the
                // wrong calling convention — also a correctness defect.
                let field = if reg.name != name {
                    Some("name")
                } else if reg.nargs != nargs {
                    Some("nargs")
                } else if reg.strict != strict {
                    Some("strict")
                } else if reg.retset != retset {
                    Some("retset")
                } else {
                    None
                };
                if let Some(field) = field {
                    gaps.push(BuiltinGap {
                        foid,
                        name,
                        kind: BuiltinGapKind::Mismatch { field },
                    });
                }
            }
        }
    }
    gaps
}

/// Boot-time completeness assertion: panic loudly (naming the first missing
/// OID + function) if the runtime registry is missing any canonical built-in.
///
/// Call this from backend init AFTER all `register_builtins` have run, to fail
/// fast at startup instead of recursing to a stack overflow on the first
/// catalog-scan comparator whose OID was never registered. The `seams-init`
/// test exercises the same check at `cargo test` time so CI catches a regression
/// before it ever reaches a running backend.
pub fn assert_builtins_complete() {
    let gaps = missing_builtins();
    if gaps.is_empty() {
        return;
    }
    let total = gaps.len();
    let detail: String = gaps
        .iter()
        .take(20)
        .map(|g| match g.kind {
            BuiltinGapKind::NotRegistered => {
                format!("\n  builtin {} {} not registered", g.foid, g.name)
            }
            BuiltinGapKind::Mismatch { field } => format!(
                "\n  builtin {} {} registered with wrong {}",
                g.foid, g.name, field
            ),
        })
        .collect();
    let more = if total > 20 {
        format!("\n  ... and {} more", total - 20)
    } else {
        String::new()
    };
    panic!(
        "fmgr built-in registry incomplete: {} of {} canonical built-ins \
         missing or mismatched (an unported/unwired adt crate). C's \
         Gen_fmgrtab.pl makes fmgr_builtins[] complete by construction; the \
         per-crate registry must too:{}{}",
        total,
        builtin_canonical::CANONICAL.len(),
        detail,
        more
    );
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
    // C: a trigger / event-trigger / CALL dispatcher sets `fcinfo->context =
    // (Node *) &LocTriggerData` on the call frame before FunctionCallInvoke, so
    // the callee's CALLED_AS_TRIGGER(fcinfo) demux fires. The issuing dispatcher
    // cannot reach this frame (it is built inside the seam), so it deposits the
    // node-tag on a thread-local that we read back here. `None` is a plain call.
    let context = types_fmgr::fmgr::current_call_context_tag()
        .map(|tag| types_fmgr::fmgr::ContextNode { tag });
    let mut fcinfo =
        FunctionCallInfoBaseData::new(flinfo.map(Box::new), nargs, collation, context, None);
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
        // C: fn_addr == fmgr_sql; the body lives in executor/functions.c. The
        // call frame is dispatched across the owner's seam (panics "seam not
        // installed" until executor/functions.c lands).
        FmgrResolution::Sql { fn_oid } => {
            backend_executor_functions_seams::fmgr_sql::call(mcx, *fn_oid, fcinfo)
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
                    // Preferred channel: the *_v1 wrapper panicked with the full
                    // structured `PgError` value. This carries every ErrorData
                    // field (hint/detail/schema/column/...), exactly as C's
                    // ereport longjmp preserves the whole errordata — no field is
                    // lost crossing the bare-Datum PGFunction dispatch boundary.
                    let payload = match payload.downcast::<PgError>() {
                        Ok(err) => return Err(*err),
                        Err(payload) => payload,
                    };
                    // Legacy string channel (a few sites still encode just
                    // sqlstate + message as "PGRUST-SQLSTATE:XXXXX:<msg>"), and
                    // generic `panic!("...")` seam-miss messages.
                    let msg = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()));
                    match msg {
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
            // C: finfo->fn_addr = fmgr_sql; finfo->fn_stats = TRACK_FUNC_PL;
            // (fmgr.c:250-252). fmgr.c only installs the `fmgr_sql` call handler
            // here — its body lives in executor/functions.c. The owned model
            // captures the resolution; `function_call_invoke` dispatches to the
            // `fmgr_sql` seam (the executor/functions.c owner) at call time,
            // mirroring how the secdef leg installs `fmgr_security_definer`.
            finfo.fn_stats = TRACK_FUNC_PL;
            FmgrResolution::Sql { fn_oid: function_id }
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
        backend_utils_misc_guc_seams::new_guc_nest_level::call()
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
            backend_utils_misc_guc_seams::set_config_with_handle::call(
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
        backend_utils_misc_guc_seams::at_eoxact_guc::call(true, save_nestlevel)?;
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

/// Same as [`invoke_io3`] but threads a soft-error context. C builds `fcinfo`
/// with the `ErrorSaveContext` node as `fcinfo->context` and the input function
/// `ereturn`s a recoverable error into it; a non-recoverable `ereport(ERROR)`
/// is thrown regardless.
///
/// The frame now carries a real `escontext` channel ([`FunctionCallInfoBaseData::
/// set_escontext`]): the input function's fmgr adapter threads it into the value
/// core, which routes a soft error there via `ereturn`. So:
///   * the soft error never reaches the `Err` path — it lands in the frame's
///     escontext, which is copied back out (`(0, true, true)`);
///   * an `Err` here is a genuine hard error (a panic that bypassed escontext);
///     it propagates even when `escontext` is `Some` — no more blanket folding
///     of every hard error into a soft one.
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
    // Install the soft-error sink on the frame (C: `fcinfo->context =
    // (Node *) escontext`), preserving the caller's `details_wanted`. With no
    // caller sink, the frame keeps a NULL escontext and the callee's `ereturn`
    // degrades to a hard error.
    if let Some(caller) = escontext.as_ref() {
        fcinfo.set_escontext(types_error::SoftErrorContext::new(caller.details_wanted()));
    }
    let invoke = function_call_invoke(mcx, res, &mut fcinfo);
    // C: SOFT_ERROR_OCCURRED(escontext) — did the callee record a soft error?
    if fcinfo.soft_error_occurred() {
        // Reflect the captured soft error back into the caller's sink, then
        // report the soft outcome. (A soft error never produces an `Err` here.)
        if let Some(caller) = escontext {
            match fcinfo.escontext.as_mut().and_then(|c| c.take_error()) {
                Some(captured) => caller.save(captured),
                None => caller.mark_error_occurred(),
            }
        }
        return Ok((Datum::null(), true, true));
    }
    match invoke {
        Ok(result) => Ok((result, fcinfo.result_is_null(), false)),
        // A hard `ereport(ERROR)`: propagate even under a soft request.
        Err(e) => Err(e),
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

/// Datum-unification bridge: the fmgr boundary `FmgrArg`/`FmgrOut` `ByVal` arm
/// is now the canonical `types_tuple::Datum<'mcx>` (Wave 3 types-fmgr migration),
/// while this crate's internal call machinery still speaks the bare-word
/// `types_datum::Datum` (the sanctioned fmgr-ABI scalar edge). Lift a bare word
/// into the canonical `ByVal` arm (a pure by-value, lifetime-free wrap).
#[inline]
fn canon_byval(word: Datum) -> types_tuple::backend_access_common_heaptuple::Datum<'static> {
    types_tuple::backend_access_common_heaptuple::Datum::ByVal(word.as_usize())
}

/// Lower a canonical by-value `Datum` back to the bare ABI word. The boundary's
/// `ByVal` arm only ever carries a scalar (`ByVal`); a `ByRef` here would be a
/// contract violation (C would read garbage treating a referent as a word).
#[inline]
fn canon_word(d: &types_tuple::backend_access_common_heaptuple::Datum<'_>) -> Datum {
    match d {
        types_tuple::backend_access_common_heaptuple::Datum::ByVal(w) => Datum::from_usize(*w),
        types_tuple::backend_access_common_heaptuple::Datum::ByRef(_)
        | types_tuple::backend_access_common_heaptuple::Datum::Cstring(_)
        | types_tuple::backend_access_common_heaptuple::Datum::Composite(_)
        | types_tuple::backend_access_common_heaptuple::Datum::Expanded(_)
        | types_tuple::backend_access_common_heaptuple::Datum::Internal(_) => {
            panic!("fmgr boundary ByVal arm carried a by-reference Datum")
        }
    }
}

/// Read the dispatched I/O function's result as an [`FmgrOut`].
fn io_result_out<'mcx>(
    result: Datum,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> FmgrOut<'mcx> {
    match fcinfo.take_ref_result() {
        Some(payload) => FmgrOut::Ref(payload),
        None => FmgrOut::ByVal(canon_byval(result)),
    }
}

/// Option-4 port of `InputFunctionCall` (`cstring` in). C's `char *str` is
/// `input: &str`; a NULL input is `None`. The result is an [`FmgrOut`].
pub fn input_function_call_typed<'mcx>(
    mcx: Mcx<'mcx>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    input: Option<&str>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<FmgrOut<'mcx>> {
    if input.is_none() && flinfo.fn_strict {
        return Ok(FmgrOut::ByVal(canon_byval(Datum::null())));
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
pub fn input_function_call_safe_typed<'mcx>(
    mcx: Mcx<'mcx>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    input: Option<&str>,
    typioparam: Oid,
    typmod: i32,
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<Option<FmgrOut<'mcx>>> {
    if input.is_none() && flinfo.fn_strict {
        return Ok(Some(FmgrOut::ByVal(canon_byval(Datum::null()))));
    }
    let fn_oid = flinfo.fn_oid;
    let arg_ref = RefPayload::Cstring(input.unwrap_or("").to_string());
    let mut fcinfo = init_io3_ref(flinfo, arg_ref, typioparam, typmod);
    // C `InputFunctionCallSafe`: install the soft-error sink on the frame
    // (`fcinfo->context = (Node *) escontext`) and call the input function with
    // NO surrounding try/catch. The input function decides which of its errors
    // are recoverable: a recoverable one `ereturn`s into the frame's escontext
    // (caught below by `soft_error_occurred`), while a non-recoverable
    // `ereport(ERROR)` (e.g. a raw-parser syntax error inside `regtypein`)
    // propagates as a hard `Err` even under a soft request. Blanket-folding every
    // `Err` into the escontext (the old behavior) wrongly soft-caught those hard
    // errors.
    let mut escontext = escontext;
    if let Some(caller) = escontext.as_ref() {
        fcinfo.set_escontext(types_error::SoftErrorContext::new(caller.details_wanted()));
    }
    let invoke = function_call_invoke(mcx, res, &mut fcinfo);
    // C: if (SOFT_ERROR_OCCURRED(escontext)) return false; (here: Ok(None)).
    if fcinfo.soft_error_occurred() {
        if let Some(caller) = escontext {
            match fcinfo.escontext.as_mut().and_then(|c| c.take_error()) {
                Some(captured) => caller.save(captured),
                None => caller.mark_error_occurred(),
            }
        }
        return Ok(None);
    }
    // A hard `ereport(ERROR)` propagates even under a soft request.
    let result = invoke?;
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
pub fn receive_function_call_typed<'mcx>(
    mcx: Mcx<'mcx>,
    res: &FmgrResolution,
    flinfo: FmgrInfo,
    buf: Option<&[u8]>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<FmgrOut<'mcx>> {
    if buf.is_none() && flinfo.fn_strict {
        return Ok(FmgrOut::ByVal(canon_byval(Datum::null())));
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
        FmgrArg::ByVal(d) => {
            init_fcinfo(Some(flinfo), InvalidOid, vec![NullableDatum::value(canon_word(&d))])
        }
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

/// `OidInputFunctionCall` over the Option-4 typed boundary, returning the typed
/// [`FmgrOut`] (`ByVal` word or owned by-reference [`RefPayload`]). This is the
/// migration-clean inner form: the by-reference referent stays an owned payload,
/// never a per-backend registry token. Callers that must hand a single
/// bare-word `Datum` across the fmgr-return ABI edge fold a `Ref` into the
/// registry themselves (the `_typed` wrapper below); callers that build the
/// canonical `Datum<'mcx>` enum map it straight onto `ByVal`/`ByRef`.
fn oid_input_function_call_out<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    input: Option<&str>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<FmgrOut<'mcx>> {
    let resolved = fmgr_info(mcx, function_id)?;
    input_function_call_typed(mcx, &resolved.resolution, resolved.finfo, input, typioparam, typmod)
}


// ===========================================================================
// fn_expr parse-tree extraction (get_fn_expr_* / get_call_expr_*).
// ===========================================================================

/// Recover the field-bearing owned `Expr` an `ExternalFnExpr` carrier holds
/// erased (`fmgr_info_set_expr` stamped it via the
/// [`function_call_invoke_datum`] dispatch). `None` is the tag-only carrier (no
/// node available — the readers then fall through to the tag-only seams). The
/// downcast targets the one concrete type the setter ever boxes; a mismatch
/// maps to the same `None` fall-through.
fn external_expr(ext: &types_fmgr::ExternalFnExpr) -> Option<&types_nodes::primnodes::Expr> {
    ext.node
        .as_ref()?
        .downcast_ref::<types_nodes::primnodes::Expr>()
}

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
                // C: `return exprType(flinfo->fn_expr)`. When the field-bearing
                // call node is carried (`fmgr_info_set_expr` stamped a real
                // `Expr`), read its result type through the field-bearing
                // `expr_type_info` seam; otherwise fall through to the tag-only
                // seam (InvalidOid).
                FnExpr::External(ext) => match external_expr(ext) {
                    Some(expr) => {
                        backend_nodes_nodeFuncs_seams::expr_type_info::call(expr)
                            .map_or(InvalidOid, |t| t.typid)
                    }
                    None => backend_nodes_nodeFuncs_seams::expr_type::call(ext.clone()),
                },
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
        // When the field-bearing call node is carried, read the declared
        // argument type through the field-bearing `get_call_expr_argtype_expr`
        // seam; otherwise fall through to the tag-only seam (InvalidOid).
        Some(FnExpr::External(ext)) => match external_expr(ext) {
            Some(e) => backend_nodes_nodeFuncs_seams::get_call_expr_argtype_expr::call(e, argnum)
                .unwrap_or(InvalidOid),
            None => backend_nodes_nodeFuncs_seams::call_expr_argtype::call(ext.clone(), argnum),
        },
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
        Some(FnExpr::External(ext)) => match external_expr(ext) {
            Some(e) => backend_nodes_nodeFuncs_seams::call_expr_arg_stable_expr::call(e, argnum),
            None => backend_nodes_nodeFuncs_seams::call_expr_arg_stable::call(ext.clone(), argnum),
        },
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
                FnExpr::External(ext) => match external_expr(ext) {
                    Some(e) => backend_nodes_nodeFuncs_seams::expr_variadic_expr::call(e),
                    None => backend_nodes_nodeFuncs_seams::expr_variadic::call(ext.clone()),
                },
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

/// `fmgr_info(functionId, &finfo)` resolving form (the `fmgr_info` seam):
/// resolve the function and return the lookup metadata callers read to *plan*
/// a call. The internal `fmgr_info()` yields a `types-fmgr::FmgrInfo` (whose
/// `fn_addr` is a typed `PGFunction`); the seam crosses with a
/// `types-core::FmgrInfo` (whose `fn_addr` is an opaque pointer word, so the
/// callable does not have to cross). The fields are copied straight across;
/// `fn_addr` is the resolved callable's address (`0` when unresolved, e.g. the
/// security-definer / SQL legs where C installs a wrapper).
fn fmgr_info_resolve(mcx: Mcx<'_>, function_id: Oid) -> PgResult<types_core::fmgr::FmgrInfo> {
    let resolved = fmgr_info(mcx, function_id)?;
    let f = &resolved.finfo;
    // PGFunction is `Option<fn(...) -> Datum>`; a function pointer casts to its
    // address. `None` (no direct callable resolved) maps to 0.
    let fn_addr = f.fn_addr.map(|p| p as usize).unwrap_or(0);
    Ok(types_core::fmgr::FmgrInfo {
        fn_addr,
        fn_oid: f.fn_oid,
        fn_nargs: f.fn_nargs,
        fn_strict: f.fn_strict,
        fn_retset: f.fn_retset,
        fn_stats: f.fn_stats,
        // C: `fmgr_info()` leaves `fn_expr = NULL`; a later
        // `fmgr_info_set_expr()` stamps the call-expression node on.
        fn_expr: None,
    })
}

/// `fmgr_info_set_expr(expr, finfo)` (fmgr.h: `finfo->fn_expr = expr`) — the
/// `fmgr_info_set_expr` seam. Boxes the owned call-expression `Expr` into the
/// `FmgrInfo.fn_expr` erased carrier (`types-core` names only `dyn Any`; this
/// crate depends on `types-nodes` and supplies the concrete `Expr`). C stores
/// the bare `Node *`; the owned model shares the node through the refcounted
/// erased box. The downcast type used by the readers below is exactly this
/// `types_nodes::primnodes::Expr`.
fn fmgr_info_set_expr_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    finfo: &mut types_core::fmgr::FmgrInfo,
    expr: &types_nodes::primnodes::Expr,
) -> types_error::PgResult<()> {
    // clone_in: the call node may be an OpExpr/FuncExpr carrying an Aggref (a
    // HAVING qual operator), whose context-allocated TargetEntry args a bare
    // derived `.clone()` panics on.
    finfo.fn_expr = Some(types_core::fmgr::FnExprErased::new(expr.clone_in(mcx)?));
    Ok(())
}

/// Recover the `&Expr` a prior `fmgr_info_set_expr` stamped onto the frame's
/// `flinfo->fn_expr`. `None` is C's "`flinfo == NULL || flinfo->fn_expr ==
/// NULL`" (then `get_fn_expr_*` returns the InvalidOid fall-through). The
/// downcast is to the one concrete type the setter ever boxes; a mismatch is
/// impossible (only `fmgr_info_set_expr_seam` writes the slot) but maps to the
/// same `None` fall-through.
fn fcinfo_fn_expr<'a>(
    fcinfo: &'a types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
) -> Option<&'a types_nodes::primnodes::Expr> {
    fcinfo
        .flinfo
        .as_ref()?
        .fn_expr
        .as_ref()?
        .downcast_ref::<types_nodes::primnodes::Expr>()
}

/// `get_fn_expr_argtype(fcinfo->flinfo, argnum)` (fmgr.h) — the `get_fn_expr_argtype`
/// seam over the `types_nodes` call frame. C: `if (!flinfo || !flinfo->fn_expr)
/// return InvalidOid; return get_call_expr_argtype(flinfo->fn_expr, argnum);`.
/// The `IsA` dispatch lives in nodeFuncs (it knows the `Expr` field shapes).
fn get_fn_expr_argtype_seam(
    fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    argnum: i32,
) -> PgResult<Oid> {
    match fcinfo_fn_expr(fcinfo) {
        None => Ok(InvalidOid),
        Some(expr) => {
            backend_nodes_nodeFuncs_seams::get_call_expr_argtype_expr::call(expr, argnum)
        }
    }
}

/// `get_fn_expr_rettype(fcinfo->flinfo)` (fmgr.h) — the `get_fn_expr_rettype`
/// seam. C: `if (!flinfo || !flinfo->fn_expr) return InvalidOid; return
/// exprType(flinfo->fn_expr);`. `exprType` is the nodeFuncs `expr_type_info`
/// read.
fn get_fn_expr_rettype_seam(
    fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
) -> PgResult<Oid> {
    match fcinfo_fn_expr(fcinfo) {
        None => Ok(InvalidOid),
        Some(expr) => {
            Ok(backend_nodes_nodeFuncs_seams::expr_type_info::call(expr)?.typid)
        }
    }
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

/// `typenameTypeMod`'s typmod-resolution tail (parse_type.c): build the
/// `cstring[]` array from the raw typmod expressions distilled to strings, then
/// `DatumGetInt32(OidFunctionCall1(typmodin, PointerGetDatum(arrtypmod)))`. The C
/// wraps the call in a `setup_parser_errposition_callback`, so a typmodin failure
/// that carries no cursor position of its own is tagged with the `TypeName`'s
/// parse `location`. Only the fmgr owner can synthesize the `cstring[]` Datum
/// argument (a by-reference array varlena image, built via the arrayfuncs
/// `construct_array_builtin(CSTRINGOID)` port) and dispatch the OID call.
fn typmodin_seam(typmodin: Oid, cstrings: &[String], location: i32) -> PgResult<i32> {
    let ctx = MemoryContext::new("typmodin");
    let mcx = ctx.mcx();
    let resolved = fmgr_info(mcx, typmodin)?;

    // construct_array_builtin(datums, n, CSTRINGOID): the cstring[] array varlena
    // image, carried on the by-reference side channel as a single arg.
    let elem_refs: Vec<&str> = cstrings.iter().map(|s| s.as_str()).collect();
    let arr = backend_utils_adt_arrayfuncs_seams::build_cstring_array::call(mcx, &elem_refs)?;

    let args = vec![NullableDatum::value(Datum::null())];
    let ref_args = vec![Some(RefPayload::Varlena(arr.as_slice().to_vec()))];

    let result = function_call_coll_ref_args(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        InvalidOid,
        args,
        ref_args,
    );

    // setup_parser_errposition_callback: tag a typmodin failure with the parse
    // location iff it has no cursor position of its own.
    match result {
        Ok(d) => Ok(d.as_i32()),
        Err(e) => {
            if location >= 0 && e.cursor_position().is_none() {
                Err(e.with_cursor_position(location))
            } else {
                Err(e)
            }
        }
    }
}

/// Marshal a tuple-attribute [`Datum`] into the boundary [`FmgrArg`] an
/// output/send function expects: a by-value scalar stays a `Datum` word; a
/// by-reference attribute's owned byte image is its `Varlena` referent (the
/// already-detoasted `struct varlena *` C would have passed).
fn tuple_value_to_arg(
    val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
) -> (Datum, Option<RefPayload>) {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
    match val {
        CanonDatum::ByVal(d) => (Datum::from_usize(*d), None),
        // Header-ful everywhere: the canonical `ByRef` image and the fmgr
        // `RefPayload::Varlena` lane are the SAME self-describing varlena image,
        // so it crosses VERBATIM (no strip).
        CanonDatum::ByRef(b) => (
            Datum::null(),
            Some(RefPayload::Varlena(b.as_slice().to_vec())),
        ),
        // A cstring referent maps directly to the cstring boundary arm.
        CanonDatum::Cstring(s) => (Datum::null(), Some(RefPayload::Cstring(s.clone()))),
        // A composite value crosses as its flat HeapTupleHeader Datum image (C:
        // the `struct varlena *`-tagged `HeapTupleHeader` pointer).
        CanonDatum::Composite(t) => (
            Datum::null(),
            Some(RefPayload::Composite(t.to_datum_image())),
        ),
        CanonDatum::Expanded(_) | CanonDatum::Internal(_) => {
            panic!("tuple_value_to_arg: Expanded/Internal Datum not yet produced — wave 2")
        }
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
    val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let (datum, ref_arg) = tuple_value_to_arg(val);
    let resolved = fmgr_info(mcx, function_id)?;
    let arg = match &ref_arg {
        Some(p) => FmgrArg::Ref(p),
        None => FmgrArg::ByVal(canon_byval(datum)),
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
    val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    // Header-ful everywhere: every by-ref value (string cores AND container I/O
    // cores) reads the same framed varlena image, so the argument crosses
    // VERBATIM (no header strip).
    let (datum, ref_arg) = tuple_value_to_arg(val);
    let resolved = fmgr_info(mcx, function_id)?;
    let arg = match &ref_arg {
        Some(p) => FmgrArg::Ref(p),
        None => FmgrArg::ByVal(canon_byval(datum)),
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

/// Marshal a canonical per-attribute [`Datum`] into the `(arg-word, ref-arg)`
/// pair `function_call_coll_ref_args_out` expects: a by-value scalar is the bare
/// word with no referent; a by-reference value is the null word plus its owned
/// detoasted bytes as a `Varlena` referent (the `struct varlena *` C would pass).
pub fn datum_to_ref_arg(
    val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
) -> (NullableDatum, Option<RefPayload>) {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
    match val {
        CanonDatum::ByVal(d) => (NullableDatum::value(Datum::from_usize(*d)), None),
        // Header-ful everywhere: the canonical `ByRef` image and the fmgr
        // `RefPayload::Varlena` lane are the SAME self-describing varlena image
        // (the adt cores read `VARDATA_ANY` off it and return a header-ful image),
        // so it crosses VERBATIM (no strip).
        CanonDatum::ByRef(b) => (
            NullableDatum::value(Datum::null()),
            Some(RefPayload::Varlena(b.as_slice().to_vec())),
        ),
        CanonDatum::Cstring(s) => (
            NullableDatum::value(Datum::null()),
            Some(RefPayload::Cstring(s.clone())),
        ),
        // A composite value crosses as its flat HeapTupleHeader Datum image (C:
        // the `struct varlena *`-tagged `HeapTupleHeader` pointer).
        CanonDatum::Composite(t) => (
            NullableDatum::value(Datum::null()),
            Some(RefPayload::Composite(t.to_datum_image())),
        ),
        CanonDatum::Internal(_) => {
            // An `internal` value cannot be cloned out of a borrow (the
            // `Box<dyn Any>` has no Clone). The by-value `datum_to_ref_arg_owned`
            // path moves it instead; reaching here means a borrowed-arg caller
            // tried to marshal an internal Datum, a wiring bug.
            panic!("datum_to_ref_arg: an `internal` Datum must cross by value (use datum_to_ref_arg_owned / function_call_invoke_datum_owned)")
        }
        CanonDatum::Expanded(_) => {
            panic!("datum_to_ref_arg: Expanded Datum not yet produced — wave 2")
        }
    }
}

/// By-value form of [`datum_to_ref_arg`]: consumes the canonical [`Datum`] so an
/// `internal` value's owned `Box<dyn Any>` moves into the `RefPayload::Internal`
/// by-reference referent (C: `args[0].value = (Datum) state`, a `void *`).
pub fn datum_to_ref_arg_owned(
    val: types_tuple::backend_access_common_heaptuple::Datum<'_>,
) -> (NullableDatum, Option<RefPayload>) {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
    match val {
        CanonDatum::Internal(state) => (
            NullableDatum::value(Datum::null()),
            Some(RefPayload::Internal(state)),
        ),
        // Every other arm has the same marshalling as the borrowed form.
        other => datum_to_ref_arg(&other),
    }
}

/// Map a `function_call_coll_ref_args_out` `(word, ref_result)` pair back onto a
/// canonical [`Datum`]: a by-reference result materializes its referent bytes
/// into `mcx` (`ByRef`); otherwise the bare word is the by-value scalar (`ByVal`).
fn ref_out_to_datum<'mcx>(
    mcx: Mcx<'mcx>,
    word: Datum,
    ref_result: Option<RefPayload>,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
    match ref_result {
        // A `cstring`-typed result (e.g. a type's output function) stays a
        // canonical `Cstring` so a downstream consumer that reads its arg via the
        // `cstring` lane (`PG_GETARG_CSTRING` → `RefPayload::Cstring`) sees it —
        // notably the input function in an I/O coercion (`text::bool` =
        // textout→boolin). Flattening it to `ByRef` here would re-marshal it as a
        // `Varlena` arg (`datum_to_ref_arg`) and the input function's
        // `as_cstring()` would miss it.
        Some(RefPayload::Cstring(s)) => Ok(CanonDatum::Cstring(s)),
        // A composite result reconstructs into the canonical `Composite` arm
        // (C: the `HeapTupleHeader` Datum is a fully-formed tuple, not raw
        // varlena bytes) so downstream consumers see the row, not bytes.
        Some(RefPayload::Composite(image)) => Ok(CanonDatum::Composite(
            types_tuple::backend_access_common_heaptuple::FormedTuple::from_datum_image(
                mcx, &image,
            )?,
        )),
        // An `internal`-returning function (an aggregate transfn) hands back its
        // live state box; carry it on the canonical `Internal` arm.
        Some(RefPayload::Internal(state)) => Ok(CanonDatum::Internal(state)),
        Some(payload) => {
            // Header-ful everywhere: a by-reference function result IS a header-ful
            // `struct varlena *` image (the adt cores return `set_varsize_4b ++
            // payload`), and the canonical `ByRef` image is likewise header-ful, so
            // the `RefPayload::Varlena` payload crosses VERBATIM — no restamp.
            // (Composite/Expanded already flatten to a header-ful image;
            // Cstring/Internal are handled in their own arms above.)
            let image = byref_element_ondisk_image(payload);
            Ok(CanonDatum::ByRef(mcx::slice_in(mcx, &image)?))
        }
        None => Ok(CanonDatum::ByVal(canon_word(&canon_byval(word)).as_usize())),
    }
}

/// `FunctionCall1Coll(flinfo, collation, arg1)` over the canonical `Datum` lane
/// (re-resolve by OID); see the seam doc. By-reference args/result cross via the
/// fmgr by-reference side channel.
fn function_call1_coll_datum_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    collation: Oid,
    arg1: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    let resolved = fmgr_info(mcx, function_id)?;
    let (a1, r1) = datum_to_ref_arg(&arg1);
    let (word, ref_result) = function_call_coll_ref_args_out(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        collation,
        vec![a1],
        vec![r1],
    )?;
    ref_out_to_datum(mcx, word, ref_result)
}

/// The fmgr leg of `evaluate_expr` (clauses.c) — the planner const-folder's
/// `EEOP_FUNCEXPR[_STRICT]` substitute. C runs the all-`Const` `FuncExpr`/`OpExpr`
/// through `ExecInitExpr` + `ExecEvalExprSwitchContext`; the in-crate fast path
/// has already established that every argument is a `Const`, so this directly
/// invokes `funcid` over the constant argument values (the executor's strict
/// opcode is the only `evaluate_expr`-relevant behavior on this shape, and it is
/// applied here explicitly).
///
/// Each argument crosses the fmgr boundary as either its bare by-value word or
/// its by-reference referent (the same `datum_to_ref_arg` marshalling the BRIN
/// `*_coll_datum` seams use), with `args[i].isnull` carried through. A strict
/// function with any NULL argument short-circuits to `(NULL, true)` WITHOUT
/// calling (C: the `EEOP_FUNCEXPR_STRICT` arg loop). The result `Datum` (by-value
/// or by-reference) is materialized into `mcx`, mirroring C's "copy result out of
/// the sub-context" step in `evaluate_expr`; `makeConst`'s detoast/`datumCopy`
/// tail lives on the clauses-crate side.
fn fmgr_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
    inputcollid: Oid,
    args: Vec<(types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool, Oid)>,
    rettype: Oid,
    fn_expr: Option<&types_nodes::primnodes::Expr>,
) -> PgResult<(types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool)> {
    let _ = rettype; // result classification rides the callee's `ref_result` arm.
    let mut resolved = fmgr_info(mcx, funcid)?;

    // C `evaluate_function` runs `fmgr_info_set_expr((Node *) newexpr, &finfo)`
    // before the call, so a polymorphic function const-folded at plan time can
    // read its declared result/argument types (`get_fn_expr_rettype/argtype`,
    // e.g. `int4range(1,5)`'s `range_constructor2`). The by-OID re-resolution
    // above produced `fn_expr == None`; stamp the call node (carried erased
    // through the tag-only `FnExpr::External`).
    if let Some(expr) = fn_expr {
        // clone_in: the call node may carry an Aggref (a HAVING qual operator),
        // whose context-allocated TargetEntry args a bare derived `.clone()`
        // panics on.
        resolved.finfo.fn_expr = Some(Box::new(FnExpr::External(types_fmgr::ExternalFnExpr {
            tag: 0,
            node: Some(types_core::fmgr::FnExprErased::new(expr.clone_in(mcx)?)),
        })));
    }

    // C: the `_STRICT` opcode's arg loop — a strict function with any NULL input
    // returns NULL without being called. `fn_strict` is `proisstrict`.
    if resolved.finfo.fn_strict && args.iter().any(|(_, isnull, _)| *isnull) {
        return Ok((
            types_tuple::backend_access_common_heaptuple::Datum::null(),
            true,
        ));
    }

    // Build the `fcinfo->args[]` frame: by-value word lane + by-reference side
    // channel, with each arg's NULL flag threaded through.
    let mut nargs: Vec<NullableDatum> = Vec::with_capacity(args.len());
    let mut ref_args: Vec<Option<RefPayload>> = Vec::with_capacity(args.len());
    for (val, isnull, _argtype) in &args {
        let (mut nd, refp) = datum_to_ref_arg(val);
        nd.isnull = *isnull;
        nargs.push(nd);
        ref_args.push(refp);
    }

    // C: fcache->flinfo.fn_expr = fcinfo->flinfo->fn_expr (fmgr.c:658).
    let fn_expr = resolved.finfo.fn_expr.clone();
    let mut fcinfo = init_fcinfo(Some(resolved.finfo), inputcollid, nargs);
    fcinfo.ref_args = ref_args;
    fcinfo.debug_assert_ref_null_consistency();
    // C: fcinfo->isnull = false; const_val = ExecEvalExpr(...);
    //    const_is_null = fcinfo->isnull;
    // Dispatch directly (NOT through `invoke_flinfo`/`null_check`): a non-strict
    // function legitimately returns NULL through `fcinfo->isnull`, which
    // `evaluate_expr` folds into a NULL `Const` — the `function returned NULL`
    // self-test does NOT apply on this path.
    fcinfo.isnull = false;
    let word = function_call_invoke_with_expr(mcx, &resolved.resolution, &mut fcinfo, fn_expr)?;
    let const_is_null = fcinfo.isnull;
    let ref_result = fcinfo.take_ref_result();
    if const_is_null {
        return Ok((
            types_tuple::backend_access_common_heaptuple::Datum::null(),
            true,
        ));
    }
    // Materialize the result into `mcx` (C: "copy result out of sub-context").
    let result = ref_out_to_datum(mcx, word, ref_result)?;
    Ok((result, false))
}

/// `FunctionCall2Coll(flinfo, collation, arg1, arg2)` over the canonical `Datum`
/// lane (re-resolve by OID); see the seam doc.
fn function_call2_coll_datum_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    collation: Oid,
    arg1: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    arg2: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    let resolved = fmgr_info(mcx, function_id)?;
    let (a1, r1) = datum_to_ref_arg(&arg1);
    let (a2, r2) = datum_to_ref_arg(&arg2);
    let (word, ref_result) = function_call_coll_ref_args_out(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        collation,
        vec![a1, a2],
        vec![r1, r2],
    )?;
    ref_out_to_datum(mcx, word, ref_result)
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
    val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    oid_output_function_call_seam(mcx, flinfo.fn_oid, val)
}

/// `SendFunctionCall(flinfo, val)` seam: re-resolve by the `FmgrInfo`'s lookup
/// `fn_oid` and invoke the type's binary send function, returning the `bytea`
/// PAYLOAD bytes (varlena header stripped) charged to `mcx`.
fn send_function_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: &types_core::fmgr::FmgrInfo,
    val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    oid_send_function_call_seam(mcx, flinfo.fn_oid, val)
}

/// `OidInputFunctionCall(functionId, str, typioparam, typmod)` seam used by
/// bootstrap's `InsertOneValue`: one-shot lookup + call of a type's text input
/// function on `str_`. The result is the canonical `Datum<'mcx>` — a by-value
/// scalar is `ByVal` (the bare word); a by-reference result is an owned `ByRef`
/// over the input function's flattened payload bytes (C's
/// `PointerGetDatum(palloc'd result)`), with no per-backend registry token.
fn oid_input_function_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    str_: &str,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
    match oid_input_function_call_out(mcx, function_id, Some(str_), typioparam, typmod)? {
        FmgrOut::ByVal(d) => Ok(d),
        FmgrOut::Ref(payload) => {
            let bytes: Vec<u8> = payload.flatten();
            Ok(CanonDatum::ByRef(mcx::slice_in(mcx, &bytes)?))
        }
    }
}

/// `InputFunctionCall(&flinfo, str, typioparam, typmod)` seam over a
/// caller-cached `FmgrInfo` (`BuildTupleFromCStrings`), returning the result
/// classified as a [`Datum`] for `heap_form_tuple`. The owned `FmgrInfo`
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
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
    // The seam contract is the canonical `Datum<'mcx>` enum, so the input
    // function's result maps straight onto it — no per-backend registry token
    // round-trip. A by-value result is `ByVal` (the bare word); a by-reference
    // result is an owned `ByRef` over the input function's flattened payload
    // bytes (C's `PointerGetDatum(palloc'd result)`).
    match oid_input_function_call_out(mcx, fn_oid, str_, typioparam, typmod)? {
        // A strict NULL / by-value scalar: keep the bare ABI word.
        FmgrOut::ByVal(d) => Ok(CanonDatum::ByVal(canon_word(&d).as_usize())),
        // C classifies by `attbyval`: a by-value type with a by-reference-shaped
        // result still reads its word back; otherwise materialize the payload.
        FmgrOut::Ref(payload) if attbyval => {
            // attbyval but the input function produced a referent — read the
            // leading machine word out of the flattened payload (C's
            // `fetch_att`: a by-value attribute reads `sizeof(Datum)` bytes from
            // the storage the pointer addresses).
            let bytes = payload.flatten();
            let mut word_bytes = [0u8; core::mem::size_of::<usize>()];
            let n = bytes.len().min(word_bytes.len());
            word_bytes[..n].copy_from_slice(&bytes[..n]);
            Ok(CanonDatum::ByVal(usize::from_ne_bytes(word_bytes)))
        }
        FmgrOut::Ref(payload) => {
            let bytes: Vec<u8> = payload.flatten();
            Ok(CanonDatum::ByRef(mcx::slice_in(mcx, &bytes)?))
        }
    }
}

/// `OidOutputFunctionCall(functionId, val)` seam over the canonical `Datum`
/// (bootstrap's `InsertOneValue` DEBUG4 trace): one-shot lookup + call of a
/// type's text output function on the `Datum` just built. The canonical enum's
/// own arm decides the boundary classification: a `ByVal` scalar crosses as the
/// machine word; a `ByRef` referent crosses as a borrowed [`RefPayload`] over
/// its bytes (no per-backend registry token). Returns the rendered cstring (no
/// NUL) as a `PgString` in `mcx`.
fn oid_output_function_call_datum_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    val: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
) -> PgResult<PgString<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
    let resolved = fmgr_info(mcx, function_id)?;
    let s = match &val {
        CanonDatum::ByVal(_) => output_function_call_typed(
            mcx,
            &resolved.resolution,
            resolved.finfo,
            FmgrArg::ByVal(val.clone()),
        )?,
        CanonDatum::ByRef(bytes) => {
            // Header-ful everywhere: the canonical `ByRef` image is the same
            // self-describing varlena the output function reads (string cores via
            // `VARDATA_ANY`, container I/O cores via the framed image), so it
            // crosses VERBATIM.
            let payload = types_fmgr::boundary::RefPayload::Varlena(bytes.as_slice().to_vec());
            output_function_call_typed(
                mcx,
                &resolved.resolution,
                resolved.finfo,
                FmgrArg::Ref(&payload),
            )?
        }
        CanonDatum::Cstring(s2) => {
            let payload = types_fmgr::boundary::RefPayload::Cstring(s2.clone());
            output_function_call_typed(
                mcx,
                &resolved.resolution,
                resolved.finfo,
                FmgrArg::Ref(&payload),
            )?
        }
        CanonDatum::Composite(t) => {
            let payload = types_fmgr::boundary::RefPayload::Composite(t.to_datum_image());
            output_function_call_typed(
                mcx,
                &resolved.resolution,
                resolved.finfo,
                FmgrArg::Ref(&payload),
            )?
        }
        CanonDatum::Expanded(_) | CanonDatum::Internal(_) => {
            panic!("output_function_call (datum) on Expanded/Internal Datum not yet produced — wave 2")
        }
    };
    PgString::from_str_in(&s, mcx)
}

/// Collapse a typed [`FmgrOut`] to the bare ABI word the `DatumWord`-returning
/// I/O seams contract for. The I/O scalar boundary is the bare-word lane: a
/// by-value result is its machine word; a by-reference result is a contract
/// anomaly here (these seams' callers wrap the word straight into a by-value
/// arm — `domains.rs`, `parse-type`, `arrayfuncs/io.rs`), mirroring `canon_word`.
#[inline]
fn fmgr_out_word(out: FmgrOut<'_>) -> Datum {
    match out {
        FmgrOut::ByVal(d) => canon_word(&d),
        FmgrOut::Ref(_) => {
            panic!("I/O seam (DatumWord lane) produced a by-reference result")
        }
    }
}

/// The on-disk byte image of a by-reference element value, as the array build
/// path (`arrayfuncs.c`'s `att_addlength_datum` / `ArrayCastAndSet`) expects to
/// find behind the element `Datum` pointer: a real `struct varlena` (4-byte
/// length header + payload) for a varlena type, the NUL-terminated bytes for a
/// `cstring` type, or the already-header-ful image for a composite/expanded
/// value.
///
/// Header-ful everywhere: the `RefPayload::Varlena` carrier already holds the
/// complete `struct varlena *` image (4-byte length header + payload, exactly the
/// `text *` memory the input function would have palloc'd), so it is the on-disk
/// element image VERBATIM. The `RefPayload::Cstring` carrier holds the text
/// without its terminating NUL, which is appended here. Composite/expanded
/// payloads are already a flat Datum image (their first word is the `datum_len_`
/// varlena header), so they flatten verbatim.
fn byref_element_ondisk_image(payload: RefPayload) -> Vec<u8> {
    match payload {
        // varlena type: the carrier is already the header-ful image — verbatim.
        RefPayload::Varlena(body) => body,
        // cstring type (typlen == -2): the element bytes are the C `char *`
        // string with its terminating NUL, which `att_addlength_datum`/
        // `ArrayCastAndSet` size with `strlen + 1`.
        RefPayload::Cstring(s) => {
            let mut bytes = s.into_bytes();
            bytes.push(0);
            bytes
        }
        // composite / expanded: already a flat, header-ful Datum image.
        other => other.flatten(),
    }
}

/// Materialize a by-reference [`FmgrOut`] element into `mcx` as its on-disk
/// image and return a bare-word `Datum` pointing at those bytes — the C
/// `InputFunctionCallSafe`/`ReceiveFunctionCall` bare-`Datum` return (a pointer
/// into `CurrentMemoryContext`) the array build path dereferences. By-value
/// results travel as the machine word. The leaked allocation stays alive in
/// `mcx` for the rest of the caller's (`array_in`/`array_recv`) build.
fn fmgr_out_element_word<'mcx>(mcx: Mcx<'mcx>, out: FmgrOut<'_>) -> PgResult<Datum> {
    match out {
        FmgrOut::ByVal(d) => Ok(canon_word(&d)),
        FmgrOut::Ref(payload) => {
            let image = byref_element_ondisk_image(payload);
            let stored: &'mcx mut [u8] = mcx::slice_in(mcx, &image)?.leak();
            Ok(Datum::from_usize(stored.as_ptr() as usize))
        }
    }
}

/// `InputFunctionCall(flinfo, str, typioparam, typmod)` seam (fmgr.c) for a
/// hard-error caller: one-shot lookup by `function_id` + call of the text input
/// function on `str` (`None` is C's NULL cstring). Returns the result as the
/// canonical `Datum<'mcx>` — a by-value scalar is `ByVal` (the bare word); a
/// by-reference result (text/name/varchar/numeric) is an owned `ByRef` over the
/// input function's flattened payload bytes in `mcx` (C's
/// `PointerGetDatum(palloc'd result)`). `parse-type`'s `stringTypeDatum` and
/// `domains.rs`/`rangetypes` thread this canonical value straight into their
/// `Const`/range carriers (no by-reference loss).
fn input_function_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    str: Option<&str>,
    typioparam: Oid,
    typmod: i32,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
    match oid_input_function_call_out(mcx, function_id, str, typioparam, typmod)? {
        FmgrOut::ByVal(d) => Ok(CanonDatum::ByVal(canon_word(&d).as_usize())),
        FmgrOut::Ref(payload) => {
            let bytes: Vec<u8> = payload.flatten();
            Ok(CanonDatum::ByRef(mcx::slice_in(mcx, &bytes)?))
        }
    }
}

/// `ReceiveFunctionCall(flinfo, buf, typioparam, typmod)` seam (fmgr.c): one-shot
/// lookup by `function_id` + call of the binary receive function on the
/// `StringInfo` payload `buf`. Returns the bare scalar word.
fn receive_function_call_seam(
    mcx: Mcx<'_>,
    function_id: Oid,
    buf: &[u8],
    typioparam: Oid,
    typmod: i32,
) -> PgResult<Datum> {
    let resolved = fmgr_info(mcx, function_id)?;
    let out = receive_function_call_typed(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        Some(buf),
        typioparam,
        typmod,
    )?;
    Ok(fmgr_out_word(out))
}

/// `InputFunctionCallSafe(&inputproc, str, typioparam, typmod, escontext,
/// &result)` seam (fmgr.c) as `array_in` drives it: call `function_id`'s text
/// input function on the element substring `str_` under an internal soft-error
/// context. `Ok(Some(word))` on success, `Ok(None)` when the conversion raised
/// a soft error (C: `array_in` returns `Ok(None)`), `Err` on a hard error.
fn input_function_call_safe_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    str_: &str,
    typioparam: Oid,
    typmod: i32,
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<Option<Datum>> {
    let resolved = fmgr_info(mcx, function_id)?;
    // C: `InputFunctionCallSafe(&my_extra->proc, ..., escontext, &result)` —
    // `array_in` passes its own `escontext` straight through, so a bad element
    // value lands in the caller's sink (this returns `Ok(None)`). With a `None`
    // escontext (a hard-error caller) a conversion error escalates to a hard
    // `Err`, exactly as C's NULL-escontext path does.
    let out = input_function_call_safe_typed(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        Some(str_),
        typioparam,
        typmod,
        escontext,
    )?;
    // C's `InputFunctionCallSafe` yields a bare `Datum`: a by-value scalar is
    // the machine word; a by-reference element (text/name/numeric/…) is a
    // pointer to the input function's palloc'd flattened result in
    // `CurrentMemoryContext`. The caller (`array_in`) is the build context's
    // owner, so materialize the by-reference payload's on-disk image into `mcx`
    // (the caller's build arena) and return a `Datum` whose word is the pointer
    // to those bytes — exactly what `att_addlength_datum`/`ArrayCastAndSet`
    // dereference. The whole arena is reclaimed when the build context is
    // dropped, mirroring C's per-context teardown.
    match out {
        None => Ok(None),
        Some(o) => Ok(Some(fmgr_out_element_word(mcx, o)?)),
    }
}

/// `pg_input_is_valid_common()` post-parse work (misc.c:804-814): given the
/// already-resolved `typoid`/`typmod`, run `getTypeInputInfo(typoid,
/// &typiofunc, &typioparam)` + `fmgr_info_cxt` + `InputFunctionCallSafe(...)`
/// over the input bytes `str_`, recording any soft error into `escontext`.
/// Returns the C `bool` (true = `str_` is valid input for the type). `Err`
/// carries any hard `ereport(ERROR)` from the type-I/O resolution. The per-call
/// `ValidIOData`/`fn_extra` caching (misc.c:777) is the fmgr shim's concern and
/// is not modeled here; this is the cache-miss body, run on every call.
fn input_is_valid_by_type_seam(
    typoid: Oid,
    typmod: i32,
    str_: &[u8],
    escontext: &mut types_error::SoftErrorContext,
) -> PgResult<bool> {
    let ctx = MemoryContext::new("input_is_valid_by_type");
    let mcx = ctx.mcx();

    // getTypeInputInfo(typoid, &typiofunc, &typioparam) (misc.c:806).
    let (typiofunc, typioparam) =
        backend_utils_cache_lsyscache_seams::get_type_input_info::call(typoid)?;

    // fmgr_info_cxt(typiofunc, &inputproc, ...) (misc.c:808).
    let resolved = fmgr_info(mcx, typiofunc)?;

    // text_to_cstring(txt) (misc.c:768): the input bytes as a C string. PG's
    // text_to_cstring would itself reject embedded NULs; UTF-8 validity is the
    // closest invariant here and a malformed string is a soft-error candidate.
    let str_s = match core::str::from_utf8(str_) {
        Ok(s) => s,
        Err(_) => {
            escontext.save(
                PgError::error("invalid byte sequence for encoding")
                    .with_sqlstate(types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
            );
            return Ok(false);
        }
    };

    // InputFunctionCallSafe(&inputproc, str, typioparam, typmod, escontext,
    // &converted) (misc.c:813): true iff the conversion succeeded (no soft
    // error saved). The converted datum is discarded.
    let out = input_function_call_safe_typed(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        Some(str_s),
        typioparam,
        typmod,
        Some(escontext),
    )?;
    Ok(out.is_some())
}

/// `OidFunctionCall0(functionId)` seam (fmgr.c): one-shot lookup + zero-argument
/// call under the default (invalid) collation. Returns the bare result word (the
/// handler's opaque `void *` pointer word for subscript-routines callers).
fn oid_function_call0_seam(function_id: Oid) -> PgResult<Datum> {
    let ctx = MemoryContext::new("oid_function_call0");
    oid_function_call0_coll(ctx.mcx(), function_id, InvalidOid)
}

/// `FunctionCallInvoke(fcinfo)` (fmgr.h) — the general arbitrary-`nargs`
/// dispatch the executor's `EEOP_FUNCEXPR[_STRICT[_1|_2]]` /
/// `EEOP_FUNCEXPR_FUSAGE` and the analogous fmgr-call expression steps drive.
/// Unlike the strict `FunctionCallN` leaves, this does NOT apply the
/// `elog(ERROR, "function returned NULL")` self-test (`null_check`): an
/// expression-level function call legitimately returns NULL through
/// `fcinfo->isnull`, which the interpreter reads back and stores. The caller has
/// already applied the strict-null arg short-circuit, so this is entered only
/// when the function is to run. The resolved `FmgrInfo` cannot cross the seam,
/// so the owner re-resolves by `fn_oid` (as the other `FunctionCallN` seams do)
/// and dispatches over the built `args` frame under `collation`. Both
/// `function_call_invoke` and `fastpath_function_call_invoke` share this body.
fn function_call_invoke_seam(
    fn_oid: Oid,
    collation: Oid,
    args: &[NullableDatum],
) -> PgResult<(Datum, bool)> {
    let ctx = MemoryContext::new("function_call_invoke");
    let mcx = ctx.mcx();
    let resolved = fmgr_info(mcx, fn_oid)?;
    // C: fcache->flinfo.fn_expr = fcinfo->flinfo->fn_expr (fmgr.c:658) — thread
    // the caller's fn_expr before the FmgrInfo is moved into fcinfo.
    let fn_expr = resolved.finfo.fn_expr.clone();
    let mut fcinfo = init_fcinfo(Some(resolved.finfo), collation, args.to_vec());
    // C: fcinfo->isnull = false; d = op->d.func.fn_addr(fcinfo);
    fcinfo.isnull = false;
    let d = function_call_invoke_with_expr(mcx, &resolved.resolution, &mut fcinfo, fn_expr)?;
    // C: *op->resnull = fcinfo->isnull;
    Ok((d, fcinfo.isnull))
}

/// Detoast a by-reference [`RefPayload::Varlena`] argument to a flat, in-line,
/// uncompressed varlena image when (and only when) it is actually TOAST-encoded.
///
/// This is the single boundary that enforces the `RefPayload::Varlena` carrier's
/// documented contract (the "already-detoasted `struct varlena *`" image). A
/// varlena attribute read off a heap tuple (`heap_deform_tuple`/`fetchatt`)
/// crosses VERBATIM through `datum_to_ref_arg` as its raw on-disk bytes — which
/// may be an INLINE-COMPRESSED (`VARATT_IS_4B_C`, low-two-bits `0b10`) or
/// OUT-OF-LINE-EXTERNAL (`VARATT_IS_1B_E`, `va_header == 0x01`) datum. The adt
/// cores that read `&image[VARHDRSZ..]` / `VARDATA_ANY` directly (md5, LIKE, and
/// the ~18 string/hash crates that do not route through `PG_GETARG_*_PP`) would
/// then read the pglz-compressed payload or the 18-byte toast pointer as if it
/// were the plain text — silently corrupting every result on a toasted column.
/// C avoids this because every `PG_GETARG_*_PP` macro calls
/// `pg_detoast_datum_packed`; here we apply the same step ONCE at the dispatch
/// chokepoint so both the macro-routed and the raw-byte readers see flat bytes.
///
/// The gate (`is external` OR `is compressed`) is essential: a fixed-length
/// by-reference value carried on the `Varlena` arm — notably a `name`
/// (`NAMEDATALEN`-byte buffer, no varlena header) — is NOT a varlena and must
/// pass through untouched. `pg_detoast_datum_packed` is itself a verbatim no-op
/// on a plain (4B-U / short) value, so the explicit gate only additionally guards
/// the non-varlena `Varlena`-arm carriers from a spurious tag misread.
#[inline]
/// Total byte length of a structurally-valid external (1B-E) TOAST pointer
/// datum with `va_tag == tag`: `VARHDRSZ_EXTERNAL + VARTAG_SIZE(tag)`
/// (`varatt.h`). `None` for an unrecognized tag (not a TOAST pointer). Used to
/// disambiguate a real external pointer from a fixed-length by-reference value
/// whose raw bytes coincidentally begin `0x01 <tag>` (e.g. a `macaddr`).
///
/// `VARHDRSZ_EXTERNAL` is 2 (the `va_header` byte + `va_tag` byte).
/// `VARTAG_SIZE`: INDIRECT/EXPANDED carry a single in-memory pointer
/// (`size_of::<usize>()`), ONDISK carries `struct varatt_external` (16 bytes).
fn external_pointer_len_for_tag(tag: u8) -> Option<usize> {
    const VARHDRSZ_EXTERNAL: usize = 2;
    const VARTAG_INDIRECT: u8 = 1;
    const VARTAG_EXPANDED_RO: u8 = 2;
    const VARTAG_EXPANDED_RW: u8 = 3;
    const VARTAG_ONDISK: u8 = 18;
    let payload = match tag {
        VARTAG_INDIRECT | VARTAG_EXPANDED_RO | VARTAG_EXPANDED_RW => {
            core::mem::size_of::<usize>()
        }
        VARTAG_ONDISK => 16,
        _ => return None,
    };
    Some(VARHDRSZ_EXTERNAL + payload)
}

fn detoast_ref_arg_if_toasted<'mcx>(
    mcx: Mcx<'mcx>,
    refp: Option<RefPayload>,
) -> PgResult<Option<RefPayload>> {
    match refp {
        Some(RefPayload::Varlena(bytes)) => {
            // A genuine inline-COMPRESSED varlena (4B-C, low two bits `0b10`) is a
            // self-consistent 4-byte-header varlena: `VARSIZE_4B(bytes) == len`
            // (`toast_compress_datum` stamps the total length via
            // `SET_VARSIZE_COMPRESSED`). A fixed-length by-reference value carried on
            // the `Varlena` arm — notably a `name` (NAMEDATALEN buffer, no varlena
            // header) — is NOT a varlena: when its first byte happens to have low two
            // bits `0b10` (e.g. an ASCII letter like 'r' = 0x72) the bare tag test
            // misfires and the value is fed to the pglz decompressor, which then
            // `ereport`s `truncated compressed datum`. Require the self-consistent
            // 4-byte length so only real compressed varlenas are detoasted. (This is
            // the same `VARSIZE == len` disambiguation `ensure_headerful_varlena`
            // applies on the deform side.)
            let is_compressed_4b = bytes.len() >= 4 && (bytes[0] & 0x03) == 0x02 && {
                // VARSIZE_4B(bytes): low 30 bits of the 4-byte length word.
                let word = u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                #[cfg(target_endian = "big")]
                let varsize_4b = (word & 0x3FFF_FFFF) as usize;
                #[cfg(target_endian = "little")]
                let varsize_4b = ((word >> 2) & 0x3FFF_FFFF) as usize;
                varsize_4b == bytes.len()
            };
            // VARATT_IS_EXTERNAL (1B-E): `va_header == 0x01`. But a fixed-length
            // by-reference value (notably `macaddr` — 6 raw bytes, no varlena
            // header) is also carried on the `Varlena` arm, and its first byte
            // can legitimately be 0x01 (e.g. the literal `01:02:03:04:05:06`).
            // The bare `bytes[0] == 0x01` test then misfires and feeds the value
            // to TOAST detoast — and when `bytes[1]` happens to be a vartag
            // (e.g. 0x02 == VARTAG_EXPANDED_RO) it dispatches into the expanded-
            // object flatten path (`eom_get_flat_size`), which panics. A genuine
            // external TOAST pointer datum is structurally exact: its total
            // length is `VARHDRSZ_EXTERNAL + VARTAG_SIZE(va_tag)` for a
            // recognized tag. Require that self-consistency so only real
            // external pointers are detoasted (the same disambiguation as the
            // `VARSIZE == len` test for the compressed-4b case above).
            let is_external_1b_e = bytes.len() >= 2
                && bytes[0] == 0x01
                && external_pointer_len_for_tag(bytes[1])
                    .is_some_and(|n| n == bytes.len());
            let toasted = is_external_1b_e || is_compressed_4b;
            if toasted {
                let flat =
                    backend_access_common_detoast_seams::pg_detoast_datum_packed::call(mcx, &bytes)?;
                Ok(Some(RefPayload::Varlena(flat.as_slice().to_vec())))
            } else {
                Ok(Some(RefPayload::Varlena(bytes)))
            }
        }
        other => Ok(other),
    }
}

/// `FunctionCallInvoke(fcinfo)` (fmgr.h) over the canonical per-attribute
/// [`Datum`] lane — the by-reference-arg form of [`function_call_invoke_seam`]
/// (the WALL-1aq by-ref execExpr arg-gather). Each canonical arg crosses the fmgr
/// boundary as its bare by-value word OR its by-reference referent bytes (the same
/// `datum_to_ref_arg` marshalling the BRIN `*_coll_datum` seams + `fmgr_call_seam`
/// use), with `args[i].isnull` carried through. Like `function_call_invoke_seam`,
/// it does NOT apply the `null_check` self-test: a non-strict function legitimately
/// returns NULL through `fcinfo->isnull`, which the caller reads back; the strict
/// short-circuit was already applied by the interpreter's `_STRICT` opcodes. The
/// result `Datum` (by-value or by-reference) is materialized into `mcx`.
/// Shared core of the by-OID `FunctionCallInvoke` over the canonical `Datum`
/// lane: re-resolve `fn_oid`, stamp `fn_expr`, build the `fcinfo` frame from the
/// already-marshalled `(word, ref)` pairs, dispatch, and materialize the result.
fn function_call_invoke_datum_core<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    collation: Oid,
    nargs: Vec<NullableDatum>,
    ref_args: Vec<Option<RefPayload>>,
    fn_expr: Option<&types_nodes::primnodes::Expr>,
) -> PgResult<(types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool)> {
    let mut resolved = fmgr_info(mcx, fn_oid)?;

    // C: ExecInitFunc does `fmgr_info_set_expr((Node *) node, flinfo)`, so the
    // FmgrInfo `EEOP_FUNCEXPR` dispatches with carries the call expression on
    // `flinfo->fn_expr`; the callee's `get_fn_expr_rettype/argtype` read it for
    // polymorphic-type resolution. The owned by-OID re-resolution above produced
    // a fresh `FmgrInfo` with `fn_expr == None`, so stamp the executor's call
    // node onto it (carried erased through the tag-only `FnExpr::External`). This
    // is the only divergence-bridge: the rest of the call frame is unchanged.
    if let Some(expr) = fn_expr {
        // clone_in: the call node may carry an Aggref (a HAVING qual operator),
        // whose context-allocated TargetEntry args a bare derived `.clone()`
        // panics on.
        resolved.finfo.fn_expr = Some(Box::new(FnExpr::External(types_fmgr::ExternalFnExpr {
            tag: 0,
            node: Some(types_core::fmgr::FnExprErased::new(expr.clone_in(mcx)?)),
        })));
    }

    // C: fcache->flinfo.fn_expr = fcinfo->flinfo->fn_expr (fmgr.c:658).
    let fn_expr = resolved.finfo.fn_expr.clone();
    let mut fcinfo = init_fcinfo(Some(resolved.finfo), collation, nargs);
    // Enforce the `RefPayload::Varlena` carrier's "already-detoasted" contract at
    // the single fmgr dispatch chokepoint: a varlena arg read off a heap tuple
    // may be inline-compressed (4B-C) or out-of-line-external, and the raw-byte
    // adt readers (md5/LIKE/etc.) would corrupt it. Detoast each toasted varlena
    // arg here so EVERY builtin sees a flat image (C: `PG_DETOAST_DATUM_PACKED`).
    let mut flat_ref_args: Vec<Option<RefPayload>> = Vec::with_capacity(ref_args.len());
    for refp in ref_args {
        flat_ref_args.push(detoast_ref_arg_if_toasted(mcx, refp)?);
    }
    fcinfo.ref_args = flat_ref_args;
    fcinfo.debug_assert_ref_null_consistency();
    // C: fcinfo->isnull = false; d = op->d.func.fn_addr(fcinfo). Dispatch directly
    // (NOT through `invoke_flinfo`/`null_check`): a function may legitimately
    // return NULL via `fcinfo->isnull`, which the caller stores.
    fcinfo.isnull = false;
    let word = function_call_invoke_with_expr(mcx, &resolved.resolution, &mut fcinfo, fn_expr)?;
    let isnull = fcinfo.isnull;
    let ref_result = fcinfo.take_ref_result();
    if isnull {
        return Ok((
            types_tuple::backend_access_common_heaptuple::Datum::null(),
            true,
        ));
    }
    // Materialize the result into `mcx` (by-value or by-reference).
    let result = ref_out_to_datum(mcx, word, ref_result)?;
    Ok((result, false))
}

fn function_call_invoke_datum_seam<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    collation: Oid,
    args: &[types_tuple::backend_access_common_heaptuple::Datum<'mcx>],
    args_null: &[bool],
    fn_expr: Option<&types_nodes::primnodes::Expr>,
) -> PgResult<(types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool)> {
    // Build the `fcinfo->args[]` frame: by-value word lane + by-reference side
    // channel. The canonical `Datum::ByVal(0)` word cannot encode SQL NULL, so
    // `args_null[i]` (`fcinfo->args[i].isnull`) is threaded explicitly — a
    // non-strict function reads `PG_ARGISNULL(i)` (the interpreter has already
    // applied the strict-null short-circuit upstream for strict functions). An
    // empty `args_null` slice means "no argument is NULL".
    let mut nargs: Vec<NullableDatum> = Vec::with_capacity(args.len());
    let mut ref_args: Vec<Option<RefPayload>> = Vec::with_capacity(args.len());
    for (i, val) in args.iter().enumerate() {
        let is_null = args_null.get(i).copied().unwrap_or(false);
        let (mut nd, refp) = datum_to_ref_arg(val);
        // A NULL arg never carries a by-reference referent (fmgr invariant (1)).
        if is_null {
            nd.isnull = true;
        }
        nargs.push(nd);
        ref_args.push(if is_null { None } else { refp });
    }
    function_call_invoke_datum_core(mcx, fn_oid, collation, nargs, ref_args, fn_expr)
}

/// By-value form of [`function_call_invoke_datum_seam`]: consumes `args`, so an
/// `internal` argument's `Box<dyn Any>` moves into the by-reference side channel
/// (and back out as the result) instead of cloning. This is the form the
/// aggregate transition machinery uses for `internal`-transtype aggregates.
fn function_call_invoke_datum_owned_seam<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    collation: Oid,
    args: Vec<types_tuple::backend_access_common_heaptuple::Datum<'mcx>>,
    args_null: Vec<bool>,
    fn_expr: Option<&types_nodes::primnodes::Expr>,
) -> PgResult<(types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool)> {
    debug_assert_eq!(args.len(), args_null.len());
    let mut nargs: Vec<NullableDatum> = Vec::with_capacity(args.len());
    let mut ref_args: Vec<Option<RefPayload>> = Vec::with_capacity(args.len());
    for (i, val) in args.into_iter().enumerate() {
        let (mut nd, refp) = datum_to_ref_arg_owned(val);
        // Thread `fcinfo->args[i].isnull` (a NULL running state / NULL input):
        // the canonical word cannot carry it, so the caller passes it alongside.
        // A NULL arg never carries a by-reference referent.
        if args_null[i] {
            nd.isnull = true;
        }
        nargs.push(nd);
        ref_args.push(if args_null[i] { None } else { refp });
    }
    function_call_invoke_datum_core(mcx, fn_oid, collation, nargs, ref_args, fn_expr)
}

/// `CreateConversionCommand`'s conversion-function empty-input self-test
/// (conversioncmds.c):
/// ```c
/// char result[1];
/// funcresult = OidFunctionCall6(funcoid,
///                               Int32GetDatum(from_encoding),
///                               Int32GetDatum(to_encoding),
///                               CStringGetDatum(""),
///                               CStringGetDatum(result),
///                               Int32GetDatum(0),
///                               BoolGetDatum(false));
/// ... DatumGetInt32(funcresult);
/// ```
/// Only the fmgr owner can synthesize the two `cstring`-shaped `Datum`s (an empty
/// source string and a 1-byte destination buffer): they cross the fmgr boundary
/// as `RefPayload::Cstring` referents on the by-reference side channel, while the
/// four `int4`/`bool` args travel as bare by-value words. The call dispatches
/// through `OidFunctionCall6` (one-shot `fmgr_info` + `FunctionCall6Coll` under
/// the default collation) and the result is read back with `DatumGetInt32`.
fn conversion_proc_empty_input_test_seam(
    funcoid: Oid,
    from_encoding: i32,
    to_encoding: i32,
) -> PgResult<i32> {
    let ctx = MemoryContext::new("conversion_proc_empty_input_test");
    let mcx = ctx.mcx();
    let resolved = fmgr_info(mcx, funcoid)?;
    // C `result[1]` is a 1-byte output buffer the conversion function writes the
    // converted string into; for empty input it stays untouched. Mirror it as a
    // 1-byte `cstring` referent. `CStringGetDatum("")` is the empty source.
    let args = vec![
        NullableDatum::value(int32_get_datum(from_encoding)),
        NullableDatum::value(int32_get_datum(to_encoding)),
        // CStringGetDatum("") — by-reference placeholder, referent in ref_args.
        NullableDatum::value(Datum::null()),
        // CStringGetDatum(result) — by-reference placeholder, referent in ref_args.
        NullableDatum::value(Datum::null()),
        NullableDatum::value(int32_get_datum(0)),
        NullableDatum::value(Datum::from_bool(false)),
    ];
    let ref_args = vec![
        None,
        None,
        Some(RefPayload::Cstring(String::new())),
        Some(RefPayload::Cstring(String::from("\0"))),
        None,
        None,
    ];
    let funcresult = function_call_coll_ref_args(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        InvalidOid,
        args,
        ref_args,
    )?;
    Ok(funcresult.as_i32())
}

/// `convert_via_proc` (mbutils.c): the `OidFunctionCall6` invocation of an
/// encoding-conversion procedure, dispatched with two pointer-shaped `cstring`
/// `Datum`s (source bytes / destination buffer) only fmgr can synthesize. The
/// conversion function NUL-terminates the converted string in the destination
/// buffer; we return the converted bytes (no trailing NUL) allocated in `mcx`.
fn convert_via_proc_seam<'mcx>(
    mcx: Mcx<'mcx>,
    proc: Oid,
    src_encoding: i32,
    dest_encoding: i32,
    src: &[u8],
    no_error: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    Ok(convert_via_proc_counted_seam(mcx, proc, src_encoding, dest_encoding, src, no_error)?.1)
}

fn convert_via_proc_counted_seam<'mcx>(
    mcx: Mcx<'mcx>,
    proc: Oid,
    src_encoding: i32,
    dest_encoding: i32,
    src: &[u8],
    no_error: bool,
) -> PgResult<(i32, PgVec<'mcx, u8>)> {
    let resolved = fmgr_info(mcx, proc)?;
    // CStringGetDatum(src) / CStringGetDatum(result): the conversion function
    // reads its source from the `src` cstring and writes the converted,
    // NUL-terminated output into the `result` cstring referent. The destination
    // referent starts empty; the callee overwrites it.
    let args = vec![
        NullableDatum::value(int32_get_datum(src_encoding)),
        NullableDatum::value(int32_get_datum(dest_encoding)),
        NullableDatum::value(Datum::null()),
        NullableDatum::value(Datum::null()),
        NullableDatum::value(int32_get_datum(src.len() as i32)),
        NullableDatum::value(Datum::from_bool(no_error)),
    ];
    // C frames both buffers as `cstring` (`char *`). A C `cstring` is a raw
    // NUL-terminated byte buffer, NOT a UTF-8 string: an encoding-conversion
    // source is by definition in the *source* encoding (e.g. raw EUC_KR bytes),
    // which need not be valid UTF-8. Carrying it through `RefPayload::Cstring`
    // (a Rust `String`) would force a lossy UTF-8 reframe of the very bytes
    // being converted, corrupting the input. The faithful carrier for a C
    // `char *` of arbitrary bytes is the raw byte lane (`RefPayload::Varlena`,
    // which the boundary treats as an owned byte image with no header strip in
    // this context): the conversion proc reads its source bytes from
    // `ref_args[2]` and writes the converted, raw output bytes into the
    // `ref_args[3]` referent. The destination referent starts empty; the callee
    // overwrites it. This path is reachable only once conversion procedures are
    // dispatchable by OID (registered as fmgr builtins) — until then `fmgr_info`
    // errors above for an unregistered proc.
    let ref_args = vec![
        None,
        None,
        Some(RefPayload::Varlena(src.to_vec())),
        Some(RefPayload::Varlena(Vec::new())),
        None,
        None,
    ];
    let oid = resolved.finfo.fn_oid;
    let fn_expr = resolved.finfo.fn_expr.clone();
    let mut fcinfo = init_fcinfo(Some(resolved.finfo), InvalidOid, args);
    fcinfo.ref_args = ref_args;
    let result = invoke_flinfo(mcx, &resolved.resolution, &mut fcinfo, oid, fn_expr)?;
    // Recover the destination buffer the conversion function wrote.
    let converted: &[u8] = match fcinfo.ref_args.get(3).and_then(|r| r.as_ref()) {
        Some(RefPayload::Varlena(b)) => b.as_slice(),
        _ => &[],
    };
    Ok((result.as_i32(), mcx::slice_in(mcx, converted)?))
}

// ===========================================================================
// Frame-widening seams (`PG_GETARG_*` / `PG_RETURN_*` / `PG_NARGS` /
// `PG_ARGISNULL` / call mcx / fn_expr readers) over the executor's
// `types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>` frame. The frame's
// `args[i].value` is a bare-word `types_datum::Datum`; only by-value scalar
// arguments can be decoded here (the by-reference `PG_GETARG_{NAME,TEXT_PP,
// VARLENA_PP,CSTRING}` readers need a by-reference channel the trimmed nodes
// frame does not carry — see DESIGN_DEBT TD-FMGR-GETARG-BYREF).
// ===========================================================================

/// `PG_NARGS()` (fmgr.h): `fcinfo->nargs`.
fn pg_nargs_seam(fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'_>) -> i32 {
    fcinfo.nargs as i32
}

/// `PG_ARGISNULL(n)` (fmgr.h): `fcinfo->args[n].isnull`. A read past `nargs` is
/// C undefined behaviour; the safe port treats a missing slot as NULL.
fn pg_argisnull_seam(
    fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    n: usize,
) -> bool {
    fcinfo.args.get(n).map_or(true, |a| a.isnull)
}

/// `PG_GETARG_OID(n)` (fmgr.h): `DatumGetObjectId(fcinfo->args[n].value)`.
fn pg_getarg_oid_seam(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    n: usize,
) -> Oid {
    fcinfo.args[n].value.as_oid()
}

/// `PG_GETARG_INT16(n)` (fmgr.h): `DatumGetInt16(fcinfo->args[n].value)`.
fn pg_getarg_int16_seam(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    n: usize,
) -> types_core::AttrNumber {
    fcinfo.args[n].value.as_i16()
}

/// `PG_GETARG_INT64(n)` (fmgr.h): `DatumGetInt64(fcinfo->args[n].value)`.
fn pg_getarg_int64_seam(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    n: usize,
) -> i64 {
    fcinfo.args[n].value.as_i64()
}

/// `PG_GETARG_BOOL(n)` (fmgr.h): `DatumGetBool(fcinfo->args[n].value)`.
fn pg_getarg_bool_seam(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    n: usize,
) -> bool {
    fcinfo.args[n].value.as_bool()
}

/// `PG_GETARG_DATUM(n)` (fmgr.h): the raw argument word `fcinfo->args[n].value`,
/// taken as given with no detoasting.
fn pg_getarg_datum_seam(
    fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    n: usize,
) -> Datum {
    fcinfo.args[n].value
}

// ---------------------------------------------------------------------------
// By-reference PG_GETARG readers (TD-FMGR-GETARG-BYREF). These read the
// executor frame's by-reference argument side channel (`ref_args`, the no_std
// mirror of the `types_fmgr` ABI frame's `ref_args`): a by-reference `text` /
// varlena / `Name` / `cstring` argument arrives as `ref_args[n] == Some(...)`
// (the dispatcher fills it from the canonical `Datum::ByRef`/`Cstring`, exactly
// as `datum_to_ref_arg` does for the `types_fmgr` frame). The `'mcx` allocation
// context is the frame's seeded `fn_mcxt` (as `pg_call_mcx`).
// ---------------------------------------------------------------------------

/// The frame's by-reference referent for arg `n`, or a loud panic when the slot
/// is by-value/empty (C: dereferencing a non-pointer `Datum` — a wiring bug, not
/// a data path). Shared by the four by-ref readers below.
fn getarg_ref<'a>(
    fcinfo: &'a types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    n: usize,
    macro_name: &str,
) -> &'a types_nodes::fmgr::FmgrArgRef {
    fcinfo.ref_arg(n).unwrap_or_else(|| {
        panic!(
            "{macro_name}: arg {n} has no by-reference payload on the executor \
             frame (the dispatcher did not seed ref_args[{n}] for a by-ref arg)"
        )
    })
}

/// `PG_GETARG_TEXT_PP(n)` (fmgr.h): the (possibly-detoasted) `text` image of arg
/// `n`. The referent already carries the full varlena image; copy it into the
/// call's context (`fn_mcxt`) as the `Bytea` C's `PG_DETOAST_DATUM_PACKED`
/// returns.
fn pg_getarg_text_pp_seam<'mcx>(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
    n: usize,
) -> PgResult<types_datum::varlena::Bytea<'mcx>> {
    let mcx = pg_call_mcx_seam(fcinfo);
    let bytes = match getarg_ref(fcinfo, n, "PG_GETARG_TEXT_PP") {
        types_nodes::fmgr::FmgrArgRef::Varlena(b) => b.as_slice(),
        types_nodes::fmgr::FmgrArgRef::Cstring(_) => panic!(
            "PG_GETARG_TEXT_PP: arg {n} is a cstring, not a varlena (a wiring bug)"
        ),
    };
    // C's `PG_GETARG_TEXT_PP` == `pg_detoast_datum_packed`: a compressed-in-line
    // or out-of-line-external varlena is fetched back / decompressed into a flat
    // in-line image; a plain (uncompressed) value is returned verbatim. Skipping
    // this step would read a compressed value's pglz payload as raw text.
    let detoasted =
        backend_access_common_detoast_seams::pg_detoast_datum_packed::call(mcx, bytes)?;
    Ok(types_datum::varlena::Bytea::from_image(mcx::slice_in(
        mcx,
        detoasted.as_slice(),
    )?))
}

/// `PG_GETARG_VARLENA_PP(n)` / `PG_GETARG_ARRAYTYPE_P(n)` (fmgr.h): the
/// (possibly-detoasted) full varlena image of arg `n` (array / `text[]` /
/// `bytea`). Identical marshalling to [`pg_getarg_text_pp_seam`].
fn pg_getarg_varlena_pp_seam<'mcx>(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
    n: usize,
) -> PgResult<types_datum::varlena::Bytea<'mcx>> {
    pg_getarg_text_pp_seam(fcinfo, n)
}

/// `PG_GETARG_NAME(n)` (fmgr.h): arg `n` as a `Name`, returning its NUL-trimmed
/// text. A `name` value crosses as its `ByRef` varlena image (the 64-byte
/// NUL-padded buffer) or, for an `unknown`-literal coerced in, a `cstring`; both
/// resolve to the NUL-trimmed text.
fn pg_getarg_name_seam(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    n: usize,
) -> String {
    match getarg_ref(fcinfo, n, "PG_GETARG_NAME") {
        types_nodes::fmgr::FmgrArgRef::Cstring(s) => s.clone(),
        types_nodes::fmgr::FmgrArgRef::Varlena(b) => {
            // C: a `Name` is the (up to) NAMEDATALEN-byte buffer, NUL-trimmed.
            let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            String::from_utf8_lossy(&b[..end]).into_owned()
        }
    }
}

/// `PG_GETARG_CSTRING(n)` / `PG_GETARG_POINTER(n)` read as the `cstring` an
/// `unknown`-typed literal arrives as (fmgr.h): the NUL-terminated C string of
/// arg `n`, returned as a `&'mcx str` allocated in the call's context.
fn pg_getarg_cstring_seam<'mcx>(
    fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
    n: usize,
) -> &'mcx str {
    let mcx = pg_call_mcx_seam(fcinfo);
    let s = match getarg_ref(fcinfo, n, "PG_GETARG_CSTRING") {
        types_nodes::fmgr::FmgrArgRef::Cstring(s) => s.clone(),
        // A varlena-imaged arg read as a cstring: its NUL-excluded text (the
        // `unknown`-literal path can present either shape).
        types_nodes::fmgr::FmgrArgRef::Varlena(b) => {
            let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            String::from_utf8_lossy(&b[..end]).into_owned()
        }
    };
    // Allocate the text in `mcx` and leak the box to obtain the `&'mcx str` the
    // seam returns (the referent itself lives on the borrowed frame; the C
    // `char *` points into the call's context, which `fn_mcxt` models).
    let ps = PgString::from_str_in(&s, mcx)
        .unwrap_or_else(|_| panic!("PG_GETARG_CSTRING: OOM allocating cstring into call context"));
    let leaked: &'mcx mut PgString<'mcx> = mcx::leak_in(
        mcx::alloc_in(mcx, ps)
            .unwrap_or_else(|_| panic!("PG_GETARG_CSTRING: OOM boxing cstring into call context")),
    );
    leaked.as_str()
}

/// `PG_RETURN_INT64(v)` (fmgr.h): `fcinfo->isnull = false; return Int64GetDatum(v);`.
fn pg_return_int64_seam(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    v: i64,
) -> Datum {
    fcinfo.isnull = false;
    Datum::from_i64(v)
}

/// `PG_RETURN_DATUM(v)` (fmgr.h): `fcinfo->isnull = false; return v;`.
fn pg_return_datum_seam(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    v: Datum,
) -> Datum {
    fcinfo.isnull = false;
    v
}

/// `PG_RETURN_BOOL(b)` (fmgr.h): `fcinfo->isnull = false; return BoolGetDatum(b);`.
fn pg_return_bool_seam(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    b: bool,
) -> Datum {
    fcinfo.isnull = false;
    Datum::from_bool(b)
}

/// `PG_RETURN_NULL()` (fmgr.h): `fcinfo->isnull = true; return (Datum) 0;`.
fn pg_return_null_seam(
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
) -> Datum {
    fcinfo.isnull = true;
    Datum::null()
}

/// The call's current memory context (C: `CurrentMemoryContext` at fmgr
/// dispatch). The executor frame carries it on the `fn_mcxt` channel; a missing
/// context is a caller-contract violation (the dispatcher must seed it before a
/// call whose callee allocates), so this is an invariant panic, not an error.
fn pg_call_mcx_seam<'mcx>(
    fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
) -> Mcx<'mcx> {
    fcinfo
        .fn_mcxt
        .expect("pg_call_mcx: fcinfo->fn_mcxt not seeded by the dispatcher")
}

/// `get_fn_expr_variadic(fcinfo->flinfo)` (fmgr.h): `IsA(fn_expr, FuncExpr) ?
/// funcvariadic : false` over the frame's stamped `fn_expr` (`None` → C's
/// `flinfo == NULL || fn_expr == NULL` fall-through, `false`).
fn get_fn_expr_variadic_seam(
    fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
) -> bool {
    match fcinfo_fn_expr(fcinfo) {
        None => false,
        Some(expr) => backend_nodes_nodeFuncs_seams::expr_variadic_expr::call(expr),
    }
}

/// `get_fn_expr_arg_stable(fcinfo->flinfo, argnum)` (fmgr.h): true iff the
/// indexed call-expression argument is a `Const` or external `Param` (`None`
/// `fn_expr` → C's `false` fall-through).
fn get_fn_expr_arg_stable_seam(
    fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    argnum: i32,
) -> bool {
    match fcinfo_fn_expr(fcinfo) {
        None => false,
        Some(expr) => {
            backend_nodes_nodeFuncs_seams::call_expr_arg_stable_expr::call(expr, argnum)
        }
    }
}

/// `(fcinfo->flinfo->fn_oid, fcinfo->flinfo->fn_expr)` — the function OID and
/// call-expression node `get_call_result_type` (funcapi.c) hands to
/// `internal_get_result_type`. The OID reads off the frame's `flinfo`; the
/// `fn_expr` node is NOT recoverable as a borrowed `&Node` here (the erased
/// carrier holds an owned `primnodes::Expr`, not a frame-borrowed `Node`), so it
/// is reported as `None` — faithful for non-polymorphic SRFs (the common case),
/// degrading only polymorphic result-type resolution. See DESIGN_DEBT
/// TD-FMGR-FN-OID-AND-EXPR-NODE.
fn fn_oid_and_expr_seam<'mcx>(
    fcinfo: &'mcx types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
) -> (Oid, Option<&'mcx types_nodes::nodes::Node<'mcx>>) {
    let fn_oid = fcinfo.flinfo.as_ref().map_or(InvalidOid, |f| f.fn_oid);
    (fn_oid, None)
}

// ===========================================================================
// Re-resolve I/O seams driven by typed (non-frame) inputs.
// (`input_is_valid_by_type_seam` is defined above and already installed.)
// ===========================================================================

/// Map a typed [`FmgrOut`] to the canonical per-attribute [`Datum`] (the
/// `record_in`/`record_recv` column result lane): a by-value scalar stays
/// `ByVal`; a by-reference result materializes its flattened payload into `mcx`.
fn fmgr_out_to_canon<'mcx>(
    mcx: Mcx<'mcx>,
    out: FmgrOut<'mcx>,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
    match out {
        FmgrOut::ByVal(d) => Ok(d),
        FmgrOut::Ref(payload) => {
            let bytes: Vec<u8> = payload.flatten();
            Ok(CanonDatum::ByRef(mcx::slice_in(mcx, &bytes)?))
        }
    }
}

/// `record_in` per-column conversion: `getTypeInputInfo` + `fmgr_info_cxt` +
/// `InputFunctionCallSafe(column_data, typioparam, atttypmod, escontext)`.
/// `Ok(None)` is C's soft-error `goto fail`; `Ok(Some(v))` is the column value
/// in `mcx`.
fn record_column_input_seam<'mcx>(
    mcx: Mcx<'mcx>,
    coltype: Oid,
    column_data: Option<&str>,
    atttypmod: i32,
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::Datum<'mcx>>> {
    let (typinput, typioparam) =
        backend_utils_cache_lsyscache_seams::get_type_input_info::call(coltype)?;
    let resolved = fmgr_info(mcx, typinput)?;
    match input_function_call_safe_typed(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        column_data,
        typioparam,
        atttypmod,
        escontext,
    )? {
        None => Ok(None),
        Some(out) => Ok(Some(fmgr_out_to_canon(mcx, out)?)),
    }
}

/// `record_recv` per-column conversion: `getTypeBinaryInputInfo` +
/// `fmgr_info_cxt` + `ReceiveFunctionCall(buf, typioparam, atttypmod)`. `item` is
/// the column's binary payload (`None` for a -1-length NULL field).
fn record_column_receive_seam<'mcx>(
    mcx: Mcx<'mcx>,
    coltype: Oid,
    item: Option<&[u8]>,
    atttypmod: i32,
    colno: i32,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    let _ = colno; // The whole-buffer cursor check lives inside the receive call
                   // (the typed helper does not surface bytes-consumed); see
                   // DESIGN_DEBT TD-FMGR-RECORD-RECV-CURSOR.
    let (typreceive, typioparam) =
        backend_utils_cache_lsyscache_seams::get_type_binary_input_info::call(coltype)?;
    let resolved = fmgr_info(mcx, typreceive)?;
    let out = receive_function_call_typed(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        item,
        typioparam,
        atttypmod,
    )?;
    fmgr_out_to_canon(mcx, out)
}

/// `record_out` per-column conversion: `getTypeOutputInfo` + `fmgr_info_cxt` +
/// `OutputFunctionCall(attr)`. Returns the output cstring's bytes (no NUL) in `mcx`.
fn record_column_output_seam<'mcx>(
    mcx: Mcx<'mcx>,
    coltype: Oid,
    val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let (typoutput, _typisvarlena) =
        backend_utils_cache_lsyscache_seams::get_type_output_info::call(coltype)?;
    let (datum, ref_arg) = tuple_value_to_arg(val);
    let resolved = fmgr_info(mcx, typoutput)?;
    let arg = match &ref_arg {
        Some(p) => FmgrArg::Ref(p),
        None => FmgrArg::ByVal(canon_byval(datum)),
    };
    let s = output_function_call_typed(mcx, &resolved.resolution, resolved.finfo, arg)?;
    bytes_into(mcx, s.as_bytes())
}

/// `record_send` per-column conversion: `getTypeBinaryOutputInfo` +
/// `fmgr_info_cxt` + `SendFunctionCall(attr)`. Returns the `bytea` PAYLOAD bytes
/// (varlena header stripped) in `mcx`.
fn record_column_send_seam<'mcx>(
    mcx: Mcx<'mcx>,
    coltype: Oid,
    val: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let (typsend, _typisvarlena) =
        backend_utils_cache_lsyscache_seams::get_type_binary_output_info::call(coltype)?;
    let (datum, ref_arg) = tuple_value_to_arg(val);
    let resolved = fmgr_info(mcx, typsend)?;
    let arg = match &ref_arg {
        Some(p) => FmgrArg::Ref(p),
        None => FmgrArg::ByVal(canon_byval(datum)),
    };
    let image = send_function_call_typed(mcx, &resolved.resolution, resolved.finfo, arg)?;
    let payload = image.get(types_datum::varlena::VARHDRSZ..).unwrap_or(&[]);
    bytes_into(mcx, payload)
}

/// `DatumGetCString(OidFunctionCall1(typmodout, Int32GetDatum(typmod)))`
/// (format_type.c `printTypmod`): call a type's `typmodout` proc on a single
/// `int4` typmod and return the resulting cstring in `mcx`.
fn typmod_out_seam<'mcx>(
    mcx: Mcx<'mcx>,
    typmodout: Oid,
    typmod: i32,
) -> PgResult<PgString<'mcx>> {
    let resolved = fmgr_info(mcx, typmodout)?;
    // OidFunctionCall1: a single by-value int4 argument; the result is a cstring
    // (by-reference) read back off the call frame's ref_result.
    let (_word, ref_result) = function_call_coll_ref_args_out(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        InvalidOid,
        vec![NullableDatum::value(int32_get_datum(typmod))],
        vec![None],
    )?;
    match ref_result {
        Some(RefPayload::Cstring(s)) => PgString::from_str_in(&s, mcx),
        _ => Err(PgError::error(format!(
            "function {typmodout} did not return a cstring"
        ))),
    }
}

// ===========================================================================
// Element-type I/O and comparison/hash seams (arrayfuncs.c). Each element value
// crosses as a safe `ArrayElementDatum` (by-value Datum or on-disk bytes); the
// owner builds the real call frame without aliasing the array buffer.
// ===========================================================================

/// Marshal an [`ArrayElementDatum`] into the `(arg-word, ref-arg)` pair the
/// `function_call_coll_ref_args` frame expects: a by-value element is the bare
/// word with no referent; a by-reference element is the null word plus its
/// on-disk bytes as a `Varlena` referent.
fn elem_to_arg(e: types_array::ArrayElementDatum<'_>) -> (NullableDatum, Option<RefPayload>) {
    match e {
        types_array::ArrayElementDatum::ByValue(d) => (NullableDatum::value(d), None),
        types_array::ArrayElementDatum::ByRef(bytes) => (
            NullableDatum::value(Datum::null()),
            Some(RefPayload::Varlena(bytes.to_vec())),
        ),
    }
}

/// `FunctionCall2Coll(eq_opr_finfo, collation, a, b)` — the cached element
/// equality operator (`array_eq` / `arrayoverlap` / `array_contain_compare`).
fn element_eq_seam(
    function_id: Oid,
    collation: Oid,
    a: types_array::ArrayElementDatum<'_>,
    b: types_array::ArrayElementDatum<'_>,
) -> PgResult<bool> {
    let ctx = MemoryContext::new("element_eq");
    let mcx = ctx.mcx();
    let resolved = fmgr_info(mcx, function_id)?;
    let (a1, r1) = elem_to_arg(a);
    let (a2, r2) = elem_to_arg(b);
    let d = function_call_coll_ref_args(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        collation,
        vec![a1, a2],
        vec![r1, r2],
    )?;
    Ok(d.as_bool())
}

/// `FunctionCall2Coll(cmp_proc_finfo, collation, a, b)` — the cached element
/// btree comparison proc (`array_cmp` / `btarraycmp`). Returns the 3-way `int32`.
fn element_cmp_seam(
    function_id: Oid,
    collation: Oid,
    a: types_array::ArrayElementDatum<'_>,
    b: types_array::ArrayElementDatum<'_>,
) -> PgResult<i32> {
    let ctx = MemoryContext::new("element_cmp");
    let mcx = ctx.mcx();
    let resolved = fmgr_info(mcx, function_id)?;
    let (a1, r1) = elem_to_arg(a);
    let (a2, r2) = elem_to_arg(b);
    let d = function_call_coll_ref_args(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        collation,
        vec![a1, a2],
        vec![r1, r2],
    )?;
    Ok(d.as_i32())
}

/// `FunctionCall1Coll(hash_proc_finfo, collation, elt)` — the cached element
/// hash proc (`hash_array`). Returns the `uint32` hash.
fn element_hash_seam(
    function_id: Oid,
    collation: Oid,
    value: types_array::ArrayElementDatum<'_>,
) -> PgResult<u32> {
    let ctx = MemoryContext::new("element_hash");
    let mcx = ctx.mcx();
    let resolved = fmgr_info(mcx, function_id)?;
    let (a1, r1) = elem_to_arg(value);
    let d = function_call_coll_ref_args(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        collation,
        vec![a1],
        vec![r1],
    )?;
    Ok(d.as_u32())
}

/// `FunctionCall2Coll(hash_extended_proc_finfo, collation, elt, seed)` — the
/// cached element extended hash proc (`hash_array_extended`). Returns the
/// `uint64` hash.
fn element_hash_extended_seam(
    function_id: Oid,
    collation: Oid,
    value: types_array::ArrayElementDatum<'_>,
    seed: u64,
) -> PgResult<u64> {
    let ctx = MemoryContext::new("element_hash_extended");
    let mcx = ctx.mcx();
    let resolved = fmgr_info(mcx, function_id)?;
    let (a1, r1) = elem_to_arg(value);
    let d = function_call_coll_ref_args(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        collation,
        vec![a1, NullableDatum::value(Datum::from_u64(seed))],
        vec![r1, None],
    )?;
    Ok(d.as_u64())
}

/// `OutputFunctionCall(outputproc, value)` as `array_out` drives it: the
/// element type's text output function on a materialized element value, returning
/// the printable bytes (NUL excluded) in `mcx`.
fn array_output_function_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    value: types_array::ArrayElementDatum<'_>,
    typlen: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let resolved = fmgr_info(mcx, function_id)?;
    let s = match value {
        types_array::ArrayElementDatum::ByValue(d) => {
            output_function_call_typed(
                mcx,
                &resolved.resolution,
                resolved.finfo,
                FmgrArg::ByVal(canon_byval(d)),
            )?
        }
        types_array::ArrayElementDatum::ByRef(bytes) => {
            // Header-ful everywhere: `bytes` is the element's verbatim on-disk
            // image — already a complete `struct varlena *` for a varlena element
            // (`typlen == -1`), the fixed buffer for a fixed-by-ref element. The
            // `RefPayload::Varlena` lane carries it VERBATIM; the output function's
            // adt core reads `VARDATA_ANY` (or the fixed buffer) off it.
            let _ = typlen;
            let payload = RefPayload::Varlena(bytes.to_vec());
            output_function_call_typed(
                mcx,
                &resolved.resolution,
                resolved.finfo,
                FmgrArg::Ref(&payload),
            )?
        }
    };
    bytes_into(mcx, s.as_bytes())
}

/// `ReceiveFunctionCall(receiveproc, buf, typioparam, typmod)` as `array_recv`
/// drives it. The result crosses as a bare `Datum` word (the arrayfuncs binary
/// reader stores `PgVec<Datum>`); a by-reference element result has no bare-word
/// home here and is reported loudly (`fmgr_out_word`), the correct frontier
/// behaviour until the array reader carries by-reference element values.
fn array_receive_function_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    buf: &[u8],
    typioparam: Oid,
    typmod: i32,
) -> PgResult<Datum> {
    let resolved = fmgr_info(mcx, function_id)?;
    let out = receive_function_call_typed(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        Some(buf),
        typioparam,
        typmod,
    )?;
    // C's `ReceiveFunctionCall` yields a bare `Datum`: by-value the machine
    // word, by-reference a pointer to the palloc'd flattened result. Mirror the
    // input path — materialize a by-reference element's on-disk image into the
    // caller's `mcx` (`array_recv`'s build arena) and return the pointer word
    // `CopyArrayEls` dereferences.
    fmgr_out_element_word(mcx, out)
}

/// `SendFunctionCall(sendproc, value)` as `array_send` drives it: the element
/// type's binary send function on a materialized element value, returning the
/// `bytea` PAYLOAD bytes (varlena header stripped) in `mcx`.
fn array_send_function_call_seam<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    value: types_array::ArrayElementDatum<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let resolved = fmgr_info(mcx, function_id)?;
    let image = match value {
        types_array::ArrayElementDatum::ByValue(d) => send_function_call_typed(
            mcx,
            &resolved.resolution,
            resolved.finfo,
            FmgrArg::ByVal(canon_byval(d)),
        )?,
        types_array::ArrayElementDatum::ByRef(bytes) => {
            let payload = RefPayload::Varlena(bytes.to_vec());
            send_function_call_typed(
                mcx,
                &resolved.resolution,
                resolved.finfo,
                FmgrArg::Ref(&payload),
            )?
        }
    };
    let payload = image.get(types_datum::varlena::VARHDRSZ..).unwrap_or(&[]);
    bytes_into(mcx, payload)
}

/// Install every seam in `backend-utils-fmgr-fmgr-seams` whose implementation is
/// `fmgr.c`'s own logic.
///
/// `render_slot_columns` (`ri_triggers.c`'s violator-column rendering) and
/// `call_bgworker_entrypoint` (the bgworker library/function dispatch) are
/// declared in this seam crate but are NOT `fmgr.c` logic; they are installed by
/// their real owners (`backend-utils-adt-ri-triggers` / loader) and panic until
/// those land, which is the correct frontier state.
///
/// The by-reference `PG_GETARG_{NAME,TEXT_PP,VARLENA_PP,CSTRING}` readers and
/// `typmodin` stay UNINSTALLED: the executor `types_nodes` frame carries
/// arguments as bare-word `Datum`s with no by-reference channel, so a
/// varlena/name/cstring argument (or a constructed cstring array for `typmodin`)
/// cannot be recovered here. See DESIGN_DEBT TD-FMGR-GETARG-BYREF (same keystone
/// class as TD-JSONFUNCS-FMGR-ARG-DETOAST). The 4 `fastpath_*` text/binary I/O
/// seams are Phase 2 (tcop/fastpath.c).
pub fn init_seams() {
    backend_utils_fmgr_fmgr_seams::fmgr_info_check::set(fmgr_info_check);
    backend_utils_fmgr_fmgr_seams::fmgr_info::set(fmgr_info_resolve);
    // `FmgrHookIsNeeded(functionId)` (fmgr.c) — the fmgr entry/exit hook gate
    // read by `inline_set_returning_function` (clauses.c).
    backend_optimizer_util_clauses_seams::fmgr_hook_is_needed::set(fmgr_hook_is_needed);
    backend_utils_fmgr_fmgr_seams::fmgr_info_set_expr::set(fmgr_info_set_expr_seam);
    backend_utils_fmgr_fmgr_seams::get_fn_expr_argtype::set(get_fn_expr_argtype_seam);
    backend_utils_fmgr_fmgr_seams::get_fn_expr_rettype::set(get_fn_expr_rettype_seam);
    backend_utils_fmgr_fmgr_seams::oid_function_call_1_deflist::set(oid_function_call_1_deflist);
    backend_utils_fmgr_fmgr_seams::typmodin::set(typmodin_seam);
    backend_utils_fmgr_fmgr_seams::oid_send_function_call::set(oid_send_function_call_seam);
    backend_utils_fmgr_fmgr_seams::oid_output_function_call::set(oid_output_function_call_seam);
    backend_utils_fmgr_fmgr_seams::function_call1_coll::set(function_call1_coll_seam);
    backend_utils_fmgr_fmgr_seams::function_call2_coll::set(function_call2_coll_seam);
    backend_utils_fmgr_fmgr_seams::function_call1_coll_datum::set(function_call1_coll_datum_seam);
    backend_utils_fmgr_fmgr_seams::function_call2_coll_datum::set(function_call2_coll_datum_seam);
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
    backend_utils_fmgr_fmgr_seams::input_function_call::set(input_function_call_seam);
    backend_utils_fmgr_fmgr_seams::receive_function_call::set(receive_function_call_seam);
    backend_utils_fmgr_fmgr_seams::input_function_call_safe::set(input_function_call_safe_seam);
    backend_utils_fmgr_fmgr_seams::input_is_valid_by_type::set(input_is_valid_by_type_seam);
    backend_utils_fmgr_fmgr_seams::oid_function_call0::set(oid_function_call0_seam);
    backend_utils_fmgr_fmgr_seams::function_call_invoke::set(function_call_invoke_seam);
    backend_utils_fmgr_fmgr_seams::fastpath_function_call_invoke::set(function_call_invoke_seam);
    backend_utils_fmgr_fmgr_seams::function_call_invoke_datum::set(function_call_invoke_datum_seam);
    backend_utils_fmgr_fmgr_seams::function_call_invoke_datum_owned::set(
        function_call_invoke_datum_owned_seam,
    );
    // The planner const-folder's `evaluate_expr` fmgr leg (clauses.c): fmgr owns
    // the function-by-OID invocation over constant args.
    backend_optimizer_util_clauses_seams::fmgr_call::set(fmgr_call_seam);
    backend_utils_fmgr_fmgr_seams::conversion_proc_empty_input_test::set(
        conversion_proc_empty_input_test_seam,
    );
    backend_utils_fmgr_fmgr_seams::convert_via_proc::set(convert_via_proc_seam);
    backend_utils_fmgr_fmgr_seams::convert_via_proc_counted::set(convert_via_proc_counted_seam);

    // Frame-widening (PG_GETARG_* / PG_RETURN_* / PG_NARGS / PG_ARGISNULL /
    // call-mcx / fn_expr readers) over the executor `types_nodes` frame.
    backend_utils_fmgr_fmgr_seams::pg_nargs::set(pg_nargs_seam);
    backend_utils_fmgr_fmgr_seams::pg_argisnull::set(pg_argisnull_seam);
    backend_utils_fmgr_fmgr_seams::pg_getarg_oid::set(pg_getarg_oid_seam);
    backend_utils_fmgr_fmgr_seams::pg_getarg_int16::set(pg_getarg_int16_seam);
    backend_utils_fmgr_fmgr_seams::pg_getarg_int64::set(pg_getarg_int64_seam);
    backend_utils_fmgr_fmgr_seams::pg_getarg_bool::set(pg_getarg_bool_seam);
    backend_utils_fmgr_fmgr_seams::pg_getarg_datum::set(pg_getarg_datum_seam);
    // By-reference PG_GETARG readers over the executor frame's `ref_args` side
    // channel (TD-FMGR-GETARG-BYREF — the by-ref-arg widen on the nodes frame).
    backend_utils_fmgr_fmgr_seams::pg_getarg_text_pp::set(pg_getarg_text_pp_seam);
    backend_utils_fmgr_fmgr_seams::pg_getarg_varlena_pp::set(pg_getarg_varlena_pp_seam);
    backend_utils_fmgr_fmgr_seams::pg_getarg_name::set(pg_getarg_name_seam);
    backend_utils_fmgr_fmgr_seams::pg_getarg_cstring::set(pg_getarg_cstring_seam);
    backend_utils_fmgr_fmgr_seams::pg_return_int64::set(pg_return_int64_seam);
    backend_utils_fmgr_fmgr_seams::pg_return_datum::set(pg_return_datum_seam);
    backend_utils_fmgr_fmgr_seams::pg_return_bool::set(pg_return_bool_seam);
    backend_utils_fmgr_fmgr_seams::pg_return_null::set(pg_return_null_seam);
    backend_utils_fmgr_fmgr_seams::pg_call_mcx::set(pg_call_mcx_seam);
    backend_utils_fmgr_fmgr_seams::get_fn_expr_variadic::set(get_fn_expr_variadic_seam);
    backend_utils_fmgr_fmgr_seams::get_fn_expr_arg_stable::set(get_fn_expr_arg_stable_seam);
    backend_utils_fmgr_fmgr_seams::fn_oid_and_expr::set(fn_oid_and_expr_seam);

    // Re-resolve I/O over typed inputs (`input_is_valid_by_type` installed above).
    backend_utils_fmgr_fmgr_seams::record_column_input::set(record_column_input_seam);
    backend_utils_fmgr_fmgr_seams::record_column_receive::set(record_column_receive_seam);
    backend_utils_fmgr_fmgr_seams::record_column_output::set(record_column_output_seam);
    backend_utils_fmgr_fmgr_seams::record_column_send::set(record_column_send_seam);
    backend_utils_fmgr_fmgr_seams::typmod_out::set(typmod_out_seam);

    // Element-type dispatch (arrayfuncs.c).
    backend_utils_fmgr_fmgr_seams::element_eq::set(element_eq_seam);
    backend_utils_fmgr_fmgr_seams::element_cmp::set(element_cmp_seam);
    backend_utils_fmgr_fmgr_seams::element_hash::set(element_hash_seam);
    backend_utils_fmgr_fmgr_seams::element_hash_extended::set(element_hash_extended_seam);
    backend_utils_fmgr_fmgr_seams::array_output_function_call::set(array_output_function_call_seam);
    backend_utils_fmgr_fmgr_seams::array_receive_function_call::set(
        array_receive_function_call_seam,
    );
    backend_utils_fmgr_fmgr_seams::array_send_function_call::set(array_send_function_call_seam);

    // pg_proc.c (ProcedureCreate) gates the language validator on
    // `CheckFunctionValidatorAccess(validatorOid, funcOid)` (fmgr.c, ported
    // here); the seam carries no caller `mcx`, so it runs behind a scratch
    // context.
    backend_catalog_pg_proc_seams::check_function_validator_access::set(
        |validator_fn_oid, func_oid| {
            let scratch = MemoryContext::new("check_function_validator_access");
            check_function_validator_access(scratch.mcx(), validator_fn_oid, func_oid)
        },
    );

    // `fmgr_internal_validator` (pg_proc.c:761-774): read the function's `prosrc`
    // and verify it names a built-in (`fmgr_internal_function(prosrc) !=
    // InvalidOid`); the fmgr-builtin lookup is this crate's. The seam carries no
    // caller `mcx`, so it runs behind a scratch context.
    backend_catalog_pg_proc_seams::validate_internal_function::set(|funcoid| {
        let scratch = MemoryContext::new("validate_internal_function");
        let mcx = scratch.mcx();
        // C: SearchSysCache1(PROCOID, funcoid); if (!valid) elog(ERROR,
        // "cache lookup failed for function %u").
        let proc = backend_utils_cache_syscache_seams::lookup_proc::call(mcx, funcoid)?
            .ok_or_else(|| PgError::error(format!("cache lookup failed for function {funcoid}")))?;
        let prosrc = proc.prosrc.as_ref().map(|s| s.as_str()).unwrap_or("");
        // C: if (fmgr_internal_function(prosrc) == InvalidOid)
        //        ereport(ERROR, ERRCODE_UNDEFINED_FUNCTION,
        //                "there is no built-in function named \"%s\"").
        if !types_core::primitive::OidIsValid(fmgr_internal_function(prosrc)) {
            return Err(PgError::error(format!(
                "there is no built-in function named \"{prosrc}\""
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
        }
        Ok(())
    });

    // `fmgr_c_validator` (pg_proc.c:781-823): make sure the library file exists,
    // is loadable, and contains the specified link symbol with a valid function
    // information record. C: load_external_function(probin, prosrc, true, &h)
    // then fetch_finfo_record(h, prosrc); both are folded into this crate's
    // `load_external_function` dfmgr seam (which returns the validated
    // `(user_fn, api_version)` pair). The seam carries no caller `mcx`, so it
    // runs behind a scratch context.
    backend_catalog_pg_proc_seams::validate_c_function::set(|funcoid| {
        let scratch = MemoryContext::new("validate_c_function");
        let mcx = scratch.mcx();
        // C: SearchSysCache1(PROCOID, funcoid); if (!valid) elog(ERROR,
        // "cache lookup failed for function %u").
        let proc = backend_utils_cache_syscache_seams::lookup_proc::call(mcx, funcoid)?
            .ok_or_else(|| PgError::error(format!("cache lookup failed for function {funcoid}")))?;
        // C: SysCacheGetAttrNotNull(PROCOID, tuple, Anum_pg_proc_prosrc/probin).
        let prosrc = proc.prosrc.as_ref().map(|s| s.as_str()).ok_or_else(|| {
            PgError::error(format!("null prosrc for function {funcoid}"))
        })?;
        let probin = proc.probin.as_ref().map(|s| s.as_str()).ok_or_else(|| {
            PgError::error(format!("null probin for function {funcoid}"))
        })?;
        // C: (void) load_external_function(probin, prosrc, true, &libraryhandle);
        //    (void) fetch_finfo_record(libraryhandle, prosrc);
        backend_utils_fmgr_dfmgr_seams::load_external_function::call(probin, prosrc, funcoid)?;
        Ok(())
    });
}
