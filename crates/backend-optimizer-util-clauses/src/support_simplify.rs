//! The planner support-function `SupportRequestSimplify` dispatch table.
//!
//! C's `simplify_function` (clauses.c:4061) calls a function's planner support
//! function (`pg_proc.prosupport`) through fmgr with a `SupportRequestSimplify`
//! node; the support function inspects the wrapped `FuncExpr` and returns either
//! a simplified clause or NULL ("no simplification"). The `OidFunctionCall1`
//! dispatch is by the `prosupport` OID.
//!
//! The owned model decomposes the request: the [`call_support_simplify`] seam
//! hands the support function the already-typed argument `Expr`s plus the
//! result/collation OIDs (C reads these off `req->fcall`). This table is the
//! `prosupport`-OID counterpart of fmgr's builtin table for the
//! `SupportRequestSimplify` request: each support-bearing crate registers its
//! decomposed simplify kernel here from its own `init_seams`.
//!
//! A `prosupport` OID with no registered simplify kernel is **not** an error: in
//! C, a support function that does not handle `SupportRequestSimplify` (e.g.
//! `generate_series_int4_support`, which serves only `SupportRequestRows`)
//! simply returns NULL. So an unregistered OID declines with `Ok(None)`,
//! matching the C "no simplification" outcome. The simplify request never reads
//! the cluster catalog, so a missing kernel is faithfully a decline, never a
//! panic.
//!
//! Process-global, like the fmgr builtin registry and the executor-frame SRF
//! table (`OnceLock<Mutex<..>>`; the single-user backend dispatches on one
//! thread, but the registry must be visible to whatever thread runs the
//! dispatch).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use types_core::Oid;
use types_error::PgResult;
use types_nodes::primnodes::Expr;

/// A decomposed `SupportRequestSimplify` kernel: the request's argument list and
/// the result/collation context, mirroring what the support function reads off
/// `req->fcall` in C. Returns the simplified clause (`Ok(Some)`) or a decline
/// (`Ok(None)`); `Err` carries the support function's `ereport(ERROR)`.
pub type SupportSimplifyFn = for<'mcx> fn(
    mcx: mcx::Mcx<'mcx>,
    funcid: Oid,
    result_type: Oid,
    result_collid: Oid,
    input_collid: Oid,
    args: &[Expr],
    funcvariadic: bool,
    estimate: bool,
) -> PgResult<Option<Expr>>;

fn table() -> &'static Mutex<HashMap<Oid, SupportSimplifyFn>> {
    static SUPPORT_SIMPLIFY_TABLE: OnceLock<Mutex<HashMap<Oid, SupportSimplifyFn>>> =
        OnceLock::new();
    SUPPORT_SIMPLIFY_TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register a decomposed `SupportRequestSimplify` kernel under its `prosupport`
/// OID. Returns the previous registration if the OID was already present.
pub fn register_support_simplify(prosupport: Oid, func: SupportSimplifyFn) -> Option<SupportSimplifyFn> {
    table()
        .lock()
        .expect("support-simplify table lock")
        .insert(prosupport, func)
}

/// `call_support_simplify` — the decomposed `SupportRequestSimplify` dispatch
/// (clauses.c:4108-4148). Resolve `prosupport` in the table and run the support
/// function's simplify kernel; an OID with no registered simplify kernel
/// declines (`Ok(None)`), the faithful counterpart of a support function that
/// does not handle `SupportRequestSimplify` returning NULL in C.
#[allow(clippy::too_many_arguments)]
pub fn call_support_simplify<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    prosupport: Oid,
    funcid: Oid,
    result_type: Oid,
    result_collid: Oid,
    input_collid: Oid,
    args: &[Expr],
    funcvariadic: bool,
    estimate: bool,
) -> PgResult<Option<Expr>> {
    let func = table()
        .lock()
        .expect("support-simplify table lock")
        .get(&prosupport)
        .copied();
    match func {
        Some(f) => f(
            mcx,
            funcid,
            result_type,
            result_collid,
            input_collid,
            args,
            funcvariadic,
            estimate,
        ),
        None => Ok(None),
    }
}

/// Helper for support functions (like `generate_series_int4_support`) that
/// serve only `SupportRequestRows` and never simplify: always declines. Kept so
/// such crates register an explicit decline (documenting the support function
/// exists but has no simplify leg) rather than relying solely on the
/// unregistered-OID default.
pub fn decline_simplify(
    _mcx: mcx::Mcx<'_>,
    _funcid: Oid,
    _result_type: Oid,
    _result_collid: Oid,
    _input_collid: Oid,
    _args: &[Expr],
    _funcvariadic: bool,
    _estimate: bool,
) -> PgResult<Option<Expr>> {
    Ok(None)
}
