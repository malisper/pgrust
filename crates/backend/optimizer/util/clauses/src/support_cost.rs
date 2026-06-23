//! The planner support-function `SupportRequestCost` dispatch table.
//!
//! `add_function_cost` (plancat.c:2125) refines a function's `(startup,
//! per_tuple)` cost by calling its `pg_proc.prosupport` support function with a
//! `SupportRequestCost` node; the support function may return refined cost
//! fields, or decline (leaving the caller to fall back on `pg_proc.procost *
//! cpu_operator_cost`). The dispatch is by the `prosupport` OID.
//!
//! The owned model decomposes the request: the [`call_support_cost`] entry
//! hands the support kernel the function OID and the call's `FuncExpr`/`OpExpr`
//! node. This table is the `prosupport`-OID counterpart of fmgr's builtin table
//! for the `SupportRequestCost` request: each support-bearing crate registers
//! its decomposed cost kernel here from its own `init_seams`.
//!
//! A `prosupport` OID with no registered cost kernel, or a kernel that declines,
//! returns `Ok(None)` — the caller then falls back on `pg_proc.procost`,
//! exactly as in C. The built-in `generate_series_int{4,8}_support` functions
//! serve only `SupportRequestRows` (not `SupportRequestCost`), so they have no
//! cost kernel here and correctly fall back on `procost`.
//!
//! Process-global, like the support-rows and support-simplify tables.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::primnodes::Expr;

/// A decomposed `SupportRequestCost` kernel: the function OID and the call's
/// node. Returns the refined `(startup, per_tuple)` cost (`Ok(Some)`) or a
/// decline (`Ok(None)`); `Err` carries the support function's `ereport(ERROR)`.
pub type SupportCostFn = fn(funcid: Oid, node: Option<&Expr>) -> PgResult<Option<(f64, f64)>>;

fn table() -> &'static Mutex<HashMap<Oid, SupportCostFn>> {
    static SUPPORT_COST_TABLE: OnceLock<Mutex<HashMap<Oid, SupportCostFn>>> = OnceLock::new();
    SUPPORT_COST_TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The by-`prosrc`-symbol counterpart of [`table`], for support functions whose
/// `prosupport` OID is assigned dynamically at `CREATE FUNCTION` time (e.g. a
/// user-created C-language support function like the regress test's
/// `test_support_func`): they cannot be keyed by a fixed builtin OID, so the
/// dispatch resolves the OID's `prosrc` symbol and looks it up here, mirroring
/// fmgr's by-`prosrc` C-language resolution.
fn symbol_table() -> &'static Mutex<HashMap<String, SupportCostFn>> {
    static T: OnceLock<Mutex<HashMap<String, SupportCostFn>>> = OnceLock::new();
    T.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register a decomposed `SupportRequestCost` kernel under its `prosupport` OID.
/// Returns the previous registration if the OID was already present.
pub fn register_support_cost(prosupport: Oid, func: SupportCostFn) -> Option<SupportCostFn> {
    table()
        .lock()
        .expect("support-cost table lock")
        .insert(prosupport, func)
}

/// Register a decomposed `SupportRequestCost` kernel under its `prosrc` symbol
/// name (for dynamically-OID'd support functions). Returns any previous one.
pub fn register_support_cost_by_symbol(prosrc: &str, func: SupportCostFn) -> Option<SupportCostFn> {
    symbol_table()
        .lock()
        .expect("support-cost symbol table lock")
        .insert(prosrc.to_string(), func)
}

/// `call_support_cost` by `prosrc` symbol — the dynamic-OID fallback. Returns
/// `Ok(None)` when no kernel is registered under the symbol (decline).
pub fn call_support_cost_by_symbol(
    prosrc: &str,
    funcid: Oid,
    node: Option<&Expr>,
) -> PgResult<Option<(f64, f64)>> {
    let func = symbol_table()
        .lock()
        .expect("support-cost symbol table lock")
        .get(prosrc)
        .copied();
    match func {
        Some(f) => f(funcid, node),
        None => Ok(None),
    }
}

/// `call_support_cost(prosupport, funcid, node)` — the decomposed
/// `SupportRequestCost` dispatch (plancat.c:2137-2150). Resolve `prosupport` in
/// the table and run the support function's cost kernel; an OID with no
/// registered kernel (or a kernel that declines) returns `Ok(None)`, the
/// faithful counterpart of "no support function, or it failed, so rely on
/// procost".
pub fn call_support_cost(
    prosupport: Oid,
    funcid: Oid,
    node: Option<&Expr>,
) -> PgResult<Option<(f64, f64)>> {
    let func = table()
        .lock()
        .expect("support-cost table lock")
        .get(&prosupport)
        .copied();
    match func {
        Some(f) => f(funcid, node),
        None => Ok(None),
    }
}
