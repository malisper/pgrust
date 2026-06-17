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
use types_nodes::execexpr::PGFunction;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

/// Process-global, matching the seam registry's `OnceLock` model (NOT
/// thread-local): the single-user backend dispatches on one thread, and the
/// registry must be visible to whatever thread runs the dispatch. The stored
/// `PGFunction` is a plain `fn` pointer (`Send + Sync`).
fn table() -> &'static Mutex<HashMap<Oid, PGFunction>> {
    static SRF_TABLE: OnceLock<Mutex<HashMap<Oid, PGFunction>>> = OnceLock::new();
    SRF_TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register an executor-frame set-returning function under its `pg_proc` OID
/// (the executor-frame counterpart of adding a `fmgr_builtins[]` row). Returns
/// the previous registration if the OID was already present.
pub fn register_srf(foid: Oid, func: PGFunction) -> Option<PGFunction> {
    table().lock().expect("SRF table lock").insert(foid, func)
}

/// Whether an OID has an executor-frame SRF registered.
pub fn srf_is_registered(foid: Oid) -> bool {
    table().lock().expect("SRF table lock").contains_key(&foid)
}

/// `FunctionCallInvoke(fcinfo)` for a set-returning function (execSRF.c) —
/// resolve `foid` in the executor-frame SRF table and dispatch the callable
/// over the LIVE call frame (whose `resultinfo` carries the `ReturnSetInfo` the
/// callee reads/writes). `Err` for an OID that has no executor-frame SRF
/// registered (the C `fmgr_isbuiltin` miss for this ABI).
pub fn srf_invoke_by_oid<'mcx>(
    foid: Oid,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let func = table().lock().expect("SRF table lock").get(&foid).copied();
    match func {
        Some(f) => Ok(f(fcinfo)),
        None => Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(alloc::format!(
                "set-returning function with OID {foid} is not registered in the \
                 executor-frame SRF table (no executor-frame PGFunction)"
            ))
            .into_error()),
    }
}
