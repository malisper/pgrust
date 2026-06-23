//! The planner support-function `SupportRequestOptimizeWindowClause` dispatch
//! table.
//!
//! `optimize_window_clauses` (planner.c:5815) narrows a WindowClause's frame
//! options by calling each of its WindowFuncs' `pg_proc.prosupport` support
//! function with a `SupportRequestOptimizeWindowClause` node; the support
//! function may rewrite `req->frameOptions` to an equivalent-but-cheaper frame
//! (e.g. the ranking window functions can always use "ROWS BETWEEN UNBOUNDED
//! PRECEDING AND CURRENT ROW" instead of the default RANGE, saving peer-row
//! checks at execution). The dispatch is by the `prosupport` OID.
//!
//! The owned model decomposes the request: the [`call_support_optimize_window`]
//! entry hands the support kernel the window-function OID and the current frame
//! options, and the kernel returns the new frame options (`Ok(Some)`) or
//! declines the request (`Ok(None)`, the C `res == NULL` path). This table is
//! the `prosupport`-OID counterpart of fmgr's builtin table for the
//! `SupportRequestOptimizeWindowClause` request.
//!
//! A `prosupport` OID with no registered kernel returns `Ok(None)` — the caller
//! then leaves the frame options unchanged, exactly as in C.
//!
//! Process-global, like the support-rows / support-simplify tables.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use ::types_core::Oid;
use ::types_error::PgResult;

/// Frame-option flag bits (`nodes/parsenodes.h`).
const FRAMEOPTION_NONDEFAULT: i32 = 0x00001;
const FRAMEOPTION_ROWS: i32 = 0x00004;
const FRAMEOPTION_START_UNBOUNDED_PRECEDING: i32 = 0x00020;
const FRAMEOPTION_END_CURRENT_ROW: i32 = 0x00400;

/// A decomposed `SupportRequestOptimizeWindowClause` kernel: the window
/// function OID (`req->window_func->winfnoid`) and the WindowClause's current
/// `frameOptions` (`req->frameOptions` on input). Returns the optimized frame
/// options (`Ok(Some)`) or a decline (`Ok(None)`); `Err` carries the support
/// function's `ereport(ERROR)`.
pub type SupportOptimizeWindowFn = fn(winfnoid: Oid, frame_options: i32) -> PgResult<Option<i32>>;

fn table() -> &'static Mutex<HashMap<Oid, SupportOptimizeWindowFn>> {
    static T: OnceLock<Mutex<HashMap<Oid, SupportOptimizeWindowFn>>> = OnceLock::new();
    T.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register a decomposed `SupportRequestOptimizeWindowClause` kernel under its
/// `prosupport` OID. Returns the previous registration if the OID was present.
pub fn register_support_optimize_window(
    prosupport: Oid,
    func: SupportOptimizeWindowFn,
) -> Option<SupportOptimizeWindowFn> {
    table()
        .lock()
        .expect("support-optimize-window table lock")
        .insert(prosupport, func)
}

/// `call_support_optimize_window(prosupport, winfnoid, frame_options)` — the
/// decomposed `SupportRequestOptimizeWindowClause` dispatch (planner.c:5848).
/// Resolve `prosupport` in the table and run the support function's kernel; an
/// OID with no registered kernel returns `Ok(None)`, the faithful counterpart
/// of "no support function for this request, so leave the frame untouched".
pub fn call_support_optimize_window(
    prosupport: Oid,
    winfnoid: Oid,
    frame_options: i32,
) -> PgResult<Option<i32>> {
    let func = table()
        .lock()
        .expect("support-optimize-window table lock")
        .get(&prosupport)
        .copied();
    match func {
        Some(f) => f(winfnoid, frame_options),
        None => Ok(None),
    }
}

// ===========================================================================
// Built-in `SupportRequestOptimizeWindowClause` kernels (registered from this
// crate's `init_seams`). These mirror the ranking window functions' support
// functions in windowfuncs.c (window_row_number_support, window_rank_support,
// window_dense_rank_support, window_percent_rank_support,
// window_cume_dist_support, window_ntile_support). Every one of them rewrites
// the frame to "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW": these
// functions all just count rows/peers from the partition start, so the cheaper
// ROWS frame yields identical results while skipping per-row peer checks.
// ===========================================================================

/// `window_row_number_support` prosupport OID (`pg_proc.dat`): 6233.
pub const WINDOW_ROW_NUMBER_SUPPORT: Oid = 6233;
/// `window_rank_support`: 6234.
pub const WINDOW_RANK_SUPPORT: Oid = 6234;
/// `window_dense_rank_support`: 6235.
pub const WINDOW_DENSE_RANK_SUPPORT: Oid = 6235;
/// `window_percent_rank_support`: 6306.
pub const WINDOW_PERCENT_RANK_SUPPORT: Oid = 6306;
/// `window_cume_dist_support`: 6307.
pub const WINDOW_CUME_DIST_SUPPORT: Oid = 6307;
/// `window_ntile_support`: 6308.
pub const WINDOW_NTILE_SUPPORT: Oid = 6308;

/// The shared `SupportRequestOptimizeWindowClause` body of every ranking window
/// function (windowfuncs.c): set the frame options to
/// `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, regardless of input.
fn ranking_optimize_window(_winfnoid: Oid, _frame_options: i32) -> PgResult<Option<i32>> {
    Ok(Some(
        FRAMEOPTION_NONDEFAULT
            | FRAMEOPTION_ROWS
            | FRAMEOPTION_START_UNBOUNDED_PRECEDING
            | FRAMEOPTION_END_CURRENT_ROW,
    ))
}

/// Register the built-in support-optimize-window kernels in the dispatch table.
pub fn register_builtin_support_optimize_window() {
    register_support_optimize_window(WINDOW_ROW_NUMBER_SUPPORT, ranking_optimize_window);
    register_support_optimize_window(WINDOW_RANK_SUPPORT, ranking_optimize_window);
    register_support_optimize_window(WINDOW_DENSE_RANK_SUPPORT, ranking_optimize_window);
    register_support_optimize_window(WINDOW_PERCENT_RANK_SUPPORT, ranking_optimize_window);
    register_support_optimize_window(WINDOW_CUME_DIST_SUPPORT, ranking_optimize_window);
    register_support_optimize_window(WINDOW_NTILE_SUPPORT, ranking_optimize_window);
}
