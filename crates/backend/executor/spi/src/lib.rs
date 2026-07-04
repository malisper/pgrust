#![allow(non_snake_case)]
// spi.c (PG 18.3). Unported lanes panic loudly: cursors, SPI_execute_extended
// dest/owner options, queryEnv/ENR registration, parserSetup hooks.

use core::cell::{Cell, RefCell};

use elog::ereport;
use mcx::{Mcx, MemoryContext};
use types_core::{InvalidSubTransactionId, SubTransactionId};
use types_error::{ErrorLocation, PgResult, ERRCODE_WARNING, WARNING};

pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("spi.c", 0, funcname)
}

mod access;
mod cursor;
mod execute;
mod plan;
mod tuptable;

pub use access::{SPI_fnumber, SPI_getbinval, SPI_getvalue, SPI_gettypeid};
pub use execute::{
    SPI_exec, SPI_execp, SPI_execute, SPI_execute_extended, SPI_execute_plan,
    SPI_execute_snapshot, SpiExecuteOptions,
};
pub use cursor::{
    SPI_cursor_close, SPI_cursor_close_portal, SPI_cursor_fetch, SPI_cursor_open,
    SPI_cursor_open_extended, SPI_scroll_cursor_fetch, SPI_scroll_cursor_move, SpiCursor,
};
pub use plan::{
    SPI_freeplan, SPI_getargcount, SPI_getargtypeid, SPI_keepplan, SPI_plan_command_tags,
    SPI_plan_is_valid, SPI_plan_single_source, SPI_prepare, SPI_prepare_cursor,
    SPI_prepare_plpgsql, SpiPlanPtr,
};
pub use tuptable::{tuptable_with, SPI_freetuptable, TuptabData, TuptabHandle};

pub const SPI_ERROR_CONNECT: i32 = -1;
pub const SPI_ERROR_COPY: i32 = -2;
pub const SPI_ERROR_OPUNKNOWN: i32 = -3;
pub const SPI_ERROR_UNCONNECTED: i32 = -4;
pub const SPI_ERROR_ARGUMENT: i32 = -6;
pub const SPI_ERROR_PARAM: i32 = -7;
pub const SPI_ERROR_TRANSACTION: i32 = -8;
pub const SPI_ERROR_NOATTRIBUTE: i32 = -9;
pub const SPI_ERROR_NOOUTFUNC: i32 = -10;
pub const SPI_ERROR_TYPUNKNOWN: i32 = -11;
pub const SPI_ERROR_REL_DUPLICATE: i32 = -12;
pub const SPI_ERROR_REL_NOT_FOUND: i32 = -13;

pub const SPI_OK_CONNECT: i32 = 1;
pub const SPI_OK_FINISH: i32 = 2;
pub const SPI_OK_FETCH: i32 = 3;
pub const SPI_OK_UTILITY: i32 = 4;
pub const SPI_OK_SELECT: i32 = 5;
pub const SPI_OK_SELINTO: i32 = 6;
pub const SPI_OK_INSERT: i32 = 7;
pub const SPI_OK_DELETE: i32 = 8;
pub const SPI_OK_UPDATE: i32 = 9;
pub const SPI_OK_CURSOR: i32 = 10;
pub const SPI_OK_INSERT_RETURNING: i32 = 11;
pub const SPI_OK_DELETE_RETURNING: i32 = 12;
pub const SPI_OK_UPDATE_RETURNING: i32 = 13;
pub const SPI_OK_REWRITTEN: i32 = 14;
pub const SPI_OK_REL_REGISTER: i32 = 15;
pub const SPI_OK_REL_UNREGISTER: i32 = 16;
pub const SPI_OK_TD_REGISTER: i32 = 17;
pub const SPI_OK_MERGE: i32 = 18;
pub const SPI_OK_MERGE_RETURNING: i32 = 19;

pub const SPI_OPT_NONATOMIC: i32 = 1 << 0;

pub(crate) struct SpiConnection {
    pub processed: u64,
    pub tuptable: Option<u64>,
    pub exec_subid: SubTransactionId,
    pub tuptables: Vec<tuptable::TuptabEntry>,
    // C procCxt/execCxt; leaked-Box pointers (plancache leak_ctx precedent)
    // so Mcx<'static> handles survive stack reallocation and reentrancy.
    pub proc_cxt: *mut MemoryContext,
    pub exec_cxt: *mut MemoryContext,
    pub connect_subid: SubTransactionId,
    pub atomic: bool,
    pub internal_xact: bool,
    pub plans: Vec<SpiPlanPtr>,
    pub outer_processed: u64,
    pub outer_tuptable: Option<TuptabHandle>,
    pub outer_result: i32,
}

thread_local! {
    pub(crate) static SPI_STACK: RefCell<Vec<SpiConnection>> = const { RefCell::new(Vec::new()) };
    // C's _SPI_connected: the per-transaction empty-stack guards (AtEOXact_SPI
    // and SPI_inside_nonatomic_context run per commit) must stay one Cell
    // load, not a RefCell borrow (+53 instr/q on the select1 gate).
    static SPI_CONNECTED: Cell<i32> = const { Cell::new(-1) };
    static SPI_PROCESSED: Cell<u64> = const { Cell::new(0) };
    static SPI_RESULT: Cell<i32> = const { Cell::new(0) };
    static SPI_TUPTABLE: Cell<Option<TuptabHandle>> = const { Cell::new(None) };
}

fn sync_connected() {
    let depth = SPI_STACK.with(|s| s.borrow().len());
    SPI_CONNECTED.with(|c| c.set(depth as i32 - 1));
}

pub fn SPI_processed() -> u64 {
    SPI_PROCESSED.with(Cell::get)
}

pub fn SPI_result() -> i32 {
    SPI_RESULT.with(Cell::get)
}

pub(crate) fn set_spi_result(v: i32) {
    SPI_RESULT.with(|c| c.set(v));
}

pub(crate) fn set_spi_processed(v: u64) {
    SPI_PROCESSED.with(|c| c.set(v));
}

pub fn SPI_tuptable() -> Option<TuptabHandle> {
    SPI_TUPTABLE.with(Cell::get)
}

pub(crate) fn set_spi_tuptable(v: Option<TuptabHandle>) {
    SPI_TUPTABLE.with(|c| c.set(v));
}

fn leak_ctx(name: &'static str) -> *mut MemoryContext {
    Box::into_raw(Box::new(MemoryContext::new(name)))
}

// C's execCxt is an aset reset wholesale at _SPI_end_call; parse/param cruft
// is never freed piecemeal, so the bump backend is the matching class.
fn leak_bump_ctx(name: &'static str) -> *mut MemoryContext {
    Box::into_raw(Box::new(MemoryContext::new_bump(name)))
}

pub(crate) fn ctx_mcx(ctx: *mut MemoryContext) -> Mcx<'static> {
    // SAFETY: leak_ctx provenance; reclaimed only when the owning stack entry
    // is popped, which cannot happen under a live borrow (pops run only from
    // SPI_finish / AtEO(Sub)Xact_SPI, after any execute frame has unwound).
    unsafe { (*ctx).mcx() }
}

fn reclaim_ctx(ctx: *mut MemoryContext) {
    // SAFETY: leak_ctx provenance; entry already unlinked from the stack.
    drop(unsafe { Box::from_raw(ctx) });
}

fn reset_ctx(ctx: *mut MemoryContext) {
    // SAFETY: leak_ctx provenance (see ctx_mcx).
    unsafe { (*ctx).reset() }
}

pub(crate) fn with_current<R>(f: impl FnOnce(&mut SpiConnection) -> R) -> Option<R> {
    SPI_STACK.with(|s| s.borrow_mut().last_mut().map(f))
}

pub fn SPI_connect() -> PgResult<i32> {
    SPI_connect_ext(0)
}

pub fn SPI_connect_ext(options: i32) -> PgResult<i32> {
    let connect_subid = xact::GetCurrentSubTransactionId();
    let conn = SpiConnection {
        processed: 0,
        tuptable: None,
        exec_subid: InvalidSubTransactionId,
        tuptables: Vec::new(),
        proc_cxt: leak_ctx("SPI Proc"),
        exec_cxt: leak_bump_ctx("SPI Exec"),
        connect_subid,
        atomic: options & SPI_OPT_NONATOMIC == 0,
        internal_xact: false,
        plans: Vec::new(),
        outer_processed: SPI_processed(),
        outer_tuptable: SPI_tuptable(),
        outer_result: SPI_result(),
    };
    SPI_STACK.with(|s| s.borrow_mut().push(conn));
    sync_connected();
    set_spi_processed(0);
    set_spi_tuptable(None);
    set_spi_result(0);
    Ok(SPI_OK_CONNECT)
}

fn teardown_connection(conn: SpiConnection) {
    plan::free_connection_plans(&conn.plans);
    drop(conn.tuptables);
    reclaim_ctx(conn.exec_cxt);
    reclaim_ctx(conn.proc_cxt);
}

pub fn SPI_finish() -> PgResult<i32> {
    if SPI_STACK.with(|s| s.borrow().is_empty()) {
        return Ok(SPI_ERROR_UNCONNECTED);
    }
    let conn = SPI_STACK.with(|s| s.borrow_mut().pop()).expect("checked nonempty");
    sync_connected();
    set_spi_processed(conn.outer_processed);
    set_spi_tuptable(conn.outer_tuptable);
    set_spi_result(conn.outer_result);
    teardown_connection(conn);
    Ok(SPI_OK_FINISH)
}

pub(crate) fn _SPI_begin_call(use_exec: bool) -> i32 {
    let connected = with_current(|conn| {
        if use_exec {
            conn.exec_subid = xact::GetCurrentSubTransactionId();
        }
    })
    .is_some();
    if connected {
        0
    } else {
        SPI_ERROR_UNCONNECTED
    }
}

pub(crate) fn _SPI_end_call(use_exec: bool) {
    if use_exec {
        let exec_cxt = with_current(|conn| {
            conn.exec_subid = InvalidSubTransactionId;
            conn.exec_cxt
        });
        if let Some(ctx) = exec_cxt {
            reset_ctx(ctx);
        }
    }
}

pub(crate) fn current_exec_mcx() -> Mcx<'static> {
    ctx_mcx(with_current(|conn| conn.exec_cxt).expect("SPI: not connected"))
}

pub fn AtEOXact_SPI(is_commit: bool) -> PgResult<()> {
    if SPI_CONNECTED.with(Cell::get) < 0 {
        return Ok(());
    }
    let mut found = false;
    loop {
        let popped = SPI_STACK.with(|s| {
            let mut stack = s.borrow_mut();
            match stack.last() {
                Some(conn) if !conn.internal_xact => stack.pop(),
                _ => None,
            }
        });
        let Some(conn) = popped else { break };
        sync_connected();
        found = true;
        set_spi_processed(conn.outer_processed);
        set_spi_tuptable(conn.outer_tuptable);
        set_spi_result(conn.outer_result);
        teardown_connection(conn);
    }
    if found && is_commit {
        ereport(WARNING)
            .errcode(ERRCODE_WARNING)
            .errmsg("transaction left non-empty SPI stack")
            .errhint("Check for missing \"SPI_finish\" calls.")
            .finish(loc("AtEOXact_SPI"))?;
    }
    Ok(())
}

pub fn AtEOSubXact_SPI(is_commit: bool, my_subid: SubTransactionId) -> PgResult<()> {
    if SPI_CONNECTED.with(Cell::get) < 0 {
        return Ok(());
    }
    let mut found = false;
    loop {
        let popped = SPI_STACK.with(|s| {
            let mut stack = s.borrow_mut();
            match stack.last() {
                Some(conn) if conn.connect_subid == my_subid && !conn.internal_xact => stack.pop(),
                _ => None,
            }
        });
        let Some(conn) = popped else { break };
        sync_connected();
        found = true;
        set_spi_processed(conn.outer_processed);
        set_spi_tuptable(conn.outer_tuptable);
        set_spi_result(conn.outer_result);
        teardown_connection(conn);
    }
    if found && is_commit {
        ereport(WARNING)
            .errcode(ERRCODE_WARNING)
            .errmsg("subtransaction left non-empty SPI stack")
            .errhint("Check for missing \"SPI_finish\" calls.")
            .finish(loc("AtEOSubXact_SPI"))?;
    }

    if !is_commit {
        let reset = with_current(|conn| {
            let exec = (conn.exec_subid != InvalidSubTransactionId && conn.exec_subid >= my_subid)
                .then(|| {
                    conn.exec_subid = InvalidSubTransactionId;
                    conn.exec_cxt
                });
            let mut dropped_current = false;
            conn.tuptables.retain(|tt| {
                let stale = tt.subid >= my_subid;
                if stale && Some(tt.id) == conn.tuptable {
                    conn.tuptable = None;
                    dropped_current = true;
                }
                !stale
            });
            (exec, dropped_current)
        });
        if let Some((exec, dropped_current)) = reset {
            if let Some(ctx) = exec {
                reset_ctx(ctx);
            }
            if dropped_current {
                set_spi_tuptable(None);
            }
        }
    }
    Ok(())
}

pub fn SPI_inside_nonatomic_context() -> bool {
    if SPI_CONNECTED.with(Cell::get) < 0 {
        return false;
    }
    // Must match _SPI_commit's atomicity tests.
    with_current(|conn| conn.atomic).is_some_and(|atomic| !atomic && !xact::IsSubTransaction())
}

#[doc(hidden)]
pub fn debug_stack_depth() -> usize {
    SPI_STACK.with(|s| s.borrow().len())
}

#[doc(hidden)]
pub fn debug_live_counts() -> (usize, usize) {
    let tuptables = SPI_STACK.with(|s| s.borrow().iter().map(|c| c.tuptables.len()).sum());
    (tuptables, plan::debug_live_plans())
}

// C SPI_cursor_find (spi.c): GetPortalByName. The found portal is not
// SPI-owned; SPI_cursor_close on it would free a NULL stmt handle — callers
// only fetch (cursor_to_xml precedent).
pub fn SPI_cursor_find(name: &str) -> Option<cursor::SpiCursor> {
    portalmem::GetPortalByName(Some(name)).map(cursor::SpiCursor::from_portal)
}

pub use cursor::SPI_cursor_find_portal;
}

pub fn SPI_register_trigger_data() -> ! {
    panic!("SPI_register_trigger_data (spi.c): ENR/queryEnv lane not ported")
}

pub fn init_seams() {
    spi_seams::spi_inside_nonatomic_context::set(SPI_inside_nonatomic_context);
    spi_seams::at_eoxact_spi::set(AtEOXact_SPI);
    spi_seams::at_eosubxact_spi::set(AtEOSubXact_SPI);
    spi_seams::spi_dest_startup::set(tuptable::spi_dest_startup);
    spi_seams::spi_printtup::set(tuptable::spi_printtup);
}

#[cfg(test)]
mod tests;
