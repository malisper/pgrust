//! Backend-lifetime side-table for compiled row variables' `rowtupdesc`.
//!
//! In C `PLpgSQL_row.rowtupdesc` is a `TupleDesc` — a live, arena-bound object
//! built once in `build_row_from_vars` (`CreateTemplateTupleDesc` +
//! `TupleDescInitEntry`/`TupleDescInitEntryCollation`) inside the compiled
//! function's long-lived memory context. The compiled `PLpgSQL_function` is
//! cached in `plpgsql_HashTable` for the whole backend's life, so its
//! `rowtupdesc` is never freed (it lives as long as the compile context, which
//! the function-cache pins until backend exit).
//!
//! The owned `plpgsql::PLpgSQL_row.rowtupdesc` is a lifetime-free `u64`
//! handle (the compiled function structs are `Clone + Debug`, so they cannot
//! carry the real `TupleDescData<'mcx>` directly). This module is the
//! handle-indirection side-table for that field, mirroring the executor's
//! `erh_table` design: the real `TupleDescData` lives in this backend-thread
//! table, each entry owning its own private `MemoryContext` (the C compile
//! context analogue) so the descriptor — and every name it owns — stays alive
//! for the backend's lifetime.
//!
//! Unlike `erh_table`, these entries are never cleared: a compiled function's
//! row tupdesc is backend-lifetime, exactly like the C compile context, so the
//! table only ever grows (one entry per `build_row_from_vars` call, which fires
//! once per compiled multi-OUT-argument function).

use core::cell::RefCell;

use mcx::MemoryContext;
use types_tuple::heaptuple::TupleDescData;

/// One compiled row's tuple descriptor: its private memory context and the
/// descriptor allocated in it (with a `'static` marker — the entry owns the
/// context that backs the descriptor's allocations and the entry is never
/// dropped, so the marker is sound).
struct RowTupdescEntry {
    /// The tuple descriptor, with every allocation it owns living in `ctx`'s
    /// arena. Declared FIRST so it would drop before `ctx` — though entries here
    /// are never dropped (see module note), this keeps the safe drop order.
    desc: TupleDescData<'static>,
    /// The owning context (the C compile context). Kept alive for the backend's
    /// lifetime so `desc`'s `'static` marker is sound.
    #[allow(dead_code)]
    ctx: Box<MemoryContext>,
}

thread_local! {
    /// The compiled-row tuple descriptors built on this backend thread. Indexed
    /// by `handle - 1` (the `u64` handle is 1-based; `0` is the C NULL / "no
    /// descriptor"). Never cleared — backend-lifetime, like the C compile
    /// contexts that own the real descriptors.
    static ROWTUPDESC_TABLE: RefCell<Vec<RowTupdescEntry>> = const { RefCell::new(Vec::new()) };
}

/// Register a freshly-built `TupleDescData` (allocated in `ctx`'s `Mcx`) and
/// return its 1-based `u64` handle for storing into `PLpgSQL_row.rowtupdesc`.
///
/// # Safety contract
/// `desc` must have been allocated through `ctx.mcx()` (so `ctx` backs every
/// allocation it transitively owns). The caller transfers ownership of `ctx`
/// here; the entry keeps it alive for the backend's lifetime.
pub fn register(ctx: Box<MemoryContext>, desc: TupleDescData<'static>) -> u64 {
    ROWTUPDESC_TABLE.with(|cell| {
        let mut t = cell.borrow_mut();
        t.push(RowTupdescEntry { ctx, desc });
        t.len() as u64
    })
}

/// Run `f` against the live `TupleDescData` for `handle` (1-based; `0` = no
/// descriptor, in which case `f` is not called and `None` is returned). The
/// descriptor is borrowed mutably so the caller can `BlessTupleDesc` it in
/// place (C blesses `row->rowtupdesc` on every `exec_eval_datum` ROW read; the
/// registered type/typmod then persists on the backend-lifetime descriptor).
pub fn with_rowtupdesc<R>(
    handle: u64,
    f: impl FnOnce(&mut TupleDescData<'static>) -> R,
) -> Option<R> {
    if handle == 0 {
        return None;
    }
    ROWTUPDESC_TABLE.with(|cell| {
        let mut t = cell.borrow_mut();
        t.get_mut((handle - 1) as usize).map(|e| f(&mut e.desc))
    })
}
