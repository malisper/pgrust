//! Call-scoped side-table for live expanded-record headers (`PLpgSQL_rec.erh`).
//!
//! In C `PLpgSQL_rec.erh` is an `ExpandedRecordHeader *` — a live, arena-bound
//! object. The owned `plpgsql::PLpgSQL_rec.erh` is a lifetime-free `u64`
//! handle (the execstate is `Clone + Debug`, so it cannot carry the real
//! `ExpandedRecordHeader<'mcx>` directly). This module is the
//! `cte_link_plan_state` side-table indirection for that handle: the real
//! `ExpandedRecordHeader` lives in this backend-thread-local table, keyed by the
//! 1-based index stored in the `erh` `u64` (`0` == "no live header", the C NULL).
//!
//! Each entry owns its own private `::mcx::MemoryContext` (the C
//! `estate->datum_context` analogue), so the header — and every by-reference
//! field value it points at — stays alive for the whole PL/pgSQL call. The
//! header is stored with a `'static` marker transmuted from the owning context's
//! `Mcx`; this is sound because the entry holds the context that backs those
//! allocations and the entry is only dropped (via [`clear`]) when the call
//! returns, after every borrow handed out by [`with_erh_mut`] / [`with_erh`] has
//! ended. (This mirrors the firing path's own `'static`-marked
//! `CURRENT_TRIGGER_DATA` side-channel.)

use core::cell::RefCell;

use ::misc2::expandedrecord::ExpandedRecordHeader;
use mcx::{MemoryContext, Mcx};

/// One live expanded record: its private memory context and the header allocated
/// in it (with a `'static` marker — see the module note).
struct ErhEntry {
    /// The expanded-record header, with every allocation it owns living in
    /// `ctx`'s arena. DECLARED FIRST so it is dropped BEFORE `ctx`: Rust drops
    /// struct fields in declaration order, and the header's `PgVec`/`TupleDesc`
    /// `Drop` deallocates through `ctx`'s `Mcx` — which must still be live. (If
    /// `ctx` dropped first, freeing the arena, the header's drop would touch
    /// freed memory → segfault.)
    header: ExpandedRecordHeader<'static>,
    /// The owning context (the C `estate->datum_context`). Kept alive for the
    /// entry's lifetime so `header`'s `'static` marker is sound; dropped (freeing
    /// the arena) AFTER `header`, when the entry is removed in [`clear`].
    ctx: Box<MemoryContext>,
}

thread_local! {
    /// The live expanded-record headers for the PL/pgSQL call currently running
    /// on this backend thread. Indexed by `erh - 1` (the `u64` handle is 1-based;
    /// `0` is the C NULL). Recursive PL/pgSQL calls (a trigger that fires a query
    /// that fires another trigger) save/restore via [`take_all`]/[`restore_all`].
    static ERH_TABLE: RefCell<Vec<Option<ErhEntry>>> = const { RefCell::new(Vec::new()) };
}

/// Register a freshly-built `ExpandedRecordHeader` (allocated in `ctx`'s `Mcx`)
/// and return its 1-based `u64` handle for storing into `PLpgSQL_rec.erh`.
///
/// # Safety contract
/// `header` must have been allocated through `ctx.mcx()` (so `ctx` backs every
/// allocation it transitively owns). The caller transfers ownership of `ctx`
/// here; the entry keeps it alive until [`clear`].
pub fn register(ctx: Box<MemoryContext>, header: ExpandedRecordHeader<'static>) -> u64 {
    ERH_TABLE.with(|cell| {
        let mut t = cell.borrow_mut();
        t.push(Some(ErhEntry { ctx, header }));
        t.len() as u64 // 1-based handle
    })
}

#[inline]
fn idx(handle: u64) -> Option<usize> {
    if handle == 0 {
        None
    } else {
        Some((handle - 1) as usize)
    }
}

/// Run `f` with `&mut ExpandedRecordHeader` and the owning context's `Mcx` for
/// the header identified by `handle`. Returns `None` for the NULL handle (`0`)
/// or an unregistered/cleared slot.
pub fn with_erh_mut<R>(
    handle: u64,
    f: impl for<'mcx> FnOnce(Mcx<'mcx>, &mut ExpandedRecordHeader<'mcx>) -> R,
) -> Option<R> {
    let i = idx(handle)?;
    ERH_TABLE.with(|cell| {
        let mut t = cell.borrow_mut();
        let entry = t.get_mut(i)?.as_mut()?;
        // SAFETY: `entry.ctx` backs `entry.header`'s allocations and outlives
        // this call (the entry is only removed in `clear`, after `f` returns).
        // We reborrow the `'static` header at the context's real `'mcx` lifetime,
        // which is bounded by the borrow of `t` for the duration of `f`.
        let mcx: Mcx<'_> = entry.ctx.mcx();
        let header: &mut ExpandedRecordHeader<'_> =
            unsafe { core::mem::transmute(&mut entry.header) };
        Some(f(mcx, header))
    })
}

/// Run `f` with `&ExpandedRecordHeader` and the owning context's `Mcx`
/// (read-only). Returns `None` for the NULL handle or an unregistered slot.
pub fn with_erh<R>(
    handle: u64,
    f: impl for<'mcx> FnOnce(Mcx<'mcx>, &ExpandedRecordHeader<'mcx>) -> R,
) -> Option<R> {
    let i = idx(handle)?;
    ERH_TABLE.with(|cell| {
        let t = cell.borrow();
        let entry = t.get(i)?.as_ref()?;
        // SAFETY: see `with_erh_mut`.
        let mcx: Mcx<'_> = entry.ctx.mcx();
        let header: &ExpandedRecordHeader<'_> = unsafe { core::mem::transmute(&entry.header) };
        Some(f(mcx, header))
    })
}

/// An opaque saved expanded-record table (the save half of recursive nesting).
pub struct SavedErhTable(Vec<Option<ErhEntry>>);

/// Take (and clear) the entire table — the save half of recursive nesting. The
/// returned opaque token must be handed back to [`restore_all`] when the inner
/// call returns.
pub fn take_all() -> SavedErhTable {
    SavedErhTable(ERH_TABLE.with(|cell| core::mem::take(&mut *cell.borrow_mut())))
}

/// Restore a table previously captured by [`take_all`], dropping any entries the
/// inner call left behind (the inner call's [`clear`] should already have done
/// so).
pub fn restore_all(saved: SavedErhTable) {
    ERH_TABLE.with(|cell| *cell.borrow_mut() = saved.0);
}

/// Drop every live header for the current call (freeing each private context).
/// Called when a PL/pgSQL function/trigger invocation returns.
pub fn clear() {
    ERH_TABLE.with(|cell| cell.borrow_mut().clear());
}
