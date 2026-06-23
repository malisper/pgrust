//! `vacuumlazy.c` — concurrent ("lazy") heap vacuuming, an idiomatic port.
//!
//! Heap relations are vacuumed in three main phases:
//!
//!   * **Phase I** ([`scan`]): scan relation pages, prune and freeze tuples, and
//!     save dead tuples' TIDs in a TID store. When the store fills or the scan
//!     finishes, vacuum advances to phase II.
//!   * **Phase II** ([`vacuum_phase`], [`index`]): index vacuuming — delete the
//!     dead index entries referenced by the TID store.
//!   * **Phase III** ([`heap_vacuum`]): scan the blocks referenced by the TIDs
//!     and reap the corresponding dead items, freeing space.
//!
//! After all three phases, the relation may be truncated ([`truncate`]) and its
//! statistics updated. [`vacuum_rel::heap_vacuum_rel`] is the public entry.
//!
//! ## Shape of this port
//!
//! `vacuumlazy.c` is the *driver* of heap lazy vacuum; the bulk of the work it
//! orchestrates lives in sibling subsystems it does not define, none of which is
//! present in this worktree (the heap-AM prune/freeze + visibility predicates,
//! the visibility map, the TID store, the buffer manager / read stream, the
//! FSM, the lock manager, relation truncation, parallel vacuum, the
//! vacuum-command cutoff/relstat layer, progress / pgstat / instrumentation, and
//! the misc backend infra). Each of those crossings is a loud-panic seam in
//! [`seams_ub_heaprest::vacuumlazy`] (the per-batch seam crate), defaulting to a
//! panic until startup installs a real implementation — there is no silent
//! fallback. The *decisions* the driver makes (the skip / eager-scan state
//! machine, the per-page prune/freeze accounting, the four-way VM-bit update,
//! the index-vacuuming bypass + wraparound failsafe, the truncation heuristics,
//! and the pg_class relstat finalization + logging) are ported 1:1 in-crate over
//! the owned [`core::LVRelState`].
//!
//! ## Idiomatic surface
//!
//! No raw pointers, `extern "C"`, `c_void`, `libc`, or `CString`. The heap
//! relation / its indexes cross the substrate seam as their bare `Oid` relcache
//! identities (`RelationGetRelid`, Relation = Oid-via-relcache; the former
//! `RelationHandle` wrapper was retired and the substrate re-resolves the live
//! `&RelationData` from the relcache); buffers cross as the
//! buffer-manager `Buffer` integer the substrate owns; error-reporting names are
//! owned `String`s; index relations / per-index stats are `Vec`s; the dead-TID
//! store, parallel-vacuum state, and visibility test cross as the small handles
//! the seam defines. Errors flow as `PgResult` (a thrown `elog(ERROR)` becomes
//! an `Err`).

#![allow(clippy::too_many_arguments)]
// PostgreSQL-faithful identifier names (file-scope `#define`s transcribed as
// `pub const`s, e.g. `InvalidBlockNumber`, `BUFFER_LOCK_SHARE`) keep the C
// spelling so the algorithm reads 1:1 against `vacuumlazy.c`.
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

pub mod consts;
pub mod core;
pub mod dead_items;
pub mod errcb;
pub mod heap_vacuum;
pub mod index;
pub mod scan;
pub mod scan_block;
pub mod scan_page;
pub mod truncate;
pub mod vacuum_phase;
pub mod vacuum_rel;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Public surface: the shared core types and the entry point.
// ---------------------------------------------------------------------------

pub use core::{LVRelState, LVSavedErrInfo, VacErrPhase};

pub use vacuum_rel::heap_vacuum_rel;

/// Install this crate's implementation into its seam crate. The driver's
/// public entry `heap_vacuum_rel` is the one inward seam; the outward
/// seams in `backend-access-heap-vacuumlazy-seams` belong to other,
/// not-yet-ported owners and are installed by them when they land.
///
/// `heap_vacuum_rel` is also this crate's body for the heap table AM's
/// `relation_vacuum` callback (`heapam_relation_vacuum` in `heapam_handler.c`,
/// which is a one-line `heap_vacuum_rel(rel, params, bstrategy)`). The
/// command layer (`commands/vacuum.c`'s `vacuum_rel`) dispatches a heap
/// relation through the table-AM `table_relation_vacuum` seam, so we install
/// that seam here to delegate to the in-crate driver — the heap AM being the
/// only ported table AM, this is the heap provider's vtable entry.
pub fn init_seams() {
    vacuumlazy_seams::heap_vacuum_rel::set(|mcx, rel, params, bstrategy| {
        heap_vacuum_rel(mcx, rel, &params, bstrategy)
    });

    // heapam_relation_vacuum — the heap table AM's `relation_vacuum`
    // callback. `vacuum.c` (`vacuum_rel`) reaches the heap vacuum driver
    // through this table-AM dispatch seam.
    vacuum_seams::table_relation_vacuum::set(|mcx, rel, params, bstrategy| {
        heap_vacuum_rel(mcx, rel, &params, bstrategy)
    });

    // read_stream_begin_relation / read_stream_end — in the owned model the C
    // read stream (which ran `heap_vac_scan_next_block` / `vacuum_reap_lp_read_
    // stream_next` inside the stream over a `void *vacrel`) is replaced by the
    // in-crate block-selection state machines in `scan_block`, and the chosen
    // block's buffer is read directly through the bufmgr `read_buffer_extended`
    // seam (see `scan::lazy_scan_heap` and `heap_vacuum::lazy_vacuum_heap_rel`).
    // The phase-III stream object therefore carries no driver state and drives
    // no I/O of its own — it is a lifecycle token only. Its owner is this model:
    // begin issues a fresh opaque id, end discards it. (Real read-ahead would
    // need the engine to call the driver's per-block callback, which lives here
    // and would cycle; the owned model deliberately bypasses it.)
    vacuumlazy_seams::read_stream_begin_relation::set(
        |_flags, _bstrategy, _rel, _fork, _callback, _reap_iter| {
            std::thread_local! {
                static NEXT_READ_STREAM_ID: std::cell::Cell<u64> = const { std::cell::Cell::new(1) };
            }
            let id = NEXT_READ_STREAM_ID.with(|c| {
                let v = c.get();
                c.set(v + 1);
                v
            });
            Ok(types_vacuum::vacuumlazy::ReadStreamHandle::new(id))
        },
    );
    vacuumlazy_seams::read_stream_end::set(|_stream| Ok(()));

    // push_error_context / pop_error_context — C pushes `vacuum_error_callback`
    // (errcb.rs) onto `error_context_stack` so a vacuum `ereport(ERROR)` is
    // annotated with the current phase/block/offset. This repo RETIRES the
    // ambient `error_context_stack` callback chain (docs/query-lifecycle-raii.md;
    // backend-utils-error: "there is no ambient callback mechanism"), exactly as
    // vacuumparallel.c's `push/pop_parallel_vacuum_error_context` are no-ops. The
    // `vacuum_error_callback` message builder stays ported (errcb.rs) for any
    // future direct attach; the push/pop are sanctioned no-ops.
    vacuumlazy_seams::push_error_context::set(|| Ok(()));
    vacuumlazy_seams::pop_error_context::set(|| Ok(()));
}
