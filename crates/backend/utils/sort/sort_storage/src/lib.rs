//! `backend-utils-sort-storage` — temporary tuple storage for `utils/sort`.
//!
//! PostgreSQL's `src/backend/utils/sort/` builds three temporary storage files
//! — `logtape.c`, `tuplestore.c`, and `sharedtuplestore.c` — that the C-ABI
//! `backend-utils-sort-storage` crate bundles into one library. This crate
//! ports:
//!
//! * [`logtape`] — the logical-tape subsystem (over `BufFile`); ported fully,
//!   exposed through the `nodeAgg`/tuplesort opaque `usize` tape handles.
//! * [`tuplestore`] — the `MinimalTuple` temporary store; ported fully onto
//!   the landed payload-bearing `FormedMinimalTuple` carrier + flat codec,
//!   storing flat `MinimalTuple` blobs.
//! * [`sharedtuplestore`] — the parallel hash join's per-batch shared
//!   tuplestores; ported onto the in-DSM `SharedTuplestore` control object +
//!   per-participant `BufFile`s in a `SharedFileSet`, with the backend-local
//!   accessor in a `thread_local` slab keyed by a 1-based handle token.
//!
//! This unit owns the inward `backend-utils-sort-storage-seams` and installs
//! every one of them from [`init_seams`].

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

pub mod logtape;
pub mod sharedtuplestore;
pub mod tuplestore;

use sort_storage_seams as seams;

/// Install every `backend-utils-sort-storage` seam.
pub fn init_seams() {
    install_logtape_seams();
    install_tuplestore_seams();
    install_tuplestore_hold_seams();
    sharedtuplestore::init_seams();
}

/// The held-cursor portal-lifetime tuplestore seam (`PortalCreateHoldStore` →
/// `tuplestore_begin_heap(randomAccess, false, work_mem)` allocated in the
/// portal's `holdContext`). The store is `'static` (outlives the per-query
/// memory). Allocation failure errors out like the C `palloc`.
fn install_tuplestore_hold_seams() {
    tuplestore_hold_seams::tuplestore_begin_heap::set(|random_access| {
        tuplestore::tuplestore_begin_heap_hold(random_access)
    });
}

/// logtape.c — the value-typed logical-tape surface (the hash-agg spill path;
/// `fileset = NULL`, `worker = -1`). The set crosses as the real owned
/// `LogicalTapeSet` value (held by the consumer); a tape is a `usize` slot
/// index into the set's `tapes` vector. No side-table registry.
fn install_logtape_seams() {
    seams::logical_tape_set_create::set(|mcx, preallocate, worker| {
        logtape::logical_tape_set_create(mcx, preallocate, worker)
    });
    seams::logical_tape_set_close::set(logtape::logical_tape_set_close);
    seams::logical_tape_set_blocks::set(logtape::logical_tape_set_blocks);
    seams::logical_tape_create::set(logtape::logical_tape_create);
    seams::logical_tape_close::set(logtape::logical_tape_close);
    seams::logical_tape_write::set(logtape::logical_tape_write);
    seams::logical_tape_rewind_for_read::set(logtape::logical_tape_rewind_for_read);
    seams::logical_tape_read::set(logtape::logical_tape_read);
}

/// tuplestore.c — the tuple-store surface consumed by nodeMaterial /
/// nodeCtescan / nodeWorktablescan / nodeRecursiveunion / nodeTableFuncscan /
/// nodeNamedtuplestorescan.
fn install_tuplestore_seams() {
    seams::tuplestore_begin_heap::set(|mcx, randomAccess, interXact, maxKBytes| {
        tuplestore::tuplestore_begin_heap(mcx, randomAccess, interXact, maxKBytes)
    });
    seams::tuplestore_set_eflags::set(|state, eflags| {
        tuplestore::tuplestore_set_eflags(state, eflags)
    });
    seams::tuplestore_alloc_read_pointer::set(|state, eflags| {
        tuplestore::tuplestore_alloc_read_pointer(state, eflags)
    });
    seams::tuplestore_ateof::set(tuplestore::tuplestore_ateof);
    seams::tuplestore_get_stats::set(tuplestore::tuplestore_get_stats_ref);
    seams::tuplestore_advance::set(|state, forward| tuplestore::tuplestore_advance(state, forward));
    seams::tuplestore_gettupleslot::set(|state, forward, copy, slot, estate| {
        tuplestore::tuplestore_gettupleslot(state, forward, copy, slot, estate)
    });
    seams::tuplestore_gettupleslot_standalone::set(|mcx, state, forward, copy, slot| {
        tuplestore::tuplestore_gettupleslot_standalone(mcx, state, forward, copy, slot)
    });
    seams::tuplestore_puttupleslot::set(|state, slot, estate| {
        tuplestore::tuplestore_puttupleslot(state, slot, estate)
    });
    seams::tuplestore_putvalues::set(|state, tdesc, values, nulls| {
        tuplestore::tuplestore_putvalues(state, tdesc, values, nulls)
    });
    seams::tuplestore_copy_read_pointer::set(|state, srcptr, destptr| {
        tuplestore::tuplestore_copy_read_pointer(state, srcptr, destptr)
    });
    seams::tuplestore_trim::set(|state| {
        // tuplestore_trim is `void` in C; the only fallible step is the
        // pre-free stat update on the (always-INMEM) trim path.
        tuplestore::tuplestore_trim(state).expect("tuplestore_trim failed")
    });
    seams::tuplestore_select_read_pointer::set(|state, ptr| {
        tuplestore::tuplestore_select_read_pointer(state, ptr)
    });
    seams::tuplestore_rescan::set(tuplestore::tuplestore_rescan);
    seams::tuplestore_clear::set(tuplestore::tuplestore_clear);
    seams::tuplestore_end::set(tuplestore::tuplestore_end);
    seams::tuplestore_skiptuples::set(|state, ntuples, forward| {
        tuplestore::tuplestore_skiptuples(state, ntuples, forward)
    });
    seams::tuplestore_in_memory::set(tuplestore::tuplestore_in_memory);
}
