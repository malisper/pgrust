//! Install every inward seam this crate owns.
//!
//! Two inward roots:
//!  * `backend-commands-vacuum-seams` — the cross-cutting VACUUM helpers other
//!    AMs call (the no-arg `vacuum_delay_point`, the btbulkdelete callback
//!    `vacuum_tid_is_dead`) plus the vacuum.c-owned cost-state globals
//!    (`VacuumFailsafeActive` / `VacuumCostActive` / `VacuumCostBalance` /
//!    `VacuumCostBalanceLocal`, owned here as thread-locals — autovacuum-ext
//!    does NOT declare matching setters).
//!  * `backend-access-heap-vacuumlazy-seams` — the cutoff / relstat / per-index
//!    command-layer entry points the lazy-vacuum driver calls, and the cost
//!    globals it also reads/writes (delegated to the same thread-locals).
//!
//! `analyze_rel` is a SEPARATE leg (declared in `backend-commands-analyze-seams`)
//! and is intentionally NOT installed here — its owner (`analyze.c`) is not yet
//! ported.
//!
//! The parent harness wires this crate into `seams-init`; we only provide the
//! complete `init_seams()` body.

use backend_access_heap_vacuumlazy_seams as vacuumlazy;
use backend_commands_vacuum_seams as vacuum;

pub fn init_seams() {
    // --- backend-commands-vacuum-seams (this unit's public helpers) ---------
    vacuum::vacuum_tid_is_dead::set(crate::vacuum_tid_is_dead_impl);
    vacuum::vacuum_delay_point::set(crate::vacuum_delay_point_noarg);

    // vacuum.c cost-state globals (owned here as thread-locals).
    vacuum::vacuum_failsafe_active::set(crate::vacuum_failsafe_active_impl);
    vacuum::set_vacuum_failsafe_active::set(crate::set_vacuum_failsafe_active_impl);
    vacuum::vacuum_cost_active::set(crate::vacuum_cost_active_impl);
    vacuum::set_vacuum_cost_active::set(crate::set_vacuum_cost_active_impl);
    vacuum::vacuum_cost_balance::set(crate::vacuum_cost_balance_impl);
    vacuum::set_vacuum_cost_balance::set(crate::set_vacuum_cost_balance_impl);
    vacuum::set_vacuum_cost_balance_local::set(crate::set_vacuum_cost_balance_local_impl);
    vacuum::add_vacuum_cost_balance_local::set(crate::add_vacuum_cost_balance_local_impl);

    // --- backend-access-heap-vacuumlazy-seams (lazy-vacuum command layer) ---
    vacuumlazy::vacuum_get_cutoffs::set(crate::vacuum_get_cutoffs);
    vacuumlazy::vacuum_xid_failsafe_check::set(crate::vacuum_xid_failsafe_check);
    vacuumlazy::vac_open_indexes::set(vac_open_indexes_rowexcl);
    vacuumlazy::vac_close_indexes::set(vac_close_indexes_nolock);
    vacuumlazy::vac_update_relstats::set(vac_update_relstats_args);
    vacuumlazy::vac_estimate_reltuples::set(crate::vac_estimate_reltuples);
    vacuumlazy::vac_bulkdel_one_index::set(crate::vac_bulkdel_one_index);
    vacuumlazy::vac_cleanup_one_index::set(crate::vac_cleanup_one_index);
    vacuumlazy::vacuum_delay_point::set(crate::vacuum_delay_point);

    // The cost-state globals the driver also touches (same thread-locals).
    vacuumlazy::vacuum_failsafe_active::set(crate::vacuum_failsafe_active_impl);
    vacuumlazy::set_vacuum_failsafe_active::set(crate::set_vacuum_failsafe_active_impl);
    vacuumlazy::set_vacuum_cost_active::set(crate::set_vacuum_cost_active_impl);
    vacuumlazy::set_vacuum_cost_balance::set(crate::set_vacuum_cost_balance_impl);
}

// --- signature adapters: the vacuumlazy-seams shapes differ slightly from the
//     vacuum.c function signatures (lock mode pre-bound; out-params returned). ---

use types_core::primitive::Oid;
use types_error::PgResult;
use types_storage::lock::{NoLock, RowExclusiveLock};
use types_vacuum::vacuumlazy::UpdateRelStatsArgs;

/// `vac_open_indexes(rel, RowExclusiveLock, &nindexes, &indrels)`.
fn vac_open_indexes_rowexcl(rel: Oid) -> PgResult<alloc::vec::Vec<Oid>> {
    crate::vac_open_indexes(rel, RowExclusiveLock)
}

/// `vac_close_indexes(nindexes, indrels, NoLock)`.
fn vac_close_indexes_nolock(indrels: alloc::vec::Vec<Oid>) -> PgResult<()> {
    crate::vac_close_indexes(&indrels, NoLock)
}

/// `vac_update_relstats(...)` driven from the packed `UpdateRelStatsArgs`,
/// returning `(frozenxid_updated, minmulti_updated)`.
fn vac_update_relstats_args(args: UpdateRelStatsArgs) -> PgResult<(bool, bool)> {
    crate::vac_update_relstats(
        args.relation,
        args.num_pages,
        args.num_tuples,
        args.num_all_visible_pages,
        args.num_all_frozen_pages,
        args.hasindex,
        args.frozenxid,
        args.minmulti,
        args.in_outer_xact,
    )
}

extern crate alloc;
