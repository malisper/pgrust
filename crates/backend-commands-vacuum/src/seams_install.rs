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
use backend_access_common_relation_seams as relation_seam;
use backend_storage_lmgr_lmgr_seams as lmgr_seam;

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

    // VacuumSharedCostBalance / VacuumActiveNWorkers — the parallel-vacuum
    // DSM-shared cost-state pointers. vacuum.c owns these globals; the leader
    // and worker (vacuumparallel.c) install the shared atomics handle through
    // the enable seams, and both vacuum.c's compute_parallel_delay and the
    // worker setup atomic-mutate the shared cell. All read/written here against
    // the one VACUUM_SHARED_COST_STATE thread-local handle.
    vacuum::set_vacuum_shared_cost_balance_enable::set(
        crate::set_vacuum_shared_cost_balance_enable_impl,
    );
    vacuum::set_vacuum_active_nworkers_enable::set(crate::set_vacuum_active_nworkers_enable_impl);
    vacuum::vacuum_shared_cost_balance_is_set::set(crate::vacuum_shared_cost_balance_is_set_impl);
    vacuum::vacuum_active_nworkers_is_set::set(crate::vacuum_active_nworkers_is_set_impl);
    vacuum::vacuum_active_nworkers_add::set(crate::vacuum_active_nworkers_add_impl);
    vacuum::vacuum_active_nworkers_sub::set(crate::vacuum_active_nworkers_sub_impl);
    vacuum::read_vacuum_active_nworkers::set(crate::read_vacuum_active_nworkers_impl);
    vacuum::vacuum_shared_cost_balance_read::set(crate::vacuum_shared_cost_balance_read_impl);
    vacuum::shared_cost_balance_add_fetch::set(crate::shared_cost_balance_add_fetch_impl);
    vacuum::shared_cost_balance_sub_fetch::set(crate::shared_cost_balance_sub_fetch_impl);

    // vacuumparallel.c's index-vacuum bridges. The parallel coordinator (leader
    // and re-opened-by-OID workers) drives each index through these same vacuum.c
    // wrappers; they pass an explicit lock mode (workers use RowExclusiveLock).
    // The Oid-keyed `vac_open_indexes`/`vac_close_indexes` are the established
    // vacuum index model (index_open returns the index Oid handle), so the
    // lockmode-parameterized seams delegate straight to the real functions.
    vacuum::vac_open_indexes_lock::set(crate::vac_open_indexes);
    vacuum::vac_close_indexes_lock::set(vac_close_indexes_lock_impl);
    vacuum::vac_bulkdel_one_index::set(crate::vac_bulkdel_one_index);
    vacuum::vac_cleanup_one_index::set(crate::vac_cleanup_one_index);

    // --- backend-access-heap-vacuumlazy-seams (lazy-vacuum command layer) ---
    vacuumlazy::vacuum_get_cutoffs::set(|rel, params, cutoffs| {
        crate::vacuum_get_cutoffs(rel.rd_id, params, cutoffs)
    });
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

    // --- catalog SCAN + inplace-WRITE seams (vacuum.c's own pg_class /
    //     pg_database seqscans + systable_inplace_update writers) ---------------
    crate::catalog_scan::install();

    // --- access/table.h + storage/lmgr.h relation lock/open seams (vacuum.c's
    //     own vacuum_open_relation / vac_open_indexes session-lock calls) -------
    // These delegate straight to their real owners (relation.c / lmgr.c). The
    // vacuum model carries the opened relation as a bare Oid: the lock is taken
    // and held until commit (the owner's `.keep()`), and the relation is
    // reopened by Oid later — vacuum.c's open-then-recover idiom.
    vacuum::try_relation_open::set(|relid, lmode| {
        // try_relation_open(relid, lmode): take the lock (kept until commit) and
        // return the rel's Oid, or None if the relation has disappeared. The
        // owned Relation drops here (relcache ref released); the lock is held.
        let cx = mcx::MemoryContext::new("vacuum_open_relation");
        let opened = relation_seam::try_relation_open::call(cx.mcx(), relid, lmode)?;
        Ok(opened.map(|rel| rel.rd_id))
    });
    vacuum::conditional_lock_relation_oid::set(|relid, lmode| {
        // ConditionalLockRelationOid(relid, lmode) -> bool. The owner returns a
        // RAII guard; on success keep the lock (transaction-scoped) and report
        // true, else false (C's `LOCKACQUIRE_NOT_AVAIL`).
        match lmgr_seam::conditional_lock_relation_oid::call(relid, lmode)? {
            Some(guard) => {
                guard.keep();
                Ok(true)
            }
            None => Ok(false),
        }
    });
    vacuum::unlock_relation_oid::set(|relid, lmode| {
        lmgr_seam::unlock_relation_oid::call(relid, lmode)
    });
    vacuum::lock_relation_id_for_session::set(|relid, lmode| {
        lmgr_seam::lock_relation_id_for_session::call(relid, lmode)
    });
    vacuum::unlock_relation_id_for_session::set(|relid, lmode| {
        lmgr_seam::unlock_relation_id_for_session::call(relid, lmode)
    });

    // --- ProcessUtility dispatch arm (utility.c VacuumStmt → ExecVacuum) ------
    backend_tcop_utility_out_seams::exec_vacuum::set(exec_vacuum_arm);

    // --- vacuum.c GUC `conf->variable` accessors + seam getters --------------
    // vacuum.c owns these plain int/bool/double GUC globals (guc_tables.c reads
    // them straight from the GUC slot; none come from ControlFile). Install the
    // GucVarAccessors over our own backing store, then install the vacuum-seams
    // getters that read the slot via `vars::<name>.read()`.
    {
        use backend_utils_misc_guc_tables::{hooks, vars, GucVarAccessors};
        use crate::guc_globals as g;

        // vacuum.c's `vacuum_buffer_usage_limit` GUC carries a check_hook
        // (guc_tables.c registers `check_vacuum_buffer_usage_limit`). The GUC
        // machinery fires it when the boot value (2048) is applied during
        // InitializeGUCOptions, so the slot must be installed by vacuum's owner
        // here. The body lives in `crate::check_vacuum_buffer_usage_limit`; on
        // rejection it reports the detail through the GUC check-error channel
        // (C's `GUC_check_errdetail`) and returns `false`.
        hooks::check_vacuum_buffer_usage_limit.install(|newval, _extra, _source| {
            let (ok, detail) = crate::check_vacuum_buffer_usage_limit(*newval);
            if let Some(detail) = detail {
                backend_utils_misc_guc_seams::guc_check_errdetail::call(detail);
            }
            Ok(ok)
        });

        vars::vacuum_freeze_min_age.install(GucVarAccessors {
            get: g::vacuum_freeze_min_age,
            set: g::set_vacuum_freeze_min_age,
        });
        vars::vacuum_freeze_table_age.install(GucVarAccessors {
            get: g::vacuum_freeze_table_age,
            set: g::set_vacuum_freeze_table_age,
        });
        vars::vacuum_multixact_freeze_min_age.install(GucVarAccessors {
            get: g::vacuum_multixact_freeze_min_age,
            set: g::set_vacuum_multixact_freeze_min_age,
        });
        vars::vacuum_multixact_freeze_table_age.install(GucVarAccessors {
            get: g::vacuum_multixact_freeze_table_age,
            set: g::set_vacuum_multixact_freeze_table_age,
        });
        vars::vacuum_failsafe_age.install(GucVarAccessors {
            get: g::vacuum_failsafe_age,
            set: g::set_vacuum_failsafe_age,
        });
        vars::vacuum_multixact_failsafe_age.install(GucVarAccessors {
            get: g::vacuum_multixact_failsafe_age,
            set: g::set_vacuum_multixact_failsafe_age,
        });
        vars::vacuum_max_eager_freeze_failure_rate.install(GucVarAccessors {
            get: g::vacuum_max_eager_freeze_failure_rate,
            set: g::set_vacuum_max_eager_freeze_failure_rate,
        });
        vars::track_cost_delay_timing.install(GucVarAccessors {
            get: g::track_cost_delay_timing,
            set: g::set_track_cost_delay_timing,
        });
        vars::vacuum_truncate.install(GucVarAccessors {
            get: g::vacuum_truncate,
            set: g::set_vacuum_truncate,
        });

        // The vacuum-seams getters read the now-installed GUC slots.
        vacuum::vacuum_freeze_min_age::set(|| Ok(vars::vacuum_freeze_min_age.read()));
        vacuum::vacuum_freeze_table_age::set(|| Ok(vars::vacuum_freeze_table_age.read()));
        vacuum::vacuum_multixact_freeze_min_age::set(|| {
            Ok(vars::vacuum_multixact_freeze_min_age.read())
        });
        vacuum::vacuum_multixact_freeze_table_age::set(|| {
            Ok(vars::vacuum_multixact_freeze_table_age.read())
        });
        vacuum::vacuum_failsafe_age::set(|| Ok(vars::vacuum_failsafe_age.read()));
        vacuum::vacuum_multixact_failsafe_age::set(|| {
            Ok(vars::vacuum_multixact_failsafe_age.read())
        });
        vacuum::vacuum_max_eager_freeze_failure_rate::set(|| {
            Ok(vars::vacuum_max_eager_freeze_failure_rate.read())
        });
        vacuum::vacuum_buffer_usage_limit::set(|| Ok(vars::VacuumBufferUsageLimit.read()));
        // AmAutoVacuumWorkerProcess() (miscadmin.h): MyBackendType == B_AUTOVAC_WORKER.
        vacuum::am_autovacuum_worker_process::set(|| {
            Ok(backend_utils_init_miscinit_seams::my_backend_type::call()
                == types_core::init::BackendType::AutovacWorker)
        });
        vacuum::track_cost_delay_timing::set(|| Ok(vars::track_cost_delay_timing.read()));
        vacuum::vacuum_truncate::set(|| Ok(vars::vacuum_truncate.read()));

        // vacuumparallel.c GUC reads. `maintenance_work_mem` (read by both the
        // leader sizing and the worker) and `max_parallel_maintenance_workers`
        // are not vacuum.c's own slots — they are owned by other GUC owners —
        // but the slot read/write API resolves once that owner installs the
        // accessor, so the seam getters/setter read/write the shared slot.
        vacuum::max_parallel_maintenance_workers::set(|| {
            Ok(vars::max_parallel_maintenance_workers.read())
        });
        vacuum::pv_maintenance_work_mem::set(|| Ok(vars::maintenance_work_mem.read()));
        vacuum::set_pv_maintenance_work_mem::set(|v| {
            vars::maintenance_work_mem.write(v);
            Ok(())
        });
    }
}

use mcx::Mcx;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::ParseState;

/// `case T_VacuumStmt: ExecVacuum(pstate, stmt, isTopLevel)` (utility.c). The
/// dispatch carries the parse tree as `&Node`; extract the `VacuumStmt` variant
/// and forward to the real entry point.
fn exec_vacuum_arm<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &Node<'mcx>,
    is_top_level: bool,
) -> PgResult<()> {
    let Some(vacstmt) = stmt.as_vacuumstmt() else {
        panic!("exec_vacuum: parse tree is not a VacuumStmt");
    };
    crate::ExecVacuum(pstate, vacstmt, is_top_level, mcx)
}

// --- signature adapters: the vacuumlazy-seams shapes differ slightly from the
//     vacuum.c function signatures (lock mode pre-bound; out-params returned). ---

use types_core::primitive::Oid;
use types_error::PgResult;
use types_rel::Relation;
use types_storage::lock::{NoLock, RowExclusiveLock};
use types_vacuum::vacuumlazy::UpdateRelStatsArgs;
use backend_access_table_table_seams as table_seam;

/// `vac_open_indexes(rel, RowExclusiveLock, &nindexes, &indrels)` — open all the
/// vacuumable (indisready) indexes of `rel` and return the live, owned index
/// `Relation`s allocated in the driver run's `mcx`. The lock is taken by
/// `index_open(RowExclusiveLock)`; `table_open(mcx, oid, NoLock)` then recovers
/// the owned `Relation` value from the relcache without re-locking (the same
/// open-then-recover idiom the heap path uses).
fn vac_open_indexes_rowexcl<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<alloc::vec::Vec<Relation<'mcx>>> {
    let indexoidlist = vacuum::relation_get_index_list::call(rel.rd_id)?;
    let mut irel: alloc::vec::Vec<Relation<'mcx>> =
        alloc::vec::Vec::with_capacity(indexoidlist.len());

    for indexoid in indexoidlist {
        let opened = vacuum::index_open::call(indexoid, RowExclusiveLock)?;
        if opened.indisready {
            irel.push(table_seam::table_open::call(mcx, opened.index, NoLock)?);
        } else {
            vacuum::index_close::call(opened.index, RowExclusiveLock)?;
        }
    }

    Ok(irel)
}

/// `vac_close_indexes(nindexes, indrels, NoLock)` — release the owned index
/// `Relation`s (each `Relation::close(NoLock)` drops the relcache reference; the
/// `RowExclusiveLock` taken by `vac_open_indexes` is held until commit).
fn vac_close_indexes_nolock<'mcx>(indrels: alloc::vec::Vec<Relation<'mcx>>) -> PgResult<()> {
    for r in indrels {
        r.close(NoLock)?;
    }
    Ok(())
}

/// `vac_close_indexes(nindexes, indrels, lockmode)` — the vacuumparallel worker
/// releases its re-opened indexes with the same lock mode it opened them under.
fn vac_close_indexes_lock_impl(
    indrels: alloc::vec::Vec<Oid>,
    lockmode: types_storage::lock::LOCKMODE,
) -> PgResult<()> {
    crate::vac_close_indexes(&indrels, lockmode)
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
