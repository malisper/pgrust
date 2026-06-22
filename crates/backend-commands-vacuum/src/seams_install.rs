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

    // vacuum_is_permitted_for_relation — the ownership/MAINTAIN gate analyze.c
    // shares (it lives in vacuum.c). The scalar adapter takes the two Form fields
    // by value.
    vacuum::vacuum_is_permitted_for_relation::set(
        crate::vacuum_is_permitted_for_relation_scalar,
    );

    // vacuum.c cost-state globals (owned here as thread-locals).
    vacuum::vacuum_failsafe_active::set(crate::vacuum_failsafe_active_impl);
    vacuum::set_vacuum_failsafe_active::set(crate::set_vacuum_failsafe_active_impl);
    vacuum::vacuum_cost_active::set(crate::vacuum_cost_active_impl);
    vacuum::set_vacuum_cost_active::set(crate::set_vacuum_cost_active_impl);
    vacuum::vacuum_cost_balance::set(crate::vacuum_cost_balance_impl);
    vacuum::set_vacuum_cost_balance::set(crate::set_vacuum_cost_balance_impl);
    // vacuum.c working cost params (`vacuum_cost_delay` / `vacuum_cost_limit`) —
    // read by vacuum_delay_point / compute_parallel_delay.
    vacuum::vacuum_cost_delay::set(crate::vacuum_cost_delay_impl);
    vacuum::vacuum_cost_limit::set(crate::vacuum_cost_limit_impl);

    install_autovacuum_ext_cost_seams();

    // vacuum_delay_point / vacuum_rel interrupt + config-reload checks. These
    // vacuum-seams copies are the cross-cutting CHECK_FOR_INTERRUPTS (postgres.c)
    // and the InterruptPending / ConfigReloadPending globals; route them to the
    // real owners. (The cost-active-only delay internals — vacuum_sleep,
    // process_config_file_sighup, etc. — are reached only when VacuumCostActive
    // is true and stay panicking until their owners install them.)
    vacuum::check_for_interrupts::set(|| {
        backend_tcop_postgres_seams::check_for_interrupts::call()
    });
    vacuum::interrupt_pending::set(|| {
        Ok(backend_utils_init_small_seams::interrupt_pending::call())
    });
    vacuum::config_reload_pending::set(|| {
        Ok(backend_postmaster_interrupt::ConfigReloadPending())
    });
    // vacuum_rel: `rel->rd_rel->relkind` vacuumable-kind check, by OID.
    vacuum::rel_relkind::set(|relid| {
        backend_utils_cache_lsyscache_seams::get_rel_relkind::call(relid)
    });
    // `RelationGetRelationName(rel)` (rel.h) — the relation's `relname`. The
    // parallel-vacuum index model carries the index as a bare OID, so the name
    // is resolved through the relcache (`get_rel_name`, lsyscache.c). A live,
    // open relation always has a name; a missing pg_class row is the C cache
    // lookup failure.
    vacuum::relation_get_relation_name::set(|relid| {
        let cx = mcx::MemoryContext::new("vacuum_relation_get_relation_name");
        let name: alloc::string::String = {
            match backend_utils_cache_lsyscache_seams::get_rel_name::call(cx.mcx(), relid)? {
                Some(name) => name.to_string(),
                None => {
                    return Err(types_error::PgError::error(alloc::format!(
                        "cache lookup failed for relation {relid}"
                    )))
                }
            }
        };
        Ok(name)
    });
    // vacuum_rel: `RELATION_IS_OTHER_TEMP(rel)` (rel.h) — `relpersistence ==
    // RELPERSISTENCE_TEMP && !rd_islocaltemp`. The vacuum model carries the
    // opened relation as a bare OID; re-open it with NoLock (the lock is already
    // held) to read the relcache fields the macro needs, then drop the handle.
    vacuum::relation_is_other_temp::set(|relid| {
        use backend_utils_cache_relcache_seams as relcache;
        let cx = mcx::MemoryContext::new("vacuum_relation_is_other_temp");
        let rel = table_seam::table_open::call(cx.mcx(), relid, NoLock)?;
        let other_temp = relcache::rd_rel_relpersistence::call(&rel)?
            == types_tuple::access::RELPERSISTENCE_TEMP as i8
            && !relcache::rd_islocaltemp::call(&rel)?;
        rel.close(NoLock)?;
        Ok(other_temp)
    });
    // `INJECTION_POINT(name)` (injection_point.h) — a no-op in a build without
    // `--enable-injection-points` (`#define INJECTION_POINT(name, arg) ((void)
    // name)`). vacuum.c's index-cleanup / truncate decision points reach it here.
    vacuum::injection_point::set(|_name| Ok(()));
    // vacuum_rel: `SetUserIdAndSecContext(...)` (miscinit.c) — switch to the
    // relation owner's privileges for the vacuum, then restore. (The matching
    // `get_user_id_and_sec_context` is already called through the miscinit seam.)
    vacuum::set_user_id_and_sec_context::set(|userid, sec_context| {
        backend_utils_init_miscinit_seams::set_user_id_and_sec_context::call(userid, sec_context);
        Ok(())
    });
    // vacuum_rel: per-table GUC nesting for the SET-clause / owner-privilege
    // window (`NewGUCNestLevel` + `AtEOXact_GUC`, guc.c). Both are owned by the
    // GUC engine and reached through its seam crate.
    vacuum::new_guc_nest_level::set(|| {
        Ok(backend_utils_misc_guc_seams::new_guc_nest_level::call())
    });
    vacuum::at_eoxact_guc::set(|is_commit, nestlevel| {
        backend_utils_misc_guc_seams::at_eoxact_guc::call(is_commit, nestlevel)
    });
    // vac_open_indexes: `RelationGetIndexList(rel)` by OID (relcache.c).
    vacuum::relation_get_index_list::set(|relid| {
        backend_utils_cache_relcache::derived::RelationGetIndexList(relid)
    });
    // expandTableLikeClause INCLUDING STATISTICS: `RelationGetStatExtList(rel)`
    // by OID (relcache.c).
    vacuum::relation_get_stat_ext_list::set(|relid| {
        backend_utils_cache_relcache::derived::RelationGetStatExtList(relid)
    });
    // vac_open_indexes: `index_open(indexoid, lockmode)` (indexam.c). C's
    // `vac_open_indexes` holds each opened index `Relation *` in `Irel[]` (one
    // relcache pin) under `lockmode`, and `vac_close_indexes` releases that same
    // pin with `index_close(NoLock)` (the `RowExclusiveLock` stays held until
    // commit). The vacuum index model carries the index as a bare OID plus its
    // `indisready` flag, so this seam OPENS the index (taking `lockmode` and a
    // pin), reads `rd_index->indisready`, then immediately CLOSES with `NoLock`
    // to release its own relcache pin while keeping the lock held — the same
    // open-then-recover idiom `relation_is_other_temp` uses. The pin C keeps in
    // `Irel[]` is instead re-acquired by the consumer's `table_open(NoLock)`
    // recover-call (lazy path) and balanced by `vac_close_indexes`, or — for the
    // not-ready / Oid-model paths — re-acquired and released by `index_close`
    // below. Net refcount returns to 0 with the lock held to commit, faithful to
    // vacuum.c (and consistent with the rd_refcnt pin-balance discipline of the
    // vacuum_open_relation fix).
    vacuum::index_open::set(|indexoid, lockmode| {
        use backend_access_index_indexam_seams as indexam;
        let cx = mcx::MemoryContext::new("vacuum_index_open");
        let irel = indexam::index_open::call(cx.mcx(), indexoid, lockmode)?;
        let indisready = irel.rd_index.as_ref().is_some_and(|i| i.indisready);
        // Release this open's relcache pin (rd_refcnt -> back down) but keep the
        // `lockmode` lock: `Relation::close(NoLock)` drops the pin without
        // releasing the lock.
        irel.close(NoLock)?;
        Ok(vacuum::OpenedIndex {
            index: indexoid,
            indisready,
        })
    });
    // vac_close_indexes: `index_close(indrel, lockmode)` (indexam.c). The vacuum
    // index model lost the owned `Relation` handle (the open closed its own pin
    // and kept only the OID + the held lock), so re-open the index by OID with
    // `NoLock` to recover an owned handle — this re-takes a relcache pin but no
    // new lock — then close it under `lockmode` to drop that pin AND release the
    // lock the matching `index_open` took. Net: the index is unpinned and (for
    // a non-`NoLock` lockmode) unlocked, matching C's `index_close(rel,lockmode)`.
    vacuum::index_close::set(|indexoid, lockmode| {
        use backend_access_index_indexam_seams as indexam;
        let cx = mcx::MemoryContext::new("vacuum_index_close");
        let irel = indexam::index_open::call(cx.mcx(), indexoid, NoLock)?;
        // indexam.c `index_close(relation, lockmode)` is just `relation_close`
        // (the relcache pin decrement + conditional lock release); the index-kind
        // validation already happened at index_open above.
        irel.close(lockmode)
    });
    // vacuum_get_cutoffs: the autovacuum freeze-max-age GUCs (autovacuum.c owns
    // the `conf->variable` backing + installs the GucVarAccessors; read the slot).
    vacuum::autovacuum_freeze_max_age::set(|| {
        Ok(backend_utils_misc_guc_tables::vars::autovacuum_freeze_max_age.read())
    });
    vacuum::autovacuum_multixact_freeze_max_age::set(|| {
        Ok(backend_utils_misc_guc_tables::vars::autovacuum_multixact_freeze_max_age.read())
    });
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
    vacuum::clear_parallel_cost_pointers::set(crate::clear_parallel_cost_pointers_impl);
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

    // cluster.c reaches `vacuum_get_cutoffs` through this seam crate for the
    // CLUSTER / VACUUM FULL heap rebuild: `memset(&params, 0,
    // sizeof(VacuumParams)); vacuum_get_cutoffs(OldHeap, &params, &cutoffs)`.
    // The zeroed VacuumParams matches the C memset; the owner returns the
    // bool (whether an aggressive freeze is needed), which the CLUSTER caller
    // discards, so we hand back just the populated cutoffs struct.
    vacuum::vacuum_get_cutoffs::set(|old_heap| {
        use types_vacuum::vacuum::{VacuumCutoffs, VacuumParams};
        let params = VacuumParams::default();
        let mut cutoffs = VacuumCutoffs::default();
        crate::vacuum_get_cutoffs(old_heap.rd_id, params, &mut cutoffs)?;
        Ok(cutoffs)
    });

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
        // return the rel's Oid, or None if the relation has disappeared.
        //
        // C's vacuum_rel keeps the opened `Relation *` (its `rd_refcnt` pin)
        // live for the WHOLE of vacuum_rel and only releases it with the
        // explicit `relation_close(rel, NoLock)` at the end (or an early-exit
        // `relation_close(rel, lmode)`). The Oid-model carries the relation as a
        // bare Oid, but the relcache pin still has to outlive the work: the
        // intermediate `table_open(rel_handle, NoLock)` recover-calls bump and
        // then drop their own pin (vacuum/cluster consume the owned value), so
        // without this held pin the entry's `rd_refcnt` would return to 0 mid
        // vacuum and the final `relation_close` would underflow it to -1.
        //
        // `mem::forget` the owned `Relation` so its `RelationIncrementReference
        // Count` (+1) stays held — the relcache-resource analog of the lock
        // guard's `.keep()`. The matching `relation_close(rel_handle, ...)` in
        // vacuum_rel releases it (rd_refcnt 1 -> 0). The lock is likewise kept.
        let cx = mcx::MemoryContext::new("vacuum_open_relation");
        let opened = relation_seam::try_relation_open::call(cx.mcx(), relid, lmode)?;
        Ok(opened.map(|rel| {
            let oid = rel.rd_id;
            // Hold the relcache pin past this closure (no Drop release): the
            // entry stays pinned until vacuum_rel's explicit relation_close.
            core::mem::forget(rel);
            oid
        }))
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

/// Install the `backend-postmaster-autovacuum-ext-seams` cost-parameter
/// accessors that `VacuumUpdateCosts()` (autovacuum.c) drives. vacuum.c owns the
/// backing state, so the install lives here (autovacuum-ext has no owner crate).
///
///  * `vacuum_cost_delay_guc` / `vacuum_cost_limit_guc` read the
///    `VacuumCostDelay` / `VacuumCostLimit` GUC source globals (globals.c, bound
///    to the GUC slot).
///  * `set_vacuum_cost_delay` / `set_vacuum_cost_limit` and the matching readers
///    write/read the vacuum.c live working params (`vacuum_cost_delay` /
///    `vacuum_cost_limit`).
///  * `vacuum_cost_active` / `set_vacuum_cost_active` / `set_vacuum_cost_balance`
///    / `vacuum_failsafe_active` back onto vacuum.c's cost-state globals (the
///    same thread-locals `vacuum_delay_point` reads).
fn install_autovacuum_ext_cost_seams() {
    use backend_postmaster_autovacuum_ext_seams as avext;
    use backend_utils_misc_guc_tables::vars;

    avext::vacuum_cost_delay_guc::set(|| vars::VacuumCostDelay.read());
    avext::vacuum_cost_limit_guc::set(|| vars::VacuumCostLimit.read());

    avext::set_vacuum_cost_delay::set(crate::set_vacuum_cost_delay_impl);
    avext::set_vacuum_cost_limit::set(crate::set_vacuum_cost_limit_impl);
    avext::vacuum_cost_delay::set(|| {
        crate::vacuum_cost_delay_impl().expect("vacuum_cost_delay is infallible")
    });
    avext::vacuum_cost_limit::set(|| {
        crate::vacuum_cost_limit_impl().expect("vacuum_cost_limit is infallible")
    });

    avext::vacuum_cost_active::set(|| {
        crate::vacuum_cost_active_impl().expect("vacuum_cost_active is infallible")
    });
    avext::set_vacuum_cost_active::set(|v| {
        let _ = crate::set_vacuum_cost_active_impl(v);
    });
    avext::set_vacuum_cost_balance::set(|v| {
        let _ = crate::set_vacuum_cost_balance_impl(v);
    });
    avext::vacuum_failsafe_active::set(|| {
        crate::vacuum_failsafe_active_impl().expect("vacuum_failsafe_active is infallible")
    });
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
/// `Relation`s allocated in the driver run's `mcx`. The `RowExclusiveLock` is
/// taken (and held until commit) by `index_open(RowExclusiveLock)`; the vacuum
/// index model then carries the index as an OID, so `index_open(mcx, oid,
/// NoLock)` recovers the owned index `Relation` value from the relcache without
/// re-locking — the open-then-recover idiom. (`table_open` cannot be used here:
/// it rejects index relkinds, so the recover goes through `indexam::index_open`,
/// which validates the index kind.)
fn vac_open_indexes_rowexcl<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<alloc::vec::Vec<Relation<'mcx>>> {
    use backend_access_index_indexam_seams as indexam;
    let indexoidlist = vacuum::relation_get_index_list::call(rel.rd_id)?;
    let mut irel: alloc::vec::Vec<Relation<'mcx>> =
        alloc::vec::Vec::with_capacity(indexoidlist.len());

    for indexoid in indexoidlist {
        let opened = vacuum::index_open::call(indexoid, RowExclusiveLock)?;
        if opened.indisready {
            irel.push(indexam::index_open::call(mcx, opened.index, NoLock)?);
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
