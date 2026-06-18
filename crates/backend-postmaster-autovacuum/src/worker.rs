//! AUTOVACUUM WORKER CODE (`autovacuum.c` lines 1368-1645, 1878-2599): the
//! worker main loop, the `WorkerInfo` lifecycle (`FreeWorkerInfo`), and the
//! top-level `do_autovacuum()` table-selection driver.

extern crate alloc;
use alloc::vec::Vec;

use backend_utils_error::{elog, PgResult};
use types_error::{LOG, WARNING};

use types_core::{InvalidOid, Oid};
use types_reloptions::AutoVacOpts;
use types_vacuum::vacuum::VACOPT_VACUUM;

use crate::core::{
    self, AutovacTable, AvRelation, OidIsValid, AutoVacRebalance, RELKIND_MATVIEW,
    RELKIND_RELATION, RELPERSISTENCE_TEMP, NUM_WORKITEMS,
};
use crate::launcher::do_start_worker;
use crate::schedule::{perform_work_item, relation_needs_vacanalyze, table_recheck_autovac};
use backend_postmaster_autovacuum_ext_seams as seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;

/// `void AutoVacWorkerMain(const void *startup_data, size_t startup_data_len)`
/// (`autovacuum.c` lines 1375-1600).
///
/// Main entry point for autovacuum worker processes. The per-backend lifecycle
/// setup is out-of-crate; the ported body claims this worker's `WorkerInfo`
/// slot from the `av_startingWorker` pointer, connects to the chosen database,
/// and runs `do_autovacuum`.
pub fn AutoVacWorkerMain() -> PgResult<()> {
    /* Get the info about the database we're going to work on. */
    seam::autovacuum_lock_acquire_exclusive::call()?;

    /*
     * beware of startingWorker being INVALID; this should normally not happen,
     * but if a worker fails after forking and before this, the launcher might
     * have decided to remove it from the queue and start again.
     */
    let starting = seam::starting_worker_slot::call();
    let dbid;
    if starting >= 0 {
        core::set_MyWorkerInfo(starting);
        dbid = seam::worker_get_dboid::call(starting);
        seam::worker_set_proc::call(starting, true);

        /* insert into the running list */
        seam::running_workers_push_head::call(starting);

        /*
         * remove from the "starting" pointer, so that the launcher can start a
         * new worker if required
         */
        seam::set_starting_worker_slot::call(-1);
        seam::autovacuum_lock_release::call()?;

        seam::register_free_worker_info::call();

        /* wake up the launcher */
        let launcherpid = seam::get_launcher_pid::call();
        if launcherpid != 0 {
            seam::kill_launcher_sigusr2::call(launcherpid);
        }
    } else {
        /* no worker entry for me, go away */
        elog(WARNING, "autovacuum worker started without a worker entry").ok();
        dbid = InvalidOid;
        seam::autovacuum_lock_release::call()?;
    }

    if OidIsValid(dbid) {
        /*
         * Report autovac startup to the cumulative stats system, before
         * InitPostgres, so the last_autovac_time gets updated even if the
         * connection attempt fails.
         */
        seam::pgstat_report_autovac::call(dbid);

        /*
         * Connect to the selected database (driven by the runtime's
         * InitPostgres), then do an appropriate amount of work.
         */
        core::set_recentXid(seam::read_next_transaction_id::call());
        core::set_recentMulti(seam::read_next_multixact_id::call());
        do_autovacuum()?;
    }

    /* All done, go away */
    seam::proc_exit::call(0);
}

/// The emergency-mode `do_start_worker()` call the launcher makes when
/// `!AutoVacuumingActive()` (`autovacuum.c` line 557).
pub fn do_autovacuum_emergency_start() -> PgResult<()> {
    do_start_worker()?;
    Ok(())
}

/// `static void FreeWorkerInfo(int code, Datum arg)` (`autovacuum.c` lines
/// 1605-1645).
///
/// Return a `WorkerInfo` to the free list (an `on_shmem_exit` callback).
pub fn FreeWorkerInfo() -> PgResult<()> {
    let my = core::MyWorkerInfo();
    if my >= 0 {
        seam::autovacuum_lock_acquire_exclusive::call()?;

        /*
         * Wake the launcher up so that he can launch a new worker immediately
         * if required.  We only save the launcher's PID in local memory here;
         * the actual signal will be sent when the PGPROC is recycled.
         */
        core::set_AutovacuumLauncherPid(seam::get_launcher_pid::call());

        seam::worker_links_delete::call(my);
        seam::worker_set_dboid::call(my, InvalidOid);
        seam::worker_set_tableoid::call(my, InvalidOid);
        seam::worker_set_sharedrel::call(my, false);
        seam::worker_set_proc::call(my, false);
        seam::worker_set_launchtime::call(my, 0);
        seam::worker_dobalance_clear::call(my);
        seam::free_workers_push_head::call(my);
        /* not mine anymore */
        core::set_MyWorkerInfo(-1);

        /*
         * now that we're inactive, cause a rebalancing of the surviving workers
         */
        seam::set_av_signal::call(AutoVacRebalance, true);
        seam::autovacuum_lock_release::call()?;
    }
    Ok(())
}

/// `static void do_autovacuum(void)` (`autovacuum.c` lines 1884-2599).
///
/// Process a database table-by-table. The two-pass pg_class walk is performed
/// by the catalog seam (which returns the main + toast rows already with
/// reloptions extracted); the in-crate body runs the
/// `relation_needs_vacanalyze` decision, builds the `table_oids` /
/// `orphan_oids` / `table_toast_map`, then the per-table
/// claim/skip/recheck/vacuum loop, and finally drains the work-item array.
pub fn do_autovacuum() -> PgResult<()> {
    /*
     * StartTransactionCommand and CommitTransactionCommand will automatically
     * switch to other contexts.  We keep AutovacMemCxt to hold the list of
     * relations to vacuum/analyze across transactions.
     */
    seam::autovac_mem_cxt_create_and_switch::call(alloc::string::String::from("Autovacuum worker"));

    /* Start a transaction so our commands have one to play into. */
    seam::start_transaction_command::call()?;

    /* Compute the multixact age for which freezing is urgent. */
    let effective_multixact_freeze_max_age = seam::multixact_member_freeze_threshold::call();

    /*
     * Find the pg_database entry and select the default freeze ages.  We use
     * zero in template and nonconnectable databases, else the system-wide
     * default.
     */
    if seam::database_uses_zero_freeze_ages::call()? {
        core::set_default_freeze_min_age(0);
        core::set_default_freeze_table_age(0);
        core::set_default_multixact_freeze_min_age(0);
        core::set_default_multixact_freeze_table_age(0);
    } else {
        core::set_default_freeze_min_age(seam::vacuum_freeze_min_age::call());
        core::set_default_freeze_table_age(seam::vacuum_freeze_table_age::call());
        core::set_default_multixact_freeze_min_age(seam::vacuum_multixact_freeze_min_age::call());
        core::set_default_multixact_freeze_table_age(seam::vacuum_multixact_freeze_table_age::call());
    }

    /* StartTransactionCommand changed elsewhere */
    seam::switch_to_autovac_mem_cxt::call();

    /*
     * Scan pg_class to determine which tables to vacuum (two passes, main then
     * toast). The seqscan + reloption extraction are out-of-crate.
     */
    let (main_rows, toast_rows) = seam::do_autovacuum_scan_pg_class::call()?;

    let mut table_oids: Vec<Oid> = Vec::new();
    let mut orphan_oids: Vec<Oid> = Vec::new();
    let mut table_toast_map: Vec<AvRelation> = Vec::new();

    /* First pass: main tables + toast→main mapping. */
    for class_form in &main_rows {
        if class_form.relkind != RELKIND_RELATION && class_form.relkind != RELKIND_MATVIEW {
            continue;
        }

        let relid = class_form.oid;

        /* Check if it is a temp table (presumably, of some other backend's). */
        if class_form.relpersistence == RELPERSISTENCE_TEMP {
            if seam::temp_namespace_is_idle::call(class_form.relnamespace)? {
                /* The table seems to be orphaned -- remember it. */
                orphan_oids.push(relid);
            }
            continue;
        }

        /* Fetch reloptions and the pgstat entry for this table */
        let relopts = core::extract_autovac_opts(class_form.relkind, class_form.relopts);
        let tabentry = seam::pgstat_fetch_stat_tabentry::call(class_form.relisshared, relid);

        /* Check if it needs vacuum or analyze */
        let (dovacuum, doanalyze, _wraparound) = relation_needs_vacanalyze(
            relid,
            relopts.as_ref(),
            class_form.relkind,
            class_form.relfrozenxid,
            class_form.relminmxid,
            class_form.reltuples,
            class_form.relpages,
            class_form.relallfrozen,
            &class_form.relname,
            tabentry,
            effective_multixact_freeze_max_age,
        );

        /* Relations that need work are added to table_oids */
        if dovacuum || doanalyze {
            table_oids.push(relid);
        }

        /* Remember TOAST associations for the second pass. */
        if OidIsValid(class_form.reltoastrelid)
            && !table_toast_map
                .iter()
                .any(|h| h.ar_toastrelid == class_form.reltoastrelid)
        {
            let mut hentry = AvRelation {
                ar_toastrelid: class_form.reltoastrelid,
                ar_relid: relid,
                ar_hasrelopts: false,
                ar_reloptions: AutoVacOpts::default(),
            };
            if let Some(ro) = relopts {
                hentry.ar_hasrelopts = true;
                hentry.ar_reloptions = ro;
            }
            table_toast_map.push(hentry);
        }
    }

    /* second pass: check TOAST tables */
    for class_form in &toast_rows {
        /* We cannot safely process other backends' temp tables, so skip 'em. */
        if class_form.relpersistence == RELPERSISTENCE_TEMP {
            continue;
        }

        let relid = class_form.oid;

        /*
         * fetch reloptions -- if this toast table does not have them, try the
         * main rel
         */
        let mut relopts = core::extract_autovac_opts(class_form.relkind, class_form.relopts);
        if relopts.is_none() {
            if let Some(hentry) = table_toast_map.iter().find(|h| h.ar_toastrelid == relid) {
                if hentry.ar_hasrelopts {
                    relopts = Some(hentry.ar_reloptions);
                }
            }
        }

        /* Fetch the pgstat entry for this table */
        let tabentry = seam::pgstat_fetch_stat_tabentry::call(class_form.relisshared, relid);

        let (dovacuum, _doanalyze, _wraparound) = relation_needs_vacanalyze(
            relid,
            relopts.as_ref(),
            class_form.relkind,
            class_form.relfrozenxid,
            class_form.relminmxid,
            class_form.reltuples,
            class_form.relpages,
            class_form.relallfrozen,
            &class_form.relname,
            tabentry,
            effective_multixact_freeze_max_age,
        );

        /* ignore analyze for toast tables */
        if dovacuum {
            table_oids.push(relid);
        }
    }

    /*
     * Recheck orphan temporary tables, and if they still seem orphaned, drop
     * them.  We'll eat a transaction per dropped table.
     */
    let my_database_id = seam::my_database_id::call();
    for relid in orphan_oids {
        /* Check for user-requested abort. */
        seam::check_for_interrupts::call()?;

        /*
         * Try to lock the table.  If we can't get the lock immediately,
         * somebody else is using (or dropping) the table, so it's not our
         * concern anymore.  Having the lock prevents race conditions below.
         */
        if !seam::conditional_lock_relation_oid_exclusive::call(relid) {
            continue;
        }

        /*
         * Re-fetch the pg_class tuple and re-check whether it still seems to be
         * an orphaned temp table.  If it's not there or no longer the same
         * relation, ignore it.
         */
        let class_form = match seam::orphan_recheck_fetch_class_row::call(relid) {
            Some(c) => c,
            None => {
                /* be sure to drop useless lock so we don't bloat lock table */
                seam::unlock_relation_oid_exclusive::call(relid);
                continue;
            }
        };

        /*
         * Make all the same tests made in the loop above.  In event of OID
         * counter wraparound, the pg_class entry we have now might be
         * completely unrelated to the one we saw before.
         */
        if !((class_form.relkind == RELKIND_RELATION || class_form.relkind == RELKIND_MATVIEW)
            && class_form.relpersistence == RELPERSISTENCE_TEMP)
        {
            seam::unlock_relation_oid_exclusive::call(relid);
            continue;
        }

        if !seam::temp_namespace_is_idle::call(class_form.relnamespace)? {
            seam::unlock_relation_oid_exclusive::call(relid);
            continue;
        }

        /*
         * Try to lock the temp namespace, too.  Even though we have lock on the
         * table itself, there's a risk of deadlock against an incoming backend
         * trying to clean out the temp namespace.  If we can get
         * AccessShareLock on the namespace, that's sufficient to ensure we're
         * not running concurrently with RemoveTempRelations.  If we can't, back
         * off and let RemoveTempRelations do its thing.
         */
        if !seam::conditional_lock_namespace_object_share::call(class_form.relnamespace) {
            seam::unlock_relation_oid_exclusive::call(relid);
            continue;
        }

        /* OK, let's delete it */
        elog(
            LOG,
            alloc::format!(
                "autovacuum: dropping orphan temp table \"{}.{}.{}\"",
                seam::get_database_name::call(my_database_id).unwrap_or_default(),
                seam::get_namespace_name::call(class_form.relnamespace).unwrap_or_default(),
                class_form.relname
            ),
        )
        .ok();

        /*
         * Deletion might involve TOAST table access, so ensure we have a valid
         * snapshot.  The performDeletion (DROP_CASCADE | INTERNAL | QUIETLY |
         * SKIP_EXTENSIONS) and the per-table commit/start are the foreign
         * leaves.
         */
        seam::push_active_snapshot::call()?;
        seam::perform_deletion_orphan_temp_table::call(relid)?;

        /*
         * To commit the deletion, end current transaction and start a new one.
         * Note this also releases the locks we took.
         */
        snapmgr_seam::pop_active_snapshot::call()?;
        seam::commit_transaction_command::call()?;
        seam::start_transaction_command::call()?;

        /* StartTransactionCommand changed current memory context */
        seam::switch_to_autovac_mem_cxt::call();
    }

    /*
     * Create a buffer access strategy object for VACUUM to use, and a fake
     * PortalContext so the contexts created in the vacuum code are cleaned up
     * for each table.
     */
    let bstrategy = seam::get_vacuum_access_strategy::call();
    seam::portal_context_create::call();

    let mut did_vacuum = false;
    let mut found_concurrent_worker = false;

    /* Perform operations on collected tables. */
    for relid in &table_oids {
        let relid = *relid;
        seam::check_for_interrupts::call()?;

        /* Check for config changes before processing each collected table. */
        if seam::config_reload_pending::call() {
            seam::set_config_reload_pending::call(false);
            seam::process_config_file::call()?;
            /*
             * You might be tempted to bail out if we see autovacuum is now
             * disabled.  Must resist that temptation.
             */
        }

        /* Find out whether the table is shared or not. */
        let isshared = match seam::syscache_rel_isshared::call(relid) {
            Some(s) => s,
            None => continue, /* somebody deleted the rel, forget it */
        };

        /* Hold schedule lock from here until we've claimed the table. */
        seam::autovacuum_schedule_lock_acquire_exclusive::call()?;
        seam::autovacuum_lock_acquire_shared::call()?;

        /*
         * Check whether the table is being vacuumed concurrently by another
         * worker.
         */
        let mut skipit = false;
        let my = core::MyWorkerInfo();
        let my_database_id = seam::my_database_id::call();
        for worker in seam::running_workers_slots::call() {
            /* ignore myself */
            if worker == my {
                continue;
            }

            /* ignore workers in other databases (unless table is shared) */
            if !seam::worker_get_sharedrel::call(worker)
                && seam::worker_get_dboid::call(worker) != my_database_id
            {
                continue;
            }

            if seam::worker_get_tableoid::call(worker) == relid {
                skipit = true;
                found_concurrent_worker = true;
                break;
            }
        }
        seam::autovacuum_lock_release::call()?;
        if skipit {
            seam::autovacuum_schedule_lock_release::call()?;
            continue;
        }

        /*
         * Store the table's OID in shared memory before releasing the schedule
         * lock, so that other workers don't try to vacuum it concurrently.
         */
        seam::worker_set_tableoid::call(my, relid);
        seam::worker_set_sharedrel::call(my, isshared);
        seam::autovacuum_schedule_lock_release::call()?;

        /* Check whether pgstat data still says we need to vacuum this table. */
        seam::switch_to_autovac_mem_cxt::call();
        let mut tab: AutovacTable = match table_recheck_autovac(
            relid,
            &table_toast_map,
            effective_multixact_freeze_max_age,
        )? {
            Some(t) => t,
            None => {
                /* someone else vacuumed the table, or it went away */
                seam::autovacuum_schedule_lock_acquire_exclusive::call()?;
                seam::worker_set_tableoid::call(my, InvalidOid);
                seam::worker_set_sharedrel::call(my, false);
                seam::autovacuum_schedule_lock_release::call()?;
                continue;
            }
        };

        /* Save the cost-related storage parameter values in global variables. */
        core::set_av_storage_param_cost_delay(tab.at_storage_param_vac_cost_delay);
        core::set_av_storage_param_cost_limit(tab.at_storage_param_vac_cost_limit);

        /* We only expect this worker to ever set the flag. */
        if tab.at_dobalance {
            seam::worker_dobalance_test_set::call(my);
        } else {
            seam::worker_dobalance_clear::call(my);
        }

        seam::autovacuum_lock_acquire_shared::call()?;
        crate::cost_balance::autovac_recalculate_workers_for_balance()?;
        seam::autovacuum_lock_release::call()?;

        /* We wait until this point to update cost delay and cost limit values. */
        crate::cost_balance::VacuumUpdateCosts()?;

        /* clean up memory before each iteration */
        seam::portal_context_reset::call();

        /*
         * Save the relation name for a possible error message.  If any of these
         * return NULL, then the relation has been dropped since last we
         * checked; skip it.
         */
        tab.at_relname = seam::get_rel_name::call(tab.at_relid);
        tab.at_nspname = seam::get_rel_namespace_name::call(tab.at_relid);
        tab.at_datname = seam::get_database_name::call(my_database_id);

        if tab.at_relname.is_some() && tab.at_nspname.is_some() && tab.at_datname.is_some() {
            /*
             * We will abort vacuuming the current table if something errors
             * out, and continue with the next one in schedule; in particular,
             * this happens if we are interrupted with SIGINT.
             */
            match seam::autovacuum_do_vac_analyze::call(
                tab.at_relid,
                tab.at_nspname.clone().unwrap(),
                tab.at_relname.clone().unwrap(),
                tab.at_params,
                bstrategy.clone(),
            ) {
                Ok(()) => {
                    /*
                     * Clear a possible query-cancel signal, to avoid a late
                     * reaction to an automatically-sent signal because of
                     * vacuuming the current table (we're done with it, so it
                     * would make no sense to cancel at this point.)
                     */
                    seam::set_query_cancel_pending::call(false);
                }
                Err(mut err) => {
                    /*
                     * Abort the transaction, start a new one, and proceed with
                     * the next table in our list.
                     *
                     * The HOLD_INTERRUPTS/EmitErrorReport/AbortOutOfAnyTransaction/
                     * FlushErrorState/MemoryContextReset(PortalContext)/
                     * StartTransactionCommand/RESUME_INTERRUPTS sequence is the
                     * foreign PG_CATCH body; here we adorn the in-flight error
                     * with autovacuum's own errcontext line first.
                     */
                    if tab.at_params.options & VACOPT_VACUUM != 0 {
                        err.add_context_line(alloc::format!(
                            "automatic vacuum of table \"{}.{}.{}\"",
                            tab.at_datname.as_deref().unwrap(),
                            tab.at_nspname.as_deref().unwrap(),
                            tab.at_relname.as_deref().unwrap()
                        ));
                    } else {
                        err.add_context_line(alloc::format!(
                            "automatic analyze of table \"{}.{}.{}\"",
                            tab.at_datname.as_deref().unwrap(),
                            tab.at_nspname.as_deref().unwrap(),
                            tab.at_relname.as_deref().unwrap()
                        ));
                    }
                    seam::emit_report_and_restart_after_table_error::call(err);
                }
            }

            /* Make sure we're back in AutovacMemCxt */
            seam::switch_to_autovac_mem_cxt::call();

            did_vacuum = true;
        }

        /* be tidy: 'deleted' label — tab is dropped at scope end */

        /*
         * Remove my info from shared memory.  We set wi_dobalance on the
         * assumption that we are more likely than not to vacuum a table with no
         * cost-related storage parameters next.
         */
        seam::autovacuum_schedule_lock_acquire_exclusive::call()?;
        seam::worker_set_tableoid::call(my, InvalidOid);
        seam::worker_set_sharedrel::call(my, false);
        seam::autovacuum_schedule_lock_release::call()?;
        seam::worker_dobalance_test_set::call(my);
    }

    /* Perform additional work items, as requested by backends. */
    let my_database_id = seam::my_database_id::call();
    seam::autovacuum_lock_acquire_exclusive::call()?;
    for i in 0..NUM_WORKITEMS {
        if !seam::workitem_get_used::call(i) {
            continue;
        }
        if seam::workitem_get_active::call(i) {
            continue;
        }
        if seam::workitem_get_database::call(i) != my_database_id {
            continue;
        }

        /* claim this one, and release lock while performing it */
        seam::workitem_set_active::call(i, true);
        seam::autovacuum_lock_release::call()?;

        seam::push_active_snapshot::call()?;
        perform_work_item(i)?;
        if snapmgr_seam::active_snapshot_set::call() {
            /* transaction could have aborted */
            snapmgr_seam::pop_active_snapshot::call()?;
        }

        /* Check for config changes before acquiring lock for further jobs. */
        seam::check_for_interrupts::call()?;
        if seam::config_reload_pending::call() {
            seam::set_config_reload_pending::call(false);
            seam::process_config_file::call()?;
            crate::cost_balance::VacuumUpdateCosts()?;
        }

        seam::autovacuum_lock_acquire_exclusive::call()?;

        /* and mark it done */
        seam::workitem_set_active::call(i, false);
        seam::workitem_set_used::call(i, false);
    }
    seam::autovacuum_lock_release::call()?;

    /*
     * Update pg_database.datfrozenxid, and truncate pg_xact if possible.  We
     * skip this if (1) we found no work to do and (2) we skipped at least one
     * table due to concurrent autovacuum activity.
     */
    if did_vacuum || !found_concurrent_worker {
        seam::vac_update_datfrozenxid::call()?;
    }

    /* Finally close out the last transaction. */
    seam::commit_transaction_command::call()?;

    Ok(())
}
