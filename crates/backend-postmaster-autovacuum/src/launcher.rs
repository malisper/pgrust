//! AUTOVACUUM LAUNCHER CODE (`autovacuum.c` lines 360-1365): the launcher main
//! loop, interrupt handling, sleep determination, the process-local
//! database-list (re)building, per-database scheduling/scoring, and worker
//! launch.
//!
//! The C launcher's `DatabaseList` is a `dlist` of `avl_dbase` living in a
//! private memory context (NOT shmem); here it is a per-backend (launcher)
//! owned `Vec<AvlDbase>`. The C list is ordered "most-distant `adl_next_worker`
//! first" and the launcher picks the *tail* (closest next_worker) — this `Vec`
//! keeps the same order: `dlist_push_head` ⇒ `insert(0, …)`, the tail element
//! ⇒ `last()`.

extern crate alloc;
use alloc::vec::Vec;
use ::core::cell::RefCell;

use backend_utils_error::{ereport, PgError, PgResult};
use types_error::{ErrorLocation, WARNING};

use types_core::{InvalidOid, Oid, TimestampTz};

use crate::core::{
    self, AvlDbase, AvwDbase, AutoVacForkFailed, AutoVacRebalance, FirstMultiXactId,
    FirstNormalTransactionId, MultiXactIdPrecedes, OidIsValid, TransactionIdPrecedes,
    MAX_AUTOVAC_SLEEPTIME, MIN_AUTOVAC_SLEEPTIME,
};
use crate::shmem::{av_worker_available, check_av_worker_gucs, AutoVacuumingActive};
use crate::worker::do_autovacuum_emergency_start;
use backend_postmaster_autovacuum_ext_seams as seam;

/// `struct timeval nap` — seconds + microseconds the launcher sleeps for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NapTime {
    pub tv_sec: i64,
    pub tv_usec: i64,
}

thread_local! {
    /// The launcher's process-local `DatabaseList` (a `dlist` of `avl_dbase`
    /// in a private memory context). Ordered most-distant-next_worker-first,
    /// exactly as the C dlist; index 0 is the head, the last element the tail.
    static DATABASE_LIST: RefCell<Vec<AvlDbase>> = const { RefCell::new(Vec::new()) };
}

/// `void AutoVacLauncherMain(const void *startup_data, size_t startup_data_len)`
/// (`autovacuum.c` lines 367-741).
///
/// Main entry point for the autovacuum launcher process. The per-backend
/// lifecycle setup (`InitProcess`/`BaseInit`/`InitPostgres`, the signal-handler
/// install, the `sigsetjmp` error-recovery body, the `SetConfigOption` forcing)
/// is out-of-crate; what is ported here is the scheduling loop body.
pub fn AutoVacLauncherMain() -> PgResult<()> {
    /*
     * In emergency mode, just start a worker (unless shutdown was requested)
     * and go away.
     */
    if !AutoVacuumingActive() {
        if !seam::shutdown_request_pending::call() {
            do_autovacuum_emergency_start()?;
        }
        seam::proc_exit::call(0); /* done */
    }

    seam::set_launcher_pid::call(seam::my_proc_pid::call());

    /*
     * Create the initial database list.  The invariant we want this list to
     * keep is that it's ordered by decreasing next_time.
     */
    rebuild_database_list(InvalidOid)?;

    /* loop until shutdown request */
    while !seam::shutdown_request_pending::call() {
        let nap = launcher_determine_sleep(av_worker_available(), false)?;

        /* Wait until naptime expires or we get some type of signal. */
        seam::wait_latch::call((nap.tv_sec * 1000) + (nap.tv_usec / 1000));

        ProcessAutoVacLauncherInterrupts()?;

        /*
         * a worker finished, or postmaster signaled failure to start a worker
         */
        if seam::got_sigusr2::call() {
            seam::set_got_sigusr2::call(false);

            /* rebalance cost limits, if needed */
            if seam::get_av_signal::call(AutoVacRebalance) {
                seam::autovacuum_lock_acquire_exclusive::call()?;
                seam::set_av_signal::call(AutoVacRebalance, false);
                crate::cost_balance::autovac_recalculate_workers_for_balance()?;
                seam::autovacuum_lock_release::call()?;
            }

            if seam::get_av_signal::call(AutoVacForkFailed) {
                /*
                 * If the postmaster failed to start a new worker, we sleep for
                 * a little while and resend the signal.
                 */
                seam::set_av_signal::call(AutoVacForkFailed, false);
                seam::pg_usleep::call(1000000); /* 1s */
                seam::send_start_autovac_worker_signal::call();
                continue;
            }
        }

        /*
         * There are some conditions that we need to check before trying to
         * start a worker.
         */
        let current_time = seam::get_current_timestamp::call();
        seam::autovacuum_lock_acquire_shared::call()?;

        let mut can_launch = av_worker_available();

        let starting = seam::starting_worker_slot::call();
        if starting >= 0 {
            /*
             * We can't launch another worker when another one is still starting
             * up, so just sleep for a bit more; that worker will wake us up
             * again as soon as it's ready.
             */
            let waittime = ::core::cmp::min(core::autovacuum_naptime(), 60) * 1000;
            let worker_launchtime = seam::worker_get_launchtime::call(starting);
            if seam::timestamp_difference_exceeds::call(worker_launchtime, current_time, waittime) {
                seam::autovacuum_lock_release::call()?;
                seam::autovacuum_lock_acquire_exclusive::call()?;

                /*
                 * No other process can put a worker in starting mode, so if
                 * startingWorker is still INVALID after exchanging our lock, we
                 * assume it's the same one we saw above.
                 */
                let worker = seam::starting_worker_slot::call();
                if worker >= 0 {
                    seam::worker_set_dboid::call(worker, InvalidOid);
                    seam::worker_set_tableoid::call(worker, InvalidOid);
                    seam::worker_set_sharedrel::call(worker, false);
                    seam::worker_set_proc::call(worker, false);
                    seam::worker_set_launchtime::call(worker, 0);
                    seam::free_workers_push_head::call(worker);
                    seam::set_starting_worker_slot::call(-1);
                    ereport(WARNING)
                        .errmsg("autovacuum worker took too long to start; canceled")
                        .finish(ErrorLocation::new(
                            "../src/backend/postmaster/autovacuum.c",
                            692,
                            "AutoVacLauncherMain",
                        ))?;
                }
            } else {
                can_launch = false;
            }
        }
        seam::autovacuum_lock_release::call()?; /* either shared or exclusive */

        /* if we can't do anything, just go back to sleep */
        if !can_launch {
            continue;
        }

        /* We're OK to start a new worker */

        if DATABASE_LIST.with_borrow(|l| l.is_empty()) {
            /*
             * Special case when the list is empty: start a worker right away.
             * This covers the initial case, when no database is in pgstats.
             */
            launch_worker(current_time)?;
        } else {
            /*
             * because rebuild_database_list constructs a list with most distant
             * adl_next_worker first, we obtain our database from the tail of the
             * list.
             */
            let avdb_next_worker = DATABASE_LIST.with_borrow(|l| {
                l.last()
                    .map(|d| d.adl_next_worker)
                    .ok_or_else(|| PgError::error("AutoVacLauncherMain: database list is empty"))
            })?;

            /*
             * launch a worker if next_worker is right now or it is in the past
             */
            if seam::timestamp_difference_exceeds::call(avdb_next_worker, current_time, 0) {
                launch_worker(current_time)?;
            }
        }
    }

    AutoVacLauncherShutdown()
}

/// `static void ProcessAutoVacLauncherInterrupts(void)` (`autovacuum.c` lines
/// 746-786).
pub fn ProcessAutoVacLauncherInterrupts() -> PgResult<()> {
    /* the normal shutdown case */
    if seam::shutdown_request_pending::call() {
        AutoVacLauncherShutdown()?;
    }

    if seam::config_reload_pending::call() {
        let autovacuum_max_workers_prev = core::autovacuum_max_workers();

        seam::set_config_reload_pending::call(false);
        seam::process_config_file::call()?;

        /* shutdown requested in config file? */
        if !AutoVacuumingActive() {
            AutoVacLauncherShutdown()?;
        }

        /*
         * If autovacuum_max_workers changed, emit a WARNING if
         * autovacuum_worker_slots < autovacuum_max_workers.
         */
        if autovacuum_max_workers_prev != core::autovacuum_max_workers() {
            check_av_worker_gucs()?;
        }

        /* rebuild the list in case the naptime changed */
        rebuild_database_list(InvalidOid)?;
    }

    /* Process barrier / memory-context-log / sinval-catchup interrupts */
    seam::process_launcher_barrier_and_catchup_interrupts::call()?;

    Ok(())
}

/// `pg_noreturn static void AutoVacLauncherShutdown(void)` (`autovacuum.c` lines
/// 791-799). Perform a normal exit from the autovac launcher.
pub fn AutoVacLauncherShutdown() -> PgResult<()> {
    seam::set_launcher_pid::call(0);
    seam::proc_exit::call(0); /* done */
}

/// `static void launcher_determine_sleep(bool canlaunch, bool recursing, struct
/// timeval *nap)` (`autovacuum.c` lines 808-877).
///
/// Determine the time to sleep, based on the database list.
pub fn launcher_determine_sleep(canlaunch: bool, recursing: bool) -> PgResult<NapTime> {
    let mut nap = NapTime::default();

    /*
     * We sleep until the next scheduled vacuum.  We trust that when the
     * database list was built, care was taken so that no entries have times in
     * the past.
     */
    if !canlaunch {
        nap.tv_sec = core::autovacuum_naptime() as i64;
        nap.tv_usec = 0;
    } else if !DATABASE_LIST.with_borrow(|l| l.is_empty()) {
        let current_time = seam::get_current_timestamp::call();
        let avdb_next_worker = DATABASE_LIST.with_borrow(|l| {
            l.last()
                .map(|d| d.adl_next_worker)
                .ok_or_else(|| PgError::error("launcher_determine_sleep: database list is empty"))
        })?;
        let next_wakeup = avdb_next_worker;
        let (secs, usecs) = seam::timestamp_difference::call(current_time, next_wakeup);

        nap.tv_sec = secs;
        nap.tv_usec = usecs as i64;
    } else {
        /* list is empty, sleep for whole autovacuum_naptime seconds */
        nap.tv_sec = core::autovacuum_naptime() as i64;
        nap.tv_usec = 0;
    }

    /*
     * If the result is exactly zero, it means a database had an entry with time
     * in the past.  Rebuild the list so that the databases are evenly
     * distributed again, and recalculate the time to sleep.
     *
     * We only recurse once.
     */
    if nap.tv_sec == 0 && nap.tv_usec == 0 && !recursing {
        rebuild_database_list(InvalidOid)?;
        return launcher_determine_sleep(canlaunch, true);
    }

    /* The smallest time we'll allow the launcher to sleep. */
    if nap.tv_sec <= 0 && nap.tv_usec <= (MIN_AUTOVAC_SLEEPTIME * 1000.0) as i64 {
        nap.tv_sec = 0;
        nap.tv_usec = (MIN_AUTOVAC_SLEEPTIME * 1000.0) as i64;
    }

    /* If the sleep time is too large, clamp it to an arbitrary maximum. */
    if nap.tv_sec > MAX_AUTOVAC_SLEEPTIME {
        nap.tv_sec = MAX_AUTOVAC_SLEEPTIME;
    }

    Ok(nap)
}

/// `static void rebuild_database_list(Oid newdb)` (`autovacuum.c` lines
/// 892-1068).
///
/// Build an updated `DatabaseList`. It must only contain databases that appear
/// in pgstats, and be sorted by next_worker from highest to lowest, distributed
/// regularly across the next `autovacuum_naptime` interval.
pub fn rebuild_database_list(newdb: Oid) -> PgResult<()> {
    let mut dbhash: Vec<AvlDbase> = Vec::new();
    let mut score: i32 = 0;

    // hash_search(HASH_ENTER) helper: insert keyed by adl_datid, scoring in
    // insertion order; skips if the datid is already present.
    fn enter(dbhash: &mut Vec<AvlDbase>, datid: Oid, score: &mut i32) {
        if dbhash.iter().any(|d| d.adl_datid == datid) {
            return;
        }
        dbhash.push(AvlDbase {
            adl_datid: datid,
            adl_next_worker: 0,
            adl_score: *score,
        });
        *score += 1;
    }

    /* start by inserting the new database */
    if OidIsValid(newdb) {
        /* only consider this database if it has a pgstat entry */
        if seam::pgstat_fetch_stat_dbentry::call(newdb).is_some() {
            /* we assume it isn't found because the hash was just created */
            enter(&mut dbhash, newdb, &mut score);
        }
    }

    /* Now insert the databases from the existing list */
    let existing: Vec<Oid> = DATABASE_LIST.with_borrow(|l| l.iter().map(|d| d.adl_datid).collect());
    for datid in existing {
        /*
         * skip databases with no stat entries -- in particular, this gets rid
         * of dropped databases
         */
        if seam::pgstat_fetch_stat_dbentry::call(datid).is_none() {
            continue;
        }
        enter(&mut dbhash, datid, &mut score);
    }

    /* finally, insert all qualifying databases not previously inserted */
    let dblist: Vec<AvwDbase> = seam::get_database_list::call()?;
    for avdb in &dblist {
        /* only consider databases with a pgstat entry */
        if seam::pgstat_fetch_stat_dbentry::call(avdb.adw_datid).is_none() {
            continue;
        }
        enter(&mut dbhash, avdb.adw_datid, &mut score);
    }
    let nelems = score;

    /* from here on, the allocated memory belongs to the new list */
    DATABASE_LIST.with_borrow_mut(|l| l.clear());

    if nelems > 0 {
        /* put all the hash elements into an array */
        let mut dbary: Vec<AvlDbase> = dbhash;

        /* sort the array by score */
        dbary.sort_by(|a, b| db_comparator(a, b));

        /* Determine the time interval between databases in the schedule. */
        let mut millis_increment: f64 =
            1000.0 * core::autovacuum_naptime() as f64 / nelems as f64;
        if millis_increment <= MIN_AUTOVAC_SLEEPTIME {
            millis_increment = MIN_AUTOVAC_SLEEPTIME * 1.1;
        }

        let mut current_time = seam::get_current_timestamp::call();

        /*
         * move the elements from the array into the dlist, setting the
         * next_worker while walking the array
         */
        for db in dbary.iter_mut() {
            current_time =
                seam::timestamp_tz_plus_milliseconds::call(current_time, millis_increment as i64);
            db.adl_next_worker = current_time;

            /* later elements should go closer to the head of the list */
            DATABASE_LIST.with_borrow_mut(|l| l.insert(0, db.clone()));
        }
    }

    Ok(())
}

/// `static int db_comparator(const void *a, const void *b)` (`autovacuum.c`
/// lines 1071-1076). qsort comparator for `avl_dbase`, using `adl_score`.
pub fn db_comparator(a: &AvlDbase, b: &AvlDbase) -> ::core::cmp::Ordering {
    a.adl_score.cmp(&b.adl_score)
}

/// `static Oid do_start_worker(void)` (`autovacuum.c` lines 1089-1288).
///
/// Determine what database to work on, set up shared memory and signal the
/// postmaster to start the worker. Returns the OID of the database the worker
/// is going to process, or `InvalidOid` if no worker was actually started.
pub fn do_start_worker() -> PgResult<Oid> {
    let mut retval = InvalidOid;

    /* return quickly when there are no free workers */
    seam::autovacuum_lock_acquire_shared::call()?;
    if !av_worker_available() {
        seam::autovacuum_lock_release::call()?;
        return Ok(InvalidOid);
    }
    seam::autovacuum_lock_release::call()?;

    /* Get a list of databases */
    let dblist: Vec<AvwDbase> = seam::get_database_list::call()?;

    /*
     * Determine the oldest datfrozenxid/relfrozenxid that we will allow to pass
     * without forcing a vacuum.
     */
    core::set_recentXid(seam::read_next_transaction_id::call());
    let mut xid_force_limit =
        core::recentXid().wrapping_sub(core::autovacuum_freeze_max_age() as u32);
    /* ensure it's a "normal" XID, else TransactionIdPrecedes misbehaves */
    if xid_force_limit < FirstNormalTransactionId {
        xid_force_limit = xid_force_limit.wrapping_sub(FirstNormalTransactionId);
    }

    /* Also determine the oldest datminmxid we will consider. */
    core::set_recentMulti(seam::read_next_multixact_id::call());
    let mut multi_force_limit =
        core::recentMulti().wrapping_sub(seam::multixact_member_freeze_threshold::call() as u32);
    if multi_force_limit < FirstMultiXactId {
        multi_force_limit = multi_force_limit.wrapping_sub(FirstMultiXactId);
    }

    /*
     * Choose a database to connect to.  We pick the database that was least
     * recently auto-vacuumed, or one that needs vacuuming to prevent Xid
     * wraparound-related data loss.
     */
    let mut avdb: Option<&AvwDbase> = None;
    let mut avdb_last_autovac: i64 = 0;
    let mut for_xid_wrap = false;
    let mut for_multi_wrap = false;
    let mut skipit = false;
    let current_time = seam::get_current_timestamp::call();

    for tmp in &dblist {
        /* Check to see if this one is at risk of wraparound */
        if TransactionIdPrecedes(tmp.adw_frozenxid, xid_force_limit) {
            if avdb.is_none()
                || TransactionIdPrecedes(tmp.adw_frozenxid, avdb.unwrap().adw_frozenxid)
            {
                avdb = Some(tmp);
            }
            for_xid_wrap = true;
            continue;
        } else if for_xid_wrap {
            continue; /* ignore not-at-risk DBs */
        } else if MultiXactIdPrecedes(tmp.adw_minmulti, multi_force_limit) {
            if avdb.is_none() || MultiXactIdPrecedes(tmp.adw_minmulti, avdb.unwrap().adw_minmulti) {
                avdb = Some(tmp);
            }
            for_multi_wrap = true;
            continue;
        } else if for_multi_wrap {
            continue; /* ignore not-at-risk DBs */
        }

        /* Find pgstat entry if any */
        let entry = seam::pgstat_fetch_stat_dbentry::call(tmp.adw_datid);

        /*
         * Skip a database with no pgstat entry; it means it hasn't seen any
         * activity.
         */
        let entry = match entry {
            Some(e) => e,
            None => continue,
        };

        /*
         * Also, skip a database that appears on the database list as having
         * been processed recently (less than autovacuum_naptime seconds ago).
         */
        skipit = false;

        /* dlist_reverse_foreach: walk the list from tail to head. */
        let found = DATABASE_LIST.with_borrow(|l| {
            for dbp in l.iter() {
                if dbp.adl_datid == tmp.adw_datid {
                    return Some(dbp.adl_next_worker);
                }
            }
            None
        });
        if let Some(adl_next_worker) = found {
            /*
             * Skip this database if its next_worker value falls between the
             * current time and the current time plus naptime.
             */
            if !seam::timestamp_difference_exceeds::call(adl_next_worker, current_time, 0)
                && !seam::timestamp_difference_exceeds::call(
                    current_time,
                    adl_next_worker,
                    core::autovacuum_naptime() * 1000,
                )
            {
                skipit = true;
            }
        }
        if skipit {
            continue;
        }

        /*
         * Remember the db with oldest autovac time.  (If we are here, both
         * tmp->entry and db->entry must be non-null.)
         */
        if avdb.is_none() || entry.last_autovac_time < avdb_last_autovac {
            avdb = Some(tmp);
            avdb_last_autovac = entry.last_autovac_time;
        }
    }

    /* Found a database -- process it */
    if let Some(avdb) = avdb {
        seam::autovacuum_lock_acquire_exclusive::call()?;

        /*
         * Get a worker entry from the freelist.  We checked above, so there
         * really should be a free slot.
         */
        let worker = seam::free_workers_pop_head::call();

        seam::worker_set_dboid::call(worker, avdb.adw_datid);
        seam::worker_set_proc::call(worker, false);
        seam::worker_set_launchtime::call(worker, seam::get_current_timestamp::call());

        seam::set_starting_worker_slot::call(worker);

        seam::autovacuum_lock_release::call()?;

        seam::send_start_autovac_worker_signal::call();

        retval = avdb.adw_datid;
    } else if skipit {
        /*
         * If we skipped all databases on the list, rebuild it, because it
         * probably contains a dropped database.
         */
        rebuild_database_list(InvalidOid)?;
    }

    Ok(retval)
}

/// `static void launch_worker(TimestampTz now)` (`autovacuum.c` lines
/// 1301-1346).
///
/// Wrapper for starting a worker from the launcher. Besides actually starting
/// it, update the database list to reflect the next time another one will need
/// to be started on the selected database.
pub fn launch_worker(now: TimestampTz) -> PgResult<()> {
    let dbid = do_start_worker()?;
    if OidIsValid(dbid) {
        /*
         * Walk the database list and update the corresponding entry.  If the
         * database is not on the list, we'll recreate the list.
         */
        let new_next_worker =
            seam::timestamp_tz_plus_milliseconds::call(now, (core::autovacuum_naptime() * 1000) as i64);
        let found = DATABASE_LIST.with_borrow_mut(|l| {
            let len = l.len();
            for idx in 0..len {
                if l[idx].adl_datid == dbid {
                    /*
                     * add autovacuum_naptime seconds to the current time, and
                     * use that as the new "next_worker" field for this database.
                     */
                    l[idx].adl_next_worker = new_next_worker;

                    /* dlist_move_head(&DatabaseList, iter.cur) */
                    let elem = l.remove(idx);
                    l.insert(0, elem);
                    return true;
                }
            }
            false
        });

        /*
         * If the database was not present in the database list, we rebuild the
         * list.
         */
        if !found {
            rebuild_database_list(dbid)?;
        }
    }

    Ok(())
}

/// `void AutoVacWorkerFailed(void)` (`autovacuum.c` lines 1353-1357).
///
/// Called from postmaster to signal a failure to fork a process to become
/// worker. The postmaster should `kill(SIGUSR2)` the launcher shortly after.
pub fn AutoVacWorkerFailed() {
    seam::set_av_signal::call(AutoVacForkFailed, true);
}

/// `static void avl_sigusr2_handler(SIGNAL_ARGS)` (`autovacuum.c` lines
/// 1360-1365). SIGUSR2: a worker is up and running, or just finished, or failed
/// to fork.
pub fn avl_sigusr2_handler() {
    seam::set_got_sigusr2::call(true);
    // SetLatch(MyLatch) is delivered by the runtime's signal layer.
}
