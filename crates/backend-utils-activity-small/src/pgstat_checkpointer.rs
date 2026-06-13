//! Port of `src/backend/utils/activity/pgstat_checkpointer.c`
//! (PostgreSQL 18.3).
//!
//! Implementation of checkpoint statistics. It is kept separate from
//! `pgstat.c` to enforce the line between the statistics access / storage
//! implementation and the details about individual types of statistics.
//!
//! `PendingCheckpointerStats` is the file-owned backend-local pending buffer
//! (a per-backend C global, mutated directly by `postmaster/checkpointer.c`,
//! `storage/buffer/bufmgr.c`, `access/transam/slru.c`, and
//! `access/transam/xlog.c`); it is a `thread_local!` reached through
//! [`with_pending_checkpointer_stats`].

use core::cell::RefCell;

use crate::changecount::{
    pgstat_begin_changecount_write, pgstat_copy_changecounted_stats,
    pgstat_end_changecount_write,
};
use backend_storage_lmgr_lwlock_seams::{lwlock_acquire, lwlock_initialize};
use backend_utils_activity_pgstat_seams::{
    assert_is_up, shmem_is_shutdown, snapshot_fixed, with_shmem_checkpointer,
    with_snapshot_checkpointer,
};
use backend_utils_activity_stat_seams::pgstat_flush_io;
use types_core::TimestampTz;
use types_error::PgResult;
use types_pgstat::activity_pgstat::{
    PgStatShared_Checkpointer, PgStat_CheckpointerStats,
};
use types_storage::{LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, LW_SHARED};

pub use types_pgstat::activity_pgstat::PGSTAT_KIND_CHECKPOINTER;

thread_local! {
    /// `PgStat_CheckpointerStats PendingCheckpointerStats = {0};` —
    /// backend-local state, so per-thread (one backend == one thread).
    static PENDING_CHECKPOINTER_STATS: RefCell<PgStat_CheckpointerStats> = const {
        RefCell::new(PgStat_CheckpointerStats {
            num_timed: 0,
            num_requested: 0,
            num_performed: 0,
            restartpoints_timed: 0,
            restartpoints_requested: 0,
            restartpoints_performed: 0,
            write_time: 0,
            sync_time: 0,
            buffers_written: 0,
            slru_written: 0,
            stat_reset_timestamp: 0,
        })
    };
}

/// Run `f` on this backend's `PendingCheckpointerStats` buffer. In C the
/// global is `PGDLLIMPORT` and other files (`postmaster/checkpointer.c`,
/// `storage/buffer/bufmgr.c`, `access/transam/slru.c`, `access/transam/xlog.c`)
/// bump its fields directly; this accessor is that access path.
pub fn with_pending_checkpointer_stats<R>(
    f: impl FnOnce(&mut PgStat_CheckpointerStats) -> R,
) -> R {
    PENDING_CHECKPOINTER_STATS.with(|p| f(&mut p.borrow_mut()))
}

/// Report checkpointer and IO statistics.
///
/// Port of `void pgstat_report_checkpointer(void)`.
pub fn pgstat_report_checkpointer() -> PgResult<()> {
    // Assert(!pgStatLocal.shmem->is_shutdown);
    debug_assert!(!shmem_is_shutdown::call());
    // pgstat_assert_is_up();
    assert_is_up::call();

    // This function can be called even if nothing at all has happened. In
    // this case, avoid unnecessarily modifying the stats entry.
    //
    // if (pg_memory_is_all_zeros(&PendingCheckpointerStats,
    //                            sizeof(struct PgStat_CheckpointerStats)))
    //     return;
    let pending = with_pending_checkpointer_stats(|p| *p);
    if pending.is_all_zeros() {
        return Ok(());
    }

    // PgStatShared_Checkpointer *stats_shmem = &pgStatLocal.shmem->checkpointer;
    with_shmem_checkpointer::call(&mut |stats_shmem: &mut PgStatShared_Checkpointer| {
        pgstat_begin_changecount_write(&stats_shmem.changecount);

        // #define CHECKPOINTER_ACC(fld) stats_shmem->stats.fld += PendingCheckpointerStats.fld
        stats_shmem.stats.num_timed += pending.num_timed;
        stats_shmem.stats.num_requested += pending.num_requested;
        stats_shmem.stats.num_performed += pending.num_performed;
        stats_shmem.stats.restartpoints_timed += pending.restartpoints_timed;
        stats_shmem.stats.restartpoints_requested += pending.restartpoints_requested;
        stats_shmem.stats.restartpoints_performed += pending.restartpoints_performed;
        stats_shmem.stats.write_time += pending.write_time;
        stats_shmem.stats.sync_time += pending.sync_time;
        stats_shmem.stats.buffers_written += pending.buffers_written;
        stats_shmem.stats.slru_written += pending.slru_written;

        pgstat_end_changecount_write(&stats_shmem.changecount);
    });

    // Clear out the statistics buffer, so it can be re-used.
    // MemSet(&PendingCheckpointerStats, 0, sizeof(PendingCheckpointerStats));
    with_pending_checkpointer_stats(|p| *p = PgStat_CheckpointerStats::default());

    // Report IO statistics
    pgstat_flush_io::call(false)?;
    Ok(())
}

/// `pgstat_fetch_stat_checkpointer()` —
///
/// Support function for the SQL-callable `pgstat*` functions. In C this
/// returns a pointer to the snapshot's checkpointer statistics struct; here
/// it is a copy of that snapshot entry.
pub fn pgstat_fetch_stat_checkpointer() -> PgResult<PgStat_CheckpointerStats> {
    snapshot_fixed::call(PGSTAT_KIND_CHECKPOINTER)?;

    // return &pgStatLocal.snapshot.checkpointer;
    let mut snap = PgStat_CheckpointerStats::default();
    with_snapshot_checkpointer::call(&mut |s: &mut PgStat_CheckpointerStats| snap = *s);
    Ok(snap)
}

/// Port of `void pgstat_checkpointer_init_shmem_cb(void *stats)`.
pub fn pgstat_checkpointer_init_shmem_cb(stats: &mut PgStatShared_Checkpointer) {
    lwlock_initialize::call(&mut stats.lock, LWTRANCHE_PGSTATS_DATA);
}

/// Port of `void pgstat_checkpointer_reset_all_cb(TimestampTz ts)`.
pub fn pgstat_checkpointer_reset_all_cb(ts: TimestampTz) -> PgResult<()> {
    let mut res: PgResult<()> = Ok(());
    with_shmem_checkpointer::call(&mut |stats_shmem: &mut PgStatShared_Checkpointer| {
        res = (|| {
            // see explanation above PgStatShared_Checkpointer for the reset protocol
            let guard = lwlock_acquire::call(&stats_shmem.lock, LW_EXCLUSIVE)?;
            {
                // pgstat_copy_changecounted_stats(&stats_shmem->reset_offset,
                //                                 &stats_shmem->stats, sizeof(...),
                //                                 &stats_shmem->changecount);
                let PgStatShared_Checkpointer {
                    ref mut reset_offset,
                    ref stats,
                    ref changecount,
                    ..
                } = *stats_shmem;
                pgstat_copy_changecounted_stats(reset_offset, stats, changecount);
            }
            stats_shmem.stats.stat_reset_timestamp = ts;
            guard.release()
        })();
    });
    res
}

/// Port of `void pgstat_checkpointer_snapshot_cb(void)`.
pub fn pgstat_checkpointer_snapshot_cb() -> PgResult<()> {
    let mut snap = PgStat_CheckpointerStats::default();
    with_snapshot_checkpointer::call(&mut |s: &mut PgStat_CheckpointerStats| snap = *s);

    let mut shmem_res: PgResult<PgStat_CheckpointerStats> =
        Ok(PgStat_CheckpointerStats::default());
    with_shmem_checkpointer::call(&mut |stats_shmem: &mut PgStatShared_Checkpointer| {
        shmem_res = (|| {
            // pgstat_copy_changecounted_stats(&pgStatLocal.snapshot.checkpointer,
            //                                 &stats_shmem->stats, sizeof(...),
            //                                 &stats_shmem->changecount);
            pgstat_copy_changecounted_stats(
                &mut snap,
                &stats_shmem.stats,
                &stats_shmem.changecount,
            );

            let guard = lwlock_acquire::call(&stats_shmem.lock, LW_SHARED)?;
            // memcpy(&reset, reset_offset, sizeof(stats_shmem->stats));
            let reset = stats_shmem.reset_offset;
            guard.release()?;

            Ok(reset)
        })();
    });
    let reset: PgStat_CheckpointerStats = shmem_res?;

    // compensate by reset offsets
    // #define CHECKPOINTER_COMP(fld) pgStatLocal.snapshot.checkpointer.fld -= reset.fld;
    snap.num_timed -= reset.num_timed;
    snap.num_requested -= reset.num_requested;
    snap.num_performed -= reset.num_performed;
    snap.restartpoints_timed -= reset.restartpoints_timed;
    snap.restartpoints_requested -= reset.restartpoints_requested;
    snap.restartpoints_performed -= reset.restartpoints_performed;
    snap.write_time -= reset.write_time;
    snap.sync_time -= reset.sync_time;
    snap.buffers_written -= reset.buffers_written;
    snap.slru_written -= reset.slru_written;

    with_snapshot_checkpointer::call(&mut |s: &mut PgStat_CheckpointerStats| *s = snap);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_seams::setup;

    #[test]
    fn report_skips_when_pending_all_zeros() {
        let env = setup();

        pgstat_report_checkpointer().unwrap();

        assert_eq!(env.checkpointer_shmem.borrow().changecount.load(core::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(env.flush_io_calls.get(), 0);
    }

    #[test]
    fn report_accumulates_and_clears_pending() {
        let env = setup();
        with_pending_checkpointer_stats(|p| {
            p.num_timed = 1;
            p.num_requested = 2;
            p.num_performed = 3;
            p.restartpoints_timed = 4;
            p.restartpoints_requested = 5;
            p.restartpoints_performed = 6;
            p.write_time = 7;
            p.sync_time = 8;
            p.buffers_written = 9;
            p.slru_written = 10;
        });
        env.checkpointer_shmem.borrow_mut().stats.num_timed = 100;

        pgstat_report_checkpointer().unwrap();

        {
            let shmem = env.checkpointer_shmem.borrow();
            assert_eq!(shmem.stats.num_timed, 101);
            assert_eq!(shmem.stats.num_requested, 2);
            assert_eq!(shmem.stats.num_performed, 3);
            assert_eq!(shmem.stats.restartpoints_timed, 4);
            assert_eq!(shmem.stats.restartpoints_requested, 5);
            assert_eq!(shmem.stats.restartpoints_performed, 6);
            assert_eq!(shmem.stats.write_time, 7);
            assert_eq!(shmem.stats.sync_time, 8);
            assert_eq!(shmem.stats.buffers_written, 9);
            assert_eq!(shmem.stats.slru_written, 10);
            assert_eq!(shmem.changecount.load(core::sync::atomic::Ordering::Relaxed), 2);
        }
        assert!(with_pending_checkpointer_stats(|p| p.is_all_zeros()));
        assert_eq!(env.flush_io_calls.get(), 1);
    }

    #[test]
    fn fetch_stat_checkpointer_snapshots_and_returns_copy() {
        let env = setup();
        env.checkpointer_snapshot.borrow_mut().num_performed = 11;

        let stats = pgstat_fetch_stat_checkpointer().unwrap();
        assert_eq!(stats.num_performed, 11);
        assert_eq!(
            env.snapshot_fixed_kinds.borrow().clone(),
            vec![PGSTAT_KIND_CHECKPOINTER]
        );
    }

    #[test]
    fn init_shmem_cb_initializes_lock() {
        let env = setup();

        let mut shared = PgStatShared_Checkpointer::default();
        pgstat_checkpointer_init_shmem_cb(&mut shared);
        assert_eq!(shared.lock.tranche, LWTRANCHE_PGSTATS_DATA as u16);
        assert_eq!(env.lwlock_inits.get(), 1);
    }

    #[test]
    fn reset_all_cb_copies_offset_and_sets_timestamp() {
        let env = setup();
        {
            let mut s = env.checkpointer_shmem.borrow_mut();
            s.stats.num_timed = 10;
            s.stats.buffers_written = 3;
        }

        pgstat_checkpointer_reset_all_cb(555).unwrap();

        {
            let shmem = env.checkpointer_shmem.borrow();
            assert_eq!(shmem.reset_offset.num_timed, 10);
            assert_eq!(shmem.reset_offset.buffers_written, 3);
            assert_eq!(shmem.stats.stat_reset_timestamp, 555);
        }
        assert_eq!(env.lwlock_acquires.borrow().clone(), vec![LW_EXCLUSIVE]);
        assert_eq!(env.lwlock_releases.get(), 1);
    }

    #[test]
    fn snapshot_cb_compensates_by_reset_offsets() {
        let env = setup();
        {
            let mut s = env.checkpointer_shmem.borrow_mut();
            s.stats.num_timed = 10;
            s.stats.write_time = 50;
            s.reset_offset.num_timed = 4;
            s.reset_offset.write_time = 20;
        }

        pgstat_checkpointer_snapshot_cb().unwrap();

        {
            let snap = env.checkpointer_snapshot.borrow();
            assert_eq!(snap.num_timed, 6);
            assert_eq!(snap.write_time, 30);
        }
        assert_eq!(env.lwlock_acquires.borrow().clone(), vec![LW_SHARED]);
        assert_eq!(env.lwlock_releases.get(), 1);
    }
}
