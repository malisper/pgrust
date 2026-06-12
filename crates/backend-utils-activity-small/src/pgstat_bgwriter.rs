//! Port of `src/backend/utils/activity/pgstat_bgwriter.c` (PostgreSQL 18.3).
//!
//! Implementation of bgwriter statistics. It is kept separate from `pgstat.c`
//! to enforce the line between the statistics access / storage implementation
//! and the details about individual types of statistics.
//!
//! `PendingBgWriterStats` is the file-owned backend-local pending buffer (a
//! process global in C, mutated directly by `storage/buffer/bufmgr.c`'s
//! `BgBufferSync`); it is kept as a file-local `static mut` reached through
//! [`pending_bgwriter_stats`].

use crate::changecount::{
    pgstat_begin_changecount_write, pgstat_copy_changecounted_stats,
    pgstat_end_changecount_write,
};
use backend_storage_lmgr_lwlock_seams::{lwlock_acquire, lwlock_initialize, lwlock_release};
use backend_utils_activity_pgstat_seams::{
    assert_is_up, shmem_is_shutdown, snapshot_fixed, with_shmem_bgwriter, with_snapshot_bgwriter,
};
use backend_utils_activity_stat_seams::pgstat_flush_io;
use types_core::TimestampTz;
use types_error::PgResult;
use types_pgstat::backend_utils_activity_pgstat_bgwriter::{
    PgStatShared_BgWriter, PgStat_BgWriterStats,
};
use types_storage::{LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, LW_SHARED};

/// `PGSTAT_KIND_BGWRITER` (`utils/pgstat_kind.h`).
pub const PGSTAT_KIND_BGWRITER: u32 = 8;

/// `PgStat_BgWriterStats PendingBgWriterStats = {0};`
static mut PENDING_BGWRITER_STATS: PgStat_BgWriterStats = PgStat_BgWriterStats {
    buf_written_clean: 0,
    maxwritten_clean: 0,
    buf_alloc: 0,
    stat_reset_timestamp: 0,
};

/// Borrow the file-owned backend-local `PendingBgWriterStats` buffer. In C
/// the global is `PGDLLIMPORT` and other files (`storage/buffer/bufmgr.c`)
/// bump its fields directly; this accessor is that access path.
///
/// SAFETY: backend-local process global, mirroring C's single-threaded
/// per-backend access. Callers take one live borrow at a time within a tight
/// scope, like C's pointer to the global.
pub fn pending_bgwriter_stats() -> &'static mut PgStat_BgWriterStats {
    unsafe { &mut *core::ptr::addr_of_mut!(PENDING_BGWRITER_STATS) }
}

/// Report bgwriter and IO statistics.
///
/// Port of `void pgstat_report_bgwriter(void)`.
pub fn pgstat_report_bgwriter() -> PgResult<()> {
    // Assert(!pgStatLocal.shmem->is_shutdown);
    debug_assert!(!shmem_is_shutdown::call());
    // pgstat_assert_is_up();
    assert_is_up::call();

    // This function can be called even if nothing at all has happened. In
    // this case, avoid unnecessarily modifying the stats entry.
    //
    // if (pg_memory_is_all_zeros(&PendingBgWriterStats,
    //                            sizeof(struct PgStat_BgWriterStats)))
    //     return;
    let pending = *pending_bgwriter_stats();
    if pending.is_all_zeros() {
        return Ok(());
    }

    // PgStatShared_BgWriter *stats_shmem = &pgStatLocal.shmem->bgwriter;
    with_shmem_bgwriter::call(&mut |stats_shmem: &mut PgStatShared_BgWriter| {
        pgstat_begin_changecount_write(&mut stats_shmem.changecount);

        // #define BGWRITER_ACC(fld) stats_shmem->stats.fld += PendingBgWriterStats.fld
        stats_shmem.stats.buf_written_clean += pending.buf_written_clean;
        stats_shmem.stats.maxwritten_clean += pending.maxwritten_clean;
        stats_shmem.stats.buf_alloc += pending.buf_alloc;

        pgstat_end_changecount_write(&mut stats_shmem.changecount);
    });

    // Clear out the statistics buffer, so it can be re-used.
    // MemSet(&PendingBgWriterStats, 0, sizeof(PendingBgWriterStats));
    *pending_bgwriter_stats() = PgStat_BgWriterStats::default();

    // Report IO statistics
    pgstat_flush_io::call(false)?;
    Ok(())
}

/// Support function for the SQL-callable `pgstat*` functions. In C this
/// returns a pointer to the snapshot's bgwriter statistics struct; here it is
/// a copy of that snapshot entry.
///
/// Port of `PgStat_BgWriterStats *pgstat_fetch_stat_bgwriter(void)`.
pub fn pgstat_fetch_stat_bgwriter() -> PgResult<PgStat_BgWriterStats> {
    snapshot_fixed::call(PGSTAT_KIND_BGWRITER)?;

    // return &pgStatLocal.snapshot.bgwriter;
    let mut snap = PgStat_BgWriterStats::default();
    with_snapshot_bgwriter::call(&mut |s: &mut PgStat_BgWriterStats| snap = *s);
    Ok(snap)
}

/// Port of `void pgstat_bgwriter_init_shmem_cb(void *stats)`.
pub fn pgstat_bgwriter_init_shmem_cb(stats: &mut PgStatShared_BgWriter) {
    lwlock_initialize::call(&mut stats.lock, LWTRANCHE_PGSTATS_DATA);
}

/// Port of `void pgstat_bgwriter_reset_all_cb(TimestampTz ts)`.
pub fn pgstat_bgwriter_reset_all_cb(ts: TimestampTz) -> PgResult<()> {
    let mut res: PgResult<()> = Ok(());
    with_shmem_bgwriter::call(&mut |stats_shmem: &mut PgStatShared_BgWriter| {
        res = (|| {
            // see explanation above PgStatShared_BgWriter for the reset protocol
            lwlock_acquire::call(&mut stats_shmem.lock, LW_EXCLUSIVE)?;
            {
                // pgstat_copy_changecounted_stats(&stats_shmem->reset_offset,
                //                                 &stats_shmem->stats, sizeof(...),
                //                                 &stats_shmem->changecount);
                let PgStatShared_BgWriter {
                    ref mut reset_offset,
                    ref stats,
                    ref changecount,
                    ..
                } = *stats_shmem;
                pgstat_copy_changecounted_stats(reset_offset, stats, changecount);
            }
            stats_shmem.stats.stat_reset_timestamp = ts;
            lwlock_release::call(&mut stats_shmem.lock)
        })();
    });
    res
}

/// Port of `void pgstat_bgwriter_snapshot_cb(void)`.
pub fn pgstat_bgwriter_snapshot_cb() -> PgResult<()> {
    let mut snap = PgStat_BgWriterStats::default();
    with_snapshot_bgwriter::call(&mut |s: &mut PgStat_BgWriterStats| snap = *s);

    let mut shmem_res: PgResult<PgStat_BgWriterStats> = Ok(PgStat_BgWriterStats::default());
    with_shmem_bgwriter::call(&mut |stats_shmem: &mut PgStatShared_BgWriter| {
        shmem_res = (|| {
            // pgstat_copy_changecounted_stats(&pgStatLocal.snapshot.bgwriter,
            //                                 &stats_shmem->stats, sizeof(...),
            //                                 &stats_shmem->changecount);
            pgstat_copy_changecounted_stats(
                &mut snap,
                &stats_shmem.stats,
                &stats_shmem.changecount,
            );

            lwlock_acquire::call(&mut stats_shmem.lock, LW_SHARED)?;
            // memcpy(&reset, reset_offset, sizeof(stats_shmem->stats));
            let reset = stats_shmem.reset_offset;
            lwlock_release::call(&mut stats_shmem.lock)?;

            Ok(reset)
        })();
    });
    let reset: PgStat_BgWriterStats = shmem_res?;

    // compensate by reset offsets
    // #define BGWRITER_COMP(fld) pgStatLocal.snapshot.bgwriter.fld -= reset.fld;
    snap.buf_written_clean -= reset.buf_written_clean;
    snap.maxwritten_clean -= reset.maxwritten_clean;
    snap.buf_alloc -= reset.buf_alloc;

    with_snapshot_bgwriter::call(&mut |s: &mut PgStat_BgWriterStats| *s = snap);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_seams::setup;

    #[test]
    fn report_skips_when_pending_all_zeros() {
        let env = setup();

        pgstat_report_bgwriter().unwrap();

        assert_eq!(env.bgwriter_shmem.borrow().changecount, 0);
        assert_eq!(env.flush_io_calls.get(), 0);
    }

    #[test]
    fn report_accumulates_and_clears_pending() {
        let env = setup();
        {
            let p = pending_bgwriter_stats();
            p.buf_written_clean = 4;
            p.maxwritten_clean = 1;
            p.buf_alloc = 9;
        }
        env.bgwriter_shmem.borrow_mut().stats.buf_alloc = 100;

        pgstat_report_bgwriter().unwrap();

        {
            let shmem = env.bgwriter_shmem.borrow();
            assert_eq!(shmem.stats.buf_written_clean, 4);
            assert_eq!(shmem.stats.maxwritten_clean, 1);
            assert_eq!(shmem.stats.buf_alloc, 109);
            assert_eq!(shmem.changecount, 2);
        }
        assert!(pending_bgwriter_stats().is_all_zeros());
        assert_eq!(env.flush_io_calls.get(), 1);
    }

    #[test]
    fn fetch_stat_bgwriter_snapshots_and_returns_copy() {
        let env = setup();
        env.bgwriter_snapshot.borrow_mut().buf_alloc = 7;

        let stats = pgstat_fetch_stat_bgwriter().unwrap();
        assert_eq!(stats.buf_alloc, 7);
        assert_eq!(
            env.snapshot_fixed_kinds.borrow().clone(),
            vec![PGSTAT_KIND_BGWRITER]
        );
    }

    #[test]
    fn init_shmem_cb_initializes_lock() {
        let env = setup();

        let mut shared = PgStatShared_BgWriter::default();
        pgstat_bgwriter_init_shmem_cb(&mut shared);
        assert_eq!(shared.lock.tranche, LWTRANCHE_PGSTATS_DATA as u16);
        assert_eq!(env.lwlock_inits.get(), 1);
    }

    #[test]
    fn reset_all_cb_copies_offset_and_sets_timestamp() {
        let env = setup();
        {
            let mut s = env.bgwriter_shmem.borrow_mut();
            s.stats.buf_written_clean = 10;
            s.stats.buf_alloc = 3;
        }

        pgstat_bgwriter_reset_all_cb(777).unwrap();

        {
            let shmem = env.bgwriter_shmem.borrow();
            assert_eq!(shmem.reset_offset.buf_written_clean, 10);
            assert_eq!(shmem.reset_offset.buf_alloc, 3);
            assert_eq!(shmem.stats.stat_reset_timestamp, 777);
        }
        assert_eq!(env.lwlock_acquires.borrow().clone(), vec![LW_EXCLUSIVE]);
        assert_eq!(env.lwlock_releases.get(), 1);
    }

    #[test]
    fn snapshot_cb_compensates_by_reset_offsets() {
        let env = setup();
        {
            let mut s = env.bgwriter_shmem.borrow_mut();
            s.stats.buf_written_clean = 10;
            s.stats.maxwritten_clean = 5;
            s.stats.buf_alloc = 20;
            s.reset_offset.buf_written_clean = 4;
            s.reset_offset.maxwritten_clean = 5;
            s.reset_offset.buf_alloc = 1;
        }

        pgstat_bgwriter_snapshot_cb().unwrap();

        {
            let snap = env.bgwriter_snapshot.borrow();
            assert_eq!(snap.buf_written_clean, 6);
            assert_eq!(snap.maxwritten_clean, 0);
            assert_eq!(snap.buf_alloc, 19);
        }
        assert_eq!(env.lwlock_acquires.borrow().clone(), vec![LW_SHARED]);
        assert_eq!(env.lwlock_releases.get(), 1);
    }
}
