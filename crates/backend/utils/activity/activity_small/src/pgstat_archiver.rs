//! Port of `src/backend/utils/activity/pgstat_archiver.c` (PostgreSQL 18.3).
//!
//! Implementation of archiver statistics. It is kept separate from `pgstat.c`
//! to enforce the line between the statistics access / storage implementation
//! and the details about individual types of statistics.

use crate::changecount::{
    pgstat_begin_changecount_write, pgstat_copy_changecounted_stats,
    pgstat_end_changecount_write,
};
use lwlock_seams::{lwlock_acquire, lwlock_initialize};
use init_small_seams::my_proc_number;
use pgstat_seams::{
    snapshot_fixed, with_shmem_archiver, with_snapshot_archiver,
};
use timestamp_seams::get_current_timestamp;
use types_core::TimestampTz;
use types_error::PgResult;
use types_pgstat::activity_pgstat::{
    PgStatShared_Archiver, PgStat_ArchiverStats, WAL_NAME_LEN,
};
use types_pgstat::pgstat_internal::{PgStat_ShmemControl, PgStat_Snapshot};
use types_storage::{LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, LW_SHARED};

pub use types_pgstat::activity_pgstat::PGSTAT_KIND_ARCHIVER;

/// Report archiver statistics.
///
/// Port of `pgstat_report_archiver(const char *xlog, bool failed)`.
///
/// C copies exactly `sizeof(last_*_wal)` (= `WAL_NAME_LEN`) bytes from `xlog`
/// with `memcpy`; the fixed-size array makes that contract compile-time.
pub fn pgstat_report_archiver(xlog: &[u8; WAL_NAME_LEN], failed: bool) {
    // TimestampTz now = GetCurrentTimestamp();
    let now: TimestampTz = get_current_timestamp::call();

    // PgStatShared_Archiver *stats_shmem = &pgStatLocal.shmem->archiver;
    with_shmem_archiver::call(&mut |stats_shmem: &mut PgStatShared_Archiver| {
        pgstat_begin_changecount_write(&stats_shmem.changecount);

        if failed {
            stats_shmem.stats.failed_count += 1;
            // memcpy(&stats_shmem->stats.last_failed_wal, xlog, sizeof(...));
            stats_shmem.stats.last_failed_wal = *xlog;
            stats_shmem.stats.last_failed_timestamp = now;
        } else {
            stats_shmem.stats.archived_count += 1;
            // memcpy(&stats_shmem->stats.last_archived_wal, xlog, sizeof(...));
            stats_shmem.stats.last_archived_wal = *xlog;
            stats_shmem.stats.last_archived_timestamp = now;
        }

        pgstat_end_changecount_write(&stats_shmem.changecount);
    });
}

/// Support function for the SQL-callable `pgstat*` functions. In C this
/// returns a pointer to the snapshot's archiver statistics struct; here it is
/// a copy of that snapshot entry.
///
/// Port of `PgStat_ArchiverStats *pgstat_fetch_stat_archiver(void)`.
pub fn pgstat_fetch_stat_archiver() -> PgResult<PgStat_ArchiverStats> {
    snapshot_fixed::call(PGSTAT_KIND_ARCHIVER)?;

    // return &pgStatLocal.snapshot.archiver;
    let mut snap = PgStat_ArchiverStats::default();
    with_snapshot_archiver::call(&mut |s: &mut PgStat_ArchiverStats| snap = *s);
    Ok(snap)
}

/// Port of `pgstat_archiver_init_shmem_cb(void *stats)`.
pub fn pgstat_archiver_init_shmem_cb(stats: &mut PgStatShared_Archiver) {
    lwlock_initialize::call(&mut stats.lock, LWTRANCHE_PGSTATS_DATA);
}

/// Port of `pgstat_archiver_reset_all_cb(TimestampTz ts)`. The adapter hands us
/// the typed shared control; we project its `archiver` region
/// (`&pgStatLocal.shmem->archiver`).
pub fn pgstat_archiver_reset_all_cb(
    ctl: &mut PgStat_ShmemControl,
    ts: TimestampTz,
) -> PgResult<()> {
    // PgStatShared_Archiver *stats_shmem = &pgStatLocal.shmem->archiver;
    let stats_shmem = &mut ctl.archiver;

    // see explanation above PgStatShared_Archiver for the reset protocol
    let guard = lwlock_acquire::call(&stats_shmem.lock, LW_EXCLUSIVE, my_proc_number::call())?;
    {
        // pgstat_copy_changecounted_stats(&stats_shmem->reset_offset,
        //                                 &stats_shmem->stats, sizeof(...),
        //                                 &stats_shmem->changecount);
        let PgStatShared_Archiver {
            ref mut reset_offset,
            ref stats,
            ref changecount,
            ..
        } = *stats_shmem;
        pgstat_copy_changecounted_stats(reset_offset, stats, changecount);
    }
    stats_shmem.stats.stat_reset_timestamp = ts;
    guard.release()
}

/// Port of `pgstat_archiver_snapshot_cb(void)`. The adapter hands us the typed
/// shared control (read, `&pgStatLocal.shmem->archiver`) and the snapshot
/// (write, `&pgStatLocal.snapshot.archiver`).
pub fn pgstat_archiver_snapshot_cb(
    ctl: &PgStat_ShmemControl,
    snap: &mut PgStat_Snapshot,
) -> PgResult<()> {
    // PgStatShared_Archiver *stats_shmem = &pgStatLocal.shmem->archiver;
    let stats_shmem = &ctl.archiver;
    // PgStat_ArchiverStats *stat_snap = &pgStatLocal.snapshot.archiver;
    let stat_snap = &mut snap.archiver;

    pgstat_copy_changecounted_stats(
        stat_snap,
        &stats_shmem.stats,
        &stats_shmem.changecount,
    );

    let guard = lwlock_acquire::call(&stats_shmem.lock, LW_SHARED, my_proc_number::call())?;
    // memcpy(&reset, reset_offset, sizeof(stats_shmem->stats));
    let reset = stats_shmem.reset_offset;
    guard.release()?;

    // compensate by reset offsets
    if stat_snap.archived_count == reset.archived_count {
        stat_snap.last_archived_wal[0] = 0;
        stat_snap.last_archived_timestamp = 0;
    }
    stat_snap.archived_count -= reset.archived_count;

    if stat_snap.failed_count == reset.failed_count {
        stat_snap.last_failed_wal[0] = 0;
        stat_snap.last_failed_timestamp = 0;
    }
    stat_snap.failed_count -= reset.failed_count;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_seams::setup;

    /// Build a `WAL_NAME_LEN`-byte buffer from a name, NUL-padded, like the
    /// caller-supplied fixed C buffer.
    fn wal_buf(name: &[u8]) -> [u8; WAL_NAME_LEN] {
        let mut buf = [0u8; WAL_NAME_LEN];
        buf[..name.len()].copy_from_slice(name);
        buf
    }

    #[test]
    fn report_archiver_success() {
        let env = setup();
        env.now.set(12345);

        let xlog = wal_buf(b"000000010000000000000001");
        pgstat_report_archiver(&xlog, false);

        let shmem = env.archiver_shmem.borrow();
        assert_eq!(shmem.stats.archived_count, 1);
        assert_eq!(shmem.stats.failed_count, 0);
        assert_eq!(shmem.stats.last_archived_wal, xlog);
        assert_eq!(shmem.stats.last_archived_timestamp, 12345);
        // changecount incremented twice (begin + end) -> even again.
        assert_eq!(shmem.changecount.load(core::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn report_archiver_failure() {
        let env = setup();
        env.now.set(999);

        let xlog = wal_buf(b"000000010000000000000002");
        pgstat_report_archiver(&xlog, true);

        let shmem = env.archiver_shmem.borrow();
        assert_eq!(shmem.stats.failed_count, 1);
        assert_eq!(shmem.stats.archived_count, 0);
        assert_eq!(shmem.stats.last_failed_wal, xlog);
        assert_eq!(shmem.stats.last_failed_timestamp, 999);
        assert_eq!(shmem.changecount.load(core::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn fetch_stat_archiver_snapshots_and_returns_copy() {
        let env = setup();
        env.archiver_snapshot.borrow_mut().archived_count = 42;

        let stats = pgstat_fetch_stat_archiver().unwrap();
        assert_eq!(stats.archived_count, 42);
        assert_eq!(
            env.snapshot_fixed_kinds.borrow().clone(),
            vec![PGSTAT_KIND_ARCHIVER]
        );
    }

    #[test]
    fn init_shmem_cb_initializes_lock() {
        let env = setup();

        let mut shared = PgStatShared_Archiver::default();
        pgstat_archiver_init_shmem_cb(&mut shared);
        assert_eq!(shared.lock.tranche, LWTRANCHE_PGSTATS_DATA as u16);
        assert_eq!(env.lwlock_inits.get(), 1);
    }

    #[test]
    fn reset_all_cb_copies_offset_and_sets_timestamp() {
        let env = setup();
        let mut ctl = PgStat_ShmemControl::default();
        ctl.archiver.stats.archived_count = 10;
        ctl.archiver.stats.failed_count = 3;

        pgstat_archiver_reset_all_cb(&mut ctl, 555).unwrap();

        // reset_offset is a copy of the current stats.
        assert_eq!(ctl.archiver.reset_offset.archived_count, 10);
        assert_eq!(ctl.archiver.reset_offset.failed_count, 3);
        assert_eq!(ctl.archiver.stats.stat_reset_timestamp, 555);
        assert_eq!(env.lwlock_acquires.borrow().clone(), vec![LW_EXCLUSIVE]);
        assert_eq!(env.lwlock_releases.get(), 1);
    }

    #[test]
    fn snapshot_cb_compensates_by_reset_offsets() {
        let env = setup();
        let mut ctl = PgStat_ShmemControl::default();
        {
            let s = &mut ctl.archiver;
            s.stats.archived_count = 10;
            s.stats.last_archived_wal = wal_buf(b"arch-wal");
            s.stats.last_archived_timestamp = 700;
            s.stats.failed_count = 5;
            s.stats.last_failed_wal = wal_buf(b"fail-wal");
            s.stats.last_failed_timestamp = 800;
            // Reset offsets: archived fully reset to current (==), failed partial.
            s.reset_offset.archived_count = 10;
            s.reset_offset.failed_count = 2;
        }
        let mut snap = PgStat_Snapshot::default();

        pgstat_archiver_snapshot_cb(&ctl, &mut snap).unwrap();

        // archived_count == reset.archived_count -> cleared then subtracted.
        assert_eq!(snap.archiver.archived_count, 0);
        assert_eq!(snap.archiver.last_archived_wal[0], 0);
        assert_eq!(snap.archiver.last_archived_timestamp, 0);
        // failed_count != reset.failed_count -> not cleared, just subtracted.
        assert_eq!(snap.archiver.failed_count, 3);
        assert_eq!(snap.archiver.last_failed_wal, wal_buf(b"fail-wal"));
        assert_eq!(snap.archiver.last_failed_timestamp, 800);
        assert_eq!(env.lwlock_acquires.borrow().clone(), vec![LW_SHARED]);
        assert_eq!(env.lwlock_releases.get(), 1);
    }
}
