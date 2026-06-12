//! Port of `src/backend/utils/activity/pgstat_archiver.c` (PostgreSQL 18.3).
//!
//! Implementation of archiver statistics. It is kept separate from `pgstat.c`
//! to enforce the line between the statistics access / storage implementation
//! and the details about individual types of statistics.

use crate::changecount::{
    pgstat_begin_changecount_write, pgstat_copy_changecounted_stats,
    pgstat_end_changecount_write,
};
use backend_storage_lmgr_lwlock_seams::{lwlock_acquire, lwlock_initialize, lwlock_release};
use backend_utils_activity_pgstat_seams::{
    snapshot_fixed, with_shmem_archiver, with_snapshot_archiver,
};
use backend_utils_adt_timestamp_seams::get_current_timestamp;
use types_core::TimestampTz;
use types_error::PgResult;
use types_pgstat::activity_pgstat::{
    PgStatShared_Archiver, PgStat_ArchiverStats, WAL_NAME_LEN,
};
use types_storage::{LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, LW_SHARED};

/// `PGSTAT_KIND_ARCHIVER` (`utils/pgstat_kind.h`).
pub const PGSTAT_KIND_ARCHIVER: u32 = 7;

/// Report archiver statistics.
///
/// Port of `pgstat_report_archiver(const char *xlog, bool failed)`.
///
/// `xlog` must be at least [`WAL_NAME_LEN`] bytes long; C copies exactly
/// `sizeof(last_*_wal)` (= `WAL_NAME_LEN`) bytes from `xlog` with `memcpy`.
pub fn pgstat_report_archiver(xlog: &[u8], failed: bool) {
    // TimestampTz now = GetCurrentTimestamp();
    let now: TimestampTz = get_current_timestamp::call();

    // PgStatShared_Archiver *stats_shmem = &pgStatLocal.shmem->archiver;
    with_shmem_archiver::call(&mut |stats_shmem: &mut PgStatShared_Archiver| {
        pgstat_begin_changecount_write(&mut stats_shmem.changecount);

        if failed {
            stats_shmem.stats.failed_count += 1;
            // memcpy(&stats_shmem->stats.last_failed_wal, xlog, sizeof(...));
            stats_shmem
                .stats
                .last_failed_wal
                .copy_from_slice(&xlog[..WAL_NAME_LEN]);
            stats_shmem.stats.last_failed_timestamp = now;
        } else {
            stats_shmem.stats.archived_count += 1;
            // memcpy(&stats_shmem->stats.last_archived_wal, xlog, sizeof(...));
            stats_shmem
                .stats
                .last_archived_wal
                .copy_from_slice(&xlog[..WAL_NAME_LEN]);
            stats_shmem.stats.last_archived_timestamp = now;
        }

        pgstat_end_changecount_write(&mut stats_shmem.changecount);
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

/// Port of `pgstat_archiver_reset_all_cb(TimestampTz ts)`.
pub fn pgstat_archiver_reset_all_cb(ts: TimestampTz) -> PgResult<()> {
    let mut res: PgResult<()> = Ok(());
    with_shmem_archiver::call(&mut |stats_shmem: &mut PgStatShared_Archiver| {
        res = (|| {
            // see explanation above PgStatShared_Archiver for the reset protocol
            lwlock_acquire::call(&mut stats_shmem.lock, LW_EXCLUSIVE)?;
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
            lwlock_release::call(&mut stats_shmem.lock)
        })();
    });
    res
}

/// Port of `pgstat_archiver_snapshot_cb(void)`.
pub fn pgstat_archiver_snapshot_cb() -> PgResult<()> {
    // PgStat_ArchiverStats *stat_snap = &pgStatLocal.snapshot.archiver;
    let mut stat_snap = PgStat_ArchiverStats::default();
    with_snapshot_archiver::call(&mut |s: &mut PgStat_ArchiverStats| stat_snap = *s);

    let mut shmem_res: PgResult<PgStat_ArchiverStats> = Ok(PgStat_ArchiverStats::default());
    with_shmem_archiver::call(&mut |stats_shmem: &mut PgStatShared_Archiver| {
        shmem_res = (|| {
            pgstat_copy_changecounted_stats(
                &mut stat_snap,
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
    let reset: PgStat_ArchiverStats = shmem_res?;

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

    with_snapshot_archiver::call(&mut |s: &mut PgStat_ArchiverStats| *s = stat_snap);
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
        assert_eq!(shmem.changecount, 2);
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
        assert_eq!(shmem.changecount, 2);
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
        {
            let mut s = env.archiver_shmem.borrow_mut();
            s.stats.archived_count = 10;
            s.stats.failed_count = 3;
        }

        pgstat_archiver_reset_all_cb(555).unwrap();

        {
            let shmem = env.archiver_shmem.borrow();
            // reset_offset is a copy of the current stats.
            assert_eq!(shmem.reset_offset.archived_count, 10);
            assert_eq!(shmem.reset_offset.failed_count, 3);
            assert_eq!(shmem.stats.stat_reset_timestamp, 555);
        }
        assert_eq!(env.lwlock_acquires.borrow().clone(), vec![LW_EXCLUSIVE]);
        assert_eq!(env.lwlock_releases.get(), 1);
    }

    #[test]
    fn snapshot_cb_compensates_by_reset_offsets() {
        let env = setup();
        {
            let mut s = env.archiver_shmem.borrow_mut();
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

        pgstat_archiver_snapshot_cb().unwrap();

        {
            let snap = env.archiver_snapshot.borrow();
            // archived_count == reset.archived_count -> cleared then subtracted.
            assert_eq!(snap.archived_count, 0);
            assert_eq!(snap.last_archived_wal[0], 0);
            assert_eq!(snap.last_archived_timestamp, 0);
            // failed_count != reset.failed_count -> not cleared, just subtracted.
            assert_eq!(snap.failed_count, 3);
            assert_eq!(snap.last_failed_wal, wal_buf(b"fail-wal"));
            assert_eq!(snap.last_failed_timestamp, 800);
        }
        assert_eq!(env.lwlock_acquires.borrow().clone(), vec![LW_SHARED]);
        assert_eq!(env.lwlock_releases.get(), 1);
    }
}
