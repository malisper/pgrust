//! Port of `src/backend/utils/activity/pgstat_wal.c` (PostgreSQL 18.3).
//!
//! Implementation of WAL statistics (`PGSTAT_KIND_WAL`, a fixed-numbered stats
//! kind). Kept separate from `pgstat.c` to enforce the line between the
//! statistics access/storage implementation and the details about individual
//! kinds of statistics.
//!
//! `prevWalUsage` is the file-owned backend-local WAL-usage snapshot from the
//! previous `pgstat_report_wal()`, a per-backend C global; it is a
//! `thread_local!` here (one backend == one thread).
//!
//! The fixed-kind callbacks (`init_backend_cb`, `init_shmem_cb`,
//! `reset_all_cb`, `snapshot_cb`, `flush_static_cb`) are registered with the
//! pgstat core's `pgstat_kind_builtin_infos[]` table via [`KindInfoBuilder`]
//! from [`init_seams`]; the core dispatches them, projecting the typed
//! `PgStatShared_Wal` / `PgStat_WalStats` fields of the owner
//! `PgStat_ShmemControl` / `PgStat_Snapshot`.

use core::cell::RefCell;

use instrument as instrument;
use ::lwlock_seams::{
    lwlock_acquire, lwlock_conditional_acquire, lwlock_initialize,
};
use ::activity_pgstat::kind_info::KindInfoBuilder;
use ::activity_pgstat::registry;
use ::init_small_seams::my_proc_number;
use ::instrument::WalUsageAccumDiff;
use ::types_core::instrument::WalUsage;
use ::types_core::TimestampTz;
use ::types_error::PgResult;
use ::types_pgstat::activity_pgstat::PGSTAT_KIND_WAL;
use ::types_pgstat::pgstat_internal::{PgStat_KindInfo, PgStat_ShmemControl, PgStat_Snapshot};
use ::types_storage::{LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, LW_SHARED};

thread_local! {
    /// `static WalUsage prevWalUsage;` — WAL usage counters saved at the
    /// previous `pgstat_report_wal()`, used to compute the per-interval delta.
    static PREV_WAL_USAGE: RefCell<WalUsage> = RefCell::new(WalUsage::default());
}

fn prev_wal_usage() -> WalUsage {
    PREV_WAL_USAGE.with(|p| *p.borrow())
}

fn set_prev_wal_usage(u: WalUsage) {
    PREV_WAL_USAGE.with(|p| *p.borrow_mut() = u);
}

// ---------------------------------------------------------------------------
// pgstat_report_wal.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_report_wal(bool force)`.
///
/// Must be called by processes that generate WAL but do not call
/// `pgstat_report_stat()` (like walwriter). Flushes WAL and IO statistics.
pub fn pgstat_report_wal(force: bool) -> PgResult<()> {
    // like in pgstat.c, don't wait for lock acquisition when !force
    let nowait = !force;

    // flush wal stats
    let _ = pgstat_wal_flush_cb(nowait)?;
    // pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_WAL): pgstat_backend.c,
    // unported — seam-and-panic.
    let _ = pgstat_wal_seams::pgstat_flush_backend_wal::call(nowait);

    // flush IO stats
    stat_seams::pgstat_flush_io::call(nowait)?;
    // pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_IO): pgstat_backend.c,
    // unported — seam-and-panic (the io-seams declaration, owned by pgstat_backend.c).
    let _ = pgstat_io_seams::pgstat_flush_backend_io::call(nowait);

    Ok(())
}

// ---------------------------------------------------------------------------
// pgstat_fetch_stat_wal.
// ---------------------------------------------------------------------------

/// Port of `PgStat_WalStats *pgstat_fetch_stat_wal(void)`. In C this returns a
/// pointer into the snapshot; here it returns a copy of the snapshot's WAL
/// stats.
pub fn pgstat_fetch_stat_wal() -> PgResult<::types_pgstat::activity_pgstat::PgStat_WalStats> {
    pgstat_seams::snapshot_fixed::call(PGSTAT_KIND_WAL)?;
    Ok(::activity_pgstat::local::with_local(|l| l.snapshot.wal))
}

// ---------------------------------------------------------------------------
// have-pending + flush.
// ---------------------------------------------------------------------------

/// Port of `static inline bool pgstat_wal_have_pending(void)`.
pub fn pgstat_wal_have_pending() -> bool {
    instrument::pgWalUsage().wal_records != prev_wal_usage().wal_records
}

/// Port of `bool pgstat_wal_flush_cb(bool nowait)`.
///
/// Calculates how much the WAL usage counters increased since the previous
/// flush and accumulates the delta into the shared WAL statistics. Returns
/// `Ok(true)` if `nowait` was set and the WAL stats lock could not be acquired,
/// `Ok(false)` otherwise.
pub fn pgstat_wal_flush_cb(nowait: bool) -> PgResult<bool> {
    // This function can be called even if nothing at all has happened. Avoid
    // taking the lock for nothing in that case.
    if !pgstat_wal_have_pending() {
        return Ok(false);
    }

    // Calculate how much the WAL usage counters increased.
    let cur = instrument::pgWalUsage();
    let prev = prev_wal_usage();
    let mut wal_usage_diff = WalUsage::default();
    WalUsageAccumDiff(&mut wal_usage_diff, &cur, &prev);

    ::activity_pgstat::local::with_local(|l| {
        let ctl: &mut PgStat_ShmemControl = l
            .shmem
            .as_mut()
            .expect("pgstat shared control not initialized (StatsShmemInit not run)");
        let stats_shmem = &mut ctl.wal;

        // if (!nowait) LWLockAcquire(...); else if (!LWLockConditionalAcquire(...)) return true;
        let guard;
        if !nowait {
            guard = lwlock_acquire::call(&stats_shmem.lock, LW_EXCLUSIVE, my_proc_number::call())?;
        } else {
            match lwlock_conditional_acquire::call(&stats_shmem.lock, LW_EXCLUSIVE)? {
                Some(g) => guard = g,
                None => return Ok(true),
            }
        }

        // WALSTAT_ACC(fld, var_to_add): stats_shmem->stats.wal_counters.fld += var.fld
        let c = &mut stats_shmem.stats.wal_counters;
        c.wal_records += wal_usage_diff.wal_records;
        c.wal_fpi += wal_usage_diff.wal_fpi;
        c.wal_bytes = c.wal_bytes.wrapping_add(wal_usage_diff.wal_bytes);
        c.wal_buffers_full += wal_usage_diff.wal_buffers_full;

        guard.release()?;

        // Save the current counters for the subsequent delta calculation.
        set_prev_wal_usage(cur);

        Ok(false)
    })
}

// ---------------------------------------------------------------------------
// Fixed-kind callbacks.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_wal_init_backend_cb(void)`.
///
/// Initialize `prevWalUsage` with the current `pgWalUsage` so the first
/// `pgstat_wal_flush_cb` computes a correct delta.
pub fn pgstat_wal_init_backend_cb() -> PgResult<()> {
    set_prev_wal_usage(instrument::pgWalUsage());
    Ok(())
}

/// Port of `void pgstat_wal_init_shmem_cb(void *stats)`.
pub fn pgstat_wal_init_shmem_cb(ctl: &mut PgStat_ShmemControl) {
    lwlock_initialize::call(&mut ctl.wal.lock, LWTRANCHE_PGSTATS_DATA);
}

/// Port of `void pgstat_wal_reset_all_cb(TimestampTz ts)`.
pub fn pgstat_wal_reset_all_cb(ctl: &mut PgStat_ShmemControl, ts: TimestampTz) -> PgResult<()> {
    let stats_shmem = &mut ctl.wal;
    let guard = lwlock_acquire::call(&stats_shmem.lock, LW_EXCLUSIVE, my_proc_number::call())?;
    // memset(&stats_shmem->stats, 0, ...); stats_shmem->stats.stat_reset_timestamp = ts;
    stats_shmem.stats = ::types_pgstat::activity_pgstat::PgStat_WalStats::default();
    stats_shmem.stats.stat_reset_timestamp = ts;
    guard.release()
}

/// Port of `void pgstat_wal_snapshot_cb(void)`. The adapter hands us the typed
/// shared control (read) and the snapshot (write).
pub fn pgstat_wal_snapshot_cb(
    ctl: &PgStat_ShmemControl,
    snap: &mut PgStat_Snapshot,
) -> PgResult<()> {
    let stats_shmem = &ctl.wal;
    let guard = lwlock_acquire::call(&stats_shmem.lock, LW_SHARED, my_proc_number::call())?;
    // memcpy(&snapshot.wal, &stats_shmem->stats, ...)
    snap.wal = stats_shmem.stats;
    guard.release()
}

// ---------------------------------------------------------------------------
// Kind registration + seam installation.
// ---------------------------------------------------------------------------

/// The `PgStat_KindInfo` metadata for `PGSTAT_KIND_WAL`
/// (`pgstat.c:pgstat_kind_builtin_infos[PGSTAT_KIND_WAL]`).
///
/// The on-disk (de)serializer reaches the typed `ctl.wal.stats` / `snap.wal`
/// field by projection, so the `shared_*_off` offsets stay 0. `shared_data_len`
/// is `sizeof(((PgStatShared_Wal *) 0)->stats)` = `sizeof(PgStat_WalStats)`.
/// `shared_size` is 0 (WAL is a fixed kind with a dedicated control-block field,
/// not a `custom_data` entry).
fn wal_kind_info() -> PgStat_KindInfo {
    PgStat_KindInfo {
        fixed_amount: true,
        accessed_across_databases: false,
        write_to_file: true,
        shared_size: 0,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        shared_data_off: 0,
        shared_data_len: core::mem::size_of::<::types_pgstat::activity_pgstat::PgStat_WalStats>()
            as u32,
        pending_size: 0,
        name: "wal",
    }
}

/// Register `PGSTAT_KIND_WAL` and install the WAL outward seams.
pub fn init_seams() {
    registry::register(
        KindInfoBuilder::new(PGSTAT_KIND_WAL, wal_kind_info())
            .init_backend_cb(pgstat_wal_init_backend_cb)
            .init_shmem_cb(pgstat_wal_init_shmem_cb)
            .reset_all_cb(pgstat_wal_reset_all_cb)
            .snapshot_cb(pgstat_wal_snapshot_cb)
            .flush_static_cb(pgstat_wal_flush_cb)
            // On-disk (de)serialization of the typed `PgStat_WalStats` field.
            .read_fixed_cb(|ctl, bytes| {
                ctl.wal.stats = ::activity_pgstat::kind_info::pgstat_deserialize_pod::<
                    ::types_pgstat::activity_pgstat::PgStat_WalStats,
                >(bytes);
                Ok(())
            })
            .write_fixed_cb(|snap| {
                ::activity_pgstat::kind_info::pgstat_serialize_pod(&snap.wal)
            }),
    );

    // pgstat_wal.c outward seams: both the walstats-seams (PgResult, 6 callers)
    // and the wal-seams (void) declarations resolve to pgstat_report_wal.
    walstats_seams::pgstat_report_wal::set(pgstat_report_wal);
    pgstat_wal_seams::pgstat_report_wal::set(|force| {
        // The wal-seams shape is void; a genuine error is unreachable on this
        // flush path (it only surfaces LWLock-acquire ereports), but the seam
        // signature has no failure channel, so drop it.
        let _ = pgstat_report_wal(force);
    });
}
