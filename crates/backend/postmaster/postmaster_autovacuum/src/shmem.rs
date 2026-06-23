//! Public API and shared-memory plumbing (`autovacuum.c` lines 3282-3475).
//!
//! Status inquiry, work-item requests, startup init, the shmem size/init
//! routines, the `check_autovacuum_work_mem` GUC hook, and the
//! worker-availability / worker-GUC sanity helpers.

extern crate alloc;
use alloc::format;

use ::utils_error::{ereport, PgResult};
use ::types_error::{ErrorLocation, ERRCODE_INVALID_PARAMETER_VALUE, WARNING};

use ::types_core::{BlockNumber, Oid};

use crate::core::{self, NUM_WORKITEMS};
use autovacuum_ext_seams as seam;

/// `ErrorLocation` for `autovacuum.c` WARNINGs raised in this module.
#[inline]
fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/postmaster/autovacuum.c", lineno, funcname)
}

/// `bool AutoVacuumingActive(void)` (`autovacuum.c` lines 3287-3293) — whether
/// autovacuum is enabled (the GUC and `track_counts` are on).
pub fn AutoVacuumingActive() -> bool {
    // `autovacuum_start_daemon` is this crate's own GUC variable (its accessor
    // is installed, so the engine writes the live value). `pgstat_track_counts`
    // is a pgstat-owned process-global; read its live value through the seam
    // rather than a stale private shadow (the GUC engine never updates a shadow
    // cell here, so it would otherwise pin `AutoVacuumingActive()` false).
    if !core::autovacuum_start_daemon() || !seam::pgstat_track_counts::call() {
        return false;
    }
    true
}

/// `void autovac_init(void)` (`autovacuum.c` lines 3341-3352) — called from the
/// postmaster at server startup; warns if autovacuum is enabled but
/// `track_counts` is off, else sanity-checks the worker GUCs.
pub fn autovac_init() -> PgResult<()> {
    if !core::autovacuum_start_daemon() {
        // return
    } else if !seam::pgstat_track_counts::call() {
        ereport(WARNING)
            .errmsg("autovacuum not started because of misconfiguration")
            .errhint("Enable the \"track_counts\" option.")
            .finish(errloc(3349, "autovac_init"))?;
    } else {
        check_av_worker_gucs()?;
    }
    Ok(())
}

/// `Size AutoVacuumShmemSize(void)` (`autovacuum.c` lines 3358-3371) — the size
/// of the autovacuum shmem area (the struct plus the `WorkerInfo` array).
///
/// The byte layout (`sizeof(AutoVacuumShmemStruct)` / `sizeof(WorkerInfoData)`)
/// is owned by the substrate; we expose the worker-slot count that drives it.
pub fn AutoVacuumShmemSize() -> usize {
    core::autovacuum_worker_slots() as usize
}

/// `void AutoVacuumShmemInit(void)` (`autovacuum.c` lines 3377-3417) — allocate
/// and initialize the autovacuum shmem area and seed the `WorkerInfo` free
/// list. The first-creation branch (`!IsUnderPostmaster`) and its free-list
/// seeding run in the substrate against the freshly-allocated shmem.
pub fn AutoVacuumShmemInit() -> PgResult<()> {
    seam::autovacuum_shmem_init::call(core::autovacuum_worker_slots())
}

/// `bool check_autovacuum_work_mem(int *newval, void **extra, GucSource source)`
/// (`autovacuum.c` lines 3422-3443) — GUC check hook for `autovacuum_work_mem`.
///
/// Returns the (possibly clamped) value alongside `true`/`false`. `-1` indicates
/// fallback (left untouched); other values are clamped to at least 64 kB.
pub fn check_autovacuum_work_mem(newval: i32) -> (i32, bool) {
    /*
     * -1 indicates fallback.
     *
     * If we haven't yet changed the boot_val default of -1, just let it be.
     * Autovacuum will look to maintenance_work_mem instead.
     */
    if newval == -1 {
        return (newval, true);
    }

    /*
     * We clamp manually-set values to at least 64kB.  Since
     * maintenance_work_mem is always set to at least this value, do the same
     * here.
     */
    let newval = if newval < 64 { 64 } else { newval };

    (newval, true)
}

/// `static bool av_worker_available(void)` (`autovacuum.c` lines 3448-3460) —
/// whether a free worker slot is available (caller holds `AutovacuumLock`).
pub fn av_worker_available() -> bool {
    let free_slots: i32 = seam::free_workers_count::call() as i32;

    let mut reserved_slots: i32 = core::autovacuum_worker_slots() - core::autovacuum_max_workers();
    reserved_slots = ::core::cmp::max(0, reserved_slots);

    free_slots > reserved_slots
}

/// `static void check_av_worker_gucs(void)` (`autovacuum.c` lines 3465-3475) —
/// warn if `autovacuum_worker_slots < autovacuum_max_workers`.
pub fn check_av_worker_gucs() -> PgResult<()> {
    if core::autovacuum_worker_slots() < core::autovacuum_max_workers() {
        let (max_workers, worker_slots) =
            (core::autovacuum_max_workers(), core::autovacuum_worker_slots());
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "\"autovacuum_max_workers\" ({max_workers}) should be less than or equal to \"autovacuum_worker_slots\" ({worker_slots})"
            ))
            .errdetail(format!(
                "The server will only start up to \"autovacuum_worker_slots\" ({worker_slots}) autovacuum workers at a given time."
            ))
            .finish(errloc(3474, "check_av_worker_gucs"))?;
    }
    Ok(())
}

/// `bool AutoVacuumRequestWork(AutoVacuumWorkItemType type, Oid relationId,
/// BlockNumber blkno)` (`autovacuum.c` lines 3299-3333) — request one work item
/// to the next autovacuum run processing our database. Returns false if the
/// request can't be recorded.
pub fn AutoVacuumRequestWork(av_type: i32, relation_id: Oid, blkno: BlockNumber) -> PgResult<bool> {
    let mut result = false;

    seam::autovacuum_lock_acquire_exclusive::call()?;

    /*
     * Locate an unused work item and fill it with the given data.
     */
    let database = seam::my_database_id::call();
    for i in 0..NUM_WORKITEMS {
        if seam::workitem_get_used::call(i) {
            continue;
        }

        seam::workitem_fill::call(i, av_type, database, relation_id, blkno);
        result = true;

        /* done */
        break;
    }

    seam::autovacuum_lock_release::call()?;

    Ok(result)
}
