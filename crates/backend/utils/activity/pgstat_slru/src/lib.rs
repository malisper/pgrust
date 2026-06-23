//! Port of `src/backend/utils/activity/pgstat_slru.c` (PostgreSQL 18.3).
//!
//! Implementation of SLRU statistics (`PGSTAT_KIND_SLRU`, a fixed-numbered stats
//! kind). Kept separate from `pgstat.c` to enforce the line between the
//! statistics access/storage implementation and the details about individual
//! kinds of statistics.
//!
//! `pending_SLRUStats` / `have_slrustats` are the file-owned backend-local
//! pending buffers (per-backend C globals reported within critical sections, so
//! they use static memory to avoid allocation); they are `thread_local!`s here
//! (one backend == one thread).
//!
//! The fixed-kind callbacks (`init_shmem_cb`, `reset_all_cb`, `snapshot_cb`,
//! `flush_static_cb`) are registered with the pgstat core's
//! `pgstat_kind_builtin_infos[]` table via [`KindInfoBuilder`] from
//! [`init_seams`]; the core dispatches them, projecting the typed
//! `PgStatShared_SLRU` / `PgStat_SLRUStats` fields of the owner
//! `PgStat_ShmemControl` / `PgStat_Snapshot`.

use core::cell::{Cell, RefCell};

use ::lwlock_seams::{lwlock_acquire, lwlock_initialize};
use ::activity_pgstat::kind_info::KindInfoBuilder;
use ::activity_pgstat::registry;
use ::init_small_seams::my_proc_number;
use ::timestamp_seams::get_current_timestamp;
use ::types_core::TimestampTz;
use ::types_error::PgResult;
use ::types_pgstat::activity_pgstat::{PgStat_SLRUStats, PGSTAT_KIND_SLRU};
use ::types_pgstat::pgstat_internal::{
    PgStat_KindInfo, PgStat_ShmemControl, PgStat_Snapshot, SLRU_NAMES, SLRU_NUM_ELEMENTS,
};
use ::types_storage::{LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, LW_SHARED};

thread_local! {
    /// `static PgStat_SLRUStats pending_SLRUStats[SLRU_NUM_ELEMENTS];` — SLRU
    /// statistics counts waiting to be flushed out. Entries are one-to-one with
    /// `slru_names[]`. Inits to zeroes.
    static PENDING_SLRU_STATS: RefCell<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]> =
        RefCell::new([PgStat_SLRUStats::default(); SLRU_NUM_ELEMENTS]);

    /// `static bool have_slrustats = false;` — whether any SLRU counter has been
    /// bumped since the last flush.
    static HAVE_SLRUSTATS: Cell<bool> = const { Cell::new(false) };
}

// ---------------------------------------------------------------------------
// get_slru_entry — the pending-counter accessor used by the count functions.
// ---------------------------------------------------------------------------

/// Port of `static inline PgStat_SLRUStats *get_slru_entry(int slru_idx)`,
/// adapted to run `f` on the pending entry (the C function returns a pointer the
/// caller immediately mutates; here we pass the mutation in).
///
/// Sets `have_slrustats` (and, in C, `pgstat_report_fixed`). The
/// `pgstat_report_fixed` flag is a pgstat.c optimization that lets
/// `pgstat_report_stat` skip the static-flush scan when no fixed kind has
/// pending data; the ported core does not model it (the per-kind
/// `flush_static_cb` short-circuits on its own `have_slrustats` check), so
/// setting it would be a no-op, exactly as documented in the IO crate.
fn get_slru_entry<R>(slru_idx: i32, f: impl FnOnce(&mut PgStat_SLRUStats) -> R) -> R {
    // pgstat_assert_is_up();
    // Assert(IsUnderPostmaster || !IsPostmasterEnvironment);
    debug_assert!((slru_idx >= 0) && ((slru_idx as usize) < SLRU_NUM_ELEMENTS));

    HAVE_SLRUSTATS.with(|h| h.set(true));

    PENDING_SLRU_STATS.with(|p| f(&mut p.borrow_mut()[slru_idx as usize]))
}

// ---------------------------------------------------------------------------
// pgstat_reset_slru.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_reset_slru(const char *name)`.
///
/// Reset counters for a single SLRU. Permission checking is managed through the
/// normal GRANT system.
pub fn pgstat_reset_slru(name: &str) -> PgResult<()> {
    let ts = get_current_timestamp::call();
    pgstat_reset_slru_counter_internal(pgstat_get_slru_index(name), ts)
}

// ---------------------------------------------------------------------------
// SLRU statistics count accumulation functions --- called from slru.c.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_count_slru_page_zeroed(int slru_idx)`.
pub fn pgstat_count_slru_page_zeroed(slru_idx: i32) {
    get_slru_entry(slru_idx, |e| e.blocks_zeroed += 1);
}

/// Port of `void pgstat_count_slru_page_hit(int slru_idx)`.
pub fn pgstat_count_slru_page_hit(slru_idx: i32) {
    get_slru_entry(slru_idx, |e| e.blocks_hit += 1);
}

/// Port of `void pgstat_count_slru_page_exists(int slru_idx)`.
pub fn pgstat_count_slru_page_exists(slru_idx: i32) {
    get_slru_entry(slru_idx, |e| e.blocks_exists += 1);
}

/// Port of `void pgstat_count_slru_page_read(int slru_idx)`.
pub fn pgstat_count_slru_page_read(slru_idx: i32) {
    get_slru_entry(slru_idx, |e| e.blocks_read += 1);
}

/// Port of `void pgstat_count_slru_page_written(int slru_idx)`.
pub fn pgstat_count_slru_page_written(slru_idx: i32) {
    get_slru_entry(slru_idx, |e| e.blocks_written += 1);
}

/// Port of `void pgstat_count_slru_flush(int slru_idx)`.
pub fn pgstat_count_slru_flush(slru_idx: i32) {
    get_slru_entry(slru_idx, |e| e.flush += 1);
}

/// Port of `void pgstat_count_slru_truncate(int slru_idx)`.
pub fn pgstat_count_slru_truncate(slru_idx: i32) {
    get_slru_entry(slru_idx, |e| e.truncate += 1);
}

// ---------------------------------------------------------------------------
// fetch / name / index.
// ---------------------------------------------------------------------------

/// Port of `PgStat_SLRUStats *pgstat_fetch_slru(void)`. In C this returns a
/// pointer into the snapshot's SLRU array; here it returns a copy.
pub fn pgstat_fetch_slru() -> PgResult<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]> {
    pgstat_seams::snapshot_fixed::call(PGSTAT_KIND_SLRU)?;
    Ok(::activity_pgstat::local::with_local(|l| l.snapshot.slru))
}

/// Port of `const char *pgstat_get_slru_name(int slru_idx)`.
///
/// Returns the SLRU name for an index. The index may be above
/// `SLRU_NUM_ELEMENTS`, in which case this returns `None` (C `NULL`).
pub fn pgstat_get_slru_name(slru_idx: i32) -> Option<&'static str> {
    if slru_idx < 0 || (slru_idx as usize) >= SLRU_NUM_ELEMENTS {
        return None;
    }
    Some(SLRU_NAMES[slru_idx as usize])
}

/// Port of `int pgstat_get_slru_index(const char *name)`.
///
/// Determine index of entry for a SLRU with a given name. If there's no exact
/// match, returns index of the last "other" entry used for SLRUs defined in
/// external projects.
pub fn pgstat_get_slru_index(name: &str) -> i32 {
    for (i, n) in SLRU_NAMES.iter().enumerate() {
        if *n == name {
            return i as i32;
        }
    }
    // return index of the last entry (which is the "other" one)
    (SLRU_NUM_ELEMENTS - 1) as i32
}

// ---------------------------------------------------------------------------
// flush.
// ---------------------------------------------------------------------------

/// Port of `bool pgstat_slru_flush_cb(bool nowait)`.
///
/// Flush out locally pending SLRU stats entries. Returns `Ok(true)` if `nowait`
/// was set and the lock could not be acquired; otherwise `Ok(false)`.
pub fn pgstat_slru_flush_cb(nowait: bool) -> PgResult<bool> {
    if !HAVE_SLRUSTATS.with(|h| h.get()) {
        return Ok(false);
    }

    ::activity_pgstat::local::with_local(|l| {
        let ctl: &mut PgStat_ShmemControl = l
            .shmem
            .as_mut()
            .expect("pgstat shared control not initialized (StatsShmemInit not run)");
        let stats_shmem = &mut ctl.slru;

        // if (!nowait) LWLockAcquire(...); else if (!LWLockConditionalAcquire(...)) return true;
        let guard;
        if !nowait {
            guard = lwlock_acquire::call(&stats_shmem.lock, LW_EXCLUSIVE, my_proc_number::call())?;
        } else {
            match ::lwlock_seams::lwlock_conditional_acquire::call(
                &stats_shmem.lock,
                LW_EXCLUSIVE,
            )? {
                Some(g) => guard = g,
                None => return Ok(true),
            }
        }

        PENDING_SLRU_STATS.with(|p| {
            let pending = &mut *p.borrow_mut();
            for i in 0..SLRU_NUM_ELEMENTS {
                let sharedent = &mut stats_shmem.stats[i];
                let pendingent = &pending[i];
                // SLRU_ACC(fld): sharedent->fld += pendingent->fld
                sharedent.blocks_zeroed += pendingent.blocks_zeroed;
                sharedent.blocks_hit += pendingent.blocks_hit;
                sharedent.blocks_read += pendingent.blocks_read;
                sharedent.blocks_written += pendingent.blocks_written;
                sharedent.blocks_exists += pendingent.blocks_exists;
                sharedent.flush += pendingent.flush;
                sharedent.truncate += pendingent.truncate;
            }

            // done, clear the pending entry
            // MemSet(pending_SLRUStats, 0, sizeof(pending_SLRUStats));
            *pending = [PgStat_SLRUStats::default(); SLRU_NUM_ELEMENTS];
        });

        guard.release()?;

        HAVE_SLRUSTATS.with(|h| h.set(false));

        Ok(false)
    })
}

// ---------------------------------------------------------------------------
// Fixed-kind callbacks.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_slru_init_shmem_cb(void *stats)`.
pub fn pgstat_slru_init_shmem_cb(ctl: &mut PgStat_ShmemControl) {
    lwlock_initialize::call(&mut ctl.slru.lock, LWTRANCHE_PGSTATS_DATA);
}

/// Port of `void pgstat_slru_reset_all_cb(TimestampTz ts)`.
pub fn pgstat_slru_reset_all_cb(ctl: &mut PgStat_ShmemControl, ts: TimestampTz) -> PgResult<()> {
    for i in 0..SLRU_NUM_ELEMENTS {
        pgstat_reset_slru_counter_internal_ctl(ctl, i, ts)?;
    }
    Ok(())
}

/// Port of `void pgstat_slru_snapshot_cb(void)`. The adapter hands us the typed
/// shared control (read) and the snapshot (write).
pub fn pgstat_slru_snapshot_cb(
    ctl: &PgStat_ShmemControl,
    snap: &mut PgStat_Snapshot,
) -> PgResult<()> {
    let stats_shmem = &ctl.slru;
    let guard = lwlock_acquire::call(&stats_shmem.lock, LW_SHARED, my_proc_number::call())?;
    // memcpy(pgStatLocal.snapshot.slru, &stats_shmem->stats, sizeof(stats_shmem->stats));
    snap.slru = stats_shmem.stats;
    guard.release()
}

// ---------------------------------------------------------------------------
// pgstat_reset_slru_counter_internal.
// ---------------------------------------------------------------------------

/// Port of `static void pgstat_reset_slru_counter_internal(int index,
/// TimestampTz ts)`. Reaches `pgStatLocal.shmem->slru` through the owner local
/// state.
fn pgstat_reset_slru_counter_internal(index: i32, ts: TimestampTz) -> PgResult<()> {
    ::activity_pgstat::local::with_local(|l| {
        let ctl: &mut PgStat_ShmemControl = l
            .shmem
            .as_mut()
            .expect("pgstat shared control not initialized (StatsShmemInit not run)");
        pgstat_reset_slru_counter_internal_ctl(ctl, index as usize, ts)
    })
}

/// The body of `pgstat_reset_slru_counter_internal` given a control block — used
/// both by [`pgstat_reset_slru_counter_internal`] (which fetches it from local
/// state) and [`pgstat_slru_reset_all_cb`] (handed the typed control block by
/// the registry adapter, so it must not re-borrow `pgStatLocal`).
fn pgstat_reset_slru_counter_internal_ctl(
    ctl: &mut PgStat_ShmemControl,
    index: usize,
    ts: TimestampTz,
) -> PgResult<()> {
    let stats_shmem = &mut ctl.slru;
    let guard = lwlock_acquire::call(&stats_shmem.lock, LW_EXCLUSIVE, my_proc_number::call())?;
    // memset(&stats_shmem->stats[index], 0, ...); stats_shmem->stats[index].stat_reset_timestamp = ts;
    stats_shmem.stats[index] = PgStat_SLRUStats::default();
    stats_shmem.stats[index].stat_reset_timestamp = ts;
    guard.release()
}

// ---------------------------------------------------------------------------
// Kind registration + seam installation.
// ---------------------------------------------------------------------------

/// The `PgStat_KindInfo` metadata for `PGSTAT_KIND_SLRU`
/// (`pgstat.c:pgstat_kind_builtin_infos[PGSTAT_KIND_SLRU]`).
///
/// The on-disk (de)serializer reaches the typed `ctl.slru.stats` / `snap.slru`
/// field by projection, so the `shared_*_off` offsets stay 0. `shared_data_len`
/// is `sizeof(((PgStatShared_SLRU *) 0)->stats)` = the full
/// `[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]` array. SLRU is a fixed kind with a
/// dedicated control-block field, so `shared_size` is 0.
fn slru_kind_info() -> PgStat_KindInfo {
    use ::types_pgstat::activity_pgstat::PgStat_SLRUStats;
    use ::types_pgstat::pgstat_internal::SLRU_NUM_ELEMENTS;
    PgStat_KindInfo {
        fixed_amount: true,
        accessed_across_databases: false,
        write_to_file: true,
        shared_size: 0,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        shared_data_off: 0,
        shared_data_len: core::mem::size_of::<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]>() as u32,
        pending_size: 0,
        name: "slru",
    }
}

/// Register `PGSTAT_KIND_SLRU` and install the SLRU outward seams.
pub fn init_seams() {
    use ::types_pgstat::activity_pgstat::PgStat_SLRUStats;
    use ::types_pgstat::pgstat_internal::SLRU_NUM_ELEMENTS;
    registry::register(
        KindInfoBuilder::new(PGSTAT_KIND_SLRU, slru_kind_info())
            .init_shmem_cb(pgstat_slru_init_shmem_cb)
            .reset_all_cb(pgstat_slru_reset_all_cb)
            .snapshot_cb(pgstat_slru_snapshot_cb)
            .flush_static_cb(pgstat_slru_flush_cb)
            // On-disk (de)serialization of the typed SLRU stats array.
            .read_fixed_cb(|ctl, bytes| {
                ctl.slru.stats = ::activity_pgstat::kind_info::pgstat_deserialize_pod::<
                    [PgStat_SLRUStats; SLRU_NUM_ELEMENTS],
                >(bytes);
                Ok(())
            })
            .write_fixed_cb(|snap| {
                ::activity_pgstat::kind_info::pgstat_serialize_pod(&snap.slru)
            }),
    );

    // pgstat_slru.c outward seams (consumed by slru.c).
    stat_seams::pgstat_get_slru_index::set(pgstat_get_slru_index);
    stat_seams::pgstat_count_slru_page_zeroed::set(
        pgstat_count_slru_page_zeroed,
    );
    stat_seams::pgstat_count_slru_page_hit::set(pgstat_count_slru_page_hit);
    stat_seams::pgstat_count_slru_page_read::set(pgstat_count_slru_page_read);
    stat_seams::pgstat_count_slru_page_written::set(
        pgstat_count_slru_page_written,
    );
    stat_seams::pgstat_count_slru_page_exists::set(
        pgstat_count_slru_page_exists,
    );
    stat_seams::pgstat_count_slru_flush::set(pgstat_count_slru_flush);
    stat_seams::pgstat_count_slru_truncate::set(pgstat_count_slru_truncate);
}
