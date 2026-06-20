//! Port of `src/backend/utils/activity/pgstat_backend.c` (PostgreSQL 18.3).
//!
//! Implementation of backend statistics (`PGSTAT_KIND_BACKEND`, a variable-
//! numbered stats kind keyed by proc number). Kept separate from `pgstat.c` to
//! enforce the line between the statistics access/storage implementation and the
//! details about individual kinds of statistics.
//!
//! Entries are created when a process is spawned and dropped when it exits;
//! they are not written to the pgstats file on disk. Pending statistics are
//! managed without direct interactions with `PgStat_EntryRef->pending`, relying
//! on `PendingBackendStats` instead, so it is possible to report data within
//! critical sections.
//!
//! `PendingBackendStats` / `backend_has_iostats` / `prevBackendWalUsage` are the
//! file-owned backend-local pending buffers (per-backend C globals using static
//! memory to avoid allocating in critical sections); they are `thread_local!`s
//! here (one backend == one thread).
//!
//! The shared stats body is reached through the real
//! [`PgStat_EntryRef::shared_stats`] raw pointer (a `*mut PgStatShared_Common`
//! into the DSA-backed shared segment): the flush callbacks cast it to
//! `*mut PgStatShared_Backend` exactly as C casts `entry_ref->shared_stats`,
//! avoiding any opaque-handle indirection.
//!
//! `pgstat_backend_flush_cb` is registered as this kind's `flush_static_cb`
//! (matching C: `PGSTAT_KIND_BACKEND` flushes through the static path, not
//! `PgStat_EntryRef->pending`), and `pgstat_backend_reset_timestamp_cb` as its
//! `reset_timestamp_cb`, via [`KindInfoBuilder`] from [`init_seams`].

use core::cell::{Cell, RefCell};

use backend_executor_instrument::{pgWalUsage, WalUsageAccumDiff};
use backend_utils_activity_pgstat::kind_info::KindInfoBuilder;
use backend_utils_activity_pgstat::registry;
use backend_utils_activity_pgstat::shmem;
use backend_utils_activity_pgstat_io::pgstat_tracks_io_op;
use backend_utils_init_small_seams::{my_backend_type, my_proc_number};
use types_core::init::BackendType;
use types_core::instrument::{instr_time, WalUsage};
use types_core::{bits32, InvalidOid, ProcNumber, TimestampTz};
use types_error::PgResult;
use types_pgstat::activity_pgstat::{
    IOContext, IOObject, IOOp, PgStat_Backend, PgStat_BackendPending, PgStat_Counter,
    PgStat_PendingIO, IOCONTEXT_NUM_TYPES, IOOBJECT_NUM_TYPES, IOOP_NUM_TYPES, PGSTAT_KIND_BACKEND,
};
use types_pgstat::pgstat_internal::{
    PgStat_KindInfo, PgStatShared_Backend, PgStatShared_Common,
};

/// `#define PGSTAT_BACKEND_FLUSH_IO (1 << 0)` (pgstat_internal.h) — flush I/O
/// statistics.
pub const PGSTAT_BACKEND_FLUSH_IO: bits32 = 1 << 0;
/// `#define PGSTAT_BACKEND_FLUSH_WAL (1 << 1)` — flush WAL statistics.
pub const PGSTAT_BACKEND_FLUSH_WAL: bits32 = 1 << 1;
/// `#define PGSTAT_BACKEND_FLUSH_ALL (PGSTAT_BACKEND_FLUSH_IO | PGSTAT_BACKEND_FLUSH_WAL)`.
pub const PGSTAT_BACKEND_FLUSH_ALL: bits32 = PGSTAT_BACKEND_FLUSH_IO | PGSTAT_BACKEND_FLUSH_WAL;

thread_local! {
    /// `static PgStat_BackendPending PendingBackendStats;` — backend statistics
    /// counts waiting to be flushed out.
    static PENDING_BACKEND_STATS: RefCell<PgStat_BackendPending> =
        RefCell::new(PgStat_BackendPending::default());

    /// `static bool backend_has_iostats = false;`.
    static BACKEND_HAS_IOSTATS: Cell<bool> = const { Cell::new(false) };

    /// `static WalUsage prevBackendWalUsage;` — WAL usage counters saved from
    /// `pgWalUsage` at the previous `pgstat_flush_backend()`.
    static PREV_BACKEND_WAL_USAGE: RefCell<WalUsage> = RefCell::new(WalUsage::default());
}

fn with_pending_backend_stats<R>(f: impl FnOnce(&mut PgStat_BackendPending) -> R) -> R {
    PENDING_BACKEND_STATS.with(|p| f(&mut p.borrow_mut()))
}

fn prev_backend_wal_usage() -> WalUsage {
    PREV_BACKEND_WAL_USAGE.with(|p| *p.borrow())
}

fn set_prev_backend_wal_usage(u: WalUsage) {
    PREV_BACKEND_WAL_USAGE.with(|p| *p.borrow_mut() = u);
}

// ---------------------------------------------------------------------------
// I/O stat reporting.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_count_backend_io_op_time(IOObject, IOContext, IOOp,
/// instr_time io_time)`.
///
/// `pgstat_report_fixed = true` is a pgstat.c optimization that lets
/// `pgstat_report_stat` skip the static-flush scan when no fixed kind has
/// pending data; the ported core does not model that flag (the
/// `flush_static_cb` short-circuits on its own `backend_has_iostats` /
/// have-pending checks), so it is not set here, exactly as in the IO crate.
pub fn pgstat_count_backend_io_op_time(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    io_time: instr_time,
) {
    // Assert(track_io_timing || track_wal_io_timing);
    debug_assert!(
        backend_utils_misc_guc_tables::vars::track_io_timing.read()
            || backend_utils_misc_guc_tables::vars::track_wal_io_timing.read()
    );

    if !pgstat_tracks_backend_bktype(my_backend_type::call()) {
        return;
    }

    // Assert(pgstat_tracks_io_op(MyBackendType, io_object, io_context, io_op));
    debug_assert!(pgstat_tracks_io_op(
        my_backend_type::call(),
        io_object,
        io_context,
        io_op
    ));

    let o = io_object as usize;
    let c = io_context as usize;
    let p = io_op as usize;
    with_pending_backend_stats(|pending| {
        // INSTR_TIME_ADD(PendingBackendStats.pending_io.pending_times[...], io_time);
        pending.pending_io.pending_times[o][c][p].add(io_time);
    });

    BACKEND_HAS_IOSTATS.with(|h| h.set(true));
}

/// Port of `void pgstat_count_backend_io_op(IOObject, IOContext, IOOp, uint32
/// cnt, uint64 bytes)`.
pub fn pgstat_count_backend_io_op(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    cnt: u32,
    bytes: u64,
) {
    if !pgstat_tracks_backend_bktype(my_backend_type::call()) {
        return;
    }

    // Assert(pgstat_tracks_io_op(MyBackendType, io_object, io_context, io_op));
    debug_assert!(pgstat_tracks_io_op(
        my_backend_type::call(),
        io_object,
        io_context,
        io_op
    ));

    let o = io_object as usize;
    let c = io_context as usize;
    let p = io_op as usize;
    with_pending_backend_stats(|pending| {
        pending.pending_io.counts[o][c][p] += cnt as PgStat_Counter;
        pending.pending_io.bytes[o][c][p] += bytes;
    });

    BACKEND_HAS_IOSTATS.with(|h| h.set(true));
}

// ---------------------------------------------------------------------------
// fetch.
// ---------------------------------------------------------------------------

/// Port of `PgStat_Backend *pgstat_fetch_stat_backend(ProcNumber procNumber)`.
///
/// Returns statistics of a backend by proc number, or `None` (C `NULL`). The
/// variable-numbered fetch goes through the generic `pgstat_fetch_entry` seam
/// (the `pgstat.c` fetch-consistency snapshot/cache machinery); the returned
/// `shared_data_len` byte blob is the `PGSTAT_KIND_BACKEND` body, which we
/// decode into the typed `PgStat_Backend` (C: `(PgStat_Backend *) ...`).
pub fn pgstat_fetch_stat_backend(proc_number: ProcNumber) -> PgResult<Option<PgStat_Backend>> {
    // backend_entry = (PgStat_Backend *) pgstat_fetch_entry(PGSTAT_KIND_BACKEND,
    //                                                        InvalidOid, procNumber);
    let bytes = backend_utils_activity_pgstat_seams::pgstat_fetch_entry::call(
        PGSTAT_KIND_BACKEND,
        InvalidOid,
        proc_number as u64,
    )?;
    Ok(bytes.map(|b| decode_backend_entry(&b)))
}

/// Decode the `shared_data_len` bytes `pgstat_fetch_entry` copies out into the
/// typed `PgStat_Backend` (C's `(PgStat_Backend *) ...`).
fn decode_backend_entry(bytes: &[u8]) -> PgStat_Backend {
    assert_eq!(
        bytes.len(),
        core::mem::size_of::<PgStat_Backend>(),
        "pgstat_fetch_stat_backend: unexpected stats blob size"
    );
    // SAFETY: the blob is exactly a `PgStat_Backend` (a Copy, pointer-free POD),
    // copied byte-for-byte by pgstat_fetch_entry.
    unsafe { core::ptr::read_unaligned(bytes.as_ptr() as *const PgStat_Backend) }
}

/// The PID-resolution prefix of `pgstat_fetch_stat_backend_by_pid`
/// (pgstat_backend.c): `BackendPidGetProc(pid)` else `AuxiliaryPidGetProc(pid)`
/// (procarray.c / proc.c), `GetNumberFromPGProc(proc)`, then
/// `pgstat_get_beentry_by_proc_number(procNumber)` (backend_status.c) projected
/// to `(st_backendType, st_procpid)`. Returns `(proc_number, st_backend_type,
/// st_procpid)`, or `None` when the pid is not a live (auxiliary or regular)
/// backend with a beentry. Installs the `pgstat_backend_pid_lookup` seam.
fn pgstat_backend_pid_lookup(pid: i32) -> Option<(ProcNumber, BackendType, i32)> {
    // proc = BackendPidGetProc(pid); if (!proc) proc = AuxiliaryPidGetProc(pid);
    let proc_number =
        match backend_storage_ipc_procarray_seams::backend_pid_get_proc_role::call(pid) {
            Some((_role, procno)) => procno,
            None => match backend_storage_lmgr_proc_seams::auxiliary_pid_get_proc::call(pid) {
                Some(procno) => procno,
                None => return None,
            },
        };

    // procNumber = GetNumberFromPGProc(proc) — already folded into the lookups.
    // beentry = pgstat_get_beentry_by_proc_number(procNumber); if (!beentry) NULL.
    let (st_backend_type, st_procpid) =
        backend_utils_activity_status_seams::beentry_backend_type_and_pid::call(proc_number)?;

    Some((proc_number, st_backend_type, st_procpid))
}

/// Port of `PgStat_Backend *pgstat_fetch_stat_backend_by_pid(int pid,
/// BackendType *bktype)`.
///
/// Returns statistics of a backend by pid, with sanity checks. When `bktype` is
/// `Some`, it is set to the [`BackendType`] of the backend whose statistics are
/// returned (or `B_INVALID`), matching the optional out-parameter in C.
///
/// The `BackendPidGetProc` / `AuxiliaryPidGetProc` / `GetNumberFromPGProc` /
/// `pgstat_get_beentry_by_proc_number` prefix is resolved by the in-crate
/// [`pgstat_backend_pid_lookup`] (each piece reached through its owner's seam);
/// the entry fetch reuses [`pgstat_fetch_stat_backend`].
pub fn pgstat_fetch_stat_backend_by_pid(
    pid: i32,
    mut bktype: Option<&mut BackendType>,
) -> PgResult<Option<PgStat_Backend>> {
    // if (bktype) *bktype = B_INVALID;
    if let Some(bt) = bktype.as_deref_mut() {
        *bt = BackendType::Invalid;
    }

    // proc = BackendPidGetProc(pid); if (!proc) proc = AuxiliaryPidGetProc(pid);
    // if (!proc) return NULL; procNumber = GetNumberFromPGProc(proc);
    // beentry = pgstat_get_beentry_by_proc_number(procNumber);
    // if (!beentry) return NULL;
    let (proc_number, st_backend_type, st_procpid) = match pgstat_backend_pid_lookup(pid) {
        None => return Ok(None),
        Some(t) => t,
    };

    // check if the backend type tracks statistics
    if !pgstat_tracks_backend_bktype(st_backend_type) {
        return Ok(None);
    }

    // if PID does not match, leave
    if st_procpid != pid {
        return Ok(None);
    }

    if let Some(bt) = bktype.as_deref_mut() {
        *bt = st_backend_type;
    }

    // Retrieve the entry. Note that "beentry" may be freed depending on the
    // value of stats_fetch_consistency, so do not access it from this point.
    let backend_stats = pgstat_fetch_stat_backend(proc_number)?;
    if backend_stats.is_none() {
        if let Some(bt) = bktype.as_deref_mut() {
            *bt = BackendType::Invalid;
        }
        return Ok(None);
    }

    Ok(backend_stats)
}

// ---------------------------------------------------------------------------
// flush helpers (locking managed by the caller).
// ---------------------------------------------------------------------------

/// Cast the entry ref's shared stats body to the typed backend shared entry.
///
/// # Safety
/// `shared_stats` must point at a live `PgStatShared_Backend` body in the shared
/// segment (it does for a `PGSTAT_KIND_BACKEND` entry: the body is
/// `dsa_allocate0(sizeof(PgStatShared_Backend))`), and the caller must hold the
/// entry's content lock for the duration of the borrow.
unsafe fn shared_backend(shared_stats: *mut PgStatShared_Common) -> *mut PgStatShared_Backend {
    shared_stats as *mut PgStatShared_Backend
}

/// Port of `static void pgstat_flush_backend_entry_io(PgStat_EntryRef *entry_ref)`.
///
/// # Safety
/// `shared_stats` must be a live, content-locked `PgStatShared_Backend` body.
unsafe fn pgstat_flush_backend_entry_io(shared_stats: *mut PgStatShared_Common) {
    // This function can be called even if nothing at all has happened for IO
    // statistics. In this case, avoid unnecessarily modifying the stats entry.
    if !BACKEND_HAS_IOSTATS.with(|h| h.get()) {
        return;
    }

    // shbackendent = (PgStatShared_Backend *) entry_ref->shared_stats;
    // bktype_shstats = &shbackendent->stats.io_stats;
    let bktype_shstats = &mut (*shared_backend(shared_stats)).stats.io_stats;
    // pending_io = PendingBackendStats.pending_io;
    let pending_io = with_pending_backend_stats(|pending| pending.pending_io);

    for o in 0..IOOBJECT_NUM_TYPES {
        for c in 0..IOCONTEXT_NUM_TYPES {
            for p in 0..IOOP_NUM_TYPES {
                bktype_shstats.counts[o][c][p] += pending_io.counts[o][c][p];
                bktype_shstats.bytes[o][c][p] += pending_io.bytes[o][c][p];
                let time = pending_io.pending_times[o][c][p];
                bktype_shstats.times[o][c][p] += time.get_microsec() as PgStat_Counter;
            }
        }
    }

    // Clear out the statistics buffer, so it can be re-used.
    // MemSet(&PendingBackendStats.pending_io, 0, sizeof(PgStat_PendingIO));
    with_pending_backend_stats(|pending| pending.pending_io = PgStat_PendingIO::default());

    BACKEND_HAS_IOSTATS.with(|h| h.set(false));
}

/// Port of `static inline bool pgstat_backend_wal_have_pending(void)`.
fn pgstat_backend_wal_have_pending() -> bool {
    pgWalUsage().wal_records != prev_backend_wal_usage().wal_records
}

/// Port of `static void pgstat_flush_backend_entry_wal(PgStat_EntryRef *entry_ref)`.
///
/// # Safety
/// `shared_stats` must be a live, content-locked `PgStatShared_Backend` body.
unsafe fn pgstat_flush_backend_entry_wal(shared_stats: *mut PgStatShared_Common) {
    // This function can be called even if nothing at all has happened for WAL
    // statistics. In this case, avoid unnecessarily modifying the stats entry.
    if !pgstat_backend_wal_have_pending() {
        return;
    }

    // shbackendent = (PgStatShared_Backend *) entry_ref->shared_stats;
    // bktype_shstats = &shbackendent->stats.wal_counters;
    let bktype_shstats = &mut (*shared_backend(shared_stats)).stats.wal_counters;

    // Calculate how much WAL usage counters were increased.
    let cur = pgWalUsage();
    let prev = prev_backend_wal_usage();
    let mut wal_usage_diff = WalUsage::default();
    WalUsageAccumDiff(&mut wal_usage_diff, &cur, &prev);

    // WALSTAT_ACC(fld, var_to_add): bktype_shstats->fld += var_to_add.fld
    bktype_shstats.wal_buffers_full += wal_usage_diff.wal_buffers_full;
    bktype_shstats.wal_records += wal_usage_diff.wal_records;
    bktype_shstats.wal_fpi += wal_usage_diff.wal_fpi;
    bktype_shstats.wal_bytes = bktype_shstats.wal_bytes.wrapping_add(wal_usage_diff.wal_bytes);

    // Save the current counters for the subsequent calculation of WAL usage.
    set_prev_backend_wal_usage(cur);
}

// ---------------------------------------------------------------------------
// pgstat_flush_backend.
// ---------------------------------------------------------------------------

/// Port of `bool pgstat_flush_backend(bool nowait, bits32 flags)`.
///
/// Flush out locally pending backend statistics. `flags` controls which to
/// flush. Returns `Ok(true)` if some statistics could not be flushed due to
/// lock contention. `Err` carries the `LWLockAcquire` `ereport` surface.
pub fn pgstat_flush_backend(nowait: bool, flags: bits32) -> PgResult<bool> {
    let mut has_pending_data = false;

    if !pgstat_tracks_backend_bktype(my_backend_type::call()) {
        return Ok(false);
    }

    // Some IO data pending?
    if (flags & PGSTAT_BACKEND_FLUSH_IO) != 0 && BACKEND_HAS_IOSTATS.with(|h| h.get()) {
        has_pending_data = true;
    }

    // Some WAL data pending?
    if (flags & PGSTAT_BACKEND_FLUSH_WAL) != 0 && pgstat_backend_wal_have_pending() {
        has_pending_data = true;
    }

    if !has_pending_data {
        return Ok(false);
    }

    let entry_ref = shmem::pgstat_get_entry_ref_locked(
        PGSTAT_KIND_BACKEND,
        InvalidOid,
        my_proc_number::call() as u64,
        nowait,
    )?;
    let entry_ref = match entry_ref {
        None => return Ok(true),
        Some(er) => er,
    };

    // SAFETY: a just-resolved, content-locked PGSTAT_KIND_BACKEND reference; its
    // shared_stats points at a live PgStatShared_Backend body, held under the
    // content lock until pgstat_unlock_entry below.
    let er = unsafe { entry_ref.get() };
    let shared_stats = er.shared_stats;

    // Flush requested statistics.
    unsafe {
        if (flags & PGSTAT_BACKEND_FLUSH_IO) != 0 {
            pgstat_flush_backend_entry_io(shared_stats);
        }
        if (flags & PGSTAT_BACKEND_FLUSH_WAL) != 0 {
            pgstat_flush_backend_entry_wal(shared_stats);
        }
    }

    shmem::pgstat_unlock_entry(er)?;

    Ok(false)
}

/// Port of `bool pgstat_backend_flush_cb(bool nowait)`.
///
/// Callback to flush out locally pending backend statistics. Returns `Ok(true)`
/// if some stats could not be flushed due to lock contention.
pub fn pgstat_backend_flush_cb(nowait: bool) -> PgResult<bool> {
    pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_ALL)
}

// ---------------------------------------------------------------------------
// pgstat_create_backend.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_create_backend(ProcNumber procnum)`.
///
/// Create the backend statistics entry for a proc number. With `nowait = false`
/// the C code dereferences the result unconditionally (it can never be NULL); a
/// `None` here is the impossible case and surfaces as an error.
pub fn pgstat_create_backend(procnum: ProcNumber) -> PgResult<()> {
    let entry_ref = shmem::pgstat_get_entry_ref_locked(
        PGSTAT_KIND_BACKEND,
        InvalidOid,
        procnum as u64,
        false,
    )?
    .ok_or_else(|| {
        types_error::PgError::error(
            "pgstat_create_backend: pgstat_get_entry_ref_locked returned NULL",
        )
    })?;

    // SAFETY: just-resolved, content-locked reference (nowait = false).
    let er = unsafe { entry_ref.get() };

    // shstatent = (PgStatShared_Backend *) entry_ref->shared_stats;
    // NB: need to accept that there might be stats from an older backend, e.g.
    // if we previously used this proc number.
    // memset(&shstatent->stats, 0, sizeof(shstatent->stats));
    // SAFETY: content-locked live PgStatShared_Backend body.
    unsafe {
        (*shared_backend(er.shared_stats)).stats = PgStat_Backend::default();
    }
    shmem::pgstat_unlock_entry(er)?;

    // MemSet(&PendingBackendStats, 0, sizeof(PgStat_BackendPending));
    with_pending_backend_stats(|pending| *pending = PgStat_BackendPending::default());
    // backend_has_iostats = false;
    BACKEND_HAS_IOSTATS.with(|h| h.set(false));

    // Initialize prevBackendWalUsage with pgWalUsage so pgstat_backend_flush_cb
    // can calculate how much pgWalUsage increased.
    set_prev_backend_wal_usage(pgWalUsage());

    Ok(())
}

// ---------------------------------------------------------------------------
// tracking gate + reset-timestamp callback.
// ---------------------------------------------------------------------------

/// Port of `bool pgstat_tracks_backend_bktype(BackendType bktype)`.
///
/// Whether a [`BackendType`] participates in the backend stats subsystem. Every
/// type is listed so a new backend type triggers a non-exhaustive-match error
/// about needing to adjust this switch (C's warning).
pub fn pgstat_tracks_backend_bktype(bktype: BackendType) -> bool {
    use BackendType::*;
    match bktype {
        Invalid | AutovacLauncher | DeadEndBackend | Archiver | Logger | BgWriter | Checkpointer
        | IoWorker | Startup => false,

        AutovacWorker | Backend | BgWorker | StandaloneBackend | SlotsyncWorker | WalReceiver
        | WalSender | WalSummarizer | WalWriter => true,
    }
}

/// Port of `void pgstat_backend_reset_timestamp_cb(PgStatShared_Common *header,
/// TimestampTz ts)`.
///
/// `((PgStatShared_Backend *) header)->stats.stat_reset_timestamp = ts;`
pub fn pgstat_backend_reset_timestamp_cb(header: &mut PgStatShared_Common, ts: TimestampTz) {
    // SAFETY: for a PGSTAT_KIND_BACKEND entry the common header is the leading
    // field of a PgStatShared_Backend body (repr(C)); the registry hands us the
    // header of exactly such a body, content-locked.
    let backend = unsafe { &mut *(header as *mut PgStatShared_Common as *mut PgStatShared_Backend) };
    backend.stats.stat_reset_timestamp = ts;
}

// ---------------------------------------------------------------------------
// Kind registration + seam installation.
// ---------------------------------------------------------------------------

/// The `PgStat_KindInfo` metadata for `PGSTAT_KIND_BACKEND`
/// (`pgstat.c:pgstat_kind_builtin_infos[PGSTAT_KIND_BACKEND]`).
///
/// BACKEND is a *variable*-numbered kind (`fixed_amount = false`), keyed by proc
/// number and accessed across databases, not written to the on-disk file. Its
/// shared body is a `PgStatShared_Backend`, so `shared_size` is its size and
/// `shared_data_off` / `shared_data_len` describe the embedded `stats` (used by
/// `pgstat_reset` to zero the body and by `pgstat_fetch_entry` to copy it).
fn backend_kind_info() -> PgStat_KindInfo {
    PgStat_KindInfo {
        fixed_amount: false,
        accessed_across_databases: true,
        write_to_file: false,
        shared_size: core::mem::size_of::<PgStatShared_Backend>() as u32,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        shared_data_off: core::mem::offset_of!(PgStatShared_Backend, stats) as u32,
        shared_data_len: core::mem::size_of::<PgStat_Backend>() as u32,
        pending_size: 0,
        name: "backend",
    }
}

/// Register `PGSTAT_KIND_BACKEND` and install the per-backend outward seams that
/// `pgstat_io.c` / `pgstat_wal.c` left to `pgstat_backend.c`.
pub fn init_seams() {
    registry::register(
        KindInfoBuilder::new(PGSTAT_KIND_BACKEND, backend_kind_info())
            .flush_static_cb(pgstat_backend_flush_cb)
            .reset_timestamp_cb(pgstat_backend_reset_timestamp_cb),
    );

    // I/O counting seams consumed by pgstat_io.c.
    backend_utils_activity_pgstat_io_seams::pgstat_count_backend_io_op::set(
        pgstat_count_backend_io_op,
    );
    backend_utils_activity_pgstat_io_seams::pgstat_count_backend_io_op_time::set(
        pgstat_count_backend_io_op_time,
    );

    // pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_IO) — consumed by the
    // pgstat_io.c flush path (pgstat_report_wal). The io-seams shape returns
    // bool (true == some left unflushed); a genuine LWLock error is unreachable
    // on this flush path, so map it to `true` defensively.
    backend_utils_activity_pgstat_io_seams::pgstat_flush_backend_io::set(|nowait| {
        pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_IO).unwrap_or(true)
    });

    // pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_WAL) — consumed by the
    // pgstat_wal.c flush path.
    backend_utils_activity_pgstat_wal_seams::pgstat_flush_backend_wal::set(|nowait| {
        pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_WAL).unwrap_or(true)
    });
}
