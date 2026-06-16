//! Port of `src/backend/utils/activity/pgstat_io.c` (PostgreSQL 18.3).
//!
//! Implementation of IO statistics (`PGSTAT_KIND_IO`, a fixed-numbered stats
//! kind). Kept separate from `pgstat.c` to enforce the line between the
//! statistics access/storage implementation and the details about individual
//! kinds of statistics.
//!
//! `PendingIOStats` / `have_iostats` are the file-owned backend-local pending
//! buffers (per-backend C globals); they are `thread_local!`s here (one backend
//! == one thread), reached through [`with_pending_io_stats`] / the accessors.
//!
//! The fixed-kind callbacks (`init_shmem_cb`, `reset_all_cb`, `snapshot_cb`,
//! `flush_static_cb`) are registered with the pgstat core's
//! `pgstat_kind_builtin_infos[]` table via [`KindInfoBuilder`] from
//! [`init_seams`]; the core dispatches them, projecting the typed
//! `PgStatShared_IO` / `PgStat_IO` fields of the owner `PgStat_ShmemControl` /
//! `PgStat_Snapshot` (the typed adapter that replaces C's `void *`).

use core::cell::{Cell, RefCell};

use backend_executor_instrument as instrument;
use backend_storage_lmgr_lwlock_seams::{
    lwlock_acquire, lwlock_conditional_acquire, lwlock_initialize,
};
use backend_utils_activity_pgstat::kind_info::KindInfoBuilder;
use backend_utils_activity_pgstat::registry;
use backend_utils_init_small_seams::{my_backend_type, my_proc_number};
use portability_instr_time::instr_time_set_current;
use types_core::instrument::instr_time;
use types_core::init::{BackendType, BACKEND_NUM_TYPES};
use types_core::TimestampTz;
use types_error::PgResult;
use types_pgstat::activity_pgstat::{
    pgstat_is_ioop_tracked_in_bytes, IOContext, IOObject, IOOp, PgStat_PendingIO,
    IOCONTEXT_NUM_TYPES, IOOBJECT_NUM_TYPES, IOOP_NUM_TYPES, PGSTAT_KIND_IO,
};
use types_pgstat::pgstat_internal::{
    PgStat_KindInfo, PgStat_ShmemControl, PgStat_Snapshot,
};
use types_storage::{LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, LW_SHARED};

thread_local! {
    /// `static PgStat_PendingIO PendingIOStats;` — backend-local pending IO
    /// counts, accumulated by `pgstat_count_io_op*` and flushed to shared
    /// memory by `pgstat_io_flush_cb`.
    static PENDING_IO_STATS: RefCell<PgStat_PendingIO> =
        RefCell::new(PgStat_PendingIO::default());

    /// `static bool have_iostats = false;` — whether any IO has been counted
    /// since the last flush.
    static HAVE_IOSTATS: Cell<bool> = const { Cell::new(false) };
}

/// Run `f` on this backend's `PendingIOStats` buffer.
pub fn with_pending_io_stats<R>(f: impl FnOnce(&mut PgStat_PendingIO) -> R) -> R {
    PENDING_IO_STATS.with(|p| f(&mut p.borrow_mut()))
}

// ---------------------------------------------------------------------------
// Counting (pgstat_count_io_op family).
// ---------------------------------------------------------------------------

/// Port of `void pgstat_count_io_op(IOObject, IOContext, IOOp, uint32 cnt,
/// uint64 bytes)`.
pub fn pgstat_count_io_op(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    cnt: u32,
    bytes: u64,
) {
    debug_assert!((io_object as u32) < IOOBJECT_NUM_TYPES as u32);
    debug_assert!((io_context as u32) < IOCONTEXT_NUM_TYPES as u32);
    debug_assert!(pgstat_is_ioop_tracked_in_bytes(io_op) || bytes == 0);
    debug_assert!(pgstat_tracks_io_op(
        my_backend_type::call(),
        io_object,
        io_context,
        io_op
    ));

    let o = io_object as usize;
    let c = io_context as usize;
    let p = io_op as usize;

    with_pending_io_stats(|pending| {
        pending.counts[o][c][p] += cnt as i64;
        pending.bytes[o][c][p] += bytes;
    });

    // Add the per-backend counts (pgstat_backend.c, PGSTAT_KIND_BACKEND).
    backend_utils_activity_pgstat_io_seams::pgstat_count_backend_io_op::call(
        io_object, io_context, io_op, cnt, bytes,
    );

    // have_iostats = true; pgstat_report_fixed = true;
    //
    // `pgstat_report_fixed` is a pgstat.c optimization flag that lets
    // pgstat_report_stat skip the static-flush scan when no fixed kind has
    // pending data. The ported core does not model that flag; the per-kind
    // `flush_static_cb` short-circuits on its own `have_iostats` check (below),
    // which is the faithful guard, so setting the flag would be a no-op here.
    HAVE_IOSTATS.with(|h| h.set(true));
}

/// Port of `instr_time pgstat_prepare_io_time(bool track_io_guc)`.
///
/// In C the caller passes the relevant IO timing GUC. The workspace seam
/// (`pgstat_prepare_io_time()`) takes no argument and is the WAL-write timing
/// path used by walreceiver / xlogrecovery; it reads `track_wal_io_timing`
/// directly, exactly as those C call sites pass it.
pub fn pgstat_prepare_io_time(track_io_guc: bool) -> instr_time {
    let mut io_start = instr_time::default();
    if track_io_guc {
        instr_time_set_current(&mut io_start);
    } else {
        // INSTR_TIME_SET_ZERO(io_start): already zero.
        io_start.set_zero();
    }
    io_start
}

/// The `pgstat_prepare_io_time` seam shape: the WAL-write timing path reads
/// `track_wal_io_timing`.
fn pgstat_prepare_io_time_seam() -> instr_time {
    pgstat_prepare_io_time(backend_utils_misc_guc_tables::vars::track_wal_io_timing.read())
}

/// Port of `void pgstat_count_io_op_time(IOObject, IOContext, IOOp, instr_time
/// start_time, uint32 cnt, uint64 bytes)`.
pub fn pgstat_count_io_op_time(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    start_time: instr_time,
    cnt: u32,
    bytes: u64,
) {
    if !start_time.is_zero() {
        // INSTR_TIME_SET_CURRENT(io_time); INSTR_TIME_SUBTRACT(io_time, start_time);
        let mut io_time = instr_time::default();
        instr_time_set_current(&mut io_time);
        io_time.subtract(start_time);

        if io_object != IOObject::IOOBJECT_WAL {
            if io_op == IOOp::IOOP_WRITE || io_op == IOOp::IOOP_EXTEND {
                backend_utils_activity_stat_seams::pgstat_count_buffer_write_time::call(
                    io_time.get_microsec(),
                );
                if io_object == IOObject::IOOBJECT_RELATION {
                    instrument::with_pgBufferUsage(|b| b.shared_blk_write_time.add(io_time));
                } else if io_object == IOObject::IOOBJECT_TEMP_RELATION {
                    instrument::with_pgBufferUsage(|b| b.local_blk_write_time.add(io_time));
                }
            } else if io_op == IOOp::IOOP_READ {
                backend_utils_activity_stat_seams::pgstat_count_buffer_read_time::call(
                    io_time.get_microsec(),
                );
                if io_object == IOObject::IOOBJECT_RELATION {
                    instrument::with_pgBufferUsage(|b| b.shared_blk_read_time.add(io_time));
                } else if io_object == IOObject::IOOBJECT_TEMP_RELATION {
                    instrument::with_pgBufferUsage(|b| b.local_blk_read_time.add(io_time));
                }
            }

            let o = io_object as usize;
            let c = io_context as usize;
            let p = io_op as usize;
            with_pending_io_stats(|pending| {
                pending.pending_times[o][c][p].add(io_time);
            });

            // Add the per-backend count (pgstat_backend.c).
            backend_utils_activity_pgstat_io_seams::pgstat_count_backend_io_op_time::call(
                io_object, io_context, io_op, io_time,
            );
        } else {
            // io_object == IOOBJECT_WAL: only accumulate the pending time and
            // the per-backend count (the buffer-time / pgBufferUsage block is
            // skipped, matching C).
            let o = io_object as usize;
            let c = io_context as usize;
            let p = io_op as usize;
            with_pending_io_stats(|pending| {
                pending.pending_times[o][c][p].add(io_time);
            });
            backend_utils_activity_pgstat_io_seams::pgstat_count_backend_io_op_time::call(
                io_object, io_context, io_op, io_time,
            );
        }
    }

    pgstat_count_io_op(io_object, io_context, io_op, cnt, bytes);
}

/// The `pgstat_count_io_op_time` seam shape used by the workspace's WAL-write
/// call sites: `pgstat_count_io_op_time(IOOBJECT_WAL, IOCONTEXT_NORMAL,
/// IOOP_WRITE, start, 1, bytes_written)`.
fn pgstat_count_io_op_time_wal_write_seam(start: instr_time, bytes_written: u32) {
    pgstat_count_io_op_time(
        IOObject::IOOBJECT_WAL,
        IOContext::IOCONTEXT_NORMAL,
        IOOp::IOOP_WRITE,
        start,
        1,
        bytes_written as u64,
    );
}

// ---------------------------------------------------------------------------
// Flush (pgstat_io_flush_cb / pgstat_flush_io).
// ---------------------------------------------------------------------------

/// Port of `void pgstat_flush_io(bool nowait)` — simpler wrapper of
/// `pgstat_io_flush_cb`.
pub fn pgstat_flush_io(nowait: bool) -> PgResult<()> {
    let _ = pgstat_io_flush_cb(nowait)?;
    Ok(())
}

/// Port of `bool pgstat_io_flush_cb(bool nowait)`.
///
/// Flushes the backend-local pending IO statistics into shared memory. Returns
/// `Ok(false)` if nothing to flush or after a successful flush, and `Ok(true)`
/// if `nowait` was set and the per-`BackendType` lock could not be acquired.
pub fn pgstat_io_flush_cb(nowait: bool) -> PgResult<bool> {
    if !HAVE_IOSTATS.with(|h| h.get()) {
        return Ok(false);
    }

    let bktype = my_backend_type::call() as usize;

    backend_utils_activity_pgstat::local::with_local(|l| {
        let ctl: &mut PgStat_ShmemControl = l
            .shmem
            .as_mut()
            .expect("pgstat shared control not initialized (StatsShmemInit not run)");
        // Split-borrow PgStatShared_IO { locks, stats } so the lock guard's
        // `&LWLock` borrow and the `&mut PgStat_BktypeIO` write borrow target
        // disjoint fields (mirrors C's `&io.locks[bktype]` / `&io.stats.stats[bktype]`).
        let types_pgstat::pgstat_internal::PgStatShared_IO {
            ref locks,
            ref mut stats,
        } = ctl.io;
        let bktype_lock = &locks[bktype];

        // if (!nowait) LWLockAcquire(...); else if (!LWLockConditionalAcquire(...)) return true;
        let guard;
        if !nowait {
            guard = lwlock_acquire::call(bktype_lock, LW_EXCLUSIVE, my_proc_number::call())?;
        } else {
            match lwlock_conditional_acquire::call(bktype_lock, LW_EXCLUSIVE)? {
                Some(g) => guard = g,
                None => return Ok(true),
            }
        }

        let bktype_shstats = &mut stats.stats[bktype];
        with_pending_io_stats(|pending| {
            for o in 0..IOOBJECT_NUM_TYPES {
                for c in 0..IOCONTEXT_NUM_TYPES {
                    for p in 0..IOOP_NUM_TYPES {
                        bktype_shstats.counts[o][c][p] += pending.counts[o][c][p];
                        bktype_shstats.bytes[o][c][p] += pending.bytes[o][c][p];
                        let time = pending.pending_times[o][c][p];
                        bktype_shstats.times[o][c][p] += time.get_microsec() as i64;
                    }
                }
            }
        });

        debug_assert!(pgstat_bktype_io_stats_valid(
            bktype_shstats,
            my_backend_type::call()
        ));

        guard.release()?;

        // MemSet(&PendingIOStats, 0, sizeof(PendingIOStats));
        with_pending_io_stats(|pending| *pending = PgStat_PendingIO::default());
        HAVE_IOSTATS.with(|h| h.set(false));

        Ok(false)
    })
}

// ---------------------------------------------------------------------------
// Fetch (pgstat_fetch_stat_io).
// ---------------------------------------------------------------------------

/// Port of `PgStat_IO *pgstat_fetch_stat_io(void)`. In C this returns a pointer
/// into the snapshot; here it returns a copy of the snapshot's IO stats.
pub fn pgstat_fetch_stat_io() -> PgResult<types_pgstat::activity_pgstat::PgStat_IO> {
    backend_utils_activity_pgstat_seams::snapshot_fixed::call(PGSTAT_KIND_IO)?;
    Ok(backend_utils_activity_pgstat::local::with_local(|l| l.snapshot.io))
}

// ---------------------------------------------------------------------------
// Validity check + name helpers.
// ---------------------------------------------------------------------------

/// Port of `bool pgstat_bktype_io_stats_valid(PgStat_BktypeIO *backend_io,
/// BackendType bktype)`.
pub fn pgstat_bktype_io_stats_valid(
    backend_io: &types_pgstat::activity_pgstat::PgStat_BktypeIO,
    bktype: BackendType,
) -> bool {
    for io_object in 0..IOOBJECT_NUM_TYPES {
        for io_context in 0..IOCONTEXT_NUM_TYPES {
            for io_op in 0..IOOP_NUM_TYPES {
                let obj = io_object_from_index(io_object);
                let ctx = io_context_from_index(io_context);
                let op = io_op_from_index(io_op);
                if pgstat_tracks_io_op(bktype, obj, ctx, op) {
                    // ensure that if IO times are non-zero, counts are > 0
                    if backend_io.times[io_object][io_context][io_op] != 0
                        && backend_io.counts[io_object][io_context][io_op] <= 0
                    {
                        return false;
                    }
                    continue;
                }
                // we don't track it, and it is not 0
                if backend_io.counts[io_object][io_context][io_op] != 0 {
                    return false;
                }
            }
        }
    }
    true
}

/// Port of `const char *pgstat_get_io_context_name(IOContext io_context)`.
pub fn pgstat_get_io_context_name(io_context: IOContext) -> &'static str {
    match io_context {
        IOContext::IOCONTEXT_BULKREAD => "bulkread",
        IOContext::IOCONTEXT_BULKWRITE => "bulkwrite",
        IOContext::IOCONTEXT_INIT => "init",
        IOContext::IOCONTEXT_NORMAL => "normal",
        IOContext::IOCONTEXT_VACUUM => "vacuum",
    }
}

/// Port of `const char *pgstat_get_io_object_name(IOObject io_object)`.
pub fn pgstat_get_io_object_name(io_object: IOObject) -> &'static str {
    match io_object {
        IOObject::IOOBJECT_RELATION => "relation",
        IOObject::IOOBJECT_TEMP_RELATION => "temp relation",
        IOObject::IOOBJECT_WAL => "wal",
    }
}

// ---------------------------------------------------------------------------
// Fixed-kind callbacks.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_io_init_shmem_cb(void *stats)`. The adapter hands us
/// the typed `PgStat_ShmemControl`; we initialize the per-`BackendType` locks
/// of its `io` region.
pub fn pgstat_io_init_shmem_cb(ctl: &mut PgStat_ShmemControl) {
    for i in 0..BACKEND_NUM_TYPES {
        lwlock_initialize::call(&mut ctl.io.locks[i], LWTRANCHE_PGSTATS_DATA);
    }
}

/// Port of `void pgstat_io_reset_all_cb(TimestampTz ts)`.
pub fn pgstat_io_reset_all_cb(ctl: &mut PgStat_ShmemControl, ts: TimestampTz) -> PgResult<()> {
    for i in 0..BACKEND_NUM_TYPES {
        let guard = lwlock_acquire::call(&ctl.io.locks[i], LW_EXCLUSIVE, my_proc_number::call())?;
        // Use the lock in the first BackendType's PgStat_BktypeIO to protect
        // the reset timestamp as well.
        if i == 0 {
            ctl.io.stats.stat_reset_timestamp = ts;
        }
        ctl.io.stats.stats[i] = types_pgstat::activity_pgstat::PgStat_BktypeIO::default();
        guard.release()?;
    }
    Ok(())
}

/// Port of `void pgstat_io_snapshot_cb(void)`. The adapter hands us the typed
/// shared control (read) and the snapshot (write).
pub fn pgstat_io_snapshot_cb(
    ctl: &PgStat_ShmemControl,
    snap: &mut PgStat_Snapshot,
) -> PgResult<()> {
    for i in 0..BACKEND_NUM_TYPES {
        let guard = lwlock_acquire::call(&ctl.io.locks[i], LW_SHARED, my_proc_number::call())?;
        // Use the first BackendType's lock to protect the reset timestamp too.
        if i == 0 {
            snap.io.stat_reset_timestamp = ctl.io.stats.stat_reset_timestamp;
        }
        // struct assignment: *bktype_snap = *bktype_shstats;
        snap.io.stats[i] = ctl.io.stats.stats[i];
        guard.release()?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tracking-gate predicates (pgstat_tracks_io_*).
// ---------------------------------------------------------------------------

/// Port of `bool pgstat_tracks_io_bktype(BackendType bktype)`.
pub fn pgstat_tracks_io_bktype(bktype: BackendType) -> bool {
    use BackendType::*;
    match bktype {
        Invalid | DeadEndBackend | Archiver | Logger => false,
        AutovacLauncher | AutovacWorker | Backend | BgWorker | BgWriter | Checkpointer
        | IoWorker | SlotsyncWorker | StandaloneBackend | Startup | WalReceiver | WalSender
        | WalSummarizer | WalWriter => true,
    }
}

/// Port of `bool pgstat_tracks_io_object(BackendType, IOObject, IOContext)`.
pub fn pgstat_tracks_io_object(
    bktype: BackendType,
    io_object: IOObject,
    io_context: IOContext,
) -> bool {
    use BackendType::*;

    if !pgstat_tracks_io_bktype(bktype) {
        return false;
    }

    // IO on IOOBJECT_WAL can only occur in IOCONTEXT_NORMAL and IOCONTEXT_INIT.
    if io_object == IOObject::IOOBJECT_WAL
        && io_context != IOContext::IOCONTEXT_NORMAL
        && io_context != IOContext::IOCONTEXT_INIT
    {
        return false;
    }

    // IO on temporary relations can only occur in IOCONTEXT_NORMAL.
    if io_context != IOContext::IOCONTEXT_NORMAL
        && io_object == IOObject::IOOBJECT_TEMP_RELATION
    {
        return false;
    }

    let no_temp_rel = matches!(
        bktype,
        AutovacLauncher
            | BgWriter
            | Checkpointer
            | AutovacWorker
            | StandaloneBackend
            | Startup
            | WalSummarizer
            | WalWriter
            | WalReceiver
    );

    if no_temp_rel
        && io_context == IOContext::IOCONTEXT_NORMAL
        && io_object == IOObject::IOOBJECT_TEMP_RELATION
    {
        return false;
    }

    // Some BackendTypes only perform IO under IOOBJECT_WAL.
    if matches!(bktype, WalSummarizer | WalReceiver | WalWriter)
        && io_object != IOObject::IOOBJECT_WAL
    {
        return false;
    }

    if matches!(bktype, Checkpointer | BgWriter)
        && matches!(
            io_context,
            IOContext::IOCONTEXT_BULKREAD
                | IOContext::IOCONTEXT_BULKWRITE
                | IOContext::IOCONTEXT_VACUUM
        )
    {
        return false;
    }

    if bktype == AutovacLauncher && io_context == IOContext::IOCONTEXT_VACUUM {
        return false;
    }

    if matches!(bktype, AutovacWorker | AutovacLauncher)
        && io_context == IOContext::IOCONTEXT_BULKWRITE
    {
        return false;
    }

    true
}

/// Port of `bool pgstat_tracks_io_op(BackendType, IOObject, IOContext, IOOp)`.
pub fn pgstat_tracks_io_op(
    bktype: BackendType,
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
) -> bool {
    use BackendType::*;

    // if (io_context, io_object) will never collect stats, we're done.
    if !pgstat_tracks_io_object(bktype, io_object, io_context) {
        return false;
    }

    // Some BackendTypes will not do certain IOOps.
    if bktype == BgWriter
        && matches!(io_op, IOOp::IOOP_READ | IOOp::IOOP_EVICT | IOOp::IOOP_HIT)
    {
        return false;
    }

    if bktype == Checkpointer
        && ((io_object != IOObject::IOOBJECT_WAL && io_op == IOOp::IOOP_READ)
            || matches!(io_op, IOOp::IOOP_EVICT | IOOp::IOOP_HIT))
    {
        return false;
    }

    if matches!(bktype, AutovacLauncher | BgWriter | Checkpointer) && io_op == IOOp::IOOP_EXTEND {
        return false;
    }

    // Some BackendTypes do not perform reads with IOOBJECT_WAL.
    if io_object == IOObject::IOOBJECT_WAL
        && io_op == IOOp::IOOP_READ
        && matches!(
            bktype,
            WalReceiver | BgWriter | AutovacLauncher | AutovacWorker | WalWriter
        )
    {
        return false;
    }

    // Temporary tables are not logged and thus do not require fsync'ing.
    if io_object == IOObject::IOOBJECT_TEMP_RELATION
        && matches!(io_op, IOOp::IOOP_FSYNC | IOOp::IOOP_WRITEBACK)
    {
        return false;
    }

    // Some IOOps are not valid in certain IOContexts.
    if io_context == IOContext::IOCONTEXT_BULKREAD && io_op == IOOp::IOOP_EXTEND {
        return false;
    }

    let strategy_io_context = matches!(
        io_context,
        IOContext::IOCONTEXT_BULKREAD
            | IOContext::IOCONTEXT_BULKWRITE
            | IOContext::IOCONTEXT_VACUUM
    );

    // IOOP_REUSE is only relevant when a BufferAccessStrategy is in use.
    if !strategy_io_context && io_op == IOOp::IOOP_REUSE {
        return false;
    }

    // IOOBJECT_WAL will not do certain IOOps depending on IOContext.
    if io_object == IOObject::IOOBJECT_WAL
        && io_context == IOContext::IOCONTEXT_INIT
        && !matches!(io_op, IOOp::IOOP_WRITE | IOOp::IOOP_FSYNC)
    {
        return false;
    }

    if io_object == IOObject::IOOBJECT_WAL
        && io_context == IOContext::IOCONTEXT_NORMAL
        && !matches!(io_op, IOOp::IOOP_WRITE | IOOp::IOOP_READ | IOOp::IOOP_FSYNC)
    {
        return false;
    }

    // IOOP_FSYNC done by a backend using a BufferAccessStrategy is counted in
    // the IOCONTEXT_NORMAL IOContext.
    if strategy_io_context && io_op == IOOp::IOOP_FSYNC {
        return false;
    }

    true
}

// Index → enum helpers (the C loops iterate raw `int` indices).
fn io_object_from_index(i: usize) -> IOObject {
    match i {
        0 => IOObject::IOOBJECT_RELATION,
        1 => IOObject::IOOBJECT_TEMP_RELATION,
        2 => IOObject::IOOBJECT_WAL,
        _ => unreachable!("io_object index out of range"),
    }
}

fn io_context_from_index(i: usize) -> IOContext {
    match i {
        0 => IOContext::IOCONTEXT_BULKREAD,
        1 => IOContext::IOCONTEXT_BULKWRITE,
        2 => IOContext::IOCONTEXT_INIT,
        3 => IOContext::IOCONTEXT_NORMAL,
        4 => IOContext::IOCONTEXT_VACUUM,
        _ => unreachable!("io_context index out of range"),
    }
}

fn io_op_from_index(i: usize) -> IOOp {
    match i {
        0 => IOOp::IOOP_EVICT,
        1 => IOOp::IOOP_FSYNC,
        2 => IOOp::IOOP_HIT,
        3 => IOOp::IOOP_REUSE,
        4 => IOOp::IOOP_WRITEBACK,
        5 => IOOp::IOOP_EXTEND,
        6 => IOOp::IOOP_READ,
        7 => IOOp::IOOP_WRITE,
        _ => unreachable!("io_op index out of range"),
    }
}

// ---------------------------------------------------------------------------
// Kind registration + seam installation.
// ---------------------------------------------------------------------------

/// The `PgStat_KindInfo` metadata for `PGSTAT_KIND_IO`
/// (`pgstat.c:pgstat_kind_builtin_infos[PGSTAT_KIND_IO]`).
///
/// The C byte offsets (`snapshot_ctl_off` / `shared_ctl_off` /
/// `shared_data_off` / `shared_data_len`) are used only by the on-disk
/// (de)serialization machinery; the runtime callback dispatch uses typed field
/// projection instead, so they are left 0 here (the serializer is a follow-on).
/// `shared_size` is 0 because IO is a fixed kind with a dedicated control-block
/// field, not a `custom_data` entry.
fn io_kind_info() -> PgStat_KindInfo {
    PgStat_KindInfo {
        fixed_amount: true,
        accessed_across_databases: false,
        write_to_file: true,
        shared_size: 0,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        shared_data_off: 0,
        shared_data_len: 0,
        pending_size: 0,
        name: "io",
    }
}

/// Register `PGSTAT_KIND_IO` and install the IO outward seams.
pub fn init_seams() {
    // Register the fixed kind's callbacks in pgstat_kind_builtin_infos[].
    registry::register(
        KindInfoBuilder::new(PGSTAT_KIND_IO, io_kind_info())
            .init_shmem_cb(pgstat_io_init_shmem_cb)
            .reset_all_cb(pgstat_io_reset_all_cb)
            .snapshot_cb(pgstat_io_snapshot_cb)
            .flush_static_cb(pgstat_io_flush_cb),
    );

    // pgstat_io.c outward seams (the WAL-write timing path consumed by
    // walreceiver / xlogrecovery).
    backend_utils_activity_pgstat_io_seams::pgstat_prepare_io_time::set(pgstat_prepare_io_time_seam);
    backend_utils_activity_pgstat_io_seams::pgstat_count_io_op_time::set(
        pgstat_count_io_op_time_wal_write_seam,
    );

    // pgstat_flush_io: both the IO-seams (-> bool, walsender) and the stat-seams
    // (-> PgResult<bool>, bgwriter / checkpointer) declarations resolve here.
    backend_utils_activity_pgstat_io_seams::pgstat_flush_io::set(|nowait| {
        // The IO-seams shape returns bool: `true` if a nowait flush could not
        // acquire the lock; the walsender caller discards it. A genuine error
        // (lock release failure) is unreachable on the flush path; map it to
        // `true` (not flushed) defensively.
        pgstat_io_flush_cb(nowait).unwrap_or(true)
    });
    backend_utils_activity_stat_seams::pgstat_flush_io::set(pgstat_io_flush_cb);
}
