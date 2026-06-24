// walreceiverfuncs.c uses C identifier conventions (CamelCase functions,
// ALLCAPS constants). Mirror the sibling replication crates with crate-level
// allows rather than scattering per-item `#[allow]`s.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
// `PgResult<_>` carries the large `PgError` payload (the project-wide error
// type); this matches the sibling backend crates' `#![allow]`.
#![allow(clippy::result_large_err)]

//! `replication/walreceiverfuncs.c` — startup-process side of the walreceiver
//! protocol.
//!
//! These functions are used by the startup process to communicate with the
//! walreceiver process. The walreceiver process itself is in `walreceiver.c`
//! (a separate, already-ported unit). This crate **owns** the process-wide
//! `WalRcvData` shared-memory control block: the walreceiver port reaches the
//! spinlock-guarded fields through the [`with_walrcv`] accessor seam, and the
//! lock-free `writtenUpto` / `force_reply` words through their own seams.
//!
//! # Shared-memory model
//!
//! The C struct is a `ShmemInitStruct` segment with a `slock_t mutex`. Here it
//! is the owned [`WalRcvShared`] singleton: a host `Mutex<WalRcvData>` for the
//! spinlock-guarded fields plus two lock-free atomics. The owner also holds
//! `startTime` (a spinlock-guarded field that walreceiver.c never reads, so it
//! is not in the shared `WalRcvData` carrier) and the `walRcvStoppedCV`
//! condition variable. Every critical section that touches `startTime` holds
//! the `WalRcvData` mutex first, matching the single C spinlock.
//!
//! # Function inventory (`walreceiverfuncs.c`, PostgreSQL 18.3 — 10 functions)
//!
//!  1. `WalRcvShmemSize`               — lines 43-51
//!  2. `WalRcvShmemInit`               — lines 54-72
//!  3. `WalRcvRunning`                 — lines 75-120
//!  4. `WalRcvStreaming`               — lines 126-172
//!  5. `ShutdownWalRcv`                — lines 178-230
//!  6. `RequestXLogStreaming`          — lines 245-321
//!  7. `GetWalRcvFlushRecPtr`          — lines 331-346
//!  8. `GetWalRcvWriteRecPtr`          — lines 352-358
//!  9. `GetReplicationApplyDelay`      — lines 364-388
//! 10. `GetReplicationTransferLatency` — lines 394-408

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use core::cell::Cell;
use core::mem::size_of;
use std::sync::atomic::Ordering;

use ::pmsignal::{PMSignalReason, SendPostmasterSignal};
use ::condition_variable::{
    ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariablePrepareToSleep, ConditionVariableSleep,
};
use ::types_core::{
    pg_time_t, Size, TimeLineID, TimestampTz, XLogRecPtr, INVALID_PROC_NUMBER,
};
use ::types_error::PgResult;
use ::types_pgstat::wait_event::WAIT_EVENT_WAL_RECEIVER_EXIT;
use ::types_storage::storage::Spinlock;
use ::types_walreceiver::{
    walrcv_strlcpy, WalRcvData, WalRcvShared, WalRcvState, MAXCONNINFO, NAMEDATALEN,
};

use transam_xlog_seams as xlog;
use xlogrecovery_seams as xlogrecovery;
use latch_seams as latch;
use walreceiverfuncs_seams as funcs_seams;
use timestamp_seams as timestamp;
use ipc_shmem_seams as shmem;
use condition_variable_seams as condvar;

/// `WALRCV_STARTUP_TIMEOUT` — how long to wait for walreceiver to start up
/// after requesting postmaster to launch it, in seconds.
/// (`walreceiverfuncs.c:40`)
pub const WALRCV_STARTUP_TIMEOUT: pg_time_t = 10;

// ===========================================================================
// `WalRcvData *WalRcv` (`walreceiverfuncs.c:34`) — the single shared control
// block, allocated by `WalRcvShmemInit` in the REAL shared-memory segment via
// `ShmemInitStruct`. The C file-scope global pointer is reproduced as a
// per-backend thread-local cell holding the genuine shmem address (each
// backend attaches to the same segment); this mirrors the proven
// `XLogRecoveryCtl` pattern in xlogrecovery/shmem.rs.
// ===========================================================================

std::thread_local! {
    /// `static WalRcvData *WalRcv = NULL;` (`walreceiverfuncs.c:34`).
    static WAL_RCV: Cell<*mut WalRcvShared> = const { Cell::new(core::ptr::null_mut()) };
}

/// The live `WalRcv` shmem pointer, or null before `WalRcvShmemInit`.
#[inline]
fn wal_rcv_ptr() -> *mut WalRcvShared {
    let p = WAL_RCV.with(Cell::get);
    debug_assert!(!p.is_null(), "WalRcv accessed before WalRcvShmemInit");
    p
}

/// Borrow the shared control block (`WalRcvData *walrcv = WalRcv;`). Panics if
/// the shmem region has not been initialized yet (the C code would dereference
/// a NULL pointer); callers run only after `WalRcvShmemInit`.
#[inline]
fn wal_rcv() -> &'static WalRcvShared {
    // SAFETY: `wal_rcv_ptr()` points at the live `ShmemInitStruct` region, which
    // outlives the process; the guarded fields are synchronized by `mutex` at
    // their points of use, the lock-free words by their atomics.
    unsafe { &*wal_rcv_ptr() }
}

// ---------------------------------------------------------------------------
// SpinLockAcquire / SpinLockRelease / SpinLockInit over `WalRcv->mutex`.
// ---------------------------------------------------------------------------

#[inline]
fn spin_lock_acquire(lock: &Spinlock) {
    s_lock::s_lock_macro(lock, Some(file!()), line!() as i32, None);
}

#[inline]
fn spin_lock_release(lock: &Spinlock) {
    s_lock::s_unlock(lock);
}

#[inline]
fn spin_lock_init(lock: &Spinlock) {
    s_lock::s_init_lock(lock);
}

/// `SpinLockAcquire(&WalRcv->mutex); r = f(&WalRcv->data); SpinLockRelease(...)`.
/// The C spinlock is non-reentrant: `f` must not take the lock again.
#[inline]
fn with_locked<R>(f: impl FnOnce(&'static mut WalRcvData) -> R) -> R {
    let p = wal_rcv_ptr();
    // SAFETY: the spinlock lives in shmem; acquiring it excludes every other
    // accessor of `data`, after which we form the exclusive `&mut` for `f`.
    let walrcv = unsafe { &*p };
    spin_lock_acquire(&walrcv.mutex);
    // SAFETY: the spinlock is held, so this `&mut` to the shmem `data` field is
    // the unique live reference; it lives only for the duration of `f`.
    let data = unsafe { &mut (*p).data };
    let r = f(data);
    spin_lock_release(&walrcv.mutex);
    r
}

/// `&mut WalRcv->startTime` under the held lock — the C `startTime` is guarded
/// by the same spinlock as `data`. Used inside a `with_locked`-bracketed
/// section where the lock is already held, so this only reborrows the field.
#[inline]
#[allow(clippy::mut_from_ref)]
fn start_time_mut(_walrcv: &'static WalRcvShared) -> &'static mut pg_time_t {
    // SAFETY: caller holds `WalRcv->mutex` (this is only called from within a
    // `with_locked` closure), so this `&mut` to the shmem `startTime` field is
    // the unique live reference.
    unsafe { &mut (*wal_rcv_ptr()).startTime }
}

/// `XLogSegmentOffset(xlogptr, wal_segsz_bytes)` (`access/xlog_internal.h`) —
/// the byte offset of `xlogptr` within its WAL segment. Pure arithmetic
/// (`xlogptr & (wal_segsz_bytes - 1)`), ported in-crate exactly as the C macro.
fn XLogSegmentOffset(xlogptr: XLogRecPtr, wal_segsz_bytes: i32) -> u32 {
    (xlogptr & (wal_segsz_bytes as u64 - 1)) as u32
}

/// `(pg_time_t) time(NULL)` — current wall-clock seconds since the Unix epoch.
/// The C uses libc `time` directly (not a PostgreSQL routine).
fn now_seconds() -> pg_time_t {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as pg_time_t,
        Err(e) => -(e.duration().as_secs() as pg_time_t),
    }
}

/// `kill(pid, SIGTERM)` — terminate the still-running walreceiver process. The
/// C calls libc `kill` directly.
fn kill_sigterm(pid: i32) {
    // SAFETY: a plain `kill(2)` syscall; it has no Rust-memory effects.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:43-51
 * ------------------------------------------------------------------------ */

/// Report shared memory space needed by [`WalRcvShmemInit`].
pub fn WalRcvShmemSize() -> PgResult<Size> {
    let mut size: Size = 0;

    size = add_size(size, core::mem::size_of::<WalRcvShared>())?;

    Ok(size)
}

/// `add_size(s1, s2)` (shmem.c) — add two sizes, erroring on overflow exactly
/// like the C `ereport(ERROR, "requested shared memory size overflows ...")`.
fn add_size(s1: Size, s2: Size) -> PgResult<Size> {
    s1.checked_add(s2).ok_or_else(|| {
        ::types_error::PgError::error("requested shared memory size overflows size_t")
    })
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:54-72
 * ------------------------------------------------------------------------ */

/// Allocate and initialize walreceiver-related shared memory.
pub fn WalRcvShmemInit() -> PgResult<()> {
    // WalRcv = (WalRcvData *) ShmemInitStruct("Wal Receiver Ctl",
    //                                         WalRcvShmemSize(), &found);
    let (raw, found) = shmem::shmem_init_struct::call("Wal Receiver Ctl", WalRcvShmemSize()?)?;
    let walrcv_ptr = raw as *mut WalRcvShared;
    WAL_RCV.with(|c| c.set(walrcv_ptr));

    if found {
        return Ok(());
    }

    // First time through, so initialize.
    //
    // SAFETY: a fresh region of `WalRcvShmemSize()` bytes; single-process init
    // (this runs in the postmaster before any backend forks).
    unsafe {
        // MemSet(WalRcv, 0, WalRcvShmemSize());
        core::ptr::write_bytes(raw, 0, size_of::<WalRcvShared>());

        let walrcv = &mut *walrcv_ptr;

        // WalRcv->walRcvState = WALRCV_STOPPED;  (zero == WALRCV_STOPPED, but
        // write the typed value to keep the enum discriminant well-formed).
        walrcv.data.walRcvState = WalRcvState::WALRCV_STOPPED;
        // ConditionVariableInit(&WalRcv->walRcvStoppedCV);
        condvar::condition_variable_init::call(&mut walrcv.walRcvStoppedCV);
        // SpinLockInit(&WalRcv->mutex);
        spin_lock_init(&walrcv.mutex);
        // pg_atomic_init_u64(&WalRcv->writtenUpto, 0);
        walrcv.writtenUpto.store(0, Ordering::SeqCst);
        // WalRcv->procno = INVALID_PROC_NUMBER;
        walrcv.data.procno = INVALID_PROC_NUMBER;
    }

    Ok(())
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:75-120
 * ------------------------------------------------------------------------ */

/// Is walreceiver running (or starting up)?
pub fn WalRcvRunning() -> PgResult<bool> {
    let walrcv = wal_rcv();

    // SpinLockAcquire / read state+startTime / SpinLockRelease.
    let (mut state, start_time) =
        with_locked(|data| (data.walRcvState, *start_time_mut(walrcv)));

    // If it has taken too long for walreceiver to start up, give up. Setting the
    // state to STOPPED ensures that if walreceiver later does start up after
    // all, it will see that it's not supposed to be running and die without
    // doing anything.
    if state == WalRcvState::WALRCV_STARTING {
        let now = now_seconds();

        if (now - start_time) > WALRCV_STARTUP_TIMEOUT {
            let stopped = with_locked(|data| {
                // Re-check the state after re-acquiring the lock.
                if data.walRcvState == WalRcvState::WALRCV_STARTING {
                    data.walRcvState = WalRcvState::WALRCV_STOPPED;
                    state = WalRcvState::WALRCV_STOPPED;
                    true
                } else {
                    false
                }
            });

            if stopped {
                ConditionVariableBroadcast(&walrcv.walRcvStoppedCV);
            }
        }
    }

    Ok(state != WalRcvState::WALRCV_STOPPED)
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:126-172
 * ------------------------------------------------------------------------ */

/// Is walreceiver running and streaming (or at least attempting to connect, or
/// starting up)?
pub fn WalRcvStreaming() -> PgResult<bool> {
    let walrcv = wal_rcv();

    let (mut state, start_time) =
        with_locked(|data| (data.walRcvState, *start_time_mut(walrcv)));

    if state == WalRcvState::WALRCV_STARTING {
        let now = now_seconds();

        if (now - start_time) > WALRCV_STARTUP_TIMEOUT {
            let stopped = with_locked(|data| {
                if data.walRcvState == WalRcvState::WALRCV_STARTING {
                    data.walRcvState = WalRcvState::WALRCV_STOPPED;
                    state = WalRcvState::WALRCV_STOPPED;
                    true
                } else {
                    false
                }
            });

            if stopped {
                ConditionVariableBroadcast(&walrcv.walRcvStoppedCV);
            }
        }
    }

    Ok(matches!(
        state,
        WalRcvState::WALRCV_STREAMING
            | WalRcvState::WALRCV_STARTING
            | WalRcvState::WALRCV_RESTARTING
    ))
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:178-230
 * ------------------------------------------------------------------------ */

/// Stop walreceiver (if running) and wait for it to die. Executed by the
/// Startup process.
pub fn ShutdownWalRcv() -> PgResult<()> {
    let walrcv = wal_rcv();
    let mut walrcvpid: i32 = 0;
    let mut stopped = false;

    // Request walreceiver to stop. Walreceiver will switch to WALRCV_STOPPED
    // mode once it's finished, and will also request postmaster to not restart
    // itself.
    with_locked(|data| {
        match data.walRcvState {
            WalRcvState::WALRCV_STOPPED => {}
            WalRcvState::WALRCV_STARTING => {
                data.walRcvState = WalRcvState::WALRCV_STOPPED;
                stopped = true;
            }
            WalRcvState::WALRCV_STREAMING
            | WalRcvState::WALRCV_WAITING
            | WalRcvState::WALRCV_RESTARTING => {
                data.walRcvState = WalRcvState::WALRCV_STOPPING;
                // fall through
                walrcvpid = data.pid;
            }
            WalRcvState::WALRCV_STOPPING => {
                walrcvpid = data.pid;
            }
        }
    });

    // Unnecessary but consistent.
    if stopped {
        ConditionVariableBroadcast(&walrcv.walRcvStoppedCV);
    }

    // Signal walreceiver process if it was still running.
    if walrcvpid != 0 {
        kill_sigterm(walrcvpid);
    }

    // Wait for walreceiver to acknowledge its death by setting state to
    // WALRCV_STOPPED.
    ConditionVariablePrepareToSleep(&walrcv.walRcvStoppedCV);
    while WalRcvRunning()? {
        ConditionVariableSleep(&walrcv.walRcvStoppedCV, WAIT_EVENT_WAL_RECEIVER_EXIT)?;
    }
    ConditionVariableCancelSleep();

    Ok(())
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:245-321
 * ------------------------------------------------------------------------ */

/// Request postmaster to start walreceiver.
///
/// `recptr` indicates the position where streaming should begin. `conninfo` is
/// a libpq connection string to use. `slotname` is, optionally, the name of a
/// replication slot to acquire. `create_temp_slot` indicates to create a
/// temporary slot when no `slotname` is given.
///
/// WAL receivers do not directly load GUC parameters used for the connection to
/// the primary, and rely on the values passed down by the caller instead.
pub fn RequestXLogStreaming(
    tli: TimeLineID,
    mut recptr: XLogRecPtr,
    conninfo: Option<&[u8]>,
    slotname: Option<&[u8]>,
    create_temp_slot: bool,
) -> PgResult<()> {
    let walrcv = wal_rcv();
    let mut launch = false;
    let now = now_seconds();

    let wal_segment_size = xlog::wal_segment_size::call();

    // We always start at the beginning of the segment. That prevents a broken
    // segment (i.e., with no records in the first half of a segment) from being
    // created by XLOG streaming, which might cause trouble later on if the
    // segment is e.g archived.
    if XLogSegmentOffset(recptr, wal_segment_size) != 0 {
        recptr -= XLogSegmentOffset(recptr, wal_segment_size) as XLogRecPtr;
    }

    let walrcv_proc = with_locked(|data| {
        // It better be stopped if we try to restart it.
        debug_assert!(
            data.walRcvState == WalRcvState::WALRCV_STOPPED
                || data.walRcvState == WalRcvState::WALRCV_WAITING
        );

        match conninfo {
            Some(conninfo) => walrcv_strlcpy(&mut data.conninfo, conninfo),
            None => data.conninfo = [0; MAXCONNINFO],
        }

        // Use configured replication slot if present, and ignore the value of
        // create_temp_slot as the slot name should be persistent. Otherwise,
        // use create_temp_slot to determine whether this WAL receiver should
        // create a temporary slot by itself and use it, or not.
        match slotname {
            Some(slotname) if !slotname.is_empty() && slotname[0] != 0 => {
                walrcv_strlcpy(&mut data.slotname, slotname);
                data.is_temp_slot = false;
            }
            _ => {
                data.slotname = [0; NAMEDATALEN];
                data.is_temp_slot = create_temp_slot;
            }
        }

        if data.walRcvState == WalRcvState::WALRCV_STOPPED {
            launch = true;
            data.walRcvState = WalRcvState::WALRCV_STARTING;
        } else {
            data.walRcvState = WalRcvState::WALRCV_RESTARTING;
        }
        *start_time_mut(walrcv) = now;

        // If this is the first startup of walreceiver (on this timeline),
        // initialize flushedUpto and latestChunkStart to the starting point.
        if data.receiveStart == 0 || data.receivedTLI != tli {
            data.flushedUpto = recptr;
            data.receivedTLI = tli;
            data.latestChunkStart = recptr;
        }
        data.receiveStart = recptr;
        data.receiveStartTLI = tli;

        data.procno
    });

    if launch {
        // SendPostmasterSignal(PMSIGNAL_START_WALRECEIVER);
        SendPostmasterSignal(PMSignalReason::PMSIGNAL_START_WALRECEIVER);
    } else if walrcv_proc != INVALID_PROC_NUMBER {
        // SetLatch(&GetPGProcByNumber(walrcv_proc)->procLatch);
        latch::set_latch_for_procno::call(walrcv_proc);
    }

    Ok(())
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:331-346
 * ------------------------------------------------------------------------ */

/// Returns the last+1 byte position that walreceiver has flushed.
///
/// Optionally returns the previous chunk start (the first byte written in the
/// most recent walreceiver flush cycle) and the receive TLI; callers not
/// interested may pass `None`.
pub fn GetWalRcvFlushRecPtr(
    latest_chunk_start: Option<&mut XLogRecPtr>,
    receive_tli: Option<&mut TimeLineID>,
) -> XLogRecPtr {
    with_locked(|data| {
        let recptr = data.flushedUpto;
        if let Some(latest_chunk_start) = latest_chunk_start {
            *latest_chunk_start = data.latestChunkStart;
        }
        if let Some(receive_tli) = receive_tli {
            *receive_tli = data.receivedTLI;
        }
        recptr
    })
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:352-358
 * ------------------------------------------------------------------------ */

/// Returns the last+1 byte position that walreceiver has written. This returns
/// a recently written value without taking a lock.
pub fn GetWalRcvWriteRecPtr() -> XLogRecPtr {
    wal_rcv().writtenUpto.load(Ordering::SeqCst)
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:364-388
 * ------------------------------------------------------------------------ */

/// Returns the replication apply delay in ms, or -1 if the apply delay info is
/// not available.
pub fn GetReplicationApplyDelay() -> i32 {
    let receive_ptr = with_locked(|data| data.flushedUpto);

    // GetXLogReplayRecPtr(NULL): the C passes NULL for the TLI out-param.
    let (replay_ptr, _tli) = xlogrecovery::get_xlog_replay_rec_ptr::call();

    if receive_ptr == replay_ptr {
        return 0;
    }

    let chunk_replay_start_time = xlogrecovery::get_current_chunk_replay_start_time::call();

    if chunk_replay_start_time == 0 {
        return -1;
    }

    timestamp::timestamp_difference_milliseconds::call(
        chunk_replay_start_time,
        timestamp::get_current_timestamp::call(),
    ) as i32
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:394-408
 * ------------------------------------------------------------------------ */

/// Returns the network latency in ms. Note that this includes any difference in
/// clock settings between the servers, as well as timezone.
pub fn GetReplicationTransferLatency() -> i32 {
    let (last_msg_send_time, last_msg_receipt_time): (TimestampTz, TimestampTz) =
        with_locked(|data| (data.lastMsgSendTime, data.lastMsgReceiptTime));

    timestamp::timestamp_difference_milliseconds::call(last_msg_send_time, last_msg_receipt_time)
        as i32
}

/* ------------------------------------------------------------------------
 * Seam accessors — the walreceiver port reaches the owner-resident block
 * through these. They are thin marshal + delegate over the owned `WalRcv`.
 * ------------------------------------------------------------------------ */

/// `SpinLockAcquire(&WalRcv->mutex); f(WalRcv); SpinLockRelease(...)` —
/// run the caller's closure with exclusive access to the spinlock-guarded
/// fields (`with_walrcv` seam).
fn with_walrcv(f: &mut dyn FnMut(&mut WalRcvData)) {
    with_locked(|data| f(data));
}

/// `pg_atomic_write_u64(&WalRcv->writtenUpto, val)` (`set_written_upto` seam).
fn set_written_upto(val: XLogRecPtr) {
    wal_rcv().writtenUpto.store(val, Ordering::SeqCst);
}

/// `pg_atomic_read_u64(&WalRcv->writtenUpto)` (`get_written_upto` seam).
fn get_written_upto() -> XLogRecPtr {
    wal_rcv().writtenUpto.load(Ordering::SeqCst)
}

/// `WalRcv->force_reply = true` with a write barrier (`set_force_reply` seam).
fn set_force_reply() {
    // C: SpinLockAcquire-free store of a sig_atomic_t with pg_memory_barrier()
    // before the store, so the reader sees a consistent walreceiver state. A
    // SeqCst store provides the same ordering.
    wal_rcv().force_reply.store(1, Ordering::SeqCst);
}

/// Read-and-clear `WalRcv->force_reply` with the barrier (`take_force_reply`
/// seam). Returns whether a reply was requested.
fn take_force_reply() -> bool {
    // C: pg_memory_barrier(); then read force_reply and, if set, clear it.
    wal_rcv().force_reply.swap(0, Ordering::SeqCst) != 0
}

/// `ConditionVariableBroadcast(&WalRcv->walRcvStoppedCV)`
/// (`wal_rcv_stopped_cv_broadcast` seam).
fn wal_rcv_stopped_cv_broadcast() {
    ConditionVariableBroadcast(&wal_rcv().walRcvStoppedCV);
}

/// Seam adapter for `request_xlog_streaming` — the recovery page-read driver
/// passes `conninfo`/`slotname` as `&str` (non-NULL `const char *` in C, where
/// `PrimaryConnInfo`/`PrimarySlotName` are always allocated strings). Map both
/// to `Some(bytes)`: a non-NULL empty C string copies zero bytes through
/// `strlcpy`, which `RequestXLogStreaming`'s `Some(b"")` arm reproduces exactly.
fn request_xlog_streaming_seam(
    tli: TimeLineID,
    recptr: XLogRecPtr,
    conninfo: &str,
    slotname: &str,
    create_temp_slot: bool,
) -> PgResult<()> {
    RequestXLogStreaming(
        tli,
        recptr,
        Some(conninfo.as_bytes()),
        Some(slotname.as_bytes()),
        create_temp_slot,
    )
}

/// Seam adapter for `get_wal_rcv_flush_rec_ptr_full` — the C
/// `GetWalRcvFlushRecPtr(&latestChunkStart, &receiveTLI)` returns the flush
/// pointer and writes the two out-params; the seam hands all three back as a
/// tuple `(flushedUpto, latestChunkStart, receiveTLI)`.
fn get_wal_rcv_flush_rec_ptr_full_seam() -> (XLogRecPtr, XLogRecPtr, TimeLineID) {
    let mut latest_chunk_start: XLogRecPtr = 0;
    let mut receive_tli: TimeLineID = 0;
    let flushed = GetWalRcvFlushRecPtr(Some(&mut latest_chunk_start), Some(&mut receive_tli));
    (flushed, latest_chunk_start, receive_tli)
}

/// Install this unit's seams (`with_walrcv`, the atomic accessors, the shmem
/// size/init pair, `WalRcvRunning`, and the apply-delay / transfer-latency
/// helpers). Wired into `init::init_all()`.
pub fn init_seams() {
    funcs_seams::wal_rcv_shmem_size::set(WalRcvShmemSize);
    funcs_seams::wal_rcv_shmem_init::set(WalRcvShmemInit);
    funcs_seams::with_walrcv::set(with_walrcv);
    funcs_seams::set_written_upto::set(set_written_upto);
    funcs_seams::get_written_upto::set(get_written_upto);
    funcs_seams::set_force_reply::set(set_force_reply);
    funcs_seams::take_force_reply::set(take_force_reply);
    funcs_seams::wal_rcv_stopped_cv_broadcast::set(wal_rcv_stopped_cv_broadcast);
    funcs_seams::get_replication_apply_delay::set(GetReplicationApplyDelay);
    funcs_seams::get_replication_transfer_latency::set(GetReplicationTransferLatency);
    funcs_seams::wal_rcv_running::set(WalRcvRunning);

    // Streaming-control entry points consumed by the recovery page-read driver
    // (xlogrecovery.c WaitForWALToBecomeAvailable). These are the genuine
    // walreceiverfuncs.c routines owned here.
    funcs_seams::wal_rcv_streaming::set(WalRcvStreaming);
    funcs_seams::request_xlog_streaming::set(request_xlog_streaming_seam);
    funcs_seams::get_wal_rcv_flush_rec_ptr_full::set(get_wal_rcv_flush_rec_ptr_full_seam);
    // `ShutdownWalRcv` is the inner walreceiverfuncs.c routine; the xlog.c
    // `XLogShutdownWalRcv` wrapper (xlog-owned) calls it via this seam. The C
    // return is void; an `ereport(ERROR)` from the condition-variable wait loop
    // unwinds (here: panic at the void seam boundary, matching the longjmp).
    funcs_seams::shutdown_wal_rcv::set(|| ShutdownWalRcv().expect("ShutdownWalRcv failed"));

    // Note: `walreceiver_seams::get_wal_rcv_flush_rec_ptr`
    // (the `(lsn, tli)` form consumed by xlog checkpoint / walsummarizer) is
    // installed by the `walreceiver` crate, where `walreceiver.h` declares it.
    // It is NOT installed here to avoid a double-install of the same seam.
}

#[cfg(test)]
mod tests;
