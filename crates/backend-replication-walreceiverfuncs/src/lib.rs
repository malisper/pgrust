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

use std::sync::atomic::Ordering;
use std::sync::{Mutex, OnceLock};

use backend_storage_ipc_pmsignal::{PMSignalReason, SendPostmasterSignal};
use backend_storage_lmgr_condition_variable::{
    ConditionVariableBroadcast, ConditionVariableCancelSleep, ConditionVariableInit,
    ConditionVariablePrepareToSleep, ConditionVariableSleep,
};
use types_condvar::ConditionVariable;
use types_core::{
    pg_time_t, Size, TimeLineID, TimestampTz, XLogRecPtr, INVALID_PROC_NUMBER,
};
use types_error::PgResult;
use types_pgstat::wait_event::WAIT_EVENT_WAL_RECEIVER_EXIT;
use types_walreceiver::{WalRcvData, WalRcvShared, WalRcvState, MAXCONNINFO, NAMEDATALEN};

use backend_access_transam_xlog_seams as xlog;
use backend_access_transam_xlogrecovery_seams as xlogrecovery;
use backend_storage_ipc_latch_seams as latch;
use backend_replication_walreceiverfuncs_seams as funcs_seams;
use backend_utils_adt_timestamp_seams as timestamp;

/// `WALRCV_STARTUP_TIMEOUT` — how long to wait for walreceiver to start up
/// after requesting postmaster to launch it, in seconds.
/// (`walreceiverfuncs.c:40`)
pub const WALRCV_STARTUP_TIMEOUT: pg_time_t = 10;

/// The owned, process-wide `WalRcvData` control block.
///
/// `shared` carries the spinlock-guarded fields walreceiver.c shares plus the
/// two lock-free atomics. `start_time` and `stopped_cv` are owner-only fields
/// of the C struct that walreceiver.c does not access through the carrier, so
/// they live here rather than in `WalRcvData`. They are still logically guarded
/// by the same spinlock: every access takes `shared.guarded` first.
struct OwnedWalRcv {
    /// `slock_t mutex` + the guarded fields, plus `writtenUpto` / `force_reply`.
    shared: WalRcvShared,
    /// `pg_time_t startTime` — guarded by the same lock as `shared.guarded`.
    start_time: Mutex<pg_time_t>,
    /// `ConditionVariable walRcvStoppedCV`.
    stopped_cv: ConditionVariable,
}

/// `WalRcvData *WalRcv = NULL;` (`walreceiverfuncs.c:34`) — the single shared
/// control block, allocated by [`WalRcvShmemInit`].
static WAL_RCV: OnceLock<OwnedWalRcv> = OnceLock::new();

/// Borrow the shared control block (`WalRcvData *walrcv = WalRcv;`).
fn wal_rcv() -> &'static OwnedWalRcv {
    WAL_RCV
        .get()
        .expect("WalRcv accessed before WalRcvShmemInit")
}

/// `XLogSegmentOffset(xlogptr, wal_segsz_bytes)` (`access/xlog_internal.h`) —
/// the byte offset of `xlogptr` within its WAL segment. Pure arithmetic
/// (`xlogptr & (wal_segsz_bytes - 1)`), ported in-crate exactly as the C macro.
fn XLogSegmentOffset(xlogptr: XLogRecPtr, wal_segsz_bytes: i32) -> u32 {
    (xlogptr & (wal_segsz_bytes as u64 - 1)) as u32
}

/// `strlcpy(dst_field, src, size)` — overwrite an owned fixed-capacity field
/// with at most `size-1` bytes of `src` (the C buffers are NUL-terminated, so
/// the cap drops the trailing NUL slot). `src` is a NUL-terminated byte slice;
/// copying stops at the first NUL, exactly as the C `strlcpy`.
fn strlcpy_field(dst: &mut String, src: &[u8], size: usize) {
    let src_len = src.iter().position(|&b| b == 0).unwrap_or(src.len());
    let n = size.saturating_sub(1);
    let copy = src_len.min(n);
    *dst = String::from_utf8_lossy(&src[..copy]).into_owned();
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
        types_error::PgError::error("requested shared memory size overflows size_t")
    })
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:54-72
 * ------------------------------------------------------------------------ */

/// Allocate and initialize walreceiver-related shared memory.
pub fn WalRcvShmemInit() -> PgResult<()> {
    // ShmemInitStruct("Wal Receiver Ctl", ...) + MemSet(WalRcv, 0, ...): build
    // the one zeroed block. `WalRcvShared::default()` is the zero state
    // (walRcvState = WALRCV_STOPPED, atomics 0); `start_time` is 0 and the
    // condition variable starts uninitialized, matching the MemSet. We then
    // perform the explicit re-initialization the C does after the MemSet.
    let _ = WAL_RCV.get_or_init(|| {
        let owned = OwnedWalRcv {
            shared: WalRcvShared::default(),
            start_time: Mutex::new(0),
            stopped_cv: ConditionVariable::default(),
        };
        // walrcv->walRcvState = WALRCV_STOPPED;  (already the default)
        // ConditionVariableInit(&walrcv->walRcvStoppedCV);
        ConditionVariableInit(&owned.stopped_cv);
        // SpinLockInit(&WalRcv->mutex);  — the host Mutex starts unlocked.
        // pg_atomic_init_u64(&WalRcv->writtenUpto, 0);  (already 0)
        owned.shared.writtenUpto.store(0, Ordering::SeqCst);
        // WalRcv->procno = INVALID_PROC_NUMBER;
        owned.shared.guarded.lock().unwrap().procno = INVALID_PROC_NUMBER;
        owned
    });

    Ok(())
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:75-120
 * ------------------------------------------------------------------------ */

/// Is walreceiver running (or starting up)?
pub fn WalRcvRunning() -> PgResult<bool> {
    let walrcv = wal_rcv();

    // SpinLockAcquire / read state+startTime / SpinLockRelease.
    let (mut state, start_time) = {
        let guard = walrcv.shared.guarded.lock().unwrap();
        let st = *walrcv.start_time.lock().unwrap();
        (guard.walRcvState, st)
    };

    // If it has taken too long for walreceiver to start up, give up. Setting the
    // state to STOPPED ensures that if walreceiver later does start up after
    // all, it will see that it's not supposed to be running and die without
    // doing anything.
    if state == WalRcvState::WALRCV_STARTING {
        let now = now_seconds();

        if (now - start_time) > WALRCV_STARTUP_TIMEOUT {
            let stopped = {
                let mut guard = walrcv.shared.guarded.lock().unwrap();
                // Re-check the state after re-acquiring the lock.
                if guard.walRcvState == WalRcvState::WALRCV_STARTING {
                    guard.walRcvState = WalRcvState::WALRCV_STOPPED;
                    state = WalRcvState::WALRCV_STOPPED;
                    true
                } else {
                    false
                }
            };

            if stopped {
                ConditionVariableBroadcast(&walrcv.stopped_cv);
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

    let (mut state, start_time) = {
        let guard = walrcv.shared.guarded.lock().unwrap();
        let st = *walrcv.start_time.lock().unwrap();
        (guard.walRcvState, st)
    };

    if state == WalRcvState::WALRCV_STARTING {
        let now = now_seconds();

        if (now - start_time) > WALRCV_STARTUP_TIMEOUT {
            let stopped = {
                let mut guard = walrcv.shared.guarded.lock().unwrap();
                if guard.walRcvState == WalRcvState::WALRCV_STARTING {
                    guard.walRcvState = WalRcvState::WALRCV_STOPPED;
                    state = WalRcvState::WALRCV_STOPPED;
                    true
                } else {
                    false
                }
            };

            if stopped {
                ConditionVariableBroadcast(&walrcv.stopped_cv);
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
    {
        let mut guard = walrcv.shared.guarded.lock().unwrap();
        match guard.walRcvState {
            WalRcvState::WALRCV_STOPPED => {}
            WalRcvState::WALRCV_STARTING => {
                guard.walRcvState = WalRcvState::WALRCV_STOPPED;
                stopped = true;
            }
            WalRcvState::WALRCV_STREAMING
            | WalRcvState::WALRCV_WAITING
            | WalRcvState::WALRCV_RESTARTING => {
                guard.walRcvState = WalRcvState::WALRCV_STOPPING;
                // fall through
                walrcvpid = guard.pid;
            }
            WalRcvState::WALRCV_STOPPING => {
                walrcvpid = guard.pid;
            }
        }
    }

    // Unnecessary but consistent.
    if stopped {
        ConditionVariableBroadcast(&walrcv.stopped_cv);
    }

    // Signal walreceiver process if it was still running.
    if walrcvpid != 0 {
        kill_sigterm(walrcvpid);
    }

    // Wait for walreceiver to acknowledge its death by setting state to
    // WALRCV_STOPPED.
    ConditionVariablePrepareToSleep(&walrcv.stopped_cv);
    while WalRcvRunning()? {
        ConditionVariableSleep(&walrcv.stopped_cv, WAIT_EVENT_WAL_RECEIVER_EXIT)?;
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

    let walrcv_proc = {
        let mut guard = walrcv.shared.guarded.lock().unwrap();

        // It better be stopped if we try to restart it.
        debug_assert!(
            guard.walRcvState == WalRcvState::WALRCV_STOPPED
                || guard.walRcvState == WalRcvState::WALRCV_WAITING
        );

        match conninfo {
            Some(conninfo) => strlcpy_field(&mut guard.conninfo, conninfo, MAXCONNINFO),
            None => guard.conninfo.clear(),
        }

        // Use configured replication slot if present, and ignore the value of
        // create_temp_slot as the slot name should be persistent. Otherwise,
        // use create_temp_slot to determine whether this WAL receiver should
        // create a temporary slot by itself and use it, or not.
        match slotname {
            Some(slotname) if !slotname.is_empty() && slotname[0] != 0 => {
                strlcpy_field(&mut guard.slotname, slotname, NAMEDATALEN);
                guard.is_temp_slot = false;
            }
            _ => {
                guard.slotname.clear();
                guard.is_temp_slot = create_temp_slot;
            }
        }

        if guard.walRcvState == WalRcvState::WALRCV_STOPPED {
            launch = true;
            guard.walRcvState = WalRcvState::WALRCV_STARTING;
        } else {
            guard.walRcvState = WalRcvState::WALRCV_RESTARTING;
        }
        *walrcv.start_time.lock().unwrap() = now;

        // If this is the first startup of walreceiver (on this timeline),
        // initialize flushedUpto and latestChunkStart to the starting point.
        if guard.receiveStart == 0 || guard.receivedTLI != tli {
            guard.flushedUpto = recptr;
            guard.receivedTLI = tli;
            guard.latestChunkStart = recptr;
        }
        guard.receiveStart = recptr;
        guard.receiveStartTLI = tli;

        guard.procno
    };

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
    let walrcv = wal_rcv();

    let guard = walrcv.shared.guarded.lock().unwrap();
    let recptr = guard.flushedUpto;
    if let Some(latest_chunk_start) = latest_chunk_start {
        *latest_chunk_start = guard.latestChunkStart;
    }
    if let Some(receive_tli) = receive_tli {
        *receive_tli = guard.receivedTLI;
    }
    recptr
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:352-358
 * ------------------------------------------------------------------------ */

/// Returns the last+1 byte position that walreceiver has written. This returns
/// a recently written value without taking a lock.
pub fn GetWalRcvWriteRecPtr() -> XLogRecPtr {
    let walrcv = wal_rcv();

    walrcv.shared.writtenUpto.load(Ordering::SeqCst)
}

/* ------------------------------------------------------------------------
 * walreceiverfuncs.c:364-388
 * ------------------------------------------------------------------------ */

/// Returns the replication apply delay in ms, or -1 if the apply delay info is
/// not available.
pub fn GetReplicationApplyDelay() -> i32 {
    let walrcv = wal_rcv();

    let receive_ptr = {
        let guard = walrcv.shared.guarded.lock().unwrap();
        guard.flushedUpto
    };

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
    let walrcv = wal_rcv();

    let (last_msg_send_time, last_msg_receipt_time): (TimestampTz, TimestampTz) = {
        let guard = walrcv.shared.guarded.lock().unwrap();
        (guard.lastMsgSendTime, guard.lastMsgReceiptTime)
    };

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
    let walrcv = wal_rcv();
    let mut guard = walrcv.shared.guarded.lock().unwrap();
    f(&mut guard);
}

/// `pg_atomic_write_u64(&WalRcv->writtenUpto, val)` (`set_written_upto` seam).
fn set_written_upto(val: XLogRecPtr) {
    wal_rcv().shared.writtenUpto.store(val, Ordering::SeqCst);
}

/// `pg_atomic_read_u64(&WalRcv->writtenUpto)` (`get_written_upto` seam).
fn get_written_upto() -> XLogRecPtr {
    wal_rcv().shared.writtenUpto.load(Ordering::SeqCst)
}

/// `WalRcv->force_reply = true` with a write barrier (`set_force_reply` seam).
fn set_force_reply() {
    // C: SpinLockAcquire-free store of a sig_atomic_t with pg_memory_barrier()
    // before the store, so the reader sees a consistent walreceiver state. A
    // SeqCst store provides the same ordering.
    wal_rcv().shared.force_reply.store(1, Ordering::SeqCst);
}

/// Read-and-clear `WalRcv->force_reply` with the barrier (`take_force_reply`
/// seam). Returns whether a reply was requested.
fn take_force_reply() -> bool {
    // C: pg_memory_barrier(); then read force_reply and, if set, clear it.
    wal_rcv().shared.force_reply.swap(0, Ordering::SeqCst) != 0
}

/// `ConditionVariableBroadcast(&WalRcv->walRcvStoppedCV)`
/// (`wal_rcv_stopped_cv_broadcast` seam).
fn wal_rcv_stopped_cv_broadcast() {
    ConditionVariableBroadcast(&wal_rcv().stopped_cv);
}

/// Install this unit's seams (`with_walrcv`, the atomic accessors, the shmem
/// size/init pair, and the apply-delay / transfer-latency helpers). Wired into
/// `seams_init::init_all()`.
///
/// NOTE: `xlog_request_wal_receiver_reply` is declared in this crate's `-seams`
/// crate but is owned by `xlogrecovery.c` (`XLogRequestWalReceiverReply`), not
/// `walreceiverfuncs.c`; it is therefore installed by the xlogrecovery owner,
/// not here.
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

    // `GetWalRcvFlushRecPtr` is also reachable across a cycle via the
    // walreceiver-seams crate (xlog checkpoint / walsummarizer consume the
    // `(lsn, tli)` form). Install that here, in the real owner.
    backend_replication_walreceiver_seams::get_wal_rcv_flush_rec_ptr::set(|| {
        let mut tli: TimeLineID = 0;
        let lsn = GetWalRcvFlushRecPtr(None, Some(&mut tli));
        (lsn, tli)
    });
}

#[cfg(test)]
mod tests;
