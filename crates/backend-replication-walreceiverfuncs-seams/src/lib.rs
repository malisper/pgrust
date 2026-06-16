//! (`replication/walreceiverfuncs.c`) — access to the `WalRcvData` shared-memory
//! control block plus the apply-delay / transfer-latency helpers it owns.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.  These seams are thin: they hand the walreceiver
//! port a lock-guarded handle on the shared block (`with_walrcv`, which brackets
//! the caller's closure with `SpinLockAcquire`/`SpinLockRelease`) and the
//! lock-free atomic words.  All `switch(walRcvState)` / state-transition
//! branching stays in the walreceiver crate, inside those closures.

use types_walreceiver::WalRcvData;

seam_core::seam!(
    /// `WalRcvShmemSize()` (walreceiverfuncs.c; ipci.c `CalculateShmemSize`
    /// accumulator) — shared-memory bytes the `WalRcvData` control block needs.
    /// `Err` carries the `add_size`/`mul_size` overflow `ereport(ERROR)`. Owner
    /// (`backend-replication-walreceiverfuncs`) unported; scaffolded slot.
    pub fn wal_rcv_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `WalRcvShmemInit()` (walreceiverfuncs.c; ipci.c
    /// `CreateOrAttachShmemStructs`) — allocate-or-attach the `WalRcvData`
    /// shared-memory block. `Err` carries the out-of-shared-memory
    /// `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn wal_rcv_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SpinLockAcquire(&WalRcv->mutex); f(WalRcv); SpinLockRelease(...)`.
    /// Runs the caller's closure with exclusive access to the spinlock-guarded
    /// fields of the shared block; the lock is released on return.
    pub fn with_walrcv(f: &mut dyn FnMut(&mut WalRcvData))
);

seam_core::seam!(
    /// `pg_atomic_write_u64(&WalRcv->writtenUpto, val)`.
    pub fn set_written_upto(val: types_core::XLogRecPtr)
);

seam_core::seam!(
    /// `pg_atomic_read_u64(&WalRcv->writtenUpto)`.
    pub fn get_written_upto() -> types_core::XLogRecPtr
);

seam_core::seam!(
    /// `WalRcv->force_reply = true` (with the memory barrier `WalRcvForceReply`
    /// relies on).
    pub fn set_force_reply()
);

seam_core::seam!(
    /// Read-and-clear `WalRcv->force_reply` with the `pg_memory_barrier()`.
    pub fn take_force_reply() -> bool
);

seam_core::seam!(
    /// `ConditionVariableBroadcast(&WalRcv->walRcvStoppedCV)`.
    pub fn wal_rcv_stopped_cv_broadcast()
);

seam_core::seam!(
    /// `GetReplicationApplyDelay()` (walreceiverfuncs.c) — ms, or -1 if N/A.
    pub fn get_replication_apply_delay() -> i32
);

seam_core::seam!(
    /// `GetReplicationTransferLatency()` (walreceiverfuncs.c) — ms.
    pub fn get_replication_transfer_latency() -> i32
);

seam_core::seam!(
    /// `bool WalRcvRunning(void)` (walreceiverfuncs.c) — whether the
    /// walreceiver is running or starting up. May lazily transition a stuck
    /// `STARTING` state to `STOPPED` (a shmem state-write), which is why it
    /// is fallible here even though the C return is a plain `bool`.
    pub fn wal_rcv_running() -> types_error::PgResult<bool>
);

// ===========================================================================
// Streaming-control entry points consumed by the recovery page-read driver
// (xlogrecovery.c WaitForWALToBecomeAvailable). The walreceiverfuncs owner is
// unported, so these stay seam-and-panic until it lands.
// ===========================================================================

seam_core::seam!(
    /// `bool WalRcvStreaming(void)` (walreceiverfuncs.c) — whether the
    /// walreceiver is streaming WAL (or about to). Like `WalRcvRunning`, may
    /// lazily transition a stuck state, hence fallible.
    pub fn wal_rcv_streaming() -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `void ShutdownWalRcv(void)` (walreceiverfuncs.c) — request the
    /// walreceiver to stop and wait for it to do so (a shmem state-write
    /// protected by the walrcv spinlock). This is the inner walreceiverfuncs.c
    /// routine; the xlog.c `XLogShutdownWalRcv` wrapper (the
    /// `xlog_shutdown_wal_rcv` seam, owned by xlog) calls this then resets the
    /// segment-active flag. The C return is void; a condition-variable sleep
    /// `ereport(ERROR)` in the wait loop unwinds via longjmp.
    pub fn shutdown_wal_rcv()
);

seam_core::seam!(
    /// `void XLogShutdownWalRcv(void)` (xlog.c) — the thin wrapper that calls
    /// `ShutdownWalRcv()` then `ResetInstallXLogFileSegmentActive()`. Its real
    /// owner is the xlog crate (it touches the xlog-owned `XLogCtl` flag), which
    /// installs this seam.
    pub fn xlog_shutdown_wal_rcv()
);

seam_core::seam!(
    /// `void RequestXLogStreaming(tli, recptr, conninfo, slotname,
    /// create_temp_slot)` — start the walreceiver streaming from `recptr` on
    /// timeline `tli`. `Err` carries the slot-validation `ereport(ERROR)`.
    pub fn request_xlog_streaming(
        tli: types_core::TimeLineID,
        recptr: types_core::XLogRecPtr,
        conninfo: &str,
        slotname: &str,
        create_temp_slot: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `void SetInstallXLogFileSegmentActive(void)` — permit WAL-segment
    /// installation into `pg_wal` (a shmem flag-write).
    pub fn set_install_xlog_file_segment_active()
);

seam_core::seam!(
    /// `void ResetInstallXLogFileSegmentActive(void)` — forbid WAL-segment
    /// installation into `pg_wal` (a shmem flag-write).
    pub fn reset_install_xlog_file_segment_active()
);

seam_core::seam!(
    /// `XLogRecPtr GetWalRcvFlushRecPtr(XLogRecPtr *latestChunkStart,
    /// TimeLineID *receiveTLI)` — the last byte+1 of WAL flushed to disk by the
    /// walreceiver, plus the start of the latest chunk and the receive TLI.
    /// Returns `(flushedUpto, latestChunkStart, receiveTLI)`.
    pub fn get_wal_rcv_flush_rec_ptr_full() -> (types_core::XLogRecPtr, types_core::XLogRecPtr, types_core::TimeLineID)
);
