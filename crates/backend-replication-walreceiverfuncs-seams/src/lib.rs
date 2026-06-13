//! Seam declarations for the `backend-replication-walreceiverfuncs` unit
//! (`replication/walreceiverfuncs.c`) — the `WalRcvData` shared-memory control
//! block plus the apply-delay / transfer-latency helpers it owns.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The owned walreceiver port only ever sees the
//! spinlocked snapshots these seams hand back and pushes spinlocked updates
//! back through them — never a raw `WalRcvData *`.

use types_core::{TimeLineID, TimestampTz, XLogRecPtr};
use types_walreceiver::{WalRcvStartupInfo, WalRcvState, WalRcvStatSnapshot};

seam_core::seam!(
    /// `WalReceiverMain` startup spinlock section: assert `pid == 0`, switch on
    /// `walRcvState`, advertise pid/procno, init message times, read streaming
    /// params. `None` ⇒ the STOPPED/STOPPING path (`proc_exit(1)`). PANICs in C
    /// for the "still running" arm.
    pub fn walrcv_start_in_shmem(now: TimestampTz) -> types_error::PgResult<Option<WalRcvStartupInfo>>
);

seam_core::seam!(
    /// `pg_atomic_write_u64(&WalRcv->writtenUpto, val)`.
    pub fn set_written_upto(val: XLogRecPtr)
);

seam_core::seam!(
    /// `pg_atomic_read_u64(&WalRcv->writtenUpto)`.
    pub fn get_written_upto() -> XLogRecPtr
);

seam_core::seam!(
    /// Save the user-visible conninfo + sender host/port and set
    /// `ready_to_display` under the spinlock.
    pub fn walrcv_save_conninfo(
        conninfo: Option<String>,
        sender_host: Option<String>,
        sender_port: i32
    )
);

seam_core::seam!(
    /// `strlcpy(walrcv->slotname, slotname, NAMEDATALEN)` under the spinlock.
    pub fn walrcv_set_slotname(slotname: String)
);

seam_core::seam!(
    /// `WalRcvWaitForStartPosition` shmem transition: under the spinlock, if
    /// STREAMING move to WAITING and clear receiveStart/TLI; returns the prior
    /// state for the caller to branch on.
    pub fn walrcv_begin_wait() -> WalRcvState
);

seam_core::seam!(
    /// Poll `WalRcv->walRcvState` in the wait loop, under the spinlock; returns
    /// the state plus (when RESTARTING) the new receiveStart / receiveStartTLI.
    pub fn walrcv_poll_wait() -> (WalRcvState, XLogRecPtr, TimeLineID)
);

seam_core::seam!(
    /// `ProcessWalSndrMessage` shmem update (latestWalEnd/Time,
    /// lastMsgSend/Receipt).
    pub fn process_walsndr_shmem(
        wal_end: XLogRecPtr,
        send_time: TimestampTz,
        receipt_time: TimestampTz
    )
);

seam_core::seam!(
    /// `XLogWalRcvFlush` shmem update: advance flushedUpto / latestChunkStart /
    /// receivedTLI under the spinlock.
    pub fn flush_advance_shmem(flush: XLogRecPtr, tli: TimeLineID)
);

seam_core::seam!(
    /// `WalRcvDie` shmem finalization: assert running, set STOPPED, clear
    /// pid/procno/ready_to_display, broadcast the stopped CV.
    pub fn walrcv_die_shmem()
);

seam_core::seam!(
    /// Read-and-clear `walrcv->force_reply` with the `pg_memory_barrier()`.
    pub fn take_force_reply() -> bool
);

seam_core::seam!(
    /// Snapshot `WalRcv` under the spinlock for `pg_stat_get_wal_receiver`.
    pub fn pg_stat_get_wal_receiver_snapshot() -> WalRcvStatSnapshot
);

seam_core::seam!(
    /// `WalRcvForceReply`'s shmem half: set `WalRcv->force_reply`, read procno
    /// under the spinlock, and `SetLatch(&GetPGProcByNumber(procno)->procLatch)`
    /// if valid.
    pub fn walrcv_force_reply()
);

seam_core::seam!(
    /// `GetReplicationApplyDelay()` (walreceiverfuncs.c) — ms, or -1 if N/A.
    pub fn get_replication_apply_delay() -> i32
);

seam_core::seam!(
    /// `GetReplicationTransferLatency()` (walreceiverfuncs.c) — ms.
    pub fn get_replication_transfer_latency() -> i32
);
