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
