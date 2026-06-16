//! WalSender shared-memory + system-view vocabulary
//! (`replication/walsender_private.h`, the `WalSnd` shmem slot, the SRF row of
//! `pg_stat_get_wal_senders`).
//!
//! These owned types are shared between `backend-replication-walsender` (the
//! owner) and crates that read the WalSnd shmem array / emit the stats SRF, so
//! they live here in the types layer rather than in the owner crate.

#![allow(non_camel_case_types)]

use types_core::primitive::{pid_t, TimestampTz, XLogRecPtr};
use types_datetime::{Interval, TimeOffset};

/// `typedef enum WalSndState` (`replication/walsender.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalSndState {
    WALSNDSTATE_STARTUP,
    WALSNDSTATE_BACKUP,
    WALSNDSTATE_CATCHUP,
    WALSNDSTATE_STREAMING,
    WALSNDSTATE_STOPPING,
}

/// A snapshot of one `WalSnd` shared-memory slot
/// (`replication/walsender_private.h`), taken under the slot's spinlock by the
/// shmem owner and handed to walsender for the SRF / `MyWalSnd`-shaped reads.
///
/// We keep the value owned here (no raw shmem pointer arithmetic in walsender);
/// the shmem subsystem is the producer.
#[derive(Clone, Copy, Debug)]
pub struct WalSnd {
    /// `pid_t pid` — this walsender's PID, or 0 if not in use.
    pub pid: pid_t,
    /// `WalSndState state`.
    pub state: WalSndState,
    /// `XLogRecPtr sentPtr` — WAL has been sent up to this point.
    pub sentPtr: XLogRecPtr,
    /// `XLogRecPtr write` — the standby's last-reported write position.
    pub write: XLogRecPtr,
    /// `XLogRecPtr flush` — the standby's last-reported flush position.
    pub flush: XLogRecPtr,
    /// `XLogRecPtr apply` — the standby's last-reported apply position.
    pub apply: XLogRecPtr,
    /// `TimeOffset writeLag` — write lag, or -1 if unknown.
    pub writeLag: TimeOffset,
    /// `TimeOffset flushLag` — flush lag, or -1 if unknown.
    pub flushLag: TimeOffset,
    /// `TimeOffset applyLag` — apply lag, or -1 if unknown.
    pub applyLag: TimeOffset,
    /// `int sync_standby_priority`.
    pub sync_standby_priority: i32,
    /// `TimestampTz replyTime` — the time of the last reply, or 0 if none.
    pub replyTime: TimestampTz,
}

/// The "more easily understood" sync-state classification reported by
/// `pg_stat_get_wal_senders` (walsender.c).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncState {
    /// "async".
    Async,
    /// "sync".
    Sync,
    /// "quorum".
    Quorum,
    /// "potential".
    Potential,
}

impl SyncState {
    /// The textual name used in the system view (not translated).
    pub fn as_str(self) -> &'static str {
        match self {
            SyncState::Async => "async",
            SyncState::Sync => "sync",
            SyncState::Quorum => "quorum",
            SyncState::Potential => "potential",
        }
    }
}

/// One assembled output row of `pg_stat_get_wal_senders` (walsender.c).  The
/// per-row decision logic is computed in the owner crate; the SRF plumbing
/// (the `Datum` conversions + `tuplestore_putvalues`) belongs to the
/// fmgr/funcapi subsystem and consumes this row.
#[derive(Clone, Debug)]
pub struct WalSenderRow {
    /// `pid` column.
    pub pid: pid_t,
    /// Whether the privileged detail columns are populated (false leaves them
    /// NULL).
    pub has_details: bool,
    /// `state` column (the textual `WalSndGetStateString`).
    pub state: &'static str,
    /// `sent_lsn` column.
    pub sent_ptr: XLogRecPtr,
    /// `write_lsn` column.
    pub write: XLogRecPtr,
    /// `flush_lsn` column.
    pub flush: XLogRecPtr,
    /// `replay_lsn` column.
    pub apply: XLogRecPtr,
    /// `write_lag` column (NULL when unknown).
    pub write_lag: Option<Interval>,
    /// `flush_lag` column (NULL when unknown).
    pub flush_lag: Option<Interval>,
    /// `replay_lag` column (NULL when unknown).
    pub apply_lag: Option<Interval>,
    /// `sync_priority` column.
    pub sync_priority: i32,
    /// `sync_state` column.
    pub sync_state: SyncState,
    /// `reply_time` column (NULL when no reply yet).
    pub reply_time: Option<TimestampTz>,
}

/// `name` column buffer constants used by the SRF (kept for parity / docs).
pub const PG_STAT_GET_WAL_SENDERS_COLS: i32 = 12;
