//! Logical-replication worker vocabulary (`replication/worker_internal.h`,
//! `catalog/pg_subscription.h` subset) shared by the launcher (owner of the
//! shared worker-slot array) and the logical-replication worker crates
//! (tablesync / apply / parallel-apply) that read and mutate their slot.

#![allow(non_upper_case_globals)]

extern crate alloc;
use alloc::string::String;

use ::types_core::primitive::{Oid, TimestampTz, XLogRecPtr};

/// `DEFAULT_NAPTIME_PER_CYCLE` (launcher.c): max sleep between cycles, 3 min.
pub const DEFAULT_NAPTIME_PER_CYCLE: i64 = 180000;

/// `LauncherLastStartTimesEntry` (launcher.c:72) — value type of the launcher's
/// `last_start_times` dshash table, keyed by `subid`. The dshash unit stores it
/// opaquely (it knows only `sizeof(Oid)` key / `sizeof(entry)` value); the
/// launcher owns the field semantics. `#[repr(C)]` so the byte image matches
/// the C struct the dshash table allocates in shared memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct LauncherLastStartTimesEntry {
    /// `subid` — OID of logrep subscription (the hash key).
    pub subid: Oid,
    /// `last_start_time` — last time its apply worker was started.
    pub last_start_time: TimestampTz,
}

/// `SUBREL_STATE_UNKNOWN` (`pg_subscription_rel.h`): `'\0'`.
pub const SUBREL_STATE_UNKNOWN: i8 = 0;

/// `DSM_HANDLE_INVALID` (`storage/dsm_impl.h`): 0.
pub const DSM_HANDLE_INVALID: u32 = 0;

/// `LogicalRepWorkerType` (worker_internal.h). Default 0-based C discriminants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum LogicalRepWorkerType {
    /// `WORKERTYPE_UNKNOWN`
    Unknown = 0,
    /// `WORKERTYPE_TABLESYNC`
    Tablesync = 1,
    /// `WORKERTYPE_APPLY`
    Apply = 2,
    /// `WORKERTYPE_PARALLEL_APPLY`
    ParallelApply = 3,
}

/// `LogicalRepWorker` (worker_internal.h) — one shared-memory worker slot.
///
/// The C `proc` member is a `PGPROC *` (NULL when no backend has attached).
/// The launcher only ever asks "is `proc` set?" and "what is `proc->pid`?", so
/// the trimmed real representation is `proc_pid: Option<i32>` (`None` == NULL).
/// The `relmutex` spinlock and `stream_fileset` pointer are subsystem-owned
/// substrate that the launcher does not read here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogicalRepWorker {
    /// `type` — what type of worker is this?
    pub wtype: LogicalRepWorkerType,
    /// Time at which this worker was launched.
    pub launch_time: TimestampTz,
    /// Indicates if this slot is used or free.
    pub in_use: bool,
    /// Increased every time the slot is taken by a new worker.
    pub generation: u16,
    /// `proc->pid` when attached, `None` (C `proc == NULL`) otherwise.
    pub proc_pid: Option<i32>,
    /// Database id to connect to.
    pub dbid: Oid,
    /// User to use for connection.
    pub userid: Oid,
    /// Subscription id for the worker.
    pub subid: Oid,
    /// Used for initial table synchronization.
    pub relid: Oid,
    /// `relstate` (`SUBREL_STATE_*`).
    pub relstate: i8,
    /// `relstate_lsn`.
    pub relstate_lsn: XLogRecPtr,
    /// PID of leader apply worker if parallel, `InvalidPid` otherwise.
    pub leader_pid: i32,
    /// Indicates whether apply can be performed in parallel.
    pub parallel_apply: bool,
    /// Stats: last LSN.
    pub last_lsn: XLogRecPtr,
    /// Stats: last send time.
    pub last_send_time: TimestampTz,
    /// Stats: last recv time.
    pub last_recv_time: TimestampTz,
    /// Stats: reply LSN.
    pub reply_lsn: XLogRecPtr,
    /// Stats: reply time.
    pub reply_time: TimestampTz,
}

impl LogicalRepWorker {
    /// `isParallelApplyWorker(worker)` (worker_internal.h):
    /// `worker.in_use && worker.type == WORKERTYPE_PARALLEL_APPLY`.
    #[inline]
    pub fn is_parallel_apply_worker(&self) -> bool {
        self.in_use && self.wtype == LogicalRepWorkerType::ParallelApply
    }

    /// `isTablesyncWorker(worker)` (worker_internal.h):
    /// `worker.in_use && worker.type == WORKERTYPE_TABLESYNC`.
    #[inline]
    pub fn is_tablesync_worker(&self) -> bool {
        self.in_use && self.wtype == LogicalRepWorkerType::Tablesync
    }
}

/// Subset of `Subscription` (`catalog/pg_subscription.h`) that
/// `get_subscription_list` fills — the launcher only reads these fields, and
/// the C code likewise leaves the rest unset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscription {
    /// `oid`.
    pub oid: Oid,
    /// `subdbid`.
    pub dbid: Oid,
    /// `subowner`.
    pub owner: Oid,
    /// `subenabled`.
    pub enabled: bool,
    /// `pstrdup(NameStr(subname))`.
    pub name: String,
}
