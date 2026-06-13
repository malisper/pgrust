//! Background-worker vocabulary (`postmaster/bgworker.h` /
//! `postmaster/bgworker_internals.h` / `postmaster/bgworker.c`), trimmed to
//! what current consumers need: the registration record, the postmaster's
//! private registry entry, the worker-handle identity and the liveness status
//! probe's result.

#![no_std]

use types_core::{pid_t, Oid, TimestampTz, MAXPGPATH};
use types_datum::Datum;

/// `BGWORKER_SHMEM_ACCESS` — worker wants shared-memory access. Required of all
/// workers (a worker without it is rejected by `SanityCheckBackgroundWorker`).
pub const BGWORKER_SHMEM_ACCESS: i32 = 0x0001;
/// `BGWORKER_BACKEND_DATABASE_CONNECTION` — worker wants to attach to a database.
pub const BGWORKER_BACKEND_DATABASE_CONNECTION: i32 = 0x0002;
/// `BGWORKER_CLASS_PARALLEL` — internal parallel-query worker class.
pub const BGWORKER_CLASS_PARALLEL: i32 = 0x0010;

/// `BGWORKER_BYPASS_ALLOWCONN` — bypass `datallowconn`/`ACL_CONNECT` on connect.
pub const BGWORKER_BYPASS_ALLOWCONN: u32 = 0x0001;
/// `BGWORKER_BYPASS_ROLELOGINCHECK` — bypass the role `rolcanlogin` check.
pub const BGWORKER_BYPASS_ROLELOGINCHECK: u32 = 0x0002;

/// `BGW_DEFAULT_RESTART_INTERVAL`.
pub const BGW_DEFAULT_RESTART_INTERVAL: i32 = 60;
/// `BGW_NEVER_RESTART`.
pub const BGW_NEVER_RESTART: i32 = -1;
/// `BGW_MAXLEN`.
pub const BGW_MAXLEN: usize = 96;
/// `BGW_EXTRALEN`.
pub const BGW_EXTRALEN: usize = 128;

/// `MAX_PARALLEL_WORKER_LIMIT` (`bgworker_internals.h`).
pub const MAX_PARALLEL_WORKER_LIMIT: i32 = 1024;

/// `InvalidPid` (`miscadmin.h`) — sentinel a freshly-claimed slot's `pid`
/// carries until the worker actually starts.
pub const INVALID_PID: pid_t = -1;

/// `BgWorkerStartTime` (`bgworker.h`) — points in time at which a bgworker can
/// request to be started. Discriminant order matches the C enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum BgWorkerStartTime {
    /// `BgWorkerStart_PostmasterStart`.
    PostmasterStart = 0,
    /// `BgWorkerStart_ConsistentState`.
    ConsistentState = 1,
    /// `BgWorkerStart_RecoveryFinished`.
    RecoveryFinished = 2,
}

/// `BackgroundWorker` (`bgworker.h`) — the registration record an extension
/// fills in. The fixed-size `char[]` fields are NUL-terminated C strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackgroundWorker {
    /// `char bgw_name[BGW_MAXLEN]`.
    pub bgw_name: [u8; BGW_MAXLEN],
    /// `char bgw_type[BGW_MAXLEN]`.
    pub bgw_type: [u8; BGW_MAXLEN],
    /// `int bgw_flags`.
    pub bgw_flags: i32,
    /// `BgWorkerStartTime bgw_start_time`.
    pub bgw_start_time: BgWorkerStartTime,
    /// `int bgw_restart_time` (seconds, or `BGW_NEVER_RESTART`).
    pub bgw_restart_time: i32,
    /// `char bgw_library_name[MAXPGPATH]`.
    pub bgw_library_name: [u8; MAXPGPATH],
    /// `char bgw_function_name[BGW_MAXLEN]`.
    pub bgw_function_name: [u8; BGW_MAXLEN],
    /// `Datum bgw_main_arg`.
    pub bgw_main_arg: Datum,
    /// `char bgw_extra[BGW_EXTRALEN]`.
    pub bgw_extra: [u8; BGW_EXTRALEN],
    /// `pid_t bgw_notify_pid` — SIGUSR1 this backend on start/stop.
    pub bgw_notify_pid: pid_t,
}

impl BackgroundWorker {
    /// A fully zeroed worker, the C analogue of a `MemSet`'d struct.
    pub const fn zeroed() -> Self {
        BackgroundWorker {
            bgw_name: [0; BGW_MAXLEN],
            bgw_type: [0; BGW_MAXLEN],
            bgw_flags: 0,
            bgw_start_time: BgWorkerStartTime::PostmasterStart,
            bgw_restart_time: 0,
            bgw_library_name: [0; MAXPGPATH],
            bgw_function_name: [0; BGW_MAXLEN],
            bgw_main_arg: Datum::null(),
            bgw_extra: [0; BGW_EXTRALEN],
            bgw_notify_pid: 0,
        }
    }
}

/// `snprintf(dst, N, "%s", src)` into a fixed-size NUL-terminated C-string
/// field: copy at most `N - 1` bytes of `src` and NUL-terminate. The tail is
/// left as already-zeroed bytes (callers pass a zeroed buffer). Mirrors the
/// `snprintf(bgw.bgw_*, BGW_MAXLEN/MAXPGPATH, ...)` fills in the bgworker
/// launch paths.
pub fn snprintf_cstr<const N: usize>(dst: &mut [u8; N], src: &str) {
    let bytes = src.as_bytes();
    let n = core::cmp::min(bytes.len(), N - 1);
    dst[..n].copy_from_slice(&bytes[..n]);
    dst[n] = 0;
}

/// `RegisteredBgWorker` (`bgworker_internals.h`) — the postmaster's private
/// registration record. The C `dlist_node rw_lnode` link is unused here (the
/// owning `Vec` is the list).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegisteredBgWorker {
    /// `BackgroundWorker rw_worker` — its registry entry.
    pub rw_worker: BackgroundWorker,
    /// `pid_t rw_pid` — 0 if not running.
    pub rw_pid: pid_t,
    /// `TimestampTz rw_crashed_at` — if not 0, time it last crashed.
    pub rw_crashed_at: TimestampTz,
    /// `int rw_shmem_slot`.
    pub rw_shmem_slot: i32,
    /// `bool rw_terminate`.
    pub rw_terminate: bool,
}

/// `InvalidOid`.
pub const INVALID_OID: Oid = 0;

/// `BgwHandleStatus` (`postmaster/bgworker.h`) — possible states of a
/// background worker as reported by `GetBackgroundWorkerPid`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum BgwHandleStatus {
    /// `BGWH_STARTED` — worker is running.
    Started = 0,
    /// `BGWH_NOT_YET_STARTED` — worker hasn't been started yet.
    NotYetStarted = 1,
    /// `BGWH_STOPPED` — worker has exited.
    Stopped = 2,
    /// `BGWH_POSTMASTER_DIED` — postmaster died; worker status unclear.
    PostmasterDied = 3,
}

/// `struct BackgroundWorkerHandle` (`postmaster/bgworker.c`): names one
/// registration in the shared `BackgroundWorkerSlots` array. Opaque to C
/// callers (bgworker.h forward-declares it); the fields are exactly the
/// bgworker.c definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackgroundWorkerHandle {
    /// `int slot` — index into the shared worker-slot array.
    pub slot: i32,
    /// `uint64 generation` — guards against slot reuse.
    pub generation: u64,
}
