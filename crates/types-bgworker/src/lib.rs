//! Background-worker vocabulary (`postmaster/bgworker.h` /
//! `postmaster/bgworker.c`), trimmed to what current consumers need: the
//! worker-handle identity and the liveness status probe's result.

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

// --- Background-worker registration vocabulary (launcher / dynamic workers) ---

/// `BGWORKER_SHMEM_ACCESS` (bgworker.h).
pub const BGWORKER_SHMEM_ACCESS: i32 = 0x0001;
/// `BGWORKER_BACKEND_DATABASE_CONNECTION` (bgworker.h).
pub const BGWORKER_BACKEND_DATABASE_CONNECTION: i32 = 0x0002;
/// `BGWORKER_CLASS_PARALLEL` (bgworker.h).
pub const BGWORKER_CLASS_PARALLEL: i32 = 0x0010;
/// `BGW_NEVER_RESTART` (bgworker.h).
pub const BGW_NEVER_RESTART: i32 = -1;
/// `BGW_MAXLEN` (bgworker.h).
pub const BGW_MAXLEN: usize = 96;
/// `BGW_EXTRALEN` (bgworker.h).
pub const BGW_EXTRALEN: usize = 128;

/// `BgWorkerStartTime` (bgworker.h). Default 0-based C enum discriminants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum BgWorkerStartTime {
    /// `BgWorkerStart_PostmasterStart`
    PostmasterStart = 0,
    /// `BgWorkerStart_ConsistentState`
    ConsistentState = 1,
    /// `BgWorkerStart_RecoveryFinished`
    RecoveryFinished = 2,
}

/// `BackgroundWorker` (postmaster/bgworker.h) — the registration request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackgroundWorker {
    /// `bgw_name` (`char[BGW_MAXLEN]`).
    pub bgw_name: String,
    /// `bgw_type` (`char[BGW_MAXLEN]`).
    pub bgw_type: String,
    /// `bgw_flags`.
    pub bgw_flags: i32,
    /// `bgw_start_time`.
    pub bgw_start_time: i32,
    /// `bgw_restart_time` (seconds, or `BGW_NEVER_RESTART`).
    pub bgw_restart_time: i32,
    /// `bgw_library_name` (`char[MAXPGPATH]`).
    pub bgw_library_name: String,
    /// `bgw_function_name` (`char[BGW_MAXLEN]`).
    pub bgw_function_name: String,
    /// `bgw_main_arg` (`Datum`; for dynamic logrep workers, the slot index via
    /// `Int32GetDatum(slot)`).
    pub bgw_main_arg: i32,
    /// `memcpy(bgw.bgw_extra, &subworker_dsm, sizeof(dsm_handle))` payload used
    /// by the parallel-apply launch path; `None` when no payload is stuffed.
    pub bgw_extra_dsm: Option<u32>,
    /// `bgw_notify_pid`.
    pub bgw_notify_pid: i32,
}
