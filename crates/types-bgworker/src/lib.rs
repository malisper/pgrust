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
