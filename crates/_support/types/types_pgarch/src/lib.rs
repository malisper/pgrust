//! Archiver shared-memory vocabulary (`postmaster/pgarch.c`'s `PgArchData`).
//!
//! `PgArchData` lives in shared memory and is concurrently touched by every
//! backend (the archiver advertises its proc number; any backend forces a
//! directory scan or wakes the archiver). The fields are therefore expressed
//! with real atomics, matching the C struct (`pgprocno` is an `int`,
//! `force_dir_scan` a `pg_atomic_uint32`).

#![no_std]

use core::sync::atomic::AtomicI32;
use ::types_core::ProcNumber;
use ::types_storage::storage::pg_atomic_uint32;

/// `typedef struct PgArchData` — the archiver's shared-memory control block.
#[repr(C)]
pub struct PgArchData {
    /// `int pgprocno` — proc number of the archiver process
    /// (`INVALID_PROC_NUMBER` when none). Written by the archiver at startup
    /// and at exit, read lock-free by `PgArchWakeup`.
    pub pgprocno: AtomicI32,

    /// `pg_atomic_uint32 force_dir_scan` — forces a directory scan in
    /// `pgarch_readyXlog()`.
    pub force_dir_scan: pg_atomic_uint32,
}

impl PgArchData {
    /// Zero-initialized block (`MemSet(PgArch, 0, ...)`); `PgArchShmemInit`'s
    /// first-time branch then sets `pgprocno = INVALID_PROC_NUMBER`.
    pub const fn new() -> Self {
        PgArchData {
            pgprocno: AtomicI32::new(0),
            force_dir_scan: pg_atomic_uint32::new(0),
        }
    }
}

impl Default for PgArchData {
    fn default() -> Self {
        Self::new()
    }
}

/// `ProcNumber` re-export for callers that store the advertised archiver proc.
pub type PgArchProcNumber = ProcNumber;

// ---------------------------------------------------------------------------
// Archive-module vocabulary (`archive/archive_module.h`).
// ---------------------------------------------------------------------------

/// `typedef struct ArchiveModuleState` — per-module private state. The single
/// field is a genuine extension slot (C `void *private_data`), so it stays
/// opaque: each archive module stores whatever it needs behind this pointer.
#[repr(C)]
pub struct ArchiveModuleState {
    /// `void *private_data` — module-owned, opaque to the archiver.
    pub private_data: *mut core::ffi::c_void,
}

impl ArchiveModuleState {
    /// `palloc0(sizeof(ArchiveModuleState))` — zeroed state.
    pub const fn new() -> Self {
        ArchiveModuleState {
            private_data: core::ptr::null_mut(),
        }
    }
}

impl Default for ArchiveModuleState {
    fn default() -> Self {
        Self::new()
    }
}

/// `ArchiveStartupCB` — `void (*)(ArchiveModuleState *state)`.
pub type ArchiveStartupCb = fn(state: &mut ArchiveModuleState);

/// `ArchiveCheckConfiguredCB` — `bool (*)(ArchiveModuleState *state)`.
pub type ArchiveCheckConfiguredCb = fn(state: &mut ArchiveModuleState) -> bool;

/// `ArchiveFileCB` — `bool (*)(ArchiveModuleState *state, const char *file,
/// const char *path)`. Required callback. May `ereport(ERROR)` in C (caught by
/// the archiver's exception handler), so it returns `PgResult`.
pub type ArchiveFileCb =
    fn(state: &mut ArchiveModuleState, file: &str, path: &str) -> types_error::PgResult<bool>;

/// `ArchiveShutdownCB` — `void (*)(ArchiveModuleState *state)`.
pub type ArchiveShutdownCb = fn(state: &mut ArchiveModuleState);

/// `typedef struct ArchiveModuleCallbacks` — the callback table an archive
/// library returns from `_PG_archive_module_init()`. `archive_file_cb` is the
/// only required callback; the rest are optional (`None` = not defined).
pub struct ArchiveModuleCallbacks {
    /// `startup_cb` — optional one-time module init.
    pub startup_cb: Option<ArchiveStartupCb>,
    /// `check_configured_cb` — optional "is archiving configured" predicate.
    pub check_configured_cb: Option<ArchiveCheckConfiguredCb>,
    /// `archive_file_cb` — required: copy one WAL file.
    pub archive_file_cb: Option<ArchiveFileCb>,
    /// `shutdown_cb` — optional teardown.
    pub shutdown_cb: Option<ArchiveShutdownCb>,
}

/// `ArchiveModuleInit` — `const ArchiveModuleCallbacks *(*)(void)`, the symbol
/// `_PG_archive_module_init` looked up when loading an archive library.
pub type ArchiveModuleInit = fn() -> &'static ArchiveModuleCallbacks;
