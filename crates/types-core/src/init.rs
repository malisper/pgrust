//! `enum BackendType` (`miscadmin.h`) — process-type discriminants. The
//! discriminants match the C enum order exactly (parity matters: they appear
//! in protocol/launch plumbing and stats indexing).

/// `enum BackendType` (`miscadmin.h:336-374`).
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BackendType {
    /// `B_INVALID`.
    Invalid = 0,

    /* Backends and other backend-like processes */
    /// `B_BACKEND`.
    Backend,
    /// `B_DEAD_END_BACKEND`.
    DeadEndBackend,
    /// `B_AUTOVAC_LAUNCHER`.
    AutovacLauncher,
    /// `B_AUTOVAC_WORKER`.
    AutovacWorker,
    /// `B_BG_WORKER`.
    BgWorker,
    /// `B_WAL_SENDER`.
    WalSender,
    /// `B_SLOTSYNC_WORKER`.
    SlotsyncWorker,
    /// `B_STANDALONE_BACKEND`.
    StandaloneBackend,

    /* Auxiliary processes (PGPROC entries, no database attachment) */
    /// `B_ARCHIVER`.
    Archiver,
    /// `B_BG_WRITER`.
    BgWriter,
    /// `B_CHECKPOINTER`.
    Checkpointer,
    /// `B_IO_WORKER`.
    IoWorker,
    /// `B_STARTUP`.
    Startup,
    /// `B_WAL_RECEIVER`.
    WalReceiver,
    /// `B_WAL_SUMMARIZER`.
    WalSummarizer,
    /// `B_WAL_WRITER`.
    WalWriter,

    /// `B_LOGGER` — not connected to shared memory; no PGPROC entry.
    Logger,
}

/// `BACKEND_NUM_TYPES` (`miscadmin.h`): `B_LOGGER + 1`.
pub const BACKEND_NUM_TYPES: usize = BackendType::Logger as usize + 1;
