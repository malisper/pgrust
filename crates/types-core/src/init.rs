//! Process-environment vocabulary from `miscadmin.h`.

/// `BackendType` (`miscadmin.h`) — the kind of a postmaster child process.
/// Discriminants match the C enum; `launch_backend.c`'s
/// `child_process_kinds[]` is indexed by these values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum BackendType {
    /// `B_INVALID`.
    Invalid = 0,
    /// `B_BACKEND`.
    Backend = 1,
    /// `B_DEAD_END_BACKEND`.
    DeadEndBackend = 2,
    /// `B_AUTOVAC_LAUNCHER`.
    AutoVacLauncher = 3,
    /// `B_AUTOVAC_WORKER`.
    AutoVacWorker = 4,
    /// `B_BG_WORKER`.
    BgWorker = 5,
    /// `B_WAL_SENDER`.
    WalSender = 6,
    /// `B_SLOTSYNC_WORKER`.
    SlotSyncWorker = 7,
    /// `B_STANDALONE_BACKEND`.
    StandaloneBackend = 8,
    /// `B_ARCHIVER`.
    Archiver = 9,
    /// `B_BG_WRITER`.
    BgWriter = 10,
    /// `B_CHECKPOINTER`.
    Checkpointer = 11,
    /// `B_IO_WORKER`.
    IoWorker = 12,
    /// `B_STARTUP`.
    Startup = 13,
    /// `B_WAL_RECEIVER`.
    WalReceiver = 14,
    /// `B_WAL_SUMMARIZER`.
    WalSummarizer = 15,
    /// `B_WAL_WRITER`.
    WalWriter = 16,
    /// `B_LOGGER`.
    Logger = 17,
}

impl BackendType {
    /// Every `BackendType`, in C enum (discriminant) order.
    pub const ALL: [BackendType; BACKEND_NUM_TYPES] = [
        BackendType::Invalid,
        BackendType::Backend,
        BackendType::DeadEndBackend,
        BackendType::AutoVacLauncher,
        BackendType::AutoVacWorker,
        BackendType::BgWorker,
        BackendType::WalSender,
        BackendType::SlotSyncWorker,
        BackendType::StandaloneBackend,
        BackendType::Archiver,
        BackendType::BgWriter,
        BackendType::Checkpointer,
        BackendType::IoWorker,
        BackendType::Startup,
        BackendType::WalReceiver,
        BackendType::WalSummarizer,
        BackendType::WalWriter,
        BackendType::Logger,
    ];
}

/// `BACKEND_NUM_TYPES` (`miscadmin.h`): `(B_LOGGER + 1)`, the number of
/// distinct [`BackendType`] values.
pub const BACKEND_NUM_TYPES: usize = BackendType::Logger as usize + 1;
