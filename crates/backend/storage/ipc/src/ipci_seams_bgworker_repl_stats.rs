//! Per-owner `*ShmemSize` / `*ShmemInit` seam routing for the
//! bgworker / replication / stats subsystems that ipci.c's
//! `CalculateShmemSize` / `CreateOrAttachShmemStructs` drive.
//!
//! | C symbol                          | owner seam crate                              |
//! |-----------------------------------|-----------------------------------------------|
//! | `CheckpointerShmemSize`/`Init`        | `backend-postmaster-checkpointer-seams`       |
//! | `AutoVacuumShmemSize`/`Init`          | `backend-postmaster-autovacuum-seams`         |
//! | `BackgroundWorkerShmemSize`/`Init`    | `backend-postmaster-bgworker-seams`           |
//! | `WalSummarizerShmemSize`/`Init`       | `backend-postmaster-walsummarizer-seams`      |
//! | `PgArchShmemSize`/`Init`              | `backend-postmaster-pgarch-seams`             |
//! | `WalSndShmemSize`/`Init`              | `backend-replication-walsender-seams`         |
//! | `WalRcvShmemSize`/`Init`              | `backend-replication-walreceiverfuncs-seams`  |
//! | `ReplicationSlotsShmemSize`/`Init`    | `backend-replication-slot-seams`              |
//! | `ReplicationOriginShmemSize`/`Init`   | `backend-replication-logical-origin-seams`    |
//! | `ApplyLauncherShmemSize`/`Init`       | `backend-replication-logical-launcher-seams`  |
//! | `SlotSyncShmemSize`/`Init`            | `backend-replication-logical-slotsync-seams`  |
//! | `AsyncShmemSize`/`Init`               | `backend-commands-async-seams`                |
//! | `StatsShmemSize`/`Init`               | `backend-utils-activity-pgstat-seams`         |
//! | `WaitEventCustomShmemSize`/`Init`     | `backend-utils-activity-waitevent-seams`      |
//! | `InjectionPointShmemSize`/`Init`      | `backend-storage-ipc-injection-point-seams`   |

use types_core::Size;
use types_error::PgResult;

pub fn checkpointer_shmem_size() -> PgResult<Size> {
    checkpointer_seams::checkpointer_shmem_size::call()
}
pub fn checkpointer_shmem_init() -> PgResult<()> {
    checkpointer_seams::checkpointer_shmem_init::call()
}

/// `AutoVacuumShmemSize()` — infallible owner seam (`-> Size`).
pub fn auto_vacuum_shmem_size() -> Size {
    autovacuum_seams::auto_vacuum_shmem_size::call()
}
pub fn auto_vacuum_shmem_init() -> PgResult<()> {
    autovacuum_seams::auto_vacuum_shmem_init::call()
}

/// `BackgroundWorkerShmemSize()` — infallible owner seam (`-> Size`).
pub fn background_worker_shmem_size() -> Size {
    bgworker_seams::background_worker_shmem_size::call()
}
pub fn background_worker_shmem_init() -> PgResult<()> {
    bgworker_seams::background_worker_shmem_init::call()
}

/// `WalSummarizerShmemSize()` — infallible owner seam (`-> Size`).
pub fn wal_summarizer_shmem_size() -> Size {
    walsummarizer_seams::wal_summarizer_shmem_size::call()
}
pub fn wal_summarizer_shmem_init() -> PgResult<()> {
    walsummarizer_seams::wal_summarizer_shmem_init::call()
}

/// `PgArchShmemSize()` — infallible owner seam (`-> Size`).
pub fn pg_arch_shmem_size() -> Size {
    pgarch_seams::pg_arch_shmem_size::call()
}
/// `PgArchShmemInit()` — infallible owner seam (`void` / `-> ()`).
pub fn pg_arch_shmem_init() {
    pgarch_seams::pg_arch_shmem_init::call()
}

pub fn wal_snd_shmem_size() -> PgResult<Size> {
    walsender_seams::wal_snd_shmem_size::call()
}
pub fn wal_snd_shmem_init() -> PgResult<()> {
    walsender_seams::wal_snd_shmem_init::call()
}

pub fn wal_rcv_shmem_size() -> PgResult<Size> {
    walreceiverfuncs_seams::wal_rcv_shmem_size::call()
}
pub fn wal_rcv_shmem_init() -> PgResult<()> {
    walreceiverfuncs_seams::wal_rcv_shmem_init::call()
}

pub fn replication_slots_shmem_size() -> PgResult<Size> {
    slot_seams::replication_slots_shmem_size::call()
}
/// `ReplicationSlotsShmemInit()` — note the owner seam is infallible (`()`),
/// matching the pre-existing `backend-replication-slot-seams` declaration.
pub fn replication_slots_shmem_init() {
    slot_seams::replication_slots_shmem_init::call()
}

/// `ReplicationOriginShmemSize()` — infallible owner seam (`-> Size`).
pub fn replication_origin_shmem_size() -> Size {
    origin_seams::replication_origin_shmem_size::call()
}
pub fn replication_origin_shmem_init() -> PgResult<()> {
    origin_seams::replication_origin_shmem_init::call()
}

/// `ApplyLauncherShmemSize()` — infallible owner seam (`-> Size`).
pub fn apply_launcher_shmem_size() -> Size {
    launcher_seams::apply_launcher_shmem_size::call()
}
pub fn apply_launcher_shmem_init() -> PgResult<()> {
    launcher_seams::apply_launcher_shmem_init::call()
}

/// `SlotSyncShmemSize()` — the pre-existing owner seam is infallible
/// (`-> Size`), matching `backend-replication-logical-slotsync-seams`.
pub fn slot_sync_shmem_size() -> Size {
    slotsync_seams::slot_sync_shmem_size::call()
}
pub fn slot_sync_shmem_init() -> PgResult<()> {
    slotsync_seams::slot_sync_shmem_init::call()
}

pub fn async_shmem_size() -> PgResult<Size> {
    async_seams::async_shmem_size::call()
}
pub fn async_shmem_init() -> PgResult<()> {
    async_seams::async_shmem_init::call()
}

pub fn stats_shmem_size() -> PgResult<Size> {
    pgstat_seams::stats_shmem_size::call()
}
pub fn stats_shmem_init() -> PgResult<()> {
    pgstat_seams::stats_shmem_init::call()
}

pub fn wait_event_custom_shmem_size() -> PgResult<Size> {
    waitevent_seams::wait_event_custom_shmem_size::call()
}
pub fn wait_event_custom_shmem_init() -> PgResult<()> {
    waitevent_seams::wait_event_custom_shmem_init::call()
}

pub fn injection_point_shmem_size() -> PgResult<Size> {
    injection_point_seams::injection_point_shmem_size::call()
}
pub fn injection_point_shmem_init() -> PgResult<()> {
    injection_point_seams::injection_point_shmem_init::call()
}
