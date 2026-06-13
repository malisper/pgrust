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
//! | `WalRcvShmemSize`/`Init`              | `backend-replication-walreceiver-seams`       |
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
    backend_postmaster_checkpointer_seams::checkpointer_shmem_size::call()
}
pub fn checkpointer_shmem_init() -> PgResult<()> {
    backend_postmaster_checkpointer_seams::checkpointer_shmem_init::call()
}

pub fn auto_vacuum_shmem_size() -> PgResult<Size> {
    backend_postmaster_autovacuum_seams::auto_vacuum_shmem_size::call()
}
pub fn auto_vacuum_shmem_init() -> PgResult<()> {
    backend_postmaster_autovacuum_seams::auto_vacuum_shmem_init::call()
}

pub fn background_worker_shmem_size() -> PgResult<Size> {
    backend_postmaster_bgworker_seams::background_worker_shmem_size::call()
}
pub fn background_worker_shmem_init() -> PgResult<()> {
    backend_postmaster_bgworker_seams::background_worker_shmem_init::call()
}

pub fn wal_summarizer_shmem_size() -> PgResult<Size> {
    backend_postmaster_walsummarizer_seams::wal_summarizer_shmem_size::call()
}
pub fn wal_summarizer_shmem_init() -> PgResult<()> {
    backend_postmaster_walsummarizer_seams::wal_summarizer_shmem_init::call()
}

pub fn pg_arch_shmem_size() -> PgResult<Size> {
    backend_postmaster_pgarch_seams::pg_arch_shmem_size::call()
}
pub fn pg_arch_shmem_init() -> PgResult<()> {
    backend_postmaster_pgarch_seams::pg_arch_shmem_init::call()
}

pub fn wal_snd_shmem_size() -> PgResult<Size> {
    backend_replication_walsender_seams::wal_snd_shmem_size::call()
}
pub fn wal_snd_shmem_init() -> PgResult<()> {
    backend_replication_walsender_seams::wal_snd_shmem_init::call()
}

pub fn wal_rcv_shmem_size() -> PgResult<Size> {
    backend_replication_walreceiver_seams::wal_rcv_shmem_size::call()
}
pub fn wal_rcv_shmem_init() -> PgResult<()> {
    backend_replication_walreceiver_seams::wal_rcv_shmem_init::call()
}

pub fn replication_slots_shmem_size() -> PgResult<Size> {
    backend_replication_slot_seams::replication_slots_shmem_size::call()
}
/// `ReplicationSlotsShmemInit()` — note the owner seam is infallible (`()`),
/// matching the pre-existing `backend-replication-slot-seams` declaration.
pub fn replication_slots_shmem_init() {
    backend_replication_slot_seams::replication_slots_shmem_init::call()
}

pub fn replication_origin_shmem_size() -> PgResult<Size> {
    backend_replication_logical_origin_seams::replication_origin_shmem_size::call()
}
pub fn replication_origin_shmem_init() -> PgResult<()> {
    backend_replication_logical_origin_seams::replication_origin_shmem_init::call()
}

pub fn apply_launcher_shmem_size() -> PgResult<Size> {
    backend_replication_logical_launcher_seams::apply_launcher_shmem_size::call()
}
pub fn apply_launcher_shmem_init() -> PgResult<()> {
    backend_replication_logical_launcher_seams::apply_launcher_shmem_init::call()
}

/// `SlotSyncShmemSize()` — the pre-existing owner seam is infallible
/// (`-> Size`), matching `backend-replication-logical-slotsync-seams`.
pub fn slot_sync_shmem_size() -> Size {
    backend_replication_logical_slotsync_seams::slot_sync_shmem_size::call()
}
pub fn slot_sync_shmem_init() -> PgResult<()> {
    backend_replication_logical_slotsync_seams::slot_sync_shmem_init::call()
}

pub fn async_shmem_size() -> PgResult<Size> {
    backend_commands_async_seams::async_shmem_size::call()
}
pub fn async_shmem_init() -> PgResult<()> {
    backend_commands_async_seams::async_shmem_init::call()
}

pub fn stats_shmem_size() -> PgResult<Size> {
    backend_utils_activity_pgstat_seams::stats_shmem_size::call()
}
pub fn stats_shmem_init() -> PgResult<()> {
    backend_utils_activity_pgstat_seams::stats_shmem_init::call()
}

pub fn wait_event_custom_shmem_size() -> PgResult<Size> {
    backend_utils_activity_waitevent_seams::wait_event_custom_shmem_size::call()
}
pub fn wait_event_custom_shmem_init() -> PgResult<()> {
    backend_utils_activity_waitevent_seams::wait_event_custom_shmem_init::call()
}

pub fn injection_point_shmem_size() -> PgResult<Size> {
    backend_storage_ipc_injection_point_seams::injection_point_shmem_size::call()
}
pub fn injection_point_shmem_init() -> PgResult<()> {
    backend_storage_ipc_injection_point_seams::injection_point_shmem_init::call()
}
