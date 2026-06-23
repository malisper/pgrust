//! Per-owner `*ShmemSize` / `*ShmemInit` seam routing for the
//! storage-access subsystems that ipci.c's `CalculateShmemSize` /
//! `CreateOrAttachShmemStructs` drive.
//!
//! ipci.c calls these owners directly in C; here each owner is reached
//! through its per-owner `*-seams` crate (the owners are unported, so the
//! seam panics loudly until the owner lands and installs it). The wrappers
//! below are 1:1 with the C call sites and exist so the call graph is wired
//! and type-checked at scaffold time; [`super::ipci_core`] composes them in
//! the C order.
//!
//! | C symbol                  | owner seam crate                          |
//! |---------------------------|-------------------------------------------|
//! | `BufferManagerShmemSize`/`Init` | `backend-storage-buffer-bufmgr-seams` |
//! | `LockManagerShmemSize`/`Init`   | `backend-storage-lmgr-lock-seams`     |
//! | `PredicateLockShmemSize`/`Init` | `backend-storage-lmgr-predicate-seams`|
//! | `ProcArrayShmemSize`/`Init`     | `backend-storage-ipc-procarray-seams` |
//! | `SharedInvalShmemSize`/`Init`   | `backend-storage-ipc-sinval-seams`    |
//! | `PMSignalShmemSize`/`Init`      | `backend-storage-ipc-pmsignal-seams`  |
//! | `ProcSignalShmemSize`/`Init`    | `backend-storage-ipc-procsignal-seams`|
//! | `AioShmemSize`/`Init`           | `backend-storage-aio-seams`           |
//! | `SyncScanShmemSize`/`Init`      | `backend-access-common-syncscan-seams`|
//! | `BTreeShmemSize`/`Init`         | `backend-access-nbtree-seams`         |

use ::types_core::Size;
use ::types_error::PgResult;

pub fn buffer_manager_shmem_size() -> PgResult<Size> {
    bufmgr_seams::buffer_manager_shmem_size::call()
}
pub fn buffer_manager_shmem_init() -> PgResult<()> {
    bufmgr_seams::buffer_manager_shmem_init::call()
}

pub fn lock_manager_shmem_size() -> PgResult<Size> {
    lock_seams::lock_manager_shmem_size::call()
}
pub fn lock_manager_shmem_init() -> PgResult<()> {
    lock_seams::lock_manager_shmem_init::call()
}

pub fn predicate_lock_shmem_size() -> PgResult<Size> {
    predicate_seams::predicate_lock_shmem_size::call()
}
pub fn predicate_lock_shmem_init() -> PgResult<()> {
    predicate_seams::predicate_lock_shmem_init::call()
}

pub fn proc_array_shmem_size() -> PgResult<Size> {
    procarray_seams::proc_array_shmem_size::call()
}
pub fn proc_array_shmem_init() -> PgResult<()> {
    procarray_seams::proc_array_shmem_init::call()
}

/// `BackendStatusShmemSize`/`Init` — owner `backend-utils-activity-status`
/// (`utils/activity/backend_status.c`); routed here because the per-backend
/// status array is sized/initialized adjacent to the process table in ipci.c.
pub fn backend_status_shmem_size() -> PgResult<Size> {
    status_seams::backend_status_shmem_size::call()
}
pub fn backend_status_shmem_init() -> PgResult<()> {
    status_seams::backend_status_shmem_init::call()
}

pub fn shared_inval_shmem_size() -> PgResult<Size> {
    sinval_seams::shared_inval_shmem_size::call()
}
pub fn shared_inval_shmem_init() -> PgResult<()> {
    sinval_seams::shared_inval_shmem_init::call()
}

pub fn pm_signal_shmem_size() -> PgResult<Size> {
    pmsignal_seams::pm_signal_shmem_size::call()
}
pub fn pm_signal_shmem_init() -> PgResult<()> {
    pmsignal_seams::pm_signal_shmem_init::call()
}

pub fn proc_signal_shmem_size() -> PgResult<Size> {
    procsignal_seams::proc_signal_shmem_size::call()
}
pub fn proc_signal_shmem_init() -> PgResult<()> {
    procsignal_seams::proc_signal_shmem_init::call()
}

pub fn aio_shmem_size() -> PgResult<Size> {
    aio_seams_2::aio_shmem_size::call()
}
pub fn aio_shmem_init() -> PgResult<()> {
    aio_seams_2::aio_shmem_init::call()
}

pub fn sync_scan_shmem_size() -> PgResult<Size> {
    syncscan_seams::sync_scan_shmem_size::call()
}
pub fn sync_scan_shmem_init() -> PgResult<()> {
    syncscan_seams::sync_scan_shmem_init::call()
}

pub fn btree_shmem_size() -> PgResult<Size> {
    nbtree_seams_2::btree_shmem_size::call()
}
pub fn btree_shmem_init() -> PgResult<()> {
    nbtree_seams_2::btree_shmem_init::call()
}
