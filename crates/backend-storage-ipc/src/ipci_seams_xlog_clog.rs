//! Per-owner `*ShmemSize` / `*ShmemInit` seam routing for the WAL/CLOG
//! subsystems that ipci.c's `CalculateShmemSize` /
//! `CreateOrAttachShmemStructs` drive.
//!
//! | C symbol                    | owner seam crate                            |
//! |-----------------------------|---------------------------------------------|
//! | `VarsupShmemSize`/`Init`        | `backend-access-transam-varsup-seams`       |
//! | `XLOGShmemSize`/`Init`          | `backend-access-transam-xlog-seams`         |
//! | `XLogPrefetchShmemSize`/`Init`  | `backend-access-transam-xlogprefetcher-seams` |
//! | `XLogRecoveryShmemSize`/`Init`  | `backend-access-transam-xlogrecovery-seams` |
//! | `CLOGShmemSize`/`Init`          | `backend-access-transam-clog-seams`         |
//! | `CommitTsShmemSize`/`Init`      | `backend-access-transam-commit-ts-seams`    |
//! | `SUBTRANSShmemSize`/`Init`      | `backend-access-transam-subtrans-seams`     |
//! | `MultiXactShmemSize`/`Init`     | `backend-access-transam-multixact-seams`    |
//! | `TwoPhaseShmemSize`/`Init`      | `backend-access-transam-twophase-seams`     |

use types_core::Size;
use types_error::PgResult;

pub fn varsup_shmem_size() -> PgResult<Size> {
    backend_access_transam_varsup_seams::varsup_shmem_size::call()
}
pub fn varsup_shmem_init() -> PgResult<()> {
    backend_access_transam_varsup_seams::varsup_shmem_init::call()
}

pub fn xlog_shmem_size() -> PgResult<Size> {
    backend_access_transam_xlog_seams::xlog_shmem_size::call()
}
pub fn xlog_shmem_init() -> PgResult<()> {
    backend_access_transam_xlog_seams::xlog_shmem_init::call()
}

pub fn xlog_prefetch_shmem_size() -> PgResult<Size> {
    backend_access_transam_xlogprefetcher_seams::xlog_prefetch_shmem_size::call()
}
pub fn xlog_prefetch_shmem_init() -> PgResult<()> {
    backend_access_transam_xlogprefetcher_seams::xlog_prefetch_shmem_init::call()
}

pub fn xlog_recovery_shmem_size() -> PgResult<Size> {
    backend_access_transam_xlogrecovery_seams::xlog_recovery_shmem_size::call()
}
pub fn xlog_recovery_shmem_init() -> PgResult<()> {
    backend_access_transam_xlogrecovery_seams::xlog_recovery_shmem_init::call()
}

pub fn clog_shmem_size() -> PgResult<Size> {
    backend_access_transam_clog_seams::clog_shmem_size::call()
}
pub fn clog_shmem_init() -> PgResult<()> {
    backend_access_transam_clog_seams::clog_shmem_init::call()
}

pub fn commit_ts_shmem_size() -> PgResult<Size> {
    backend_access_transam_commit_ts_seams::commit_ts_shmem_size::call()
}
pub fn commit_ts_shmem_init() -> PgResult<()> {
    backend_access_transam_commit_ts_seams::commit_ts_shmem_init::call()
}

pub fn sub_trans_shmem_size() -> PgResult<Size> {
    backend_access_transam_subtrans_seams::sub_trans_shmem_size::call()
}
pub fn sub_trans_shmem_init() -> PgResult<()> {
    backend_access_transam_subtrans_seams::sub_trans_shmem_init::call()
}

pub fn multi_xact_shmem_size() -> PgResult<Size> {
    backend_access_transam_multixact_seams::multi_xact_shmem_size::call()
}
pub fn multi_xact_shmem_init() -> PgResult<()> {
    backend_access_transam_multixact_seams::multi_xact_shmem_init::call()
}

pub fn two_phase_shmem_size() -> PgResult<Size> {
    backend_access_transam_twophase_seams::two_phase_shmem_size::call()
}
pub fn two_phase_shmem_init() -> PgResult<()> {
    backend_access_transam_twophase_seams::two_phase_shmem_init::call()
}
