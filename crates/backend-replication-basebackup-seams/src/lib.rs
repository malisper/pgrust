//! Seam declarations for the `backend-replication-basebackup` unit
//! (`replication/basebackup.c`) and the upload-manifest path
//! (`backend/backup/basebackup_incremental.c`), consumed by walsender's
//! `BASE_BACKUP` / `UPLOAD_MANIFEST` replication commands.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_replication::replnodes::BaseBackupCmd;

seam_core::seam!(
    /// `SendBaseBackup(BaseBackupCmd *cmd, IncrementalBackupInfo *ib)`
    /// (basebackup.c) — perform a base backup and stream it to the client.
    /// Can `ereport(ERROR)`.
    pub fn send_base_backup(cmd: BaseBackupCmd) -> PgResult<()>
);

seam_core::seam!(
    /// `UploadManifest()` (walsender.c) — read the uploaded backup manifest from
    /// the client into an `IncrementalBackupInfo` for a subsequent incremental
    /// `BASE_BACKUP`. The manifest parsing + COPY-in protocol live in the
    /// backup subsystem. Can `ereport(ERROR)`.
    pub fn upload_manifest() -> PgResult<()>
);
