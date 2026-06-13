//! Seam declarations for the `backend-commands-foreigncmds` unit
//! (`commands/foreigncmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `AlterForeignServerOwner_oid(srvId, newOwnerId)` (foreigncmds.c):
    /// change a foreign server's owner during REASSIGN OWNED. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_foreign_server_owner_oid(srv_id: Oid, new_owner_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterForeignDataWrapperOwner_oid(fdwId, newOwnerId)` (foreigncmds.c):
    /// change a foreign-data wrapper's owner during REASSIGN OWNED. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_foreign_data_wrapper_owner_oid(fdw_id: Oid, new_owner_id: Oid) -> PgResult<()>
);
