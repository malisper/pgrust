//! Seam declarations for the `backend-commands-publicationcmds` unit
//! (`commands/publicationcmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `AlterPublicationOwner_oid(pubid, newOwnerId)` (publicationcmds.c):
    /// change a publication's owner during REASSIGN OWNED. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_publication_owner_oid(pubid: Oid, new_owner_id: Oid) -> PgResult<()>
);
