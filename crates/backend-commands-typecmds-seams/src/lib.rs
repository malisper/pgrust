//! Seam declarations for the `backend-commands-typecmds` unit
//! (`commands/typecmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `AlterTypeOwner_oid(typeOid, newOwnerId, hasDependEntry)` (typecmds.c):
    /// change a type's owner during REASSIGN OWNED. `hasDependEntry` is the C
    /// flag telling the routine a pg_shdepend OWNER entry already exists. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_type_owner_oid(
        type_oid: Oid,
        new_owner_id: Oid,
        has_depend_entry: bool,
    ) -> PgResult<()>
);
