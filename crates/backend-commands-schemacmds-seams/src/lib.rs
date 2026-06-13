//! Seam declarations for the `backend-commands-schemacmds` unit
//! (`commands/schemacmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `AlterSchemaOwner_oid(schemaOid, newOwnerId)` (schemacmds.c): change a
    /// schema's owner during REASSIGN OWNED. Can `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn alter_schema_owner_oid(schema_oid: Oid, new_owner_id: Oid) -> PgResult<()>
);
