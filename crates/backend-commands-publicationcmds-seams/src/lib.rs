//! Seam declarations for the `backend-commands-publicationcmds` unit
//! (`commands/publicationcmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `AlterPublicationOwner_oid(pubid, newOwnerId)` (publicationcmds.c):
    /// change a publication's owner during REASSIGN OWNED. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_publication_owner_oid(pubid: Oid, new_owner_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `RemovePublicationById(pubid)` (commands/publicationcmds.c): the
    /// per-class `OCLASS_PUBLICATION` drop handler dependency.c's `doDeletion`
    /// invokes for a `pg_publication` object. Can `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn RemovePublicationById(pubid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `RemovePublicationRelById(proid)` (commands/publicationcmds.c): the
    /// per-class `OCLASS_PUBLICATION_REL` drop handler dependency.c's
    /// `doDeletion` invokes for a `pg_publication_rel` object. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn RemovePublicationRelById(proid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `RemovePublicationSchemaById(psoid)` (commands/publicationcmds.c): the
    /// per-class `OCLASS_PUBLICATION_NAMESPACE` drop handler dependency.c's
    /// `doDeletion` invokes for a `pg_publication_namespace` object. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn RemovePublicationSchemaById(psoid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterPublicationOwner(const char *name, Oid newOwnerId)`
    /// (publicationcmds.c) — ALTER PUBLICATION ... OWNER TO.
    pub fn AlterPublicationOwner(
        name: &str,
        new_owner_id: Oid,
    ) -> PgResult<types_catalog::catalog_dependency::ObjectAddress>
);

seam_core::seam!(
    /// `InvalidatePubRelSyncCache(Oid pubid, bool puballtables)`
    /// (publicationcmds.c) — invalidate the relsync cache entries for a renamed
    /// publication.
    pub fn InvalidatePubRelSyncCache(pubid: Oid, puballtables: bool) -> PgResult<()>
);
