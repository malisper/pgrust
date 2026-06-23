//! Seam declarations for the `backend-commands-publicationcmds` unit
//! (`commands/publicationcmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use rel::Relation;

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
    /// `pub_rf_contains_invalid_column(pubid, relation, ancestors, pubviaroot)`
    /// (publicationcmds.c): whether the publication's row-filter expression
    /// references any column not part of the relation's REPLICA IDENTITY.
    /// `RelationBuildPublicationDesc` calls this per publishing publication. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn pub_rf_contains_invalid_column<'mcx>(
        mcx: Mcx<'mcx>,
        pubid: Oid,
        relation: &Relation<'mcx>,
        ancestors: &[Oid],
        pubviaroot: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `pub_contains_invalid_column(pubid, relation, ancestors, pubviaroot,
    /// pubgencols_type, &invalid_column_list, &invalid_gen_col)`
    /// (publicationcmds.c): returns `(found, invalid_column_list,
    /// invalid_gen_col)` — whether the publication's column list / published
    /// generated columns fail to cover the relation's REPLICA IDENTITY.
    /// `RelationBuildPublicationDesc` calls this per publishing publication. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn pub_contains_invalid_column<'mcx>(
        mcx: Mcx<'mcx>,
        pubid: Oid,
        relation: &Relation<'mcx>,
        ancestors: &[Oid],
        pubviaroot: bool,
        pubgencols_type: i8,
    ) -> PgResult<(bool, bool, bool)>
);

seam_core::seam!(
    /// `InvalidatePubRelSyncCache(Oid pubid, bool puballtables)`
    /// (publicationcmds.c) — invalidate the relsync cache entries for a renamed
    /// publication.
    pub fn InvalidatePubRelSyncCache(pubid: Oid, puballtables: bool) -> PgResult<()>
);
