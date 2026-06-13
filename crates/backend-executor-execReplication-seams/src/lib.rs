//! Seam declarations for the `backend-executor-execReplication` unit
//! (`executor/execReplication.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `GetRelationIdentityOrPK(rel)` (execReplication.c): the OID of the
    /// relation's replica identity index, falling back to the primary key
    /// index, or `InvalidOid` if it has neither. The relcache index lookups
    /// can `ereport(ERROR)`, carried on `Err`.
    pub fn get_relation_identity_or_pk(
        rel: &types_rel::Relation<'_>,
    ) -> types_error::PgResult<types_core::Oid>
);
