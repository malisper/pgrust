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
        rel: &rel::Relation<'_>,
    ) -> types_error::PgResult<types_core::Oid>
);

seam_core::seam!(
    /// `CheckCmdReplicaIdentity(rel, cmd)` (execReplication.c): verify the
    /// relation has a suitable replica identity for the command (the publication
    /// row-filter / column-list / REPLICA IDENTITY checks for `UPDATE`/`DELETE`;
    /// a no-op for `INSERT` and for partitioned tables). Consumed by
    /// `CheckValidResultRel` (execMain.c). The relcache publication-desc build
    /// can `ereport(ERROR)`, carried on `Err`.
    pub fn check_cmd_replica_identity<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &rel::Relation<'mcx>,
        cmd: nodes::nodes::CmdType,
    ) -> types_error::PgResult<()>
);
