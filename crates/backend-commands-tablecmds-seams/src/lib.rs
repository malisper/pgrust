//! Seam declarations for the `backend-commands-tablecmds` unit
//! (`commands/tablecmds.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::Oid;
use types_core::SubTransactionId;
use types_error::PgResult;
use types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `RangeVarCallbackOwnsRelation(relation, relId, oldRelId, arg)`
    /// (tablecmds.c) — the `RangeVarGetRelidExtended` callback used by
    /// `AlterSequence` (and others): nothing to do for a not-found relation
    /// (`!OidIsValid(relId)`), else `SearchSysCache1(RELOID)` and reject a
    /// relation the current user does not own (`object_ownercheck` /
    /// `aclcheck_error`), and a system catalog when `!allowSystemTableMods`
    /// (`IsSystemClass`). `relation` is only read for `relation->relname` in
    /// the error messages, so the seam passes the name alone. `Err` carries
    /// the lookup/ACL `ereport(ERROR)`s.
    pub fn range_var_callback_owns_relation(
        relname: &str,
        rel_id: Oid,
        old_rel_id: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PreCommit_on_commit_actions()` — ON COMMIT DROP / DELETE ROWS work;
    /// can `ereport(ERROR)`.
    pub fn pre_commit_on_commit_actions() -> PgResult<()>
);

seam_core::seam!(
    /// `AtEOXact_on_commit_actions(isCommit)`.
    pub fn at_eoxact_on_commit_actions(is_commit: bool)
);

seam_core::seam!(
    /// `AtEOSubXact_on_commit_actions(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_on_commit_actions(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    )
);

seam_core::seam!(
    /// `ATExecChangeOwner(relationOid, newOwnerId, recursing, lockmode)`
    /// (tablecmds.c): change a relation's owner (and its dependent objects:
    /// indexes, owned sequences, toast tables). REASSIGN OWNED passes
    /// `recursing = true` so visiting a dependent before its parent doesn't
    /// fail. Can `ereport(ERROR)`, carried on `Err`.
    pub fn at_exec_change_owner(
        relation_oid: Oid,
        new_owner_id: Oid,
        recursing: bool,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

/* ---- CLUSTER finish-heap-swap helpers (backend-commands-cluster) --------- */

seam_core::seam!(
    /// `CheckTableNotInUse(rel, stmt)` (tablecmds.c).
    pub fn check_table_not_in_use(rel: &types_rel::Relation<'_>, stmt: &str) -> PgResult<()>
);
seam_core::seam!(
    /// `RenameRelationInternal(myrelid, newrelname, is_internal, is_index)`
    /// (tablecmds.c).
    pub fn rename_relation_internal<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        myrelid: Oid,
        newrelname: &str,
        is_internal: bool,
        is_index: bool,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ResetRelRewrite(myrelid)` (tablecmds.c).
    pub fn reset_rel_rewrite(myrelid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `DefineRelation(stmt, RELKIND_SEQUENCE, seq->ownerId, NULL, NULL)`
    /// (tablecmds.c) for a sequence (sequence.c `DefineSequence`): the owner
    /// builds the `CreateStmt` carrying the three NOT NULL columns
    /// (`last_value int8`, `log_cnt int8`, `is_called bool`) from `seq`'s
    /// `RangeVar` + `if_not_exists`, runs `DefineRelation`, and returns the new
    /// sequence relation's `ObjectAddress`. The owned-tree `CreateSeqStmt`
    /// crosses by reference; `Err` carries the `ereport(ERROR)`s.
    pub fn define_sequence_relation<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        seq: &types_nodes::ddlnodes::CreateSeqStmt<'_>,
    ) -> PgResult<types_catalog::catalog_dependency::ObjectAddress>
);

seam_core::seam!(
    /// `AlterRelationNamespaceInternal(classRel, relOid, oldNspOid, newNspOid,
    /// hasDependEntry, objsMoved)` (tablecmds.c): move a pg_class entry to a new
    /// schema (the composite-type rel path of `AlterTypeNamespaceInternal`,
    /// typecmds.c:4250). PANICS until tablecmds.c is ported (no owner crate yet).
    pub fn alter_relation_namespace_internal(
        rel_oid: Oid,
        old_nsp_oid: Oid,
        new_nsp_oid: Oid,
        has_depend_entry: bool,
        objs_moved: &mut types_catalog::catalog_dependency::ObjectAddresses,
    ) -> PgResult<()>
);

// ---------------------------------------------------------------------------
// Generic ALTER dispatch targets driven by commands/alter.c (ExecRenameStmt /
// ExecAlterObjectSchemaStmt / AlterObjectNamespace_oid).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `RenameRelation(RenameStmt *stmt)` (tablecmds.c) — ALTER TABLE/VIEW/...
    /// RENAME TO. Returns the renamed relation's [`ObjectAddress`].
    pub fn RenameRelation<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &types_parsenodes::RenameStmt,
    ) -> PgResult<types_catalog::catalog_dependency::ObjectAddress>
);

seam_core::seam!(
    /// `renameatt(RenameStmt *stmt)` (tablecmds.c) — ALTER ... RENAME COLUMN.
    pub fn renameatt<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &types_parsenodes::RenameStmt,
    ) -> PgResult<types_catalog::catalog_dependency::ObjectAddress>
);

seam_core::seam!(
    /// `RenameConstraint(RenameStmt *stmt)` (tablecmds.c) — ALTER ... RENAME
    /// CONSTRAINT (for OBJECT_TABCONSTRAINT / OBJECT_DOMCONSTRAINT).
    pub fn RenameConstraint<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &types_parsenodes::RenameStmt,
    ) -> PgResult<types_catalog::catalog_dependency::ObjectAddress>
);

seam_core::seam!(
    /// `AlterTableNamespace(AlterObjectSchemaStmt *stmt, Oid *oldschema)`
    /// (tablecmds.c) — ALTER TABLE/SEQUENCE/VIEW/... SET SCHEMA. When
    /// `want_oldschema` is true the previous schema OID is returned in the
    /// tuple's second slot (the C `*oldschema` out-parameter); otherwise that
    /// slot is `InvalidOid`.
    pub fn AlterTableNamespace<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &types_parsenodes::AlterObjectSchemaStmt,
        want_oldschema: bool,
    ) -> PgResult<(types_catalog::catalog_dependency::ObjectAddress, Oid)>
);

seam_core::seam!(
    /// `AlterTableNamespaceInternal(Relation rel, Oid oldNspOid, Oid nspOid,
    /// ObjectAddresses *objsMoved)` (tablecmds.c) — move an already-open
    /// relation (and its dependent objects) to `nspOid`. Used by ALTER
    /// EXTENSION SET SCHEMA via `AlterObjectNamespace_oid`.
    pub fn AlterTableNamespaceInternal<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
        old_nsp_oid: Oid,
        nsp_oid: Oid,
        objs_moved: &mut types_catalog::catalog_dependency::ObjectAddresses,
    ) -> PgResult<()>
);
