//! Seam declarations for the `backend-commands-tablecmds` unit
//! (`commands/tablecmds.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::Oid;
use types_core::SubTransactionId;
use types_error::PgResult;
use types_storage::lock::LOCKMODE;

// ---------------------------------------------------------------------------
// F0 inward seams (relation create / drop / truncate). The owning unit
// `backend-commands-tablecmds` installs these from its `init_seams()`.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `DefineRelation(stmt, relkind, ownerId, typaddress, queryString)`
    /// (tablecmds.c:764) — the CREATE TABLE / CREATE relation driver. Returns
    /// the new relation's [`ObjectAddress`]. (`typaddress` out-parameter is
    /// NULL at the F0 call sites and so is not carried.)
    pub fn define_relation<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: types_nodes::ddlnodes::CreateStmt<'mcx>,
        relkind: u8,
        owner_id: Oid,
        query_string: Option<&str>,
    ) -> PgResult<types_catalog::catalog_dependency::ObjectAddress>
);

seam_core::seam!(
    /// `RemoveRelations(DropStmt *drop)` (tablecmds.c:1538) — DROP
    /// TABLE/INDEX/SEQUENCE/VIEW/MATVIEW/FOREIGN TABLE.
    pub fn remove_relations<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        drop: &types_nodes::ddlnodes::DropStmt<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecuteTruncate(TruncateStmt *stmt)` (tablecmds.c:1861) — TRUNCATE.
    pub fn execute_truncate<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &types_nodes::ddlnodes::TruncateStmt<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `BuildDescForRelation(const List *columns)` (tablecmds.c:1380) — build a
    /// [`TupleDescData`](types_tuple::heaptuple::TupleDescData) from a list of
    /// owned `ColumnDef` nodes. Consumed by view.c / createas.
    pub fn build_desc_for_relation<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        columns: &[types_nodes::rawnodes::ColumnDef<'mcx>],
    ) -> PgResult<types_tuple::heaptuple::TupleDescData<'mcx>>
);

seam_core::seam!(
    /// `SetRelationHasSubclass(relationId, relhassubclass)` (tablecmds.c:3647).
    pub fn set_relation_has_subclass<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: Oid,
        relhassubclass: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CheckRelationTableSpaceMove(rel, newTableSpaceId)` (tablecmds.c:3693) —
    /// returns true if a move is required (and possible), false if a no-op;
    /// raises an error otherwise.
    pub fn check_relation_tablespace_move<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        new_tablespace_id: Oid,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `SetRelationTableSpace(rel, newTableSpaceId, newRelFilenumber)`
    /// (tablecmds.c:3750).
    pub fn set_relation_tablespace<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
        new_tablespace_id: Oid,
        new_relfilenumber: Oid,
    ) -> PgResult<()>
);

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
    /// `register_on_commit_action(relid, action)` (tablecmds.c): record a
    /// special ON COMMIT action (DELETE ROWS / DROP) for a temp relation in the
    /// backend-local `on_commits` list (allocated in `CacheMemoryContext`).
    /// Called by `heap_create_with_catalog` when `oncommit != ONCOMMIT_NOOP`.
    pub fn register_on_commit_action(
        relid: Oid,
        action: types_nodes::primnodes::OnCommitAction,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `remove_on_commit_action(relid)` (tablecmds.c): drop the ON COMMIT
    /// bookkeeping entry for `relid` from the backend-local `on_commits` list.
    /// Called by `heap_drop_with_catalog`.
    pub fn remove_on_commit_action(relid: Oid) -> PgResult<()>
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

// ===========================================================================
// OUTWARD seams consumed by the F0 tablecmds port whose owners are not yet
// ported. These are declared HERE (on the tablecmds-seams crate, the unit's
// own seam crate) and installed by their owners when they land; until then a
// call panics loudly. Each mirrors the C function it stands for faithfully.
// ===========================================================================

/* ---- MergeAttributes + AddRelation* (tablecmds.c, owner: tablecmds.c F1+) - */

/// `MergeAttributesResult` — the destructively-rewritten `tableElts` plus the
/// `old_constraints` / `old_notnulls` lists that `MergeAttributes` produces
/// (the C out-parameters `supconstr` / `supnotnulls`). All three are owned
/// node lists in the owned model.
pub struct MergeAttributesResult<'mcx> {
    /// rewritten `stmt->tableElts` (a list of `ColumnDef`).
    pub columns: mcx::PgVec<'mcx, types_nodes::rawnodes::ColumnDef<'mcx>>,
    /// `*supconstr` — inherited CHECK constraints (`CookedConstraint` nodes,
    /// carried as raw `Node`s).
    pub old_constraints: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
    /// `*supnotnulls` — inherited NOT NULL constraints.
    pub old_notnulls: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
}

seam_core::seam!(
    /// `MergeAttributes(columns, supers, relpersistence, is_partition,
    /// &supconstr, &supnotnulls)` (tablecmds.c): look up inheritance ancestors
    /// and generate the relation schema, including inherited attributes.
    /// Destructively rewrites `columns`. `Err` carries its `ereport(ERROR)`s.
    pub fn merge_attributes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        columns: mcx::PgVec<'mcx, types_nodes::rawnodes::ColumnDef<'mcx>>,
        supers: &[Oid],
        relpersistence: u8,
        is_partition: bool,
    ) -> PgResult<MergeAttributesResult<'mcx>>
);

seam_core::seam!(
    /// `AddRelationNewConstraints(rel, newColDefaults, newConstraints,
    /// allow_merge, is_local, is_internal, queryString)` (heap.c): add column
    /// default / generation expressions and CHECK constraints to a freshly
    /// created relation. Returns the list of `CookedConstraint`s (carried as
    /// `Node`s) — the C return value used to harvest the constraint names.
    /// `raw_defaults` carries the `(attnum, raw_default, generated)` triples
    /// the caller assembled from the column list. `Err` carries the
    /// transform/insert `ereport(ERROR)`s.
    pub fn add_relation_new_constraints<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
        raw_defaults: &[(types_core::AttrNumber, types_nodes::nodes::NodePtr<'mcx>, i8)],
        new_constraints: &[types_nodes::nodes::NodePtr<'mcx>],
        allow_merge: bool,
        is_local: bool,
        is_internal: bool,
        query_string: Option<&str>,
    ) -> PgResult<mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>>
);

seam_core::seam!(
    /// `AddRelationNotNullConstraints(rel, constraints, old_notnulls,
    /// connames)` (heap.c): merge the directly-declared NOT NULL constraints
    /// with the inherited ones, create them, and return the list of column
    /// attnums that gained the constraint (so the caller can set `attnotnull`).
    /// `Err` carries its `ereport(ERROR)`s.
    pub fn add_relation_not_null_constraints<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
        constraints: &[types_nodes::nodes::NodePtr<'mcx>],
        old_notnulls: &[types_nodes::nodes::NodePtr<'mcx>],
        connames: &[String],
    ) -> PgResult<mcx::PgVec<'mcx, types_core::AttrNumber>>
);

seam_core::seam!(
    /// `set_attnotnull(NULL, rel, attnum, is_valid, queryString==NULL)`
    /// (tablecmds.c): set the `attnotnull` flag on a column of a freshly
    /// created relation. `Err` carries the catalog-update `ereport(ERROR)`s.
    pub fn set_attnotnull<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
        attnum: types_core::AttrNumber,
        is_valid: bool,
        queue_validation: bool,
    ) -> PgResult<()>
);

/* ---- reloptions (reloptions.c, owner: backend-access-common-reloptions) --- */

seam_core::seam!(
    /// `transformRelOptions((Datum) 0, options, NULL, validnsps, true, false)`
    /// then `view_reloptions` / `partitioned_table_reloptions` /
    /// `heap_reloptions` validation (reloptions.c): parse and validate the
    /// `WITH (...)` reloptions for a relation of kind `relkind`, returning the
    /// opaque reloptions token the catalog owner stores. `Err` carries the
    /// validation `ereport(ERROR)`s.
    pub fn transform_and_check_reloptions<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        options: &[types_nodes::nodes::NodePtr<'mcx>],
        relkind: u8,
    ) -> PgResult<types_cluster::RelOptionsToken>
);

/* ---- access-method lookup (amapi/tableam, owner: backend-utils-cache) ----- */

seam_core::seam!(
    /// `get_table_am_oid(amname, missing_ok)` (amcmds.c): look up the OID of a
    /// table access method by name. `Err` carries the not-found / wrong-kind
    /// `ereport(ERROR)` (with `missing_ok = false`).
    pub fn get_table_am_oid(amname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `default_table_access_method` (guc.c) — the GUC string naming the
    /// default table AM (e.g. `"heap"`), read for relations that don't specify
    /// `USING`.
    pub fn default_table_access_method<'mcx>(mcx: mcx::Mcx<'mcx>) -> PgResult<mcx::PgString<'mcx>>
);

/* ---- type-name / collation / storage resolution (parse_type.c, heap.c) ---- */

seam_core::seam!(
    /// `typenameTypeId(NULL, typeName)` (parse_type.c): resolve a `TypeName`
    /// node to its type OID. `Err` carries the lookup `ereport(ERROR)`s.
    pub fn typename_type_id<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        type_name: &types_nodes::rawnodes::TypeName<'mcx>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `typenameTypeIdAndMod(NULL, typeName, &typid, &typmod)` (parse_type.c):
    /// resolve a `TypeName` to `(typeOid, typmod)`. `Err` carries the lookup
    /// `ereport(ERROR)`s.
    pub fn typename_type_id_and_mod<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        type_name: &types_nodes::rawnodes::TypeName<'mcx>,
    ) -> PgResult<(Oid, i32)>
);

seam_core::seam!(
    /// `GetColumnDefCollation(NULL, coldef, typeOid)` (parse_type.c): determine
    /// the collation a column should have, given its `ColumnDef` (which may
    /// carry an explicit `COLLATE`) and resolved type. `Err` carries the
    /// `ereport(ERROR)`s.
    pub fn get_column_def_collation<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        coldef: &types_nodes::rawnodes::ColumnDef<'mcx>,
        type_oid: Oid,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetAttributeCompression(atttypid, compression)` (tablecmds.c): resolve
    /// the per-column compression method name to the `attcompression` char
    /// stored in pg_attribute. `Err` carries the validation `ereport(ERROR)`s.
    pub fn get_attribute_compression(atttypid: Oid, compression: Option<&str>) -> PgResult<i8>
);

seam_core::seam!(
    /// `GetAttributeStorage(atttypid, storagemode)` (tablecmds.c): resolve the
    /// per-column STORAGE mode name to the `attstorage` char. `Err` carries the
    /// validation `ereport(ERROR)`s.
    pub fn get_attribute_storage(atttypid: Oid, storagemode: &str) -> PgResult<i8>
);

/* ---- relcache reads (relcache.c, owner: backend-utils-cache-relcache) ----- */

seam_core::seam!(
    /// `RelationIsLogicallyLogged(rel)` (rel.h): is this relation WAL-logged for
    /// logical decoding?
    pub fn relation_is_logically_logged(rel: &types_rel::Relation<'_>) -> PgResult<bool>
);

seam_core::seam!(
    /// `RELATION_IS_OTHER_TEMP(rel)` (rel.h): is this a temp table of *another*
    /// backend?
    pub fn relation_is_other_temp(rel: &types_rel::Relation<'_>) -> PgResult<bool>
);

seam_core::seam!(
    /// `rel->rd_refcnt` (rel.h): the relcache pin count — read by
    /// `CheckTableNotInUse`.
    pub fn relation_get_refcount(rel: &types_rel::Relation<'_>) -> PgResult<i32>
);

seam_core::seam!(
    /// `rel->rd_isnailed` (rel.h): is this relation nailed in the relcache?
    pub fn relation_is_nailed(rel: &types_rel::Relation<'_>) -> PgResult<bool>
);

seam_core::seam!(
    /// `rel->rd_createSubid` (rel.h): the sub-transaction that created this
    /// relation (`InvalidSubTransactionId` if not new in this xact).
    pub fn relation_get_create_subid(rel: &types_rel::Relation<'_>) -> PgResult<SubTransactionId>
);

seam_core::seam!(
    /// `rel->rd_newRelfilelocatorSubid` (rel.h): the sub-transaction that gave
    /// this relation a new relfilenumber.
    pub fn relation_get_new_relfilelocator_subid(
        rel: &types_rel::Relation<'_>,
    ) -> PgResult<SubTransactionId>
);

/* ---- pg_class drop/truncate syscache projection (catalog read) ----------- */

/// The fields of a relation's `pg_class` row read by the DROP / TRUNCATE
/// `RangeVarGetRelidExtended` callbacks (a `SearchSysCache1(RELOID)` projection).
pub struct PgClassDropInfo {
    /// `classform->relkind`.
    pub relkind: u8,
    /// `classform->relpersistence`.
    pub relpersistence: u8,
    /// `classform->relispartition`.
    pub relispartition: bool,
    /// `classform->relnamespace`.
    pub relnamespace: Oid,
    /// `NameStr(classform->relname)` — used in the TRUNCATE callback's error
    /// text (`truncate_check_rel`/`truncate_check_perms`).
    pub relname: String,
}

seam_core::seam!(
    /// `SearchSysCache1(RELOID, relid)` projection of the fields the DROP /
    /// TRUNCATE callbacks read; `Ok(None)` for a concurrently-dropped relation
    /// (`!HeapTupleIsValid`). `Err` carries any cache `ereport(ERROR)`.
    pub fn get_pg_class_drop_info(relid: Oid) -> PgResult<Option<PgClassDropInfo>>
);

seam_core::seam!(
    /// `IsSystemClass(relid, classform)` (catalog.c): is this relation a system
    /// catalog (incl. toast, shared, information_schema)?
    pub fn is_system_class_relid(relid: Oid, relkind: u8, relnamespace: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCache1(INDEXRELID, relid)` then `indexform->indisvalid`
    /// (index.c): the `indisvalid` flag of an index's `pg_index` row, or
    /// `Ok(None)` if the row is gone.
    pub fn get_index_isvalid(relid: Oid) -> PgResult<Option<bool>>
);

seam_core::seam!(
    /// `IsBinaryUpgrade` (miscinit.c global): true while `pg_upgrade` is
    /// driving the server.
    pub fn is_binary_upgrade() -> PgResult<bool>
);

seam_core::seam!(
    /// `MyDatabaseId` (globals.c): the OID of the database this backend is
    /// connected to (the TRUNCATE WAL record's `dbId`).
    pub fn my_database_id() -> PgResult<Oid>
);

/* ---- heap truncate machinery (heap.c, owner: backend-catalog-heap) -------- */

seam_core::seam!(
    /// `heap_truncate(relids)` (heap.c): the ON COMMIT DELETE ROWS truncation
    /// path — open, truncate, and reindex each relation in-place. `Err` carries
    /// its `ereport(ERROR)`s.
    pub fn heap_truncate<'mcx>(mcx: mcx::Mcx<'mcx>, relids: &[Oid]) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_truncate_one_rel(rel)` (heap.c): immediate, non-rollbackable
    /// truncation of a single relation (and its toast table). `Err` carries its
    /// `ereport(ERROR)`s.
    pub fn heap_truncate_one_rel<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_truncate_find_FKs(relids)` (heap.c): find relations that have FK
    /// references to (any of) `relids` and are not already in the list —
    /// CASCADE truncate's fixpoint step. Returns the new relids.
    pub fn heap_truncate_find_fks<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relids: &[Oid],
    ) -> PgResult<mcx::PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `heap_truncate_check_FKs(relations, tempTables)` (heap.c): verify that
    /// every FK reference into the truncated group is internal to the group
    /// (RESTRICT) — raises otherwise. The owned model passes the relids.
    pub fn heap_truncate_check_fks(relids: &[Oid], temp_tables: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `pgstat_count_truncate(rel)` (pgstat.c): account a TRUNCATE in the
    /// relation's pending stats.
    pub fn pgstat_count_truncate(rel: &types_rel::Relation<'_>) -> PgResult<()>
);

/* ---- owned-sequence reset (pg_depend.c / sequence.c) ---------------------- */

seam_core::seam!(
    /// `getOwnedSequences(relid)` (pg_depend.c): the OIDs of the sequences
    /// owned by (any column of) `relid`.
    pub fn get_owned_sequences<'mcx>(mcx: mcx::Mcx<'mcx>, relid: Oid) -> PgResult<mcx::PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `ResetSequence(seq_relid)` (sequence.c): reset a sequence to its start
    /// value (TRUNCATE ... RESTART IDENTITY). `Err` carries its
    /// `ereport(ERROR)`s.
    pub fn reset_sequence(seq_relid: Oid) -> PgResult<()>
);

/* ---- FDW truncate dispatch (foreign.c + FdwRoutine) ----------------------- */

seam_core::seam!(
    /// `GetForeignServerIdByRelId(relid)` (foreign.c): the OID of the foreign
    /// server backing a foreign table. `Err` carries its `ereport(ERROR)`s.
    pub fn get_foreign_server_id_by_rel_id(relid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetFdwRoutineByServerId(serverid)->ExecForeignTruncate != NULL`
    /// (foreign.c + FdwRoutine): does the FDW backing `serverid` implement
    /// `ExecForeignTruncate`? Used by `truncate_check_rel`. `Err` carries the
    /// lookup `ereport(ERROR)`s.
    pub fn fdw_supports_truncate(serverid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `GetFdwRoutineByServerId(serverid)->ExecForeignTruncate(rels, behavior,
    /// restart_seqs)` (FdwRoutine): truncate all foreign tables of one server in
    /// bulk. The owned model passes the relids of the foreign tables. `Err`
    /// carries the FDW callback's `ereport(ERROR)`s.
    pub fn exec_foreign_truncate(
        serverid: Oid,
        relids: &[Oid],
        behavior: types_nodes::parsenodes::DropBehavior,
        restart_seqs: bool,
    ) -> PgResult<()>
);

/* ---- AFTER-trigger machinery (trigger.c) ---------------------------------- */

seam_core::seam!(
    /// `AfterTriggerPendingOnRel(relid)` (trigger.c): are there pending AFTER
    /// trigger events queued on this relation? Read by `CheckTableNotInUse`.
    pub fn after_trigger_pending_on_rel(relid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `AfterTriggerBeginQuery() + CreateExecutorState() + InitResultRelInfo()
    /// per rel + ExecBSTruncateTriggers()` (trigger.c / execUtils.c): fire the
    /// BEFORE STATEMENT TRUNCATE triggers for the given relations. Coarse seam:
    /// the EState / ResultRelInfo / trigger machinery is unported, so the whole
    /// before-trigger block crosses by relids. `Err` carries the trigger
    /// `ereport(ERROR)`s.
    pub fn exec_truncate_fire_before_triggers<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relids: &[Oid],
        run_as_table_owner: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecASTruncateTriggers() per rel + AfterTriggerEndQuery() +
    /// FreeExecutorState()` (trigger.c / execUtils.c): fire the AFTER STATEMENT
    /// TRUNCATE triggers for the given relations and tear down the EState.
    /// Coarse seam (see [`exec_truncate_fire_before_triggers`]). `Err` carries
    /// the trigger `ereport(ERROR)`s.
    pub fn exec_truncate_fire_after_triggers<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relids: &[Oid],
        run_as_table_owner: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `InvokeObjectTruncateHook(relid)` (objectaccess.h): invoke the
    /// object-access hook for a TRUNCATE on `relid`.
    pub fn invoke_object_truncate_hook(relid: Oid) -> PgResult<()>
);

/* ---- WAL truncate record (heapam.c / xloginsert) -------------------------- */

seam_core::seam!(
    /// `xl_heap_truncate` WAL record assembly + `XLogInsert(RM_HEAP_ID,
    /// XLOG_HEAP_TRUNCATE)` (heapam.c): write the single WAL record allowing the
    /// whole TRUNCATE action to be logically decoded. Only called when
    /// `relids_logged` is non-empty (`wal_level >= logical`). `Err` carries the
    /// insert `ereport(ERROR)`s.
    pub fn write_heap_truncate_wal(
        db_id: Oid,
        relids: &[Oid],
        cascade: bool,
        restart_seqs: bool,
    ) -> PgResult<()>
);

/* ---- snapshot management (snapmgr.c) -------------------------------------- */

// NOTE: `push_active_snapshot_transaction` / `pop_active_snapshot` are owned by
// snapmgr.c and already declared in `backend-utils-time-snapmgr-seams`;
// tablecmds calls them through that crate.

/* ---- ON COMMIT temp-namespace flag (xact.c) ------------------------------- */

seam_core::seam!(
    /// `(MyXactFlags & XACT_FLAGS_ACCESSEDTEMPNAMESPACE) != 0` (xact.c): has the
    /// current transaction accessed any temporary relations? When false the ON
    /// COMMIT DELETE ROWS truncation can be skipped.
    pub fn xact_accessed_temp_namespace() -> PgResult<bool>
);

/* ---- tablespace default (tablespace.c) ------------------------------------ */

seam_core::seam!(
    /// `GetDefaultTablespace(relpersistence, partitioned)` (tablespace.c): the
    /// default tablespace OID for a relation of the given persistence (honoring
    /// the `default_tablespace` / `temp_tablespaces` GUCs), or `InvalidOid` for
    /// the database default. `Err` carries the lookup/ACL `ereport(ERROR)`s.
    pub fn get_default_tablespace(relpersistence: u8, partitioned: bool) -> PgResult<Oid>
);

/* ---- DefineRelation partition blocks (tablecmds.c F5) --------------------- */

seam_core::seam!(
    /// The `stmt->partbound != NULL` bound-processing block of `DefineRelation`
    /// (tablecmds.c:1114-1201): `table_open(parent)` + relkind check +
    /// default-partition lock + `transformPartitionBound` /
    /// `check_new_partition_bound` / `check_default_partition_contents` +
    /// `StorePartitionBound`. The partition machinery (transformPartitionBound,
    /// ParseState, addRangeTableEntryForRelation) is unported (F5), so the whole
    /// block crosses by `(relationId, inheritOids, relname, queryString)`. `Err`
    /// carries its `ereport(ERROR)`s.
    pub fn define_relation_partbound<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: Oid,
        inherit_oids: &[Oid],
        relname: &str,
        query_string: Option<&str>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The `partitioned` partition-key-processing block of `DefineRelation`
    /// (tablecmds.c:1210-1249): `transformPartitionSpec` /
    /// `ComputePartitionAttrs` / `StorePartitionKey`. Partition machinery is
    /// unported (F5). `Err` carries its `ereport(ERROR)`s.
    pub fn define_relation_partspec<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: Oid,
        query_string: Option<&str>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The partition `stmt->partbound` clone-indexes/triggers/FKs block of
    /// `DefineRelation` (tablecmds.c:1258-1328): `RelationGetIndexList` +
    /// `generateClonedIndexStmt` / `DefineIndex` per parent index +
    /// `CloneRowTriggersToPartition` + `CloneForeignKeyConstraints`. Unported
    /// (F5). `Err` carries its `ereport(ERROR)`s.
    pub fn define_relation_clone_partition_objects<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: Oid,
        inherit_oids: &[Oid],
    ) -> PgResult<()>
);

/* ---- StoreCatalogInheritance write loop (tablecmds.c F1+) ----------------- */

seam_core::seam!(
    /// The pg_inherits write loop of `StoreCatalogInheritance` (tablecmds.c):
    /// `table_open(InheritsRelationId)` + `StoreCatalogInheritance1` per parent
    /// (which stores the pg_inherits row, records the dependency, runs the
    /// post-alter hook, and `SetRelationHasSubclass(parent, true)`). The early
    /// `supers == NIL` return is handled in-owner; this is only reached for a
    /// non-empty `supers`. `Err` carries the catalog-write `ereport(ERROR)`s.
    pub fn store_catalog_inheritance_supers<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: Oid,
        supers: &[Oid],
        child_is_partition: bool,
    ) -> PgResult<()>
);

/* ---- SetRelationHasSubclass / SetRelationTableSpace catalog writes -------- */

seam_core::seam!(
    /// The catalog-write body of `SetRelationHasSubclass` (tablecmds.c):
    /// `SearchSysCacheCopy1(RELOID)` + (conditional) `CatalogTupleUpdate` /
    /// `CacheInvalidateRelcacheByTuple`. Separated because the syscache
    /// modifiable-copy + `GETSTRUCT` mutation model is not expressible at the
    /// command layer. `Err` carries the catalog `ereport(ERROR)`s.
    pub fn set_relation_has_subclass_catalog(
        relation_id: Oid,
        relhassubclass: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The catalog-write body of `SetRelationTableSpace` (tablecmds.c):
    /// `SearchSysCacheLockedCopy1(RELOID)` + `CatalogTupleUpdate` +
    /// `UnlockTuple` + (for storageless relkinds) `changeDependencyOnTablespace`.
    /// Separated for the same reason as
    /// [`set_relation_has_subclass_catalog`]. `Err` carries the catalog
    /// `ereport(ERROR)`s.
    pub fn set_relation_tablespace_catalog(
        reloid: Oid,
        relkind: u8,
        new_tablespace_id: Oid,
        new_relfilenumber: Oid,
    ) -> PgResult<()>
);

/* ---- ALTER TABLE phase machinery outward seams (unported owners) --------- */

seam_core::seam!(
    /// `EventTriggerAlterTableRelid(Oid objectId)` (commands/event_trigger.c):
    /// remember the relation OID currently being altered so that any
    /// event-trigger-driven subcommands can attribute themselves to it. The
    /// owner (`backend-commands-event-trigger`) installs this when it lands;
    /// `AlterTableInternal` calls it before driving the phases.
    pub fn event_trigger_alter_table_relid(object_id: Oid) -> PgResult<()>
);
