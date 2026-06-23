//! Seam declarations for the `backend-catalog-indexing` unit
//! (`catalog/indexing.c` catalog-tuple mutators).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as
//! `&::rel::RelationData`; pg_depend tuples cross as the deformed
//! `FormData_pg_depend` row (the caller-shaped projection precedent) — the
//! owner forms the heap tuple against the pg_depend descriptor.

use ::types_catalog::catalog_dependency::FormData_pg_depend;
use ::types_catalog::pg_sequence::FormData_pg_sequence;
use ::types_catalog::catalog_shdepend::FormData_pg_shdepend;
use ::types_catalog::opclasscmds_catalog::{
    FormData_pg_amop, FormData_pg_amproc, FormData_pg_opclass, FormData_pg_opfamily,
};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::rel::RelationData;
use ::types_tuple::heaptuple::ItemPointerData;

seam_core::seam!(
    /// `CatalogTupleDelete(rel, tid)` (catalog/indexing.c): delete the
    /// addressed tuple from a catalog relation (`simple_heap_delete`). `Err`
    /// carries the heap-mutation `ereport(ERROR)`s.
    ///
    pub fn catalog_tuple_delete(rel: &RelationData<'_>, tid: ItemPointerData) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogTupleUpdate(rel, tid, tup)` (catalog/indexing.c) for a
    /// pg_depend row: `simple_heap_update` plus `CatalogIndexInsert` index
    /// maintenance. The replacement tuple crosses as its deformed form. `Err`
    /// carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_depend(
        rel: &RelationData<'_>,
        tid: ItemPointerData,
        form: &FormData_pg_depend,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogOpenIndexes(rel)` + `CatalogTuplesMultiInsertWithInfo(rel,
    /// slot, ntuples, indstate)` + `CatalogCloseIndexes(indstate)`
    /// (catalog/indexing.c) for one batch of pg_depend rows: form and
    /// multi-insert the tuples with index maintenance. The caller keeps the C
    /// batching (`MAX_CATALOG_MULTI_INSERT_BYTES`); the index-state lifecycle
    /// is per-batch here rather than spanning batches, which is
    /// logic-invisible. `Err` carries the heap/index-mutation
    /// `ereport(ERROR)`s.
    pub fn catalog_tuples_multi_insert_pg_depend<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        forms: &[FormData_pg_depend],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `reltup = SearchSysCacheCopy1(RELOID, rel_oid)`; if the tuple is found,
    /// write `((Form_pg_class) GETSTRUCT(reltup))->reltoastrelid = toast_relid`
    /// and `CatalogTupleUpdate(class_rel, &reltup->t_self, reltup)`, then
    /// `heap_freetuple(reltup)` (toasting.c `create_toast_table`, normal path).
    /// The genuine unported callees only: the syscache copy, the `Form_pg_class`
    /// GETSTRUCT field write, and the transactional `CatalogTupleUpdate`. The
    /// returned `bool` is `HeapTupleIsValid(reltup)` — the caller raises the
    /// `cache lookup failed for relation %u` `elog(ERROR)` when it is `false`,
    /// and the GETSTRUCT write / `CatalogTupleUpdate` run only when it is
    /// `true`, matching the C control flow. (pg_class's `Form_pg_class` is a
    /// trimmed projection that cannot losslessly reform the on-disk tuple, so
    /// the field write must happen on the owner's full syscache copy in place.)
    /// The open pg_class relation (opened RowExclusiveLock by the caller)
    /// crosses by reference. `Err` carries the heap/index-mutation
    /// `ereport(ERROR)`s.
    pub fn set_pg_class_reltoastrelid(
        class_rel: &RelationData<'_>,
        rel_oid: Oid,
        toast_relid: Oid,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `SetRelationRuleStatus` pg_class write (rewriteSupport.c): `tuple =
    /// SearchSysCacheCopy1(RELOID, relationId)`; if found,
    /// `classForm = (Form_pg_class) GETSTRUCT(tuple)`; when
    /// `classForm->relhasrules != relHasRules`, set the field and
    /// `CatalogTupleUpdate(class_rel, &tuple->t_self, tuple)`, otherwise
    /// `CacheInvalidateRelcacheByTuple(tuple)` to force a relcache rebuild
    /// anyway; finally `heap_freetuple(tuple)`. The compare/update-or-invalidate
    /// lives in the owner because pg_class's `Form_pg_class` is a trimmed
    /// projection that cannot losslessly reform the on-disk tuple (same
    /// constraint as `set_pg_class_reltoastrelid`): the field write and the
    /// invalidation must run against the owner's full syscache copy. The
    /// returned `bool` is `HeapTupleIsValid(tuple)` — the caller raises the
    /// `cache lookup failed for relation %u` `elog(ERROR)` when it is `false`.
    /// The open pg_class relation (opened RowExclusiveLock by the caller)
    /// crosses by reference. `Err` carries the heap/index-mutation and
    /// invalidation `ereport(ERROR)`s.
    pub fn set_relation_rule_status(
        class_rel: &RelationData<'_>,
        relation_id: Oid,
        rel_has_rules: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ATExecSetRowSecurity` / `ATExecForceNoForceRowSecurity` (tablecmds.c):
    /// open pg_class RowExclusiveLock, `SearchSysCacheCopy1(RELOID, relid)`, poke
    /// `relrowsecurity` (or `relforcerowsecurity`) on the `GETSTRUCT` copy, and
    /// `CatalogTupleUpdate`. Exactly one of the two `Option`s is `Some`. The
    /// boolean result is `HeapTupleIsValid(tuple)`; the caller raises the
    /// `cache lookup failed for relation %u` `elog(ERROR)` when it is `false`.
    pub fn set_pg_class_row_security(
        relid: Oid,
        relrowsecurity: Option<bool>,
        relforcerowsecurity: Option<bool>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `relation_mark_replica_identity`'s pg_class leg (tablecmds.c:18411-18429):
    /// open pg_class RowExclusiveLock, `SearchSysCacheCopy1(RELOID, relid)`, and —
    /// only if `relreplident != ri_type` — poke `relreplident = ri_type` on the
    /// `GETSTRUCT` copy and `CatalogTupleUpdate`. The boolean result is
    /// `HeapTupleIsValid(tuple)`; the caller raises the `cache lookup failed for
    /// relation %s` `elog(ERROR)` when it is `false`.
    pub fn set_pg_class_relreplident(relid: Oid, ri_type: i8) -> PgResult<bool>
);

seam_core::seam!(
    /// `ResetRelRewrite`'s pg_class write (tablecmds.c:4363): open pg_class
    /// RowExclusiveLock, `SearchSysCacheCopy1(RELOID, relid)`, poke
    /// `relrewrite = relrewrite` on the `GETSTRUCT` copy and `CatalogTupleUpdate`.
    /// The boolean result is `HeapTupleIsValid(tuple)`; the caller raises the
    /// `cache lookup failed for relation %u` `elog(ERROR)` when it is `false`.
    pub fn set_pg_class_relrewrite(relid: Oid, relrewrite: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `ATExecSetAccessMethodNoStorage`'s pg_class write (tablecmds.c:16525):
    /// open pg_class RowExclusiveLock, `SearchSysCacheCopy1(RELOID, reloid)`,
    /// poke `relam = newam` and `CatalogTupleUpdate`. Returns `Some(old_relam)`
    /// where `old_relam` is the pre-update `relam` (so the caller can fix up the
    /// pg_am dependency); `None` ⇒ `!HeapTupleIsValid(tuple)`, caller raises
    /// `cache lookup failed for relation %u`. When `old_relam == newam` no write
    /// is performed (the no-op early-out), but `Some(old_relam)` is still
    /// returned.
    pub fn set_pg_class_relam(relid: Oid, newam: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `ATExecDropOf`'s pg_class write (tablecmds.c:18383): open pg_class
    /// RowExclusiveLock, `SearchSysCacheCopy1(RELOID, relid)`, poke `reloftype
    /// = reloftype` and `CatalogTupleUpdate`. The boolean result is
    /// `HeapTupleIsValid(tuple)`; the caller raises `cache lookup failed for
    /// relation %u` when it is `false`.
    pub fn set_pg_class_reloftype(relid: Oid, reloftype: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `relation_mark_replica_identity`'s per-index pg_index leg
    /// (tablecmds.c:18435-18481): open pg_index RowExclusiveLock,
    /// `SearchSysCacheCopy1(INDEXRELID, index_oid)`, and — only if
    /// `indisreplident != want` — poke `indisreplident = want` on the `GETSTRUCT`
    /// copy and `CatalogTupleUpdate`. Returns `(found, dirty)`: `found` is
    /// `HeapTupleIsValid(tuple)` (the caller raises `cache lookup failed for index
    /// %u` when `false`), and `dirty` is whether the flag actually changed (the
    /// caller owns the per-dirty-index `CacheInvalidateRelcache(rel)`).
    pub fn set_index_isreplident(index_oid: Oid, want: bool) -> PgResult<(bool, bool)>
);

seam_core::seam!(
    /// `systable_inplace_update_begin(class_rel, ClassOidIndexId, true, NULL,
    /// key[oid = rel_oid], &reltup, &state)`; if the tuple is found, write
    /// `((Form_pg_class) GETSTRUCT(reltup))->reltoastrelid = toast_relid` and
    /// `systable_inplace_update_finish(state, reltup)` (toasting.c
    /// `create_toast_table`, bootstrap path, where UPDATE is not possible). The
    /// genuine unported callees only: the inplace-update begin/finish and the
    /// `Form_pg_class` GETSTRUCT field write. The returned `bool` is
    /// `HeapTupleIsValid(reltup)` — the caller raises the
    /// `cache lookup failed for relation %u` `elog(ERROR)` when it is `false`,
    /// and the GETSTRUCT write / `systable_inplace_update_finish` run only when
    /// it is `true`, matching the C control flow. The open pg_class relation
    /// (opened RowExclusiveLock by the caller) crosses by reference. `Err`
    /// carries the heap-mutation `ereport(ERROR)`s.
    pub fn set_pg_class_reltoastrelid_inplace(
        class_rel: &RelationData<'_>,
        rel_oid: Oid,
        toast_relid: Oid,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `rel = table_open(SequenceRelationId, RowExclusiveLock)`;
    /// `heap_form_tuple(RelationGetDescr(rel), pgs_values, pgs_nulls)`;
    /// `CatalogTupleInsert(rel, tuple)`; `heap_freetuple`;
    /// `table_close(rel, RowExclusiveLock)` (sequence.c `DefineSequence`
    /// pg_sequence fill). The whole open/form/insert/close cycle is in the
    /// owner; the new row crosses as the deformed `FormData_pg_sequence`. `Err`
    /// carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_insert_pg_sequence(form: &FormData_pg_sequence) -> PgResult<()>
);

seam_core::seam!(
    /// `rel = table_open(SequenceRelationId, RowExclusiveLock)`;
    /// `seqtuple = SearchSysCacheCopy1(SEQRELID, seqrelid)`; overwrite the
    /// `Form_pg_sequence` fields from `form`; `CatalogTupleUpdate(rel,
    /// &seqtuple->t_self, seqtuple)`; `InvokeObjectPostAlterHook(
    /// RelationRelationId, seqrelid, 0)`; `table_close(rel, RowExclusiveLock)`
    /// (sequence.c `AlterSequence`). Keyed by `form.seqrelid`. The returned
    /// `bool` is `HeapTupleIsValid(seqtuple)` — the caller raises
    /// `cache lookup failed for sequence %u` when `false`. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_update_pg_sequence(form: &FormData_pg_sequence) -> PgResult<bool>
);

seam_core::seam!(
    /// `rel = table_open(SequenceRelationId, RowExclusiveLock)`;
    /// `tuple = SearchSysCache1(SEQRELID, relid)`; `CatalogTupleDelete(rel,
    /// &tuple->t_self)`; `ReleaseSysCache(tuple)`; `table_close(rel,
    /// RowExclusiveLock)` (sequence.c `DeleteSequenceTuple`). The returned
    /// `bool` is `HeapTupleIsValid(tuple)` — the caller raises
    /// `cache lookup failed for sequence %u` when `false`. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_delete_pg_sequence(relid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `CatalogTupleInsert(rel, tup)` (catalog/indexing.c) for a pg_shdepend
    /// row: `simple_heap_insert` plus `CatalogIndexInsert` index maintenance.
    /// The new tuple crosses as its deformed form; the owner forms the heap
    /// tuple against the pg_shdepend descriptor. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_shdepend(
        rel: &RelationData<'_>,
        form: &FormData_pg_shdepend,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogTupleUpdate(rel, tid, tup)` (catalog/indexing.c) for a
    /// pg_shdepend row: `simple_heap_update` plus `CatalogIndexInsert` index
    /// maintenance. The replacement tuple crosses as its deformed form. `Err`
    /// carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_shdepend(
        rel: &RelationData<'_>,
        tid: ItemPointerData,
        form: &FormData_pg_shdepend,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogOpenIndexes(rel)` + `CatalogTuplesMultiInsertWithInfo(rel,
    /// slot, ntuples, indstate)` + `CatalogCloseIndexes(indstate)`
    /// (catalog/indexing.c) for one batch of pg_shdepend rows. The caller
    /// keeps the C batching (`MAX_CATALOG_MULTI_INSERT_BYTES`); the index-state
    /// lifecycle is per-batch here, which is logic-invisible. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuples_multi_insert_pg_shdepend<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        forms: &[FormData_pg_shdepend],
    ) -> PgResult<()>
);

/* ---- CLUSTER pg_class / pg_index row updates (backend-commands-cluster) --- */

seam_core::seam!(
    /// `CatalogTupleUpdate(pg_class_rel, &tup->t_self, tup)` after reforming
    /// the mutated `PgClassForm` (indexing.c).
    pub fn catalog_tuple_update_pg_class<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'_>,
        tid: ItemPointerData,
        form: &types_cluster::PgClassForm,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ATExecSetRelOptions`'s pg_class write (tablecmds.c:16758-16772): update
    /// only the variable `pg_class.reloptions` (`text[]`) column of `relid`.
    /// `new_reloptions` is the constructed `text[]` varlena image
    /// (`transformRelOptions`), or `None` for the C `(Datum) 0` (store SQL NULL).
    /// `table_open(RelationRelationId)` → fetch by oid → deform → set the
    /// reloptions column (or NULL) with `repl_repl = true` → `heap_modify_tuple`
    /// → `CatalogTupleUpdate`; all other columns ride from the old tuple. `Err`
    /// carries "cache lookup failed for relation %u" + heap/index-mutation
    /// `ereport(ERROR)`s.
    pub fn update_pg_class_reloptions<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: Oid,
        new_reloptions: Option<&[u8]>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `CatalogTupleUpdate(pg_index_rel, &tup->t_self, tup)` after reforming
    /// the mutated `PgIndexForm` (indexing.c).
    pub fn catalog_tuple_update_pg_index<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'_>,
        tid: ItemPointerData,
        form: &types_cluster::PgIndexForm,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `CatalogOpenIndexes(rel)` (indexing.c). Returns the real owned
    /// [`types_cluster::CatalogIndexState`] tied to the caller's `mcx`; the
    /// cluster / large-object consumers hold it live across their
    /// `*_with_info_*` calls and pass it to [`catalog_close_indexes`].
    pub fn catalog_open_indexes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
    ) -> PgResult<types_cluster::CatalogIndexState<'mcx>>
);
seam_core::seam!(
    /// `CatalogTupleUpdateWithInfo(rel, &tup->t_self, tup, indstate)`.
    pub fn catalog_tuple_update_with_info_pg_class<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        tid: ItemPointerData,
        form: &types_cluster::PgClassForm,
        indstate: &mut types_cluster::CatalogIndexState<'mcx>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `CatalogCloseIndexes(indstate)` (indexing.c).
    pub fn catalog_close_indexes<'mcx>(
        indstate: types_cluster::CatalogIndexState<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AppendAttributeTuples(indexRelation, attopts, stattargets)`
    /// (catalog/index.c): `table_open(AttributeRelationId, RowExclusiveLock)`,
    /// `CatalogOpenIndexes`, `InsertPgAttributeTuples(pg_attribute,
    /// RelationGetDescr(indexRelation), InvalidOid, attrs_extra, indstate)`,
    /// `CatalogCloseIndexes`, `table_close`. The per-attribute `attoptions`
    /// (`attopts[i]` as a reloptions text array) and `attstattarget`
    /// (`stattargets[i]`, `None` ⇒ SQL NULL) overrides ride in optional parallel
    /// arrays indexed by attno-1; `None` for the whole array == the C NULL
    /// `attopts` / `stattargets` (the only shape the `index_create` call site
    /// here passes). Delegated to the catalog-indexing / heap layer (which owns
    /// `InsertPgAttributeTuples` and the index's stored `RelationGetDescr`); the
    /// owner installs it from `init_seams()`. `Err` carries the heap/index
    /// mutation `ereport(ERROR)`s.
    pub fn append_attribute_tuples<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index_relation: &::rel::Relation<'mcx>,
        attopts: Option<&[Option<std::vec::Vec<u8>>]>,
        stattargets: Option<&[Option<i16>]>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `GetNewOidWithIndex(rel, OpfamilyOidIndexId, Anum_pg_opfamily_oid)` +
    /// `heap_form_tuple` + `CatalogTupleInsert` for one pg_opfamily row
    /// (opclasscmds.c `CreateOpFamily`): assign the row OID, form the tuple
    /// against the descriptor, insert it (with index maintenance), and return
    /// the assigned OID. `Err` carries the heap/index-mutation
    /// `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_opfamily(
        rel: &RelationData<'_>,
        form: &FormData_pg_opfamily,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetNewOidWithIndex(rel, OpclassOidIndexId, Anum_pg_opclass_oid)` +
    /// `heap_form_tuple` + `CatalogTupleInsert` for one pg_opclass row
    /// (opclasscmds.c `DefineOpClass`): returns the assigned OID. `Err`
    /// carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_opclass(
        rel: &RelationData<'_>,
        form: &FormData_pg_opclass,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetNewOidWithIndex(rel, AccessMethodOperatorOidIndexId,
    /// Anum_pg_amop_oid)` + `heap_form_tuple` + `CatalogTupleInsert` for one
    /// pg_amop row (opclasscmds.c `storeOperators`): returns the assigned OID
    /// (the `entryoid` the caller records dependencies against). `Err` carries
    /// the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_amop(
        rel: &RelationData<'_>,
        form: &FormData_pg_amop,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetNewOidWithIndex(rel, AccessMethodProcedureOidIndexId,
    /// Anum_pg_amproc_oid)` + `heap_form_tuple` + `CatalogTupleInsert` for one
    /// pg_amproc row (opclasscmds.c `storeProcedures`): returns the assigned
    /// OID. `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_amproc(
        rel: &RelationData<'_>,
        form: &FormData_pg_amproc,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetNewOidWithIndex(rel, NamespaceOidIndexId, Anum_pg_namespace_oid)` +
    /// `namestrcpy` + `heap_form_tuple` + `CatalogTupleInsert` for one
    /// pg_namespace row (pg_namespace.c `NamespaceCreate`): assigns and returns
    /// the new namespace OID, building the row from the schema name, owner, and
    /// optional default ACL (`acl == None` ⇒ `nulls[Anum_pg_namespace_nspacl
    /// - 1] = true`). The `acl` varlena (`Acl *`) crosses as its full on-disk
    /// `aclitem[]` image. `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_namespace(
        rel: &RelationData<'_>,
        nspname: &str,
        nspowner: Oid,
        nspacl: Option<&[u8]>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RenameSchema`'s tuple write (schemacmds.c): `table_open(NamespaceRelationId,
    /// RowExclusiveLock)` + `SearchSysCacheCopy1(NAMESPACEOID, nspoid)`,
    /// `namestrcpy(&nspform->nspname, newname)`, `CatalogTupleUpdate(rel,
    /// &tup->t_self, tup)`, `table_close(rel, NoLock)`, `heap_freetuple(tup)`.
    /// The caller has already verified `nspoid` exists and resolved the new
    /// name; the cache-miss path is unreachable here. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn rename_namespace_tuple(nspoid: Oid, newname: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterSchemaOwner_internal`'s tuple write (schemacmds.c): `table_open(
    /// NamespaceRelationId, RowExclusiveLock)` + `SearchSysCacheCopy1(
    /// NAMESPACEOID, nspoid)`, set `repl_repl[nspowner] = true` /
    /// `repl_val[nspowner] = new_owner`, and when `SysCacheGetAttr(nspacl)` is
    /// non-NULL `aclnewowner(nspacl, old_owner, new_owner)` into
    /// `repl_val[nspacl]`, then `heap_modify_tuple` + `CatalogTupleUpdate` +
    /// `heap_freetuple` + `table_close(rel, RowExclusiveLock)`. `Err` carries
    /// the heap/index-mutation `ereport(ERROR)`s.
    pub fn update_namespace_owner_tuple(
        nspoid: Oid,
        old_owner: Oid,
        new_owner: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterObjectOwner_internal`'s modified-tuple write (alter.c:1013-1046) for
    /// an arbitrary simple catalog. On the already-open `rel` (catalog OID
    /// `catalogId`, RowExclusiveLock) re-fetch the locked row for `object_id`
    /// via `get_catalog_object_by_oid_extended(rel, anum_oid, object_id, true)`,
    /// set `repl_repl[anum_owner] = true` / `repl_val[anum_owner] = new_owner`,
    /// and when `anum_acl != InvalidAttrNumber` and `SysCacheGetAttr(anum_acl)`
    /// is non-NULL, write `aclnewowner(acl, old_owner, new_owner)` into
    /// `repl_val[anum_acl]`; then `heap_modify_tuple` + `CatalogTupleUpdate(rel,
    /// &newtup->t_self, newtup)` + `UnlockTuple(rel, &oldtup->t_self,
    /// InplaceUpdateTupleLock)`. The generic `aclitem[]` varlena re-serialization
    /// into the modified tuple is the unported primitive this seam encapsulates
    /// (mirroring the per-catalog typed owner-tuple writers like
    /// `update_namespace_owner_tuple`). `Err` carries the heap/index-mutation
    /// `ereport(ERROR)`s.
    pub fn update_object_owner_tuple(
        rel: &RelationData<'_>,
        anum_oid: i16,
        object_id: Oid,
        anum_owner: i16,
        anum_acl: i16,
        old_owner: Oid,
        new_owner: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_form_tuple` + `CatalogTupleInsert` + `heap_freetuple` for one
    /// pg_largeobject_metadata row (pg_largeobject.c `LargeObjectCreate`): form
    /// the metadata tuple from the already-chosen large-object OID, owner, and
    /// optional default ACL, then insert it with index maintenance. Unlike the
    /// pg_namespace/pg_am inserts, the OID is assigned by the caller (the C
    /// `OidIsValid(loid) ? loid : GetNewOidWithIndex(...)` branch runs in
    /// `LargeObjectCreate` itself), so this seam does no OID allocation; it just
    /// builds `values[]` (`oid = loid`, `lomowner = lomowner`) and, when
    /// `lomacl == None`, sets `nulls[Anum_pg_largeobject_metadata_lomacl - 1] =
    /// true`. The `lomacl` varlena (`Acl *`) crosses as its full on-disk
    /// `aclitem[]` image. `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_largeobject_metadata(
        rel: &RelationData<'_>,
        loid: Oid,
        lomowner: Oid,
        lomacl: Option<&[u8]>,
    ) -> PgResult<()>
);

/// One `pg_largeobject` data-page row, deformed and detoasted by
/// [`deform_lo_page`]. Mirrors inv_api.c's `Form_pg_largeobject` access plus the
/// file-static `getdatafield` (detoast + `VARSIZE` length-sanity raising
/// `ERRCODE_DATA_CORRUPTED`): the owner does the `HeapTupleHasNulls` paranoia,
/// the `GETSTRUCT` deform of `loid`/`pageno`, and the `data` bytea detoast,
/// surfacing the `LOBLKSIZE`-bounded page bytes plus the tuple's `t_self` TID
/// (for `CatalogTupleUpdateWithInfo` / `CatalogTupleDelete`).
#[derive(Debug, Clone)]
pub struct LoPageRow {
    /// `data->pageno` — the 0-based page number within the large object.
    pub pageno: i32,
    /// `getdatafield(data, ...)` — the detoasted page payload (`VARDATA`,
    /// `VARSIZE - VARHDRSZ` bytes, `0..=LOBLKSIZE`).
    pub data: Vec<u8>,
    /// `tuple->t_self` — the page tuple's item pointer.
    pub tid: ItemPointerData,
}

seam_core::seam!(
    /// inv_api.c's per-page deform of a scanned `pg_largeobject` tuple: the
    /// `HeapTupleHasNulls` "null field found in pg_largeobject" paranoia
    /// (`elog(ERROR)`), the `GETSTRUCT(Form_pg_largeobject)` access of
    /// `pageno`, and the file-static `getdatafield` (detoast of the `data`
    /// bytea + the `VARSIZE - VARHDRSZ` length-sanity check raising
    /// `ERRCODE_DATA_CORRUPTED` for a size outside `0..=LOBLKSIZE`). The owner
    /// has the relation's descriptor; the tuple crosses as the scanned
    /// [`FormedTuple`]. `Err` carries the deform/detoast/corruption error
    /// surface.
    pub fn deform_lo_page(
        rel: &RelationData<'_>,
        tuple: &::types_tuple::heaptuple::FormedTuple<'_>,
    ) -> PgResult<LoPageRow>
);

seam_core::seam!(
    /// inv_api.c's brand-new `pg_largeobject` page insert: build
    /// `values[]`/`nulls[]` (`loid`, `pageno`, and the `data` bytea framed
    /// `SET_VARSIZE(len + VARHDRSZ)`), `heap_form_tuple(lo_heap_r->rd_att, ...)`,
    /// `CatalogTupleInsertWithInfo(lo_heap_r, newtup, indstate)`, then
    /// `heap_freetuple`. The page payload crosses as the owned `data` slice (the
    /// `workbuf` scratch page, already trimmed to the valid length). `Err`
    /// carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_with_info_pg_largeobject<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &RelationData<'mcx>,
        loid: Oid,
        pageno: i32,
        data: &[u8],
        indstate: &mut types_cluster::CatalogIndexState<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// inv_api.c's existing-`pg_largeobject`-page update: build
    /// `values[]`/`replace[]` for the `data` column only (framed
    /// `SET_VARSIZE(len + VARHDRSZ)`), `heap_modify_tuple` against the old page
    /// tuple (addressed by `tid`), `CatalogTupleUpdateWithInfo(lo_heap_r,
    /// &newtup->t_self, newtup, indstate)`, then `heap_freetuple`. The owner
    /// re-reads the old tuple at `tid` to supply `heap_modify_tuple`'s base
    /// (the caller already holds the row from the scan; only the page payload
    /// changes). `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_with_info_pg_largeobject<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &RelationData<'mcx>,
        tid: ItemPointerData,
        data: &[u8],
        indstate: &mut types_cluster::CatalogIndexState<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `GetNewOidWithIndex(rel, AmOidIndexId, Anum_pg_am_oid)` +
    /// `namein(amname)` + `heap_form_tuple` + `CatalogTupleInsert` for one
    /// pg_am row (amcmds.c `CreateAccessMethod`): assign the row OID, form the
    /// tuple (`values[]` = oid / `namein(amname)` / amhandler / amtype) against
    /// the descriptor, insert it with index maintenance, and return the
    /// assigned OID. pg_am has no shared `FormData_pg_am` mirror in the type
    /// crates, and the C fills `values[]` inline rather than from a struct, so
    /// the row's four columns cross as scalars. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_am(
        rel: &RelationData<'_>,
        amname: &str,
        amhandler: Oid,
        amtype: u8,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `CreateConstraintEntry`'s tuple build + insert: `GetNewOidWithIndex(rel,
    /// ConstraintOidIndexId, Anum_pg_constraint_oid)` + `namestrcpy` +
    /// `construct_array_builtin` of the array columns + `CStringGetTextDatum`
    /// of `conbin` + `heap_form_tuple(RelationGetDescr(rel), values, nulls)` +
    /// `CatalogTupleInsert(rel, tup)` (catalog/indexing.c + heap/array). Returns
    /// the freshly-allocated constraint OID. `Err` carries the heap/index
    /// mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_constraint(
        rel: &RelationData<'_>,
        row: &::types_catalog::pg_constraint::PgConstraintInsertRow,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `CatalogTupleUpdate(rel, &tup->t_self, tup)` for the in-place
    /// `pg_constraint` mutators: the owner reads the existing tuple addressed by
    /// `tid`, overwrites the `ConstraintFieldUpdate` columns (the ones the
    /// AdjustNotNullInheritance / RenameConstraintById / AlterConstraintNamespaces
    /// / ConstraintSetParentConstraint paths scribble on the copied tuple),
    /// re-forms, and stores it (catalog/indexing.c). `Err` carries the
    /// heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_constraint(
        rel: &RelationData<'_>,
        tid: ItemPointerData,
        fields: &::types_catalog::pg_constraint::ConstraintFieldUpdate,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogTupleUpdate(rel, &tup->t_self, tup)` for `renametrig_internal`
    /// (commands/trigger.c): the owner reads the existing `pg_trigger` tuple
    /// addressed by `tid`, overwrites the `TriggerFieldUpdate` columns (just
    /// `tgname`, which the rename path scribbles via `namestrcpy`), re-forms,
    /// and stores it. `Err` carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_trigger(
        rel: &RelationData<'_>,
        tid: ItemPointerData,
        fields: &::types_catalog::pg_trigger::TriggerFieldUpdate,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CastCreate`'s tuple build + insert: `GetNewOidWithIndex(rel,
    /// CastOidIndexId, Anum_pg_cast_oid)` + `heap_form_tuple(RelationGetDescr(
    /// rel), values, nulls)` + `CatalogTupleInsert(rel, tup)` (catalog/
    /// indexing.c + heapam). Returns the freshly-allocated cast OID. `Err`
    /// carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_cast<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_cast::PgCastInsertRow,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `CreateTransform`'s tuple build + insert/update of `pg_transform`. When
    /// `replace_oid` is `InvalidOid`, a new row is allocated (`GetNewOidWithIndex(
    /// rel, TransformOidIndexId, Anum_pg_transform_oid)` + `heap_form_tuple` +
    /// `CatalogTupleInsert`) and its OID returned. When `replace_oid` is valid,
    /// the existing row (found by the caller via `TRFTYPELANG`) is updated in
    /// place (`heap_modify_tuple` of `trffromsql`/`trftosql` + `CatalogTupleUpdate`
    /// at `tid`) and `replace_oid` is returned unchanged. `Err` carries the
    /// heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_transform<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_transform::PgTransformInsertRow,
        replace_oid: Oid,
        replace_tid: ItemPointerData,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `StoreSingleInheritance`'s tuple build + insert: `heap_form_tuple(
    /// RelationGetDescr(rel), values, nulls)` + `CatalogTupleInsert(rel, tup)`
    /// (catalog/indexing.c + heapam). `pg_inherits` has no OID column, so
    /// nothing is returned. `Err` carries the heap/index mutation
    /// `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_inherits<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_inherits::PgInheritsInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `MarkInheritDetached`'s in-place flip of `inhdetachpending`: the C
    /// `heap_copytuple(inheritsTuple)` + `GETSTRUCT(newtup)->inhdetachpending =
    /// true` + `CatalogTupleUpdate(rel, &inheritsTuple->t_self, newtup)`
    /// (commands/tablecmds.c + catalog/indexing.c + heapam). The replacement
    /// crosses as the full deformed row carrier (every pg_inherits column is
    /// fixed-width NOT NULL, so re-forming the whole row from the scanned values
    /// with the one changed column is bit-identical), applied at the scanned
    /// tuple's `tid`. `Err` carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_inherits<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        tid: ItemPointerData,
        row: &::types_catalog::pg_inherits::PgInheritsUpdateRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ConversionCreate`'s tuple build + insert: `GetNewOidWithIndex(rel,
    /// ConversionOidIndexId, Anum_pg_conversion_oid)` + `namestrcpy` +
    /// `heap_form_tuple(RelationGetDescr(rel), values, nulls)` +
    /// `CatalogTupleInsert(rel, tup)` (catalog/indexing.c + heapam). Returns the
    /// freshly-allocated conversion OID. `Err` carries the heap/index mutation
    /// `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_conversion<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_conversion::PgConversionInsertRow,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RangeCreate`'s tuple build + insert: `heap_form_tuple(RelationGetDescr(
    /// rel), values, nulls)` + `CatalogTupleInsert(rel, tup)` (catalog/
    /// indexing.c + heapam). `pg_range` has no OID column, so nothing is
    /// returned. `Err` carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_range<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_range::PgRangeInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AddEnumLabel`'s single-row tuple build + insert: `heap_form_tuple(
    /// RelationGetDescr(rel), values, nulls)` + `CatalogTupleInsert(rel, tup)`
    /// (catalog/indexing.c + heapam). The caller has already allocated `row.oid`
    /// (the even/odd OID-selection logic lives in the port). `Err` carries the
    /// heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_enum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_enum::PgEnumInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CreateProceduralLanguage`'s create branch: `langoid = GetNewOidWithIndex(
    /// rel, LanguageOidIndexId, Anum_pg_language_oid)` + `heap_form_tuple(
    /// RelationGetDescr(rel), values, nulls)` + `CatalogTupleInsert(rel, tup)`
    /// (proclang.c:159-163 + catalog/indexing.c + heapam). Returns the freshly
    /// allocated `pg_language` OID. `lanacl` is always inserted NULL. `Err`
    /// carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_language<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_language::PgLanguageInsertRow,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `CreateProceduralLanguage`'s replace branch: `tup = heap_modify_tuple(
    /// oldtup, RelationGetDescr(rel), values, nulls, replaces)` +
    /// `CatalogTupleUpdate(rel, &tup->t_self, tup)` (proclang.c:149-150 +
    /// catalog/indexing.c + heapam). The `replaces[]` mask leaves
    /// `oid`/`lanowner`/`lanacl` unchanged (taken from `oldtup`), so the existing
    /// OID, ownership and ACL are preserved exactly as C. `oldtup` supplies both
    /// the unchanged columns and the `t_self` update target. `Err` carries the
    /// heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_language<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        oldtup: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        row: &::types_catalog::pg_language::PgLanguageInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `GetNewOidWithIndex(pg_type, TypeOidIndexId, Anum_pg_type_oid)`
    /// (catalog/catalog.c): allocate a fresh `pg_type` row OID that does not
    /// collide with any existing row, verified against `TypeOidIndexId`.
    /// Returned to the `pg_type.c` port so its forced-OID / binary-upgrade /
    /// new-OID branch (`TypeShellMake` / `TypeCreate`) keeps the OID-assignment
    /// decision in the owner. `Err` carries the index-probe error surface.
    pub fn get_new_oid_with_index_pg_type<'mcx>(rel: &::rel::Relation<'mcx>) -> PgResult<Oid>
);

seam_core::seam!(
    /// `TypeShellMake` / `TypeCreate`'s new-row path: `heap_form_tuple(
    /// RelationGetDescr(rel), values, nulls)` + `CatalogTupleInsert(rel, tup)` +
    /// `heap_freetuple` for one `pg_type` row (catalog/indexing.c + heapam). The
    /// row's fixed columns plus the three `CATALOG_VARLEN` columns
    /// (`typdefaultbin` `pg_node_tree` text, `typdefault` text, `typacl`
    /// `aclitem[]`) cross as the deformed [`PgTypeInsertRow`]; `None` for a
    /// varlen column is `nulls[Anum_pg_type_* - 1] = true`. The owner has
    /// already assigned `row.fields.oid` (forced / binary-upgrade / new). `Err`
    /// carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_type(
        rel: &RelationData<'_>,
        row: &::types_catalog::pg_type::PgTypeInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `TypeCreate`'s shell-replacement path: locate the existing shell row by
    /// `oid` (the caller read it off `SearchSysCacheCopy2(TYPENAMENSP)`),
    /// `heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces)`
    /// with every column replaced *except* `oid` (`replaces[Anum_pg_type_oid -
    /// 1] = false`), then `CatalogTupleUpdate(rel, &tup->t_self, tup)`
    /// (catalog/indexing.c + heapam). The replacement columns cross as the
    /// deformed [`PgTypeInsertRow`]; `row.fields.oid` identifies the row to
    /// rewrite. `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_type(
        rel: &RelationData<'_>,
        row: &::types_catalog::pg_type::PgTypeInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RenameTypeInternal`'s rename path: `SearchSysCacheCopy1(TYPEOID,
    /// type_oid)` (the owner re-fetches the held tuple), `namestrcpy(&typ->
    /// typname, new_type_name)`, then `CatalogTupleUpdate(rel, &tuple->t_self,
    /// tuple)` (catalog/indexing.c + heapam). Only the `typname` column changes.
    /// `Err` carries the heap/index-mutation `ereport(ERROR)`s (including the
    /// `cache lookup failed for type %u` `elog(ERROR)` when the row is gone,
    /// which the C raises before reaching this point — here a missing row is an
    /// internal error).
    pub fn catalog_tuple_update_typname_pg_type(
        rel: &RelationData<'_>,
        type_oid: Oid,
        new_type_name: &str,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `GetNewOidWithIndex(pg_enum, EnumOidIndexId, Anum_pg_enum_oid)`
    /// (catalog/catalog.c): allocate a fresh OID that does not collide with any
    /// existing `pg_enum` row, verified against `EnumOidIndexId`. Returned to
    /// the port so its even/odd-OID sort-order selection logic (`EnumValuesCreate`
    /// / `AddEnumLabel`) can inspect the candidate before forming the tuple.
    /// `Err` carries the index-probe error surface.
    pub fn get_new_oid_with_index_pg_enum<'mcx>(rel: &::rel::Relation<'mcx>) -> PgResult<Oid>
);

seam_core::seam!(
    /// `EnumValuesCreate`'s batched insert: `CatalogOpenIndexes(rel)` +
    /// `MakeSingleTupleTableSlot`/`ExecStoreVirtualTuple` +
    /// `CatalogTuplesMultiInsertWithInfo(rel, slots, n, indstate)` +
    /// `CatalogCloseIndexes` (catalog/indexing.c + executor tuple slots). Each
    /// caller-supplied row's `oid` is pre-allocated (the even-OID +
    /// wraparound-sort logic lives in the port). `Err` carries the heap/index
    /// mutation `ereport(ERROR)`s.
    pub fn catalog_tuples_multi_insert_pg_enum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        rows: &[::types_catalog::pg_enum::PgEnumInsertRow],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogTupleUpdate(pg_enum, &tup->t_self, tup)` for the in-place
    /// `pg_enum` mutators (`RenameEnumLabel` rewrites `enumlabel`;
    /// `RenumberEnumType` rewrites `enumsortorder`): the owner re-forms the
    /// tuple addressed by `tid` from the supplied row and stores it (catalog/
    /// indexing.c). `Err` carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_enum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        tid: ItemPointerData,
        row: &::types_catalog::pg_enum::PgEnumInsertRow,
    ) -> PgResult<()>
);

/* ---- pg_foreign_* catalog DML (commands/foreigncmds.c) -------------------- */

seam_core::seam!(
    /// `GetNewOidWithIndex(rel, ForeignDataWrapperOidIndexId,
    /// Anum_pg_foreign_data_wrapper_oid)` is performed by the caller; this seam
    /// does the `heap_form_tuple(rel->rd_att, values, nulls)` +
    /// `CatalogTupleInsert(rel, tuple)` + `heap_freetuple` for one
    /// `pg_foreign_data_wrapper` row (`CreateForeignDataWrapper`). The row's
    /// columns cross as the deformed [`PgForeignDataWrapperInsertRow`]
    /// (`fdwacl` is always SQL NULL on create; `options = None` ⇒ `fdwoptions`
    /// SQL NULL). `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_foreign_data_wrapper(
        rel: &RelationData<'_>,
        row: &types_foreigncmds::PgForeignDataWrapperInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterForeignDataWrapper`'s tuple update: `SearchSysCacheCopy1(
    /// FOREIGNDATAWRAPPEROID, fdwid)`, build `repl_val`/`repl_null`/`repl_repl`
    /// for the `Some` columns (`fdwhandler`/`fdwvalidator`/`fdwoptions`),
    /// `heap_modify_tuple` + `CatalogTupleUpdate(rel, &tup->t_self, tup)`. The
    /// owner locates the on-disk tuple from `fdwid`. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_foreign_data_wrapper(
        rel: &RelationData<'_>,
        fdwid: Oid,
        row: &types_foreigncmds::PgForeignDataWrapperUpdateRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterForeignDataWrapperOwner_internal`'s tuple update: `SearchSysCacheCopy1(
    /// FOREIGNDATAWRAPPEROID, fdwid)`, set `fdwowner = new_owner` and
    /// `aclnewowner(fdwacl, old, new)` when the ACL is non-NULL, then
    /// `heap_modify_tuple` + `CatalogTupleUpdate`. The ACL rewrite + the
    /// `heap_getattr(fdwacl)` read live in the owner. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_owner_pg_foreign_data_wrapper(
        rel: &RelationData<'_>,
        fdwid: Oid,
        old_owner: Oid,
        new_owner: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_form_tuple` + `CatalogTupleInsert` + `heap_freetuple` for one
    /// `pg_foreign_server` row (`CreateForeignServer`); the OID is caller-
    /// assigned (`GetNewOidWithIndex(rel, ForeignServerOidIndexId, ...)`).
    /// `srvacl` is always SQL NULL on create; `srvtype`/`srvversion` and the
    /// `srvoptions` honor their `None` ⇒ SQL NULL. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_foreign_server(
        rel: &RelationData<'_>,
        row: &types_foreigncmds::PgForeignServerInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterForeignServer`'s tuple update: `SearchSysCacheCopy1(
    /// FOREIGNSERVEROID, serverid)`, build `repl_*` for the `Some` columns
    /// (`srvversion`/`srvoptions`), `heap_modify_tuple` + `CatalogTupleUpdate`.
    /// `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_foreign_server(
        rel: &RelationData<'_>,
        serverid: Oid,
        row: &types_foreigncmds::PgForeignServerUpdateRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterForeignServerOwner_internal`'s tuple update: `SearchSysCacheCopy1(
    /// FOREIGNSERVEROID, serverid)`, set `srvowner = new_owner` and
    /// `aclnewowner(srvacl, old, new)` when the ACL is non-NULL, then
    /// `heap_modify_tuple` + `CatalogTupleUpdate`. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_owner_pg_foreign_server(
        rel: &RelationData<'_>,
        serverid: Oid,
        old_owner: Oid,
        new_owner: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_form_tuple` + `CatalogTupleInsert` + `heap_freetuple` for one
    /// `pg_user_mapping` row (`CreateUserMapping`); the OID is caller-assigned
    /// (`GetNewOidWithIndex(rel, UserMappingOidIndexId, ...)`). `options = None`
    /// ⇒ `umoptions` SQL NULL. `Err` carries the heap/index-mutation
    /// `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_user_mapping(
        rel: &RelationData<'_>,
        row: &types_foreigncmds::PgUserMappingInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterUserMapping`'s tuple update: `SearchSysCacheCopy1(
    /// USERMAPPINGOID, umid)`, build `repl_*` for `umoptions`,
    /// `heap_modify_tuple` + `CatalogTupleUpdate`. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_user_mapping(
        rel: &RelationData<'_>,
        umid: Oid,
        row: &types_foreigncmds::PgUserMappingUpdateRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_form_tuple` + `CatalogTupleInsert` + `heap_freetuple` for one
    /// `pg_foreign_table` row (`CreateForeignTable`). `pg_foreign_table` has no
    /// OID column (`ftrelid` is the key), so no OID is allocated/returned.
    /// `options = None` ⇒ `ftoptions` SQL NULL. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_foreign_table(
        rel: &RelationData<'_>,
        row: &types_foreigncmds::PgForeignTableInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_modify_tuple` + `CatalogTupleUpdate` for one `pg_foreign_table`
    /// row keyed by `ftrelid` (`ATExecGenericOptions`). `row.options` is the
    /// outer-"replace?"/inner-"NULL?" idiom: `Some(Some(pairs))` writes the
    /// `ftoptions` array, `Some(None)` writes SQL NULL, `None` leaves it.
    pub fn catalog_tuple_update_pg_foreign_table(
        rel: &RelationData<'_>,
        ftrelid: Oid,
        row: &types_foreigncmds::PgForeignTableUpdateRow,
    ) -> PgResult<()>
);

/* ---- get_catalog_object_by_oid scan primitive (objectaddress.c) ----------- */

seam_core::seam!(
    /// `get_catalog_object_by_oid(Relation catalog, AttrNumber oidcol, Oid
    /// objectId)` scan primitive (objectaddress.c 2790): over the already-open
    /// `catalog` relation, `systable_beginscan` keyed on `oidcol = objectId`
    /// (index scan when `oidcol` is the catalog's OID column, sequential
    /// otherwise), `systable_getnext` the single matching row, `systable_endscan`,
    /// returning the located tuple copied into `mcx` (or `None` when absent).
    /// Backs `get_catalog_object_by_oid[_extended]`; declared additively here
    /// because the indexing/genam owner (a sibling decomp) has not landed yet.
    /// The `locktuple` flag mirrors the `_extended` variant
    /// (`get_catalog_object_by_oid_extended`): when `true` a `LockTuple` is taken
    /// on the located row before it is returned. `Err` carries the
    /// index/heap-fetch error surface.
    pub fn get_catalog_object_by_oid<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        catalog: &RelationData<'mcx>,
        oidcol: i16,
        object_id: Oid,
        locktuple: bool,
    ) -> PgResult<Option<::types_tuple::heaptuple::FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `pg_get_acl(classId, objectId, objsubid)`'s catalog read (objectaddress.c
    /// 4426): resolve the object's catalog (`pg_largeobject_metadata` substitutes
    /// for `pg_largeobject`), `get_object_attnum_acl(catalogId)` — return `None`
    /// when there is no ACL column — then read the `aclitem[]` column. For a
    /// relation attribute (`classId == RelationRelationId && objsubid != 0`) the
    /// ACL is `pg_attribute.attacl` fetched via `SearchSysCache2(ATTNUM,
    /// objectId, objsubid)` + `SysCacheGetAttr(attacl)`; otherwise
    /// `table_open(catalogId, AccessShareLock)` + `get_catalog_object_by_oid(rel,
    /// get_object_attnum_oid(catalogId), objectId)` + `heap_getattr(Anum_acl)`.
    /// `None` is the C `PG_RETURN_NULL` (missing object, missing ACL column, or a
    /// SQL-NULL ACL); `Some(datum)` is the raw `aclitem[]` varlena `Datum`
    /// (`PG_RETURN_DATUM`) copied into `mcx`. Owned by the indexing/catalog-form
    /// layer (it holds `table_open` / the genam scan / `heap_getattr`); the
    /// `anum_acl` / `anum_oid` resolution crosses in as parameters from the
    /// objectaddress caller (`get_object_attnum_*`). `Err` carries the
    /// heap/cache-fetch `ereport(ERROR)`s.
    pub fn get_acl_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        catalog_id: Oid,
        anum_oid: i16,
        anum_acl: i16,
        object_id: Oid,
        objsubid: i32,
        is_relation_attr: bool,
    ) -> PgResult<Option<::types_tuple::heaptuple::Datum<'mcx>>>
);

/* ---- pg_db_role_setting catalog-tuple decode / mutators ------------------- *
 *
 * pg_db_role_setting.c's `setconfig text[]` column crosses as its decoded
 * `Vec<String>` of `"name=value"` entries (the repo-wide GUC-array
 * value-model). The relation crosses as the caller's open `&RelationData` and
 * the addressed tuple as its heap TID, exactly as the pg_depend mutators above.
 * Owned by the indexing/catalog-form layer (the array deconstruct/detoast and
 * `heap_form_tuple`/`heap_modify_tuple`/`CatalogTuple*` live below this crate);
 * declared-unset, so a call panics until that owner lands. */

seam_core::seam!(
    /// `heap_getattr(tuple, Anum_pg_db_role_setting_setconfig,
    /// RelationGetDescr(rel), &isnull)` followed by
    /// `DatumGetArrayTypeP(datum)` decode of the `setconfig text[]` column
    /// (pg_db_role_setting.c). `None` is the C `isnull` (SQL NULL setconfig);
    /// `Some(v)` is the array detoasted into a `Vec<String>` of `"name=value"`
    /// entries. `Err` carries the detoast error surface.
    pub fn decode_db_role_setting_setconfig<'mcx>(
        rel: &RelationData<'mcx>,
        tuple: &::types_tuple::heaptuple::FormedTuple<'mcx>,
    ) -> PgResult<Option<Vec<String>>>
);

seam_core::seam!(
    /// `heap_modify_tuple(tuple, RelationGetDescr(rel), repl_val, repl_null,
    /// repl_repl)` replacing only `setconfig` with `new_array`, then
    /// `CatalogTupleUpdate(rel, &tuple->t_self, newtuple)`
    /// (pg_db_role_setting.c). The addressed row crosses as its heap TID; the
    /// replacement `setconfig text[]` as its `Vec<String>` form. `Err` carries
    /// the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_db_role_setting(
        rel: &RelationData<'_>,
        tid: ItemPointerData,
        new_setconfig: Vec<String>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_form_tuple(RelationGetDescr(rel), values, nulls)` with
    /// `setdatabase = databaseid`, `setrole = roleid`,
    /// `setconfig = setconfig` (the `text[]` formed from the `Vec<String>`),
    /// then `CatalogTupleInsert(rel, newtuple)` (pg_db_role_setting.c). `Err`
    /// carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_db_role_setting(
        rel: &RelationData<'_>,
        databaseid: Oid,
        roleid: Oid,
        setconfig: Vec<String>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `InsertRule`'s create branch (rewriteDefine.c:126-134): `rewriteObjectId
    /// = GetNewOidWithIndex(rel, RewriteOidIndexId, Anum_pg_rewrite_oid)` +
    /// `heap_form_tuple(rd_att, values, nulls)` + `CatalogTupleInsert(rel, tup)`
    /// + `heap_freetuple`. The `ev_qual` / `ev_action` `pg_node_tree` columns
    /// cross as their already-`nodeToString`'d text; `ev_enabled` is always
    /// inserted `RULE_FIRES_ON_ORIGIN`. Returns the freshly allocated rule OID.
    /// `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_rewrite<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        rulename: &str,
        ev_class: Oid,
        ev_type: u8,
        is_instead: bool,
        ev_qual: &str,
        ev_action: &str,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `InsertRule`'s replace branch (rewriteDefine.c:99-123): `tup =
    /// heap_modify_tuple(oldtup, rd_att, values, nulls, replaces)` with
    /// `replaces[]` true only for `ev_type` / `is_instead` / `ev_qual` /
    /// `ev_action`, then `CatalogTupleUpdate(rel, &tup->t_self, tup)`. The OID,
    /// `rulename`, `ev_class` and `ev_enabled` are taken from `oldtup`. Returns
    /// `((Form_pg_rewrite) GETSTRUCT(tup))->oid`. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_rewrite<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        oldtup: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        ev_type: u8,
        is_instead: bool,
        ev_qual: &str,
        ev_action: &str,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `EnableDisableRule`'s in-place toggle (rewriteDefine.c:731-732):
    /// `ruleform->ev_enabled = CharGetDatum(fires_when)` then
    /// `CatalogTupleUpdate(rel, &ruletup->t_self, ruletup)`. Re-forms the row
    /// from `oldtup` with only `ev_enabled` replaced and updates at
    /// `oldtup->t_self`. `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_rewrite_enabled<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        oldtup: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        ev_enabled: u8,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RenameRewriteRule`'s in-place rename (rewriteDefine.c:848-850):
    /// `namestrcpy(&(ruleform->rulename), newName)` then `CatalogTupleUpdate(
    /// rel, &ruletup->t_self, ruletup)`. Re-forms the row from `oldtup` with
    /// only `rulename` replaced and updates at `oldtup->t_self`. `Err` carries
    /// the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_rewrite_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        oldtup: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        new_name: &str,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `GetNewOidWithIndex(pg_proc, ProcedureOidIndexId, Anum_pg_proc_oid)`
    /// (catalog/catalog.c): allocate a fresh `pg_proc` row OID that does not
    /// collide with any existing row, verified against `ProcedureOidIndexId`.
    /// Returned to the `pg_proc.c` port (`ProcedureCreate`'s new-row branch) so
    /// the OID-assignment decision stays in the owner. `Err` carries the
    /// index-probe error surface.
    pub fn get_new_oid_with_index_pg_proc<'mcx>(rel: &::rel::Relation<'mcx>) -> PgResult<Oid>
);

seam_core::seam!(
    /// `ProcedureCreate`'s new-row path: `heap_form_tuple(RelationGetDescr(rel),
    /// values, nulls)` + `CatalogTupleInsert(rel, tup)` + `heap_freetuple` for
    /// one `pg_proc` row (catalog/indexing.c + heapam). The fixed columns plus
    /// the `oidvector` `proargtypes`, the `CATALOG_VARLEN` columns
    /// (`proallargtypes`/`proargmodes`/`proargnames`/`proargdefaults`/
    /// `protrftypes`/`probin`/`prosqlbody`/`proconfig`/`proacl`) cross as the
    /// deformed [`PgProcInsertRow`]; `None` for a varlen column is
    /// `nulls[Anum_pg_proc_* - 1] = true`. The owner has already assigned
    /// `row.fields.oid`. `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_proc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_proc::PgProcInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ProcedureCreate`'s replace path: `heap_modify_tuple(oldtup,
    /// RelationGetDescr(rel), values, nulls, replaces)` with every column
    /// replaced *except* `oid`/`proowner`/`proacl`
    /// (`replaces[Anum_pg_proc_{oid,proowner,proacl} - 1] = false`,
    /// pg_proc.c:580-585), then `CatalogTupleUpdate(rel, &tup->t_self, tup)`
    /// (catalog/indexing.c + heapam). The replacement columns cross as the
    /// deformed [`PgProcInsertRow`]; the held `oldtup` supplies the
    /// not-replaced columns and the `t_self` update target. `Err` carries the
    /// heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_proc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        oldtup: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        row: &::types_catalog::pg_proc::PgProcInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CreateStatistics`'s tuple build + insert (commands/statscmds.c +
    /// catalog/indexing.c): `GetNewOidWithIndex(rel, StatisticExtOidIndexId,
    /// Anum_pg_statistic_ext_oid)` + the `values[]`/`nulls[]` fill — including
    /// `buildint2vector(stxkeys)`, `construct_array_builtin(stxkind, CHAROID)`,
    /// and `CStringGetTextDatum(stxexprs)` for the variable-length columns,
    /// `stxstattarget` left NULL — + `heap_form_tuple(RelationGetDescr(rel),
    /// values, nulls)` + `CatalogTupleInsert(rel, tup)`. Returns the
    /// freshly-allocated statistics-object OID. `Err` carries the heap/index
    /// mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_statistic_ext<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_statistic_ext::PgStatisticExtInsertRow,
    ) -> PgResult<Oid>
);

/* ---- pg_type narrow single-column mutators (commands/typecmds.c F3/F4) ----
 *
 * These mirror `catalog_tuple_update_typname_pg_type`: the owner re-fetches the
 * held tuple (`SearchSysCacheCopy1(TYPEOID, type_oid)`), sets only the targeted
 * `replaces[]` columns (leaving every varlen column intact, exactly like C),
 * `heap_modify_tuple`, then `CatalogTupleUpdate(rel, &tup->t_self, tup)`. The
 * `GenerateTypeDependencies`/hook calls stay on the pg_type owner side.
 * UNINSTALLED until the catalog-indexing decomp owner lands (panic = sanctioned,
 * consistent with the sibling `catalog_tuple_{insert,update}_pg_type`).
 */

seam_core::seam!(
    /// `AlterTypeOwnerInternal`'s single-row write (typecmds.c:3985): set
    /// `typowner = new_owner_id` and, when the row's `typacl` is non-NULL,
    /// `typacl = aclnewowner(old_acl, old_owner, new_owner)`, then
    /// `CatalogTupleUpdate`. The owner reads the held tuple's old owner/ACL.
    pub fn catalog_tuple_update_typowner_typacl_pg_type(
        rel: &::rel::RelationData<'_>,
        type_oid: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterTypeNamespaceInternal`'s single-row write (typecmds.c:4233): set
    /// `typnamespace = nsp_oid` on the held tuple, then `CatalogTupleUpdate`.
    pub fn catalog_tuple_update_typnamespace_pg_type(
        rel: &::rel::RelationData<'_>,
        type_oid: Oid,
        nsp_oid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterDomainNotNull` / `AlterDomainAddConstraint` / `AlterDomainDropConstraint`
    /// single-row write (typecmds.c:2806/3014/2885): set `typnotnull = not_null`
    /// on the held tuple, then `CatalogTupleUpdate`.
    pub fn catalog_tuple_update_typnotnull_pg_type(
        rel: &::rel::RelationData<'_>,
        type_oid: Oid,
        not_null: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterDomainDefault`'s pg_type write (typecmds.c:2707): replace `typdefault`
    /// (text) and `typdefaultbin` (text) on the held tuple — `None` for either
    /// sets `nulls[Anum_pg_type_* - 1] = true` (the ALTER ... DROP DEFAULT and
    /// NULL-constant arms) — then `CatalogTupleUpdate`.
    pub fn catalog_tuple_update_typdefault_pg_type(
        rel: &::rel::RelationData<'_>,
        type_oid: Oid,
        default_value: Option<String>,
        default_bin: Option<String>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterTypeRecurse`'s per-row update (typecmds.c:4618): build `replaces[]`
    /// from the [`TypeAttrUpdate`] gates (`typstorage`/`typreceive`/`typsend`/
    /// `typmodin`/`typmodout`/`typanalyze`/`typsubscript`), `heap_modify_tuple`,
    /// `CatalogTupleUpdate`. Returns the row's `typarray` OID so the caller can
    /// recurse to the array type.
    pub fn catalog_tuple_update_attrs_pg_type(
        rel: &::rel::RelationData<'_>,
        type_oid: Oid,
        attr: ::types_catalog::pg_type::TypeAttrUpdate,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `AggregateCreate`'s fresh-row path (pg_aggregate.c:730-731 +
    /// catalog/indexing.c + heapam): `tup = heap_form_tuple(RelationGetDescr(rel),
    /// values, nulls)` + `CatalogTupleInsert(rel, tup)`. The `pg_aggregate` row has
    /// no OID column — the key column `aggfnoid` is the pre-assigned `pg_proc` OID
    /// carried in `row.form.aggfnoid`. The 20 fixed columns cross as
    /// `row.form`; the two `CATALOG_VARLEN` `text` columns (`agginitval` /
    /// `aggminitval`) are `row.agginitval` / `row.aggminitval`, each `None` ⇒
    /// `nulls[Anum_pg_aggregate_* - 1] = true`. `Err` carries the heap/index
    /// mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_aggregate<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_aggregate::PgAggregateInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AggregateCreate`'s REPLACE path (pg_aggregate.c:724-726 +
    /// catalog/indexing.c + heapam): `tup = heap_modify_tuple(oldtup,
    /// RelationGetDescr(rel), values, nulls, replaces)` + `CatalogTupleUpdate(rel,
    /// &tup->t_self, tup)`. `oldtup` is the held `SearchSysCache1(AGGFNOID, procOid)`
    /// tuple (supplying both the not-replaced columns and the `t_self` update
    /// target); the replacement columns cross as `row`; `replaces` pins
    /// `aggfnoid`/`aggkind`/`aggnumdirectargs` to the old tuple
    /// (`replaces[..] = false`), every other column replaced. `Err` carries the
    /// heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_aggregate<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        oldtup: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        row: &::types_catalog::pg_aggregate::PgAggregateInsertRow,
        replaces: ::types_catalog::pg_aggregate::PgAggregateReplaces,
    ) -> PgResult<()>
);

/* ======================================================================== *
 * DDL-cluster catalog writes (catalog/heap.c — InsertPgClassTuple,
 * InsertPgAttributeTuples, StoreAttrDefault). Consumed by the heap.c port.
 * ======================================================================== */

seam_core::seam!(
    /// `InsertPgClassTuple(pg_class_desc, new_rel_desc, new_rel_oid, relacl,
    /// reloptions)` (catalog/heap.c): build the full 34-column `pg_class` row
    /// from the new relation's `rd_rel` (carried in [`PgClassInsertRow`], which
    /// folds in `new_rel_oid` and the `relacl` / `reloptions` Datums), then
    /// `heap_form_tuple(RelationGetDescr(pg_class_desc), values, nulls)` +
    /// `CatalogTupleInsert(pg_class_desc, tup)`. `rel` is the open pg_class
    /// relation. `relpartbound` is stored NULL (set later by updating the
    /// tuple). `Err` carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_class<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_class::PgClassInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `InsertPgAttributeTuples(pg_attribute_rel, tupdesc, new_rel_oid, extra,
    /// indstate)` (catalog/heap.c): form one `pg_attribute` heap tuple per row
    /// (the fixed-layout part from each `Form_pg_attribute`, the nullable tail
    /// from `FormExtraData_pg_attribute`, carried in [`PgAttributeInsertRow`])
    /// and multi-insert the batch with index maintenance
    /// (`CatalogOpenIndexes` + `CatalogTuplesMultiInsertWithInfo` +
    /// `CatalogCloseIndexes`). The caller pre-resolves each row's `attrelid`
    /// (the C `new_rel_oid != InvalidOid` selection). `rel` is the open
    /// pg_attribute relation. `Err` carries the heap/index mutation
    /// `ereport(ERROR)`s.
    pub fn catalog_insert_pg_attribute_tuples<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        rows: &[::types_catalog::pg_attribute::PgAttributeInsertRow],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The `ALTER TABLE` per-`Anum` `pg_attribute` field-modify path (the
    /// `ATExec*` pattern, commands/tablecmds.c): `SearchSysCacheCopy(ATTNUM,
    /// relid, attnum)` → modify the `Form_pg_attribute` field(s) →
    /// `heap_modify_tuple(attr_tuple, RelationGetDescr(pg_attribute_rel),
    /// values, isnull, replaces)` → `CatalogTupleUpdate(pg_attribute_rel,
    /// &new_tuple->t_self, new_tuple)`, over the selectively-replaced columns
    /// carried in [`PgAttributeUpdateRow`]. `attr_tuple` is the original scanned
    /// pg_attribute tuple (the C `heap_modify_tuple` starts from it so the
    /// non-replaced columns are preserved); the update is applied at
    /// `attr_tuple->t_self`. `rel` is the open pg_attribute relation. This is the
    /// shared write leaf for the portable `ALTER TABLE` F2+ subcommand families
    /// (`SET`/`DROP NOT NULL`, `SET`/`DROP DEFAULT`, `SET STATISTICS`,
    /// `SET STORAGE`, `SET`/`RESET (...)` options, `ALTER COLUMN TYPE`,
    /// `DROP COLUMN`, `RENAME COLUMN`). `Err` carries the heap/index mutation
    /// `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_attribute<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        attr_tuple: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        row: &::types_catalog::pg_attribute::PgAttributeUpdateRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The `pg_class.relchecks`-preserving field-modify path (the C
    /// `RemoveConstraintById` relchecks decrement, catalog/pg_constraint.c;
    /// also `MergeConstraintsIntoExisting` / `StoreRelCheck`,
    /// commands/tablecmds.c & catalog/heap.c): `SearchSysCacheCopy1(RELOID,
    /// relid)` → `classForm->relchecks-- ` → `heap_modify_tuple(relTup,
    /// RelationGetDescr(pgrel), values, isnull, replaces)` replacing ONLY the
    /// `relchecks` column → `CatalogTupleUpdate(pgrel, &relTup->t_self,
    /// relTup)`. `class_tuple` is the original scanned pg_class tuple (the C
    /// `heap_modify_tuple` starts from it so all other 33 columns are preserved
    /// verbatim — no lossy reform of the fixed-length `Form_pg_class`); the
    /// update is applied at `class_tuple->t_self`. `rel` is the open pg_class
    /// relation. This is the relchecks-shaped analog of
    /// [`catalog_tuple_update_pg_attribute`] and the write leaf for the
    /// catalog-write relchecks-update family (tablecmds / event-trigger / DROP
    /// CHECK CONSTRAINT). `Err` carries the heap/index mutation
    /// `ereport(ERROR)`s.
    pub fn catalog_tuple_update_relchecks_pg_class<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        class_tuple: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        new_relchecks: i16,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The `pg_class.relowner`/`relacl`-preserving field-modify path
    /// (`ATExecChangeOwner`, commands/tablecmds.c): `heap_modify_tuple` replacing
    /// ONLY the `relowner` column (and the `relacl` column when `new_acl` is
    /// `Some`) over the original scanned pg_class tuple, then
    /// `CatalogTupleUpdate`. `class_tuple` is the live `SearchSysCache1(RELOID)`
    /// tuple (all other columns preserved verbatim); `new_acl` is the owner-fixed
    /// `aclitem[]` `Datum` computed via `aclnewowner`, or `None` when the existing
    /// relacl is SQL-null. The owner-shaped analog of
    /// [`catalog_tuple_update_relchecks_pg_class`]. `Err` carries the heap/index
    /// mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_relowner_pg_class<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        class_tuple: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        new_owner_id: ::types_core::Oid,
        new_acl: Option<::types_tuple::heaptuple::Datum<'mcx>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `StoreAttrDefault`'s pg_attrdef INSERT (catalog/heap.c): `attrdefOid =
    /// GetNewOidWithIndex(adrel, AttrDefaultOidIndexId, Anum_pg_attrdef_oid)` +
    /// `heap_form_tuple(RelationGetDescr(adrel), values, nulls)` +
    /// `CatalogTupleInsert(adrel, tuple)`, returning the freshly-allocated
    /// pg_attrdef OID. `rel` is the open pg_attrdef relation. `Err` carries the
    /// heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_attrdef<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_attrdef::PgAttrdefInsertRow,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `CreatePolicy`'s pg_policy INSERT (commands/policy.c): `policy_id =
    /// GetNewOidWithIndex(pg_policy_rel, PolicyOidIndexId, Anum_pg_policy_oid)` +
    /// the 8-column row build (`oid`, `polname` via `namein`, `polrelid`,
    /// `polcmd`, `polpermissive`, the `polroles` `oid[]` array via
    /// `construct_array_builtin(.., OIDOID)`, and the `nodeToString`
    /// `polqual`/`polwithcheck` `pg_node_tree` images — NULL when `None`) +
    /// `heap_form_tuple(RelationGetDescr(pg_policy_rel), values, isnull)` +
    /// `CatalogTupleInsert(pg_policy_rel, policy_tuple)`, returning the
    /// freshly-allocated pg_policy OID. `rel` is the open pg_policy relation.
    /// `Err` carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_policy<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_policy::PgPolicyInsertRow,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `CreateEventTrigger`'s pg_event_trigger INSERT (commands/event_trigger.c
    /// `insert_event_trigger_tuple`): allocate the OID via
    /// `GetNewOidWithIndex(tgrel, EventTriggerOidIndexId,
    /// Anum_pg_event_trigger_oid)`, build the 7-column row (`oid`, `evtname` /
    /// `evtevent` via `namein`, `evtowner`, `evtfoid`, `evtenabled`, and the
    /// text[]-or-NULL `evttags`), then `heap_form_tuple(tgrel->rd_att, values,
    /// nulls)` + `CatalogTupleInsert(tgrel, tuple)`, returning the
    /// freshly-allocated OID. `rel` is the open pg_event_trigger relation. `Err`
    /// carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_event_trigger<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_event_trigger::PgEventTriggerInsertRow,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `AlterEventTrigger`'s pg_event_trigger UPDATE (commands/event_trigger.c):
    /// `evtForm->evtenabled = tgenabled;` over the syscache-copied tuple
    /// (`evt_tuple`), then `heap_modify_tuple` replacing only the `evtenabled`
    /// column + `CatalogTupleUpdate(tgrel, &tup->t_self, tup)`. `rel` is the open
    /// pg_event_trigger relation. `Err` carries the heap/index mutation
    /// `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_event_trigger_enabled<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        evt_tuple: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        tgenabled: i8,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterEventTriggerOwner_internal`'s pg_event_trigger UPDATE
    /// (commands/event_trigger.c): `form->evtowner = newOwnerId;` over the
    /// syscache-copied tuple, then `heap_modify_tuple` replacing only the
    /// `evtowner` column + `CatalogTupleUpdate`. `rel` is the open
    /// pg_event_trigger relation. `Err` carries the heap/index mutation
    /// `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_event_trigger_owner<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        evt_tuple: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        new_owner_id: ::types_core::Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CreateTrigger`'s pg_trigger INSERT/UPDATE (commands/trigger.c): allocate
    /// the trigger OID (when `row.existing` is `None`) via
    /// `GetNewOidWithIndex(tgrel, TriggerOidIndexId, Anum_pg_trigger_oid)`, build
    /// the 19-column row (`oid`, `tgrelid`, `tgparentid`, `tgname` via `namein`,
    /// `tgfoid`, `tgtype`, `tgenabled`, `tgisinternal`, `tgconstrrelid`,
    /// `tgconstrindid`, `tgconstraint`, `tgdeferrable`, `tginitdeferred`,
    /// `tgnargs`, the `tgattr` `int2vector`, the `tgargs` bytea, and the
    /// text-or-NULL `tgqual`/`tgoldtable`/`tgnewtable`), then
    /// `heap_form_tuple(RelationGetDescr(tgrel), values, nulls)` +
    /// `CatalogTupleInsert(tgrel, tuple)` (fresh) or
    /// `CatalogTupleUpdate(tgrel, &otid, newtup)` (when `row.existing` carries
    /// the OID and `t_self` of the row being replaced). Returns the trigger OID.
    /// `rel` is the open pg_trigger relation. `Err` carries the heap/index
    /// mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_trigger<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_trigger::PgTriggerInsertRow,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `CreateTrigger`'s pg_class `relhastriggers` poke (commands/trigger.c):
    /// `pgrel = table_open(RelationRelationId, RowExclusiveLock)`;
    /// `tuple = SearchSysCacheCopy1(RELOID, relid)`; if `relhastriggers` is not
    /// already set, set it and `CatalogTupleUpdate` + `CommandCounterIncrement`,
    /// else `CacheInvalidateRelcacheByTuple(tuple)`; `table_close`. Returns
    /// `HeapTupleIsValid(tuple)` — the caller raises `cache lookup failed for
    /// relation %u` when `false`. `Err` carries the heap/index-mutation
    /// `ereport(ERROR)`s.
    pub fn set_pg_class_relhastriggers(relid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `AlterPolicy` / `RemoveRoleFromObjectPolicy`'s pg_policy UPDATE
    /// (commands/policy.c): `heap_modify_tuple(policy_tuple,
    /// RelationGetDescr(pg_policy_rel), values, isnull, replaces)` +
    /// `CatalogTupleUpdate(pg_policy_rel, &new_tuple->t_self, new_tuple)` over
    /// the selectively-replaced columns carried in [`PgPolicyUpdateRow`]
    /// (`polroles` `oid[]` array, `polqual`/`polwithcheck` `pg_node_tree`
    /// text-or-NULL). The addressed tuple is located by `(polrelid, polname)`
    /// (`PolicyPolrelidPolnameIndexId`) for the named-policy case or by OID
    /// (`PolicyOidIndexId`) for the by-OID case. `policy_tuple` is the original
    /// scanned tuple (the C `heap_modify_tuple` starts from it so the
    /// non-replaced columns are preserved); the update is applied at
    /// `policy_tuple->t_self`. `rel` is the open pg_policy relation. `Err`
    /// carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_policy<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        policy_tuple: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        row: &::types_catalog::pg_policy::PgPolicyUpdateRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `rename_policy`'s pg_policy rename (commands/policy.c): `policy_tuple =
    /// heap_copytuple(policy_tuple); namestrcpy(&((Form_pg_policy) GETSTRUCT(
    /// policy_tuple))->polname, stmt->newname); CatalogTupleUpdate(pg_policy_rel,
    /// &policy_tuple->t_self, policy_tuple)`. Rewrites only the `polname`
    /// `NameData` column of the scanned tuple in place, then updates. `rel` is
    /// the open pg_policy relation; `policy_tuple` is the original scanned
    /// tuple. `Err` carries the heap/index mutation `ereport(ERROR)`s.
    pub fn rename_policy_tuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        policy_tuple: &::types_tuple::heaptuple::FormedTuple<'mcx>,
        newname: &str,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `UpdateIndexRelation`'s pg_index INSERT (catalog/index.c): build the full
    /// 21-column `pg_index` row from a typed [`PgIndexInsertRow`] (the
    /// `IndexInfo` scalars, the `buildint2vector`/`buildoidvector`-packed
    /// `indkey`/`indcollation`/`indclass`/`indoption` vectors, and the
    /// `nodeToString` `indexprs`/`indpred` `pg_node_tree` images), then
    /// `heap_form_tuple(RelationGetDescr(pg_index), values, nulls)` +
    /// `CatalogTupleInsert(pg_index, tuple)`. `rel` is the open pg_index
    /// relation. `indexprs`/`indpred` are stored NULL when `None` (the C
    /// `exprsDatum`/`predDatum == (Datum) 0`). `Err` carries the heap/index
    /// mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_index<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &::rel::Relation<'mcx>,
        row: &::types_catalog::pg_index::PgIndexInsertRow,
    ) -> PgResult<()>
);

/* ======================================================================== *
 * pg_authid / pg_auth_members (commands/user.c catalog-write value layer).
 *
 * The owner (`backend-catalog-indexing`) forms the heap tuple against the
 * relation's descriptor from the value-typed row structs and runs the
 * `heap_form_tuple`/`heap_modify_tuple` + `CatalogTuple{Insert,Update,Delete}`
 * machinery. `rel` is the open pg_authid / pg_auth_members relation (the
 * `commands/user.c` orchestration re-opens it by OID through the user seam).
 * ======================================================================== */

seam_core::seam!(
    /// `GetNewOidWithIndex(pg_authid, AuthIdOidIndexId, Anum_pg_authid_oid)`.
    pub fn get_new_oid_with_index_pg_authid<'mcx>(
        rel: &::rel::Relation<'mcx>,
    ) -> PgResult<Oid>
);
seam_core::seam!(
    /// `GetNewOidWithIndex(pg_auth_members, AuthMemOidIndexId,
    /// Anum_pg_auth_members_oid)`.
    pub fn get_new_oid_with_index_pg_auth_members<'mcx>(
        rel: &::rel::Relation<'mcx>,
    ) -> PgResult<Oid>
);
seam_core::seam!(
    /// CreateRole's `heap_form_tuple(pg_authid_dsc, new_record, nulls)` +
    /// `CatalogTupleInsert(pg_authid_rel, tuple)`.
    pub fn catalog_tuple_insert_pg_authid(
        rel: &RelationData<'_>,
        rec: &authid::NewAuthRecord,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// AlterRole's re-fetch of the `pg_authid` row by `roleid` +
    /// `heap_modify_tuple` with the per-attribute deltas + `CatalogTupleUpdate`.
    pub fn catalog_tuple_update_pg_authid(
        rel: &RelationData<'_>,
        roleid: Oid,
        upd: &authid::AuthIdUpdate,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// RenameRole's re-fetch of the `pg_authid` row by `roleid`, set `rolname`
    /// (and clear `rolpassword` when `clear_md5`) + `CatalogTupleUpdate`.
    pub fn rename_tuple_pg_authid(
        rel: &RelationData<'_>,
        roleid: Oid,
        newname: &str,
        clear_md5: bool,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// DropRole's `CatalogTupleDelete(pg_authid_rel, &tuple->t_self)` for the
    /// `pg_authid` row addressed by `roleid` (re-fetched to find its TID).
    pub fn delete_tuple_pg_authid(rel: &RelationData<'_>, roleid: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// AddRoleMems's `heap_form_tuple(pg_authmem_dsc, new_record, nulls)` +
    /// `CatalogTupleInsert(pg_authmem_rel, tuple)`.
    pub fn catalog_tuple_insert_pg_auth_members(
        rel: &RelationData<'_>,
        rec: &authid::NewAuthMemRecord,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// AddRoleMems/DelRoleMems's re-fetch of the `pg_auth_members` row by its
    /// OID + `heap_modify_tuple` with the per-option deltas + `CatalogTupleUpdate`.
    pub fn catalog_tuple_update_pg_auth_members(
        rel: &RelationData<'_>,
        authmem_oid: Oid,
        upd: &authid::AuthMemUpdate,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `CatalogTupleDelete(pg_auth_members_rel, &tuple->t_self)` for the
    /// `pg_auth_members` row addressed by its OID (re-fetched to find its TID).
    pub fn delete_tuple_pg_auth_members(rel: &RelationData<'_>, authmem_oid: Oid)
        -> PgResult<()>
);
seam_core::seam!(
    /// `systable` scan of `pg_auth_members` on `roleid == role` (the
    /// `AuthMemRoleMemIndexId` probe), returning the OID of every matching row
    /// (DropRole's first silent-removal scan).
    pub fn authmem_oids_by_roleid(rel: &RelationData<'_>, role: Oid)
        -> PgResult<Vec<Oid>>
);
seam_core::seam!(
    /// `systable` scan of `pg_auth_members` on `member == role` (the
    /// `AuthMemMemRoleIndexId` probe), returning the OID of every matching row
    /// (DropRole's second silent-removal scan).
    pub fn authmem_oids_by_member(rel: &RelationData<'_>, role: Oid)
        -> PgResult<Vec<Oid>>
);
