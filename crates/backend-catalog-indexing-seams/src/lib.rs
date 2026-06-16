//! Seam declarations for the `backend-catalog-indexing` unit
//! (`catalog/indexing.c` catalog-tuple mutators).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as
//! `&types_rel::RelationData`; pg_depend tuples cross as the deformed
//! `FormData_pg_depend` row (the caller-shaped projection precedent) — the
//! owner forms the heap tuple against the pg_depend descriptor.

use types_catalog::catalog_dependency::FormData_pg_depend;
use types_catalog::pg_sequence::FormData_pg_sequence;
use types_catalog::catalog_shdepend::FormData_pg_shdepend;
use types_catalog::opclasscmds_catalog::{
    FormData_pg_amop, FormData_pg_amproc, FormData_pg_opclass, FormData_pg_opfamily,
};
use types_core::Oid;
use types_error::PgResult;
use types_rel::RelationData;
use types_tuple::heaptuple::ItemPointerData;

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
        rel: &types_rel::Relation<'mcx>,
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
        rel: &types_rel::Relation<'mcx>,
        forms: &[FormData_pg_shdepend],
    ) -> PgResult<()>
);

/* ---- CLUSTER pg_class / pg_index row updates (backend-commands-cluster) --- */

seam_core::seam!(
    /// `CatalogTupleUpdate(pg_class_rel, &tup->t_self, tup)` after reforming
    /// the mutated `PgClassForm` (indexing.c).
    pub fn catalog_tuple_update_pg_class<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'_>,
        tid: ItemPointerData,
        form: &types_cluster::PgClassForm,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `CatalogTupleUpdate(pg_index_rel, &tup->t_self, tup)` after reforming
    /// the mutated `PgIndexForm` (indexing.c).
    pub fn catalog_tuple_update_pg_index<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'_>,
        tid: ItemPointerData,
        form: &types_cluster::PgIndexForm,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `CatalogOpenIndexes(rel)` (indexing.c).
    pub fn catalog_open_indexes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'_>,
    ) -> PgResult<types_cluster::CatalogIndexStateToken>
);
seam_core::seam!(
    /// `CatalogTupleUpdateWithInfo(rel, &tup->t_self, tup, indstate)`.
    pub fn catalog_tuple_update_with_info_pg_class<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'_>,
        tid: ItemPointerData,
        form: &types_cluster::PgClassForm,
        indstate: &types_cluster::CatalogIndexStateToken,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `CatalogCloseIndexes(indstate)` (indexing.c).
    pub fn catalog_close_indexes(indstate: types_cluster::CatalogIndexStateToken) -> PgResult<()>
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
    /// - 1] = true`). The `acl` varlena (`Acl *` = `ArrayType`) crosses
    /// unchanged. `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_namespace(
        rel: &RelationData<'_>,
        nspname: &str,
        nspowner: Oid,
        nspacl: Option<types_array::ArrayType>,
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
    /// `heap_form_tuple` + `CatalogTupleInsert` + `heap_freetuple` for one
    /// pg_largeobject_metadata row (pg_largeobject.c `LargeObjectCreate`): form
    /// the metadata tuple from the already-chosen large-object OID, owner, and
    /// optional default ACL, then insert it with index maintenance. Unlike the
    /// pg_namespace/pg_am inserts, the OID is assigned by the caller (the C
    /// `OidIsValid(loid) ? loid : GetNewOidWithIndex(...)` branch runs in
    /// `LargeObjectCreate` itself), so this seam does no OID allocation; it just
    /// builds `values[]` (`oid = loid`, `lomowner = lomowner`) and, when
    /// `lomacl == None`, sets `nulls[Anum_pg_largeobject_metadata_lomacl - 1] =
    /// true`. The `lomacl` varlena (`Acl *` = `ArrayType`) crosses unchanged.
    /// `Err` carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_largeobject_metadata(
        rel: &RelationData<'_>,
        loid: Oid,
        lomowner: Oid,
        lomacl: Option<types_array::ArrayType>,
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
        tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>,
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
    pub fn catalog_tuple_insert_with_info_pg_largeobject(
        rel: &RelationData<'_>,
        loid: Oid,
        pageno: i32,
        data: &[u8],
        indstate: &types_cluster::CatalogIndexStateToken,
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
    pub fn catalog_tuple_update_with_info_pg_largeobject(
        rel: &RelationData<'_>,
        tid: ItemPointerData,
        data: &[u8],
        indstate: &types_cluster::CatalogIndexStateToken,
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
        row: &types_catalog::pg_constraint::PgConstraintInsertRow,
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
        fields: &types_catalog::pg_constraint::ConstraintFieldUpdate,
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
        rel: &types_rel::Relation<'mcx>,
        row: &types_catalog::pg_cast::PgCastInsertRow,
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
        rel: &types_rel::Relation<'mcx>,
        row: &types_catalog::pg_inherits::PgInheritsInsertRow,
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
        rel: &types_rel::Relation<'mcx>,
        row: &types_catalog::pg_conversion::PgConversionInsertRow,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RangeCreate`'s tuple build + insert: `heap_form_tuple(RelationGetDescr(
    /// rel), values, nulls)` + `CatalogTupleInsert(rel, tup)` (catalog/
    /// indexing.c + heapam). `pg_range` has no OID column, so nothing is
    /// returned. `Err` carries the heap/index mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_insert_pg_range<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
        row: &types_catalog::pg_range::PgRangeInsertRow,
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
        rel: &types_rel::Relation<'mcx>,
        row: &types_catalog::pg_enum::PgEnumInsertRow,
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
        rel: &types_rel::Relation<'mcx>,
        row: &types_catalog::pg_language::PgLanguageInsertRow,
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
        rel: &types_rel::Relation<'mcx>,
        oldtup: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
        row: &types_catalog::pg_language::PgLanguageInsertRow,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `GetNewOidWithIndex(pg_type, TypeOidIndexId, Anum_pg_type_oid)`
    /// (catalog/catalog.c): allocate a fresh `pg_type` row OID that does not
    /// collide with any existing row, verified against `TypeOidIndexId`.
    /// Returned to the `pg_type.c` port so its forced-OID / binary-upgrade /
    /// new-OID branch (`TypeShellMake` / `TypeCreate`) keeps the OID-assignment
    /// decision in the owner. `Err` carries the index-probe error surface.
    pub fn get_new_oid_with_index_pg_type<'mcx>(rel: &types_rel::Relation<'mcx>) -> PgResult<Oid>
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
        row: &types_catalog::pg_type::PgTypeInsertRow,
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
        row: &types_catalog::pg_type::PgTypeInsertRow,
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
    pub fn get_new_oid_with_index_pg_enum<'mcx>(rel: &types_rel::Relation<'mcx>) -> PgResult<Oid>
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
        rel: &types_rel::Relation<'mcx>,
        rows: &[types_catalog::pg_enum::PgEnumInsertRow],
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
        rel: &types_rel::Relation<'mcx>,
        tid: ItemPointerData,
        row: &types_catalog::pg_enum::PgEnumInsertRow,
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
    ) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>>
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
        tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
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
        rel: &types_rel::Relation<'mcx>,
        row: &types_catalog::pg_statistic_ext::PgStatisticExtInsertRow,
    ) -> PgResult<Oid>
);
