//! Seam declarations for the `backend-catalog-indexing` unit
//! (`catalog/indexing.c` catalog-tuple mutators).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as
//! `&types_rel::RelationData`; pg_depend tuples cross as the deformed
//! `FormData_pg_depend` row (the caller-shaped projection precedent) — the
//! owner forms the heap tuple against the pg_depend descriptor.

use types_catalog::catalog_dependency::FormData_pg_depend;
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
    pub fn catalog_tuples_multi_insert_pg_depend(
        rel: &RelationData<'_>,
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
    pub fn catalog_tuples_multi_insert_pg_shdepend(
        rel: &RelationData<'_>,
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
