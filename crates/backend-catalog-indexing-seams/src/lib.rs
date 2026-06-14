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
