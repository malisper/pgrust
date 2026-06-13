//! Seam declarations for the `backend-catalog-indexing` unit
//! (`catalog/indexing.c` catalog-tuple mutators).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as
//! `&types_rel::RelationData`; pg_depend tuples cross as the deformed
//! `FormData_pg_depend` row (the caller-shaped projection precedent) — the
//! owner forms the heap tuple against the pg_depend descriptor.

use types_catalog::catalog_dependency::FormData_pg_depend;
use types_core::primitive::Oid;
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
