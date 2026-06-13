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
