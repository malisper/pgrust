//! Seam declarations for the `backend-catalog-indexing` unit
//! (`catalog/indexing.c` catalog-tuple mutators).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as
//! `&types_rel::rel::RelationData`; pg_depend tuples cross as the deformed
//! `FormData_pg_depend` row (the caller-shaped projection precedent) — the
//! owner forms the heap tuple against the pg_depend descriptor.

use types_catalog::catalog_dependency::FormData_pg_depend;
use types_error::PgResult;
use types_rel::rel::RelationData;
use types_tuple::heaptuple::ItemPointerData;

seam_core::seam!(
    /// `CatalogTupleDelete(rel, tid)` (catalog/indexing.c): delete the
    /// addressed tuple from a catalog relation (`simple_heap_delete`). `Err`
    /// carries the heap-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_delete(rel: &RelationData, tid: ItemPointerData) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogTupleUpdate(rel, tid, tup)` (catalog/indexing.c) for a
    /// pg_depend row: `simple_heap_update` plus `CatalogIndexInsert` index
    /// maintenance. The replacement tuple crosses as its deformed form. `Err`
    /// carries the heap/index-mutation `ereport(ERROR)`s.
    pub fn catalog_tuple_update_pg_depend(
        rel: &RelationData,
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
        rel: &RelationData,
        forms: &[FormData_pg_depend],
    ) -> PgResult<()>
);
