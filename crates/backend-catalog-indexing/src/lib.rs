//! `catalog/indexing.c` — routines to support indexes defined on system
//! catalogs.
//!
//! F0 — the generic catalog-mutation engine. This crate owns the faithful
//! port of `catalog/indexing.c`'s generic engine: the [`keystone`] module
//! exposes `CatalogTupleInsert` / `CatalogTupleInsertWithInfo` /
//! `CatalogTupleUpdate` / `CatalogTupleUpdateWithInfo` / `CatalogTupleDelete`
//! plus `CatalogIndexInsert` (static), `CatalogOpenIndexes`,
//! `CatalogCloseIndexes`, and the [`keystone::CatalogIndexState`] carrier, all
//! over the real catalog [`types_rel::Relation`] and the owned
//! [`types_tuple::backend_access_common_heaptuple::FormedTuple`] (header +
//! user-data area, #161/#289).
//!
//! NO GENERIC INWARD SEAM. `backend-catalog-indexing-seams` declares only
//! per-catalog *typed* seams (the F1 family layer: a `FormData_*` row crosses
//! and the owner forms the tuple) plus the cluster-family
//! `catalog_open_indexes` / `catalog_close_indexes`. There is no generic
//! `catalog_tuple_insert(rel, FormedTuple)` inward seam for the engine, so the
//! engine is exposed as `pub` functions (the F1 family fills will call these
//! directly behind their typed seam wrappers). This crate therefore installs
//! **no** inward seam at F0; [`init_seams`] is intentionally empty.

#![allow(non_snake_case)]

pub mod family1;
pub mod keystone;

/// Install every inward seam this unit owns. Wired into `seams-init::init_all`.
///
/// F0 (the generic engine) owns no inward seam: the engine is consumed as
/// `pub` functions, and the per-catalog typed seams declared by
/// `backend-catalog-indexing-seams` are installed by the F1 family fills (which
/// form the heap tuple from their crossed `FormData_*` row before calling the
/// engine here). [`family1::install`] sets the F1 seams whose substrate is
/// fully present (pure `heap_form_tuple` + engine + `GetNewOidWithIndex`),
/// including the three multi-insert seams
/// (`catalog_tuples_multi_insert_pg_{depend,shdepend,enum}`): the engine
/// `CatalogTuplesMultiInsertWithInfo` is ported here, calling the
/// `backend-access-heap-heapam-seams::heap_multi_insert` seam (mirror-pg-and-
/// panic until the heapam insert family lands) and running `CatalogIndexInsert`
/// per inserted tuple. The rest stay uninstalled (mirror-pg-and-panic) until
/// their substrate lands (`construct_array_builtin` / `SearchSysCacheCopy1` /
/// ACL rewrite for the array/syscache/owner-update seams).
pub fn init_seams() {
    family1::install();
}
