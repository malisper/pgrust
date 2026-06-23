//! ABI for the catalog-core relation/index-creation subsystem
//! (`backend/catalog/{heap,index,storage,toasting,partition,pg_proc,pg_class,
//! pg_namespace}.c`).
//!
//! Most of the catalog form structs (`FormData_pg_class`, `FormData_pg_index`,
//! `FormData_pg_attribute`, `FormData_pg_proc`), the scalar types (`Oid`,
//! `AttrNumber`, `RelFileNumber`, `BlockNumber`, `ForkNumber`, `Datum`,
//! `NullableDatum`, `LOCKMODE`), the relcache/smgr handles (`Relation`,
//! `SMgrRelation`, `TupleDesc`), the node opaques (`IndexInfo`, `ParseState`,
//! `PartitionBoundSpec`, `oidvector`, `AttrMap`), and the WAL handle
//! (`XLogReaderState`) already live in their own pg-ffi modules.  This module
//! adds only the small remaining ABI those eight `.c` files need.

use core::ffi::c_void;

/// `bits16` — `uint16` bit-flags type (`index_create`/`index_constraint_create`
/// `flags`/`constr_flags`).
pub type bits16 = u16;

/// `float4` — single-precision float (`pg_proc.procost`/`prorows`).
pub type float4 = f32;

/// `CatalogIndexState` — opaque `struct CatalogIndexStateData *`, the
/// `CatalogOpenIndexes` handle threaded through `InsertPgAttributeTuples`.
pub type CatalogIndexState = *mut c_void;

// NOTE: IndexStmt / ReindexStmt / ReindexParams / IndexStateFlagsAction are
// defined canonically in `commands_ddl_parsenodes.rs` (full repr(C) structs +
// identical ReindexParams/IndexStateFlagsAction) and reused here via the
// pgrust-pg-ffi glob re-export — defining them here too caused a merge-time
// duplicate/ambiguous-glob collision when catalog-core + commands-ddl landed.

/// `FormExtraData_pg_attribute` (catalog/pg_attribute.h) — the
/// `CATALOG_VARLEN`-excluded attribute fields DDL code passes alongside
/// `FormData_pg_attribute`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormExtraData_pg_attribute {
    pub attstattarget: crate::fmgr::NullableDatum,
    pub attoptions: crate::fmgr::NullableDatum,
}
