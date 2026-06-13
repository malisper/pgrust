//! `CacheInvalidate*` entry points (inval.c) plus the callback registration
//! and dispatch (`CacheRegister*Callback`, `CallSyscacheCallbacks`,
//! `CallRelSyncCallbacks`).

use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_rel::RelationData;
use types_storage::RelFileLocatorBackend;
use types_tuple::HeapTupleData;

use crate::RelSyncCallbackFunction;
use types_cache::{RelcacheCallbackFunction, SyscacheCallbackFunction};

/// `CacheInvalidateHeapTupleCommon` — shared end-of-command / inplace logic.
pub(crate) fn cache_invalidate_heap_tuple_common(
    _relation: &RelationData<'_>,
    _tuple: &HeapTupleData<'_>,
    _newtuple: Option<&HeapTupleData<'_>>,
    _use_inplace: bool,
) -> PgResult<()> {
    todo!("CacheInvalidateHeapTupleCommon")
}

/// `CacheInvalidateHeapTuple`.
pub fn CacheInvalidateHeapTuple(
    _relation: &RelationData<'_>,
    _tuple: &HeapTupleData<'_>,
    _newtuple: Option<&HeapTupleData<'_>>,
) -> PgResult<()> {
    todo!("CacheInvalidateHeapTuple")
}

/// `CacheInvalidateHeapTupleInplace`.
pub fn CacheInvalidateHeapTupleInplace(
    _relation: &RelationData<'_>,
    _key_equivalent_tuple: &HeapTupleData<'_>,
) -> PgResult<()> {
    todo!("CacheInvalidateHeapTupleInplace")
}

/// `CacheInvalidateCatalog`.
pub fn CacheInvalidateCatalog(_catalogId: Oid) -> PgResult<()> {
    todo!("CacheInvalidateCatalog")
}

/// `CacheInvalidateRelcache`.
pub fn CacheInvalidateRelcache(_relation: &RelationData<'_>) -> PgResult<()> {
    todo!("CacheInvalidateRelcache")
}

/// `CacheInvalidateRelcacheAll`.
pub fn CacheInvalidateRelcacheAll() -> PgResult<()> {
    todo!("CacheInvalidateRelcacheAll")
}

/// `CacheInvalidateRelcacheByTuple`.
pub fn CacheInvalidateRelcacheByTuple(_classTuple: &HeapTupleData<'_>) -> PgResult<()> {
    todo!("CacheInvalidateRelcacheByTuple")
}

/// `CacheInvalidateRelcacheByRelid`.
pub fn CacheInvalidateRelcacheByRelid(_relid: Oid) -> PgResult<()> {
    todo!("CacheInvalidateRelcacheByRelid")
}

/// `CacheInvalidateRelSync`.
pub fn CacheInvalidateRelSync(_relid: Oid) -> PgResult<()> {
    todo!("CacheInvalidateRelSync")
}

/// `CacheInvalidateRelSyncAll`.
pub fn CacheInvalidateRelSyncAll() -> PgResult<()> {
    todo!("CacheInvalidateRelSyncAll")
}

/// `CacheInvalidateSmgr` — broadcast an smgr-close invalidation immediately.
pub fn CacheInvalidateSmgr(_rlocator: RelFileLocatorBackend) -> PgResult<()> {
    todo!("CacheInvalidateSmgr")
}

/// `CacheInvalidateRelmap` — broadcast a relmap-change invalidation immediately.
pub fn CacheInvalidateRelmap(_databaseId: Oid) -> PgResult<()> {
    todo!("CacheInvalidateRelmap")
}

/* ------------------------------------------------------------------------
 *  Callback registration / dispatch
 * ------------------------------------------------------------------------ */

/// `CacheRegisterSyscacheCallback`.
pub fn CacheRegisterSyscacheCallback(
    _cacheid: i32,
    _func: SyscacheCallbackFunction,
    _arg: Datum,
) -> PgResult<()> {
    todo!("CacheRegisterSyscacheCallback")
}

/// `CacheRegisterRelcacheCallback`.
pub fn CacheRegisterRelcacheCallback(_func: RelcacheCallbackFunction, _arg: Datum) -> PgResult<()> {
    todo!("CacheRegisterRelcacheCallback")
}

/// `CacheRegisterRelSyncCallback`.
pub fn CacheRegisterRelSyncCallback(_func: RelSyncCallbackFunction, _arg: Datum) -> PgResult<()> {
    todo!("CacheRegisterRelSyncCallback")
}

/// `CallSyscacheCallbacks`.
pub fn CallSyscacheCallbacks(_cacheid: i32, _hashvalue: u32) -> PgResult<()> {
    todo!("CallSyscacheCallbacks")
}

/// `CallRelSyncCallbacks`.
pub fn CallRelSyncCallbacks(_relid: Oid) -> PgResult<()> {
    todo!("CallRelSyncCallbacks")
}
