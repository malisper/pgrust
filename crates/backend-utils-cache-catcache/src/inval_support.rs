//! Invalidation support for inval.c (`catcache.c`):
//! `PrepareToInvalidateCacheTuple`.
//!
//! For each cache on `RelationGetRelid(relation)`, compute the tuple's hash
//! value(s) and emit one [`types_storage::PrepareToInvalidateCacheTuple`]
//! request per `(*function)` invocation the C code makes (one for the old
//! tuple's keys, plus one for the new tuple's keys on an update when the hash
//! differs). The key columns are deformed from the real `HeapTupleData` via the
//! cache's `cc_tupdesc` (read through the catcache's own descriptor) and
//! `heap_getattr` (genam/heaptuple substrate); `dbId` is
//! `cc_relisshared ? InvalidOid : MyDatabaseId`.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_rel::RelationData;
use types_storage::PrepareToInvalidateCacheTuple;
use types_tuple::HeapTupleData;

/// `PrepareToInvalidateCacheTuple(relation, tuple, newtuple, function, context)`.
pub fn prepare_to_invalidate_cache_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _relation: &RelationData<'_>,
    _tuple: &HeapTupleData<'_>,
    _newtuple: Option<&HeapTupleData<'_>>,
) -> PgResult<PgVec<'mcx, PrepareToInvalidateCacheTuple>> {
    todo!("catcache::inval_support::prepare_to_invalidate_cache_tuple")
}
