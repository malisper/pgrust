//! Seam declarations for the `backend-catalog-heap` unit (`catalog/heap.c`).
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::Mcx;
use types_cluster::RelOptionsToken;
use types_core::Oid;
use types_error::PgResult;
use types_rel::Relation;

seam_core::seam!(
    /// `heap_create_with_catalog(...)` (heap.c) as specialized for the cluster
    /// transient heap: the NewHeap clones OldHeap's tuple descriptor, owner,
    /// AM, persistence, mapped-ness and reloptions, with `relid = OIDOldHeap`
    /// passed for the relrewrite/identity bookkeeping. Returns the new OID.
    pub fn heap_create_with_catalog_transient<'mcx>(
        mcx: Mcx<'mcx>,
        new_heap_name: &str,
        namespaceid: Oid,
        new_tablespace: Oid,
        owner: Oid,
        new_access_method: Oid,
        old_heap: &Relation<'_>,
        relpersistence: u8,
        mapped: bool,
        reloptions: RelOptionsToken,
        old_heap_oid: Oid,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RelationClearMissing(rel)` (heap.c).
    pub fn relation_clear_missing(rel: &Relation<'_>) -> PgResult<()>
);
