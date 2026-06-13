//! Seam declarations for the `backend-partitioning-partbounds` unit
//! (`partitioning/partbounds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Allocating builders take the target context
//! handle (C: they palloc the qual node list in `CurrentMemoryContext`).

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_rel::RelationData;

seam_core::seam!(
    /// The `relpartbound`-to-qual leg of `generate_partition_qual`
    /// (partcache.c): `SearchSysCache1(RELOID, relid)` (→ `elog(ERROR, "cache
    /// lookup failed for relation %u")` as `Err`), `SysCacheGetAttr(RELOID,
    /// ..., relpartbound, &isnull)`, and when not null `castNode(
    /// PartitionBoundSpec, stringToNode(TextDatumGetCString(boundDatum)))`
    /// then `get_qual_from_partbound(parent, bound)` (partbounds.c). Returns
    /// the implicit-AND qual list `my_qual` (empty when `relpartbound` is
    /// null), allocated in `mcx`. `Err` carries the cache-lookup failure, the
    /// bound-parse errors, and OOM.
    pub fn qual_from_partbound<'mcx, 'p>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        parent: &RelationData<'p>,
    ) -> PgResult<PgVec<'mcx, Node<'mcx>>>
);
