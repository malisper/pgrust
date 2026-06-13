//! Seam declarations for the `backend-optimizer-plan-planner` unit
//! (`optimizer/plan/planner.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `plan_cluster_use_sort(tableOid, indexOid)` (planner.c): whether a
    /// seqscan+sort beats an indexscan for the cluster copy.
    pub fn plan_cluster_use_sort(table_oid: Oid, index_oid: Oid) -> PgResult<bool>
);
