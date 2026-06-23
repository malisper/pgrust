//! Seam declarations for the `backend-commands-cluster` unit
//! (`commands/cluster.c`): the public CLUSTER / VACUUM FULL entry points that
//! other commands call across a dependency cycle (tablecmds' ALTER TABLE,
//! vacuum's VACUUM FULL, matview's REFRESH).
//!
//! `backend-commands-cluster` installs every one of these from its
//! `init_seams()`. Until it is loaded, a call panics loudly.

#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_cluster::ClusterParams;
use types_core::{MultiXactId, Oid, TransactionId};
use ::types_error::PgResult;
use ::rel::Relation;
use ::types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `cluster_rel(OldHeap, indexOid, params)` (cluster.c): cluster one
    /// already-opened, AccessExclusiveLock-held relation (closes it, keeps
    /// the lock). `indexOid == InvalidOid` is VACUUM FULL. `Err` carries the
    /// full `ereport(ERROR)` surface.
    pub fn cluster_rel<'mcx>(
        mcx: Mcx<'mcx>,
        OldHeap: Relation<'mcx>,
        indexOid: Oid,
        params: ClusterParams,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `check_index_is_clusterable(OldHeap, indexOid, lockmode)` (cluster.c):
    /// verify the heap/index pair is valid to cluster on; locks the index.
    pub fn check_index_is_clusterable<'mcx>(
        mcx: Mcx<'mcx>,
        OldHeap: &Relation<'mcx>,
        indexOid: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `mark_index_clustered(rel, indexOid, is_internal)` (cluster.c): mark
    /// `indexOid` as the clustered index of `rel` (clearing the bit on the
    /// others); `InvalidOid` marks all not-clustered.
    pub fn mark_index_clustered<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        indexOid: Oid,
        is_internal: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `make_new_heap(OIDOldHeap, NewTableSpace, NewAccessMethod,
    /// relpersistence, lockmode)` (cluster.c): create the transient heap that
    /// receives the rebuilt data; returns its OID.
    pub fn make_new_heap(
        mcx: Mcx<'_>,
        OIDOldHeap: Oid,
        NewTableSpace: Oid,
        NewAccessMethod: Oid,
        relpersistence: u8,
        lockmode: LOCKMODE,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `finish_heap_swap(OIDOldHeap, OIDNewHeap, is_system_catalog,
    /// swap_toast_by_content, check_constraints, is_internal, frozenXid,
    /// cutoffMulti, newrelpersistence)` (cluster.c): swap physical files,
    /// rebuild indexes, drop the transient table.
    #[allow(clippy::too_many_arguments)]
    pub fn finish_heap_swap(
        mcx: Mcx<'_>,
        OIDOldHeap: Oid,
        OIDNewHeap: Oid,
        is_system_catalog: bool,
        swap_toast_by_content: bool,
        check_constraints: bool,
        is_internal: bool,
        frozenXid: TransactionId,
        cutoffMulti: MultiXactId,
        newrelpersistence: u8,
    ) -> PgResult<()>
);
