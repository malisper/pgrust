use std::rc::Rc;

use tcop_dest::DestReceiver;
use types_dest::CommandDest;
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_nodes::plannodes::PlannedStmt;
use types_portal::{ParamListHandle, QueryDescHandle, QueryEnvHandle};
use types_scan::sdir::ScanDirection;
use types_core::instrument::{
    AggregateInstrumentation, BitmapHeapScanInstrumentation, HashInstrumentation,
    IncrementalSortInfo, Instrumentation, MemoizeInstrumentation, TuplesortInstrumentation,
    TuplestoreInstrumentation,
};
use types_snapshot::SnapshotData;
use types_tuple::TupleDescData;

pub type Snapshot = Rc<SnapshotData<'static>>;

seam_core::seam!(
    // Retention contract: caller keeps plannedstmt/source_text alive until
    // free_query_desc (C's raw-pointer rule); the live receiver threads
    // per-run, dest is only the marker.
    pub fn create_query_desc<'p, 'a, 's>(
        plannedstmt: &'p PlannedStmt<'a>,
        source_text: &'s str,
        snapshot: Option<Snapshot>,
        crosscheck_snapshot: Option<Snapshot>,
        dest: CommandDest,
        params: ParamListHandle,
        query_env: QueryEnvHandle,
        instrument_options: i32,
    ) -> PgResult<QueryDescHandle>
);

seam_core::seam!(
    pub fn free_query_desc(query_desc: QueryDescHandle)
);

seam_core::seam!(
    // Abort-path reclamation: C frees the QueryDesc with the portal context
    // and never runs ExecutorEnd; snapshot registrations are the resource
    // owner's to release.
    pub fn release_query_desc(query_desc: QueryDescHandle)
);

seam_core::seam!(
    pub fn executor_start(query_desc: QueryDescHandle, eflags: i32) -> PgResult<()>
);

seam_core::seam!(
    pub fn executor_run<'d, 'mcx>(
        query_desc: QueryDescHandle,
        direction: ScanDirection,
        count: u64,
        dest: &'d mut DestReceiver<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn executor_finish(query_desc: QueryDescHandle) -> PgResult<()>
);

seam_core::seam!(
    pub fn executor_rewind(query_desc: QueryDescHandle) -> PgResult<()>
);

seam_core::seam!(
    pub fn executor_end(query_desc: QueryDescHandle) -> PgResult<()>
);

seam_core::seam!(
    pub fn query_desc_es_processed(query_desc: QueryDescHandle) -> u64
);

seam_core::seam!(
    pub fn query_desc_snapshot(query_desc: QueryDescHandle) -> Option<Snapshot>
);

seam_core::seam!(
    pub fn query_desc_result_tupdesc(
        query_desc: QueryDescHandle,
    ) -> Option<Rc<TupleDescData<'static>>>
);

seam_core::seam!(
    pub fn query_desc_operation(query_desc: QueryDescHandle) -> CmdType
);

seam_core::seam!(
    // ExplainNode's planstate->instrument read (runs C's forced InstrEndLoop).
    pub fn query_desc_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<Instrumentation>
);

seam_core::seam!(
    pub fn query_desc_agg_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<AggregateInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_sort_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<TuplesortInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_incsort_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<IncrementalSortInfo>
);

seam_core::seam!(
    pub fn query_desc_index_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<u64>
);

seam_core::seam!(
    pub fn query_desc_hash_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<HashInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_tuplestore_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<TuplestoreInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_memoize_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<MemoizeInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_bitmap_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<BitmapHeapScanInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_index_searches(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<u64>
);

seam_core::seam!(
    pub fn exec_clean_type_from_tl<'p, 'a>(
        pstmt: &'p PlannedStmt<'a>,
    ) -> PgResult<Rc<TupleDescData<'static>>>
);
