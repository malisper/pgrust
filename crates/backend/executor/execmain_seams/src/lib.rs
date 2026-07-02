use std::rc::Rc;

use tcop_dest::DestReceiver;
use types_dest::CommandDest;
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_nodes::plannodes::PlannedStmt;
use types_portal::{ParamListHandle, QueryDescHandle, QueryEnvHandle};
use types_scan::sdir::ScanDirection;
use types_snapshot::SnapshotData;
use types_tuple::TupleDescData;

pub type Snapshot = Rc<SnapshotData<'static>>;

seam_core::seam!(
    // CreateQueryDesc (tcop/pquery.c) body: the QueryDesc struct is
    // executor-owned (execdesc.h), reached by handle. The impl retains
    // `plannedstmt`/`source_text` with C's raw-pointer contract: the caller
    // keeps both alive until free_query_desc (portal drops before its stmt
    // list / MessageContext reset, as in C). `dest` is the CommandDest marker;
    // the live receiver is threaded per-run (executor_run).
    pub fn create_query_desc<'a>(
        plannedstmt: &'a PlannedStmt<'a>,
        source_text: &'a str,
        snapshot: Option<Snapshot>,
        crosscheck_snapshot: Option<Snapshot>,
        dest: CommandDest,
        params: ParamListHandle,
        query_env: QueryEnvHandle,
        instrument_options: i32,
    ) -> PgResult<QueryDescHandle>
);

seam_core::seam!(
    // FreeQueryDesc (pquery.c): UnregisterSnapshot x2 + pfree.
    pub fn free_query_desc(query_desc: QueryDescHandle)
);

seam_core::seam!(
    // ExecutorStart (executor/execMain.c).
    pub fn executor_start(query_desc: QueryDescHandle, eflags: i32) -> PgResult<()>
);

seam_core::seam!(
    // ExecutorRun (execMain.c). C reads queryDesc->dest; the enum receiver is
    // passed by ref instead (same per-fetch override point, pquery.c:888).
    pub fn executor_run<'a, 'mcx>(
        query_desc: QueryDescHandle,
        direction: ScanDirection,
        count: u64,
        dest: &'a mut DestReceiver<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    // ExecutorFinish (execMain.c).
    pub fn executor_finish(query_desc: QueryDescHandle) -> PgResult<()>
);

seam_core::seam!(
    // ExecutorEnd (execMain.c).
    pub fn executor_end(query_desc: QueryDescHandle) -> PgResult<()>
);

seam_core::seam!(
    // queryDesc->estate->es_processed.
    pub fn query_desc_es_processed(query_desc: QueryDescHandle) -> u64
);

seam_core::seam!(
    // queryDesc->snapshot (the registered copy).
    pub fn query_desc_snapshot(query_desc: QueryDescHandle) -> Option<Snapshot>
);

seam_core::seam!(
    // queryDesc->tupDesc, set by ExecutorStart.
    pub fn query_desc_result_tupdesc(
        query_desc: QueryDescHandle,
    ) -> Option<Rc<TupleDescData<'static>>>
);

seam_core::seam!(
    // queryDesc->operation.
    pub fn query_desc_operation(query_desc: QueryDescHandle) -> CmdType
);

seam_core::seam!(
    // ExecCleanTypeFromTL(pstmt->planTree->targetlist) (execTuples.c); takes
    // the stmt (not the list) because Plan-tree traversal is executor-side.
    pub fn exec_clean_type_from_tl<'a>(
        pstmt: &'a PlannedStmt<'a>,
    ) -> PgResult<Rc<TupleDescData<'static>>>
);
