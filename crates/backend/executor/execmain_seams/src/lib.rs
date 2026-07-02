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
    pub fn exec_clean_type_from_tl<'p, 'a>(
        pstmt: &'p PlannedStmt<'a>,
    ) -> PgResult<Rc<TupleDescData<'static>>>
);
