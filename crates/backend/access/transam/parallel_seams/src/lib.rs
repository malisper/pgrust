use types_core::{SubTransactionId, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    // IsParallelWorker() (parallel.h): ParallelWorkerNumber >= 0.
    pub fn is_parallel_worker() -> bool
);

seam_core::seam!(
    // ParallelWorkerNumber (parallel.h global): -1 on the leader. Consumed by
    // execgrouping's variable hash IV (BuildTupleHashTable, execGrouping.c).
    pub fn parallel_worker_number() -> i32
);

seam_core::seam!(
    // InitializingParallelWorker (parallel.c): true between worker start and
    // the entrypoint call.
    pub fn initializing_parallel_worker() -> bool
);

seam_core::seam!(
    pub fn at_eoxact_parallel(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn at_eosubxact_parallel(is_commit: bool, my_subid: SubTransactionId) -> PgResult<()>
);

seam_core::seam!(
    pub fn parallel_worker_report_last_rec_end(last_rec_end: XLogRecPtr) -> PgResult<()>
);

seam_core::seam!(
    pub fn handle_parallel_message_interrupt()
);

seam_core::seam!(
    pub fn process_parallel_messages() -> PgResult<()>
);

seam_core::seam!(
    // pgstat_progress_parallel_incr_param's worker leg: send a PqMsg_Progress
    // equivalent (WorkerMessage::Progress) to the leader.
    pub fn parallel_worker_report_progress(index: i32, incr: i64)
);
