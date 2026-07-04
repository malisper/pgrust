use types_core::{SubTransactionId, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    // IsParallelWorker() (parallel.h): ParallelWorkerNumber >= 0.
    pub fn is_parallel_worker() -> bool
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
