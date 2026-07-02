use types_core::{ProcNumber, TransactionId};
use types_error::PgResult;

seam_core::seam!(
    pub fn proc_array_add(procno: ProcNumber) -> PgResult<()>
);

seam_core::seam!(
    pub fn proc_array_remove(procno: ProcNumber, latest_xid: TransactionId) -> PgResult<()>
);
