use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    pub fn at_eoxact_multixact()
);

seam_core::seam!(
    pub fn at_prepare_multixact() -> PgResult<()>
);

seam_core::seam!(
    pub fn post_prepare_multixact(xid: TransactionId)
);
