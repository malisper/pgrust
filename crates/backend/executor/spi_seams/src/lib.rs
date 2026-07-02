use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    pub fn at_eoxact_spi(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn at_eosubxact_spi(is_commit: bool, my_subid: SubTransactionId) -> PgResult<()>
);

seam_core::seam!(
    pub fn spi_inside_nonatomic_context() -> bool
);
