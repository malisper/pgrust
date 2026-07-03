use types_core::SubTransactionId;
use types_error::PgResult;
use types_slot::SlotData;
use types_tuple::TupleDescData;

seam_core::seam!(
    pub fn at_eoxact_spi(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn at_eosubxact_spi(is_commit: bool, my_subid: SubTransactionId) -> PgResult<()>
);

seam_core::seam!(
    pub fn spi_inside_nonatomic_context() -> bool
);

seam_core::seam!(
    pub fn spi_dest_startup(operation: i32, typeinfo: &TupleDescData<'_>) -> PgResult<()>
);

seam_core::seam!(
    pub fn spi_printtup<'mcx>(slot: &mut SlotData<'mcx>) -> PgResult<bool>
);
