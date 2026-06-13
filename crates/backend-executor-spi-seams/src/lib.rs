//! Seam declarations for the `backend-executor-spi` unit
//! (`executor/spi.c`). The owning unit installs these from its `init_seams()`
//! when it lands; until then a call panics loudly.

use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `AtEOXact_SPI(isCommit)` — clean up SPI state; WARNs about leaked
    /// connections at commit.
    pub fn at_eoxact_spi(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AtEOSubXact_SPI(isCommit, mySubid)`.
    pub fn at_eosubxact_spi(is_commit: bool, my_subid: SubTransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `SPI_inside_nonatomic_context()` — true when running inside a
    /// nonatomic SPI context (procedures).
    pub fn spi_inside_nonatomic_context() -> bool
);
