//! Seam declarations for the timeout manager (`utils/misc/timeout.c`,
//! catalog unit `backend-utils-misc-more2`). The owning unit installs these
//! from its `init_seams()` when it lands; until then a call panics loudly.

use types_error::PgResult;
use types_timeout::EnableTimeoutParams;

seam_core::seam!(
    /// `enable_timeouts(timeouts, count)` — arm multiple timeouts at once.
    pub fn enable_timeouts(timeouts: &[EnableTimeoutParams]) -> PgResult<()>
);

seam_core::seam!(
    /// `disable_all_timeouts(keep_indicators)`.
    pub fn disable_all_timeouts(keep_indicators: bool) -> PgResult<()>
);
