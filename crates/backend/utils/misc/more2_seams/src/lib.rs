//! Seam declarations for the `backend-utils-misc-more2` unit (it covers
//! `utils/misc/timeout.c`, among others). The owning unit installs these from
//! its `init_seams()` when it lands; until then a call panics loudly.

use types_core::xact::TimeoutId;
use types_error::PgResult;

seam_core::seam!(
    /// `enable_timeout_after(id, delay_ms)` (timeout.c).
    pub fn enable_timeout_after(id: TimeoutId, delay_ms: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `disable_timeout(id, keep_indicator)` (timeout.c).
    pub fn disable_timeout(id: TimeoutId, keep_indicator: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `reschedule_timeouts()` (timeout.c) — re-arm the timer after a
    /// longjmp out of a signal handler.
    pub fn reschedule_timeouts() -> PgResult<()>
);
