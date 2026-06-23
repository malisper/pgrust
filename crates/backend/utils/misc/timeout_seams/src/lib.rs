//! Seam declarations for the timeout manager (`utils/misc/timeout.c`,
//! catalog unit `backend-utils-misc-more2`). The owning unit installs these
//! from its `init_seams()` when it lands; until then a call panics loudly.

use ::types_error::PgResult;
use ::types_timeout::EnableTimeoutParams;
use ::types_core::TimestampTz;
use ::types_timeout::{TimeoutHandlerProc, TimeoutId};

seam_core::seam!(
    /// `enable_timeouts(timeouts, count)` — arm multiple timeouts at once.
    pub fn enable_timeouts(timeouts: &[EnableTimeoutParams]) -> PgResult<()>
);

seam_core::seam!(
    /// `disable_all_timeouts(keep_indicators)`.
    pub fn disable_all_timeouts(keep_indicators: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `InitializeTimeouts()` (timeout.c) — initialize the timeout subsystem
    /// and establish the SIGALRM handler.
    pub fn initialize_timeouts()
);

seam_core::seam!(
    /// `RegisterTimeout(id, handler)` (timeout.c) — register a timeout
    /// reason. Returns the (possibly assigned, for `USER_TIMEOUT`) id.
    pub fn register_timeout(id: TimeoutId, handler: TimeoutHandlerProc) -> TimeoutId
);

seam_core::seam!(
    /// `enable_timeout_every(id, fin_time, delay_ms)` (timeout.c) — arm a
    /// periodic timeout, first firing at `fin_time`.
    pub fn enable_timeout_every(id: TimeoutId, fin_time: TimestampTz, delay_ms: i32)
);

seam_core::seam!(
    /// `disable_timeout(id, keep_indicator)` (timeout.c) — cancel a timeout,
    /// optionally preserving its already-fired indicator.
    pub fn disable_timeout(id: TimeoutId, keep_indicator: bool)
);

seam_core::seam!(
    /// `enable_timeout_after(id, delay_ms)` (timeout.c) — arm a single one-shot
    /// timeout to fire `delay_ms` milliseconds from now. Can `ereport(ERROR)` on
    /// a bad timeout state, carried on `Err`.
    pub fn enable_timeout_after(id: TimeoutId, delay_ms: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `disable_timeouts(timeouts, count)` (timeout.c) — cancel several
    /// timeouts at once.
    pub fn disable_timeouts(timeouts: &[::types_timeout::DisableTimeoutParams]) -> PgResult<()>
);

seam_core::seam!(
    /// `get_timeout_start_time(id)` (timeout.c) — the timestamp at which the
    /// given timeout was last armed.
    pub fn get_timeout_start_time(id: TimeoutId) -> TimestampTz
);
