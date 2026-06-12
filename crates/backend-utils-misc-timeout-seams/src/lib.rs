//! Seam declarations for the timeout machinery (`utils/misc/timeout.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! Failure surface: on the paths reachable from these entry points
//! `timeout.c` raises only `elog(FATAL)`/`ereport(FATAL)` (bad index,
//! setitimer failure, out of user timeouts), which terminates the process
//! rather than propagating, so the signatures are infallible.

use types_core::TimestampTz;
use types_timeout::{TimeoutHandlerProc, TimeoutId};

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
