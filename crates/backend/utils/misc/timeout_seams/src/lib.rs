// utils/timeout.h TimeoutId order (service priority order).
pub type TimeoutId = i32;
pub const STARTUP_PACKET_TIMEOUT: TimeoutId = 0;
pub const DEADLOCK_TIMEOUT: TimeoutId = 1;
pub const LOCK_TIMEOUT: TimeoutId = 2;
pub const STATEMENT_TIMEOUT: TimeoutId = 3;
pub const STANDBY_DEADLOCK_TIMEOUT: TimeoutId = 4;
pub const STANDBY_TIMEOUT: TimeoutId = 5;
pub const STANDBY_LOCK_TIMEOUT: TimeoutId = 6;
pub const IDLE_IN_TRANSACTION_SESSION_TIMEOUT: TimeoutId = 7;
pub const TRANSACTION_TIMEOUT: TimeoutId = 8;
pub const IDLE_SESSION_TIMEOUT: TimeoutId = 9;
pub const IDLE_STATS_UPDATE_TIMEOUT: TimeoutId = 10;
pub const CLIENT_CONNECTION_CHECK_TIMEOUT: TimeoutId = 11;
pub const STARTUP_PROGRESS_TIMEOUT: TimeoutId = 12;
pub const USER_TIMEOUT: TimeoutId = 13;
pub const MAX_TIMEOUTS: TimeoutId = USER_TIMEOUT + 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DisableTimeoutParams {
    pub id: TimeoutId,
    pub keep_indicator: bool,
}

seam_core::seam!(
    pub fn disable_timeouts<'a>(timeouts: &'a [DisableTimeoutParams])
);

seam_core::seam!(
    pub fn enable_timeout_after(id: TimeoutId, delay_ms: i32) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn disable_timeout(id: TimeoutId, keep_indicator: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn reschedule_timeouts() -> types_error::PgResult<()>
);

// Only lock.c's TMPARAM_AFTER shape crosses this seam.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnableTimeoutAfterParams {
    pub id: TimeoutId,
    pub delay_ms: i32,
}

seam_core::seam!(
    pub fn enable_timeouts<'a>(timeouts: &'a [EnableTimeoutAfterParams]) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn get_timeout_start_time(id: TimeoutId) -> types_core::TimestampTz
);

seam_core::seam!(
    pub fn get_timeout_indicator(id: TimeoutId, reset_indicator: bool) -> bool
);

seam_core::seam!(
    pub fn get_timeout_finish_time(id: TimeoutId) -> types_core::TimestampTz
);

seam_core::seam!(
    pub fn initialize_timeouts()
);

seam_core::seam!(
    pub fn register_timeout(id: TimeoutId, handler: fn()) -> TimeoutId
);

seam_core::seam!(
    pub fn get_timeout_active(id: TimeoutId) -> bool
);

seam_core::seam!(
    pub fn disable_all_timeouts(keep_indicators: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    // Synchronous SIGALRM delivery (notes/timeout-threads.md).
    pub fn process_timeout_interrupt()
);
