//! `enum TimeoutId` (`utils/timeout.h`): identifiers for timeout reasons.
//! In case multiple timeouts trigger at the same time, they are serviced in
//! the order of this enum, so the discriminants are part of the contract.

pub type TimeoutId = u32;

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
/// First user-definable timeout reason.
pub const USER_TIMEOUT: TimeoutId = 13;
/// Maximum number of timeout reasons (`USER_TIMEOUT + 10`).
pub const MAX_TIMEOUTS: TimeoutId = USER_TIMEOUT + 10;

/// `timeout_handler_proc` (`utils/timeout.h`): callback fired when a timeout
/// expires. Runs in signal-handler context.
pub type TimeoutHandlerProc = fn();
