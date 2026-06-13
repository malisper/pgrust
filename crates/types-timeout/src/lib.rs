//! Timeout-manager vocabulary (`utils/timeout.h`), trimmed to the items
//! ports consume: the predefined timeout reasons and the parameter structs
//! for arming timeouts.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::TimestampTz;

/// `TimeoutId` (`utils/timeout.h`) — predefined timeout reasons.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeoutId {
    STARTUP_PACKET_TIMEOUT = 0,
    DEADLOCK_TIMEOUT = 1,
    LOCK_TIMEOUT = 2,
    STATEMENT_TIMEOUT = 3,
    STANDBY_DEADLOCK_TIMEOUT = 4,
    STANDBY_TIMEOUT = 5,
    STANDBY_LOCK_TIMEOUT = 6,
    IDLE_IN_TRANSACTION_SESSION_TIMEOUT = 7,
    TRANSACTION_TIMEOUT = 8,
    IDLE_SESSION_TIMEOUT = 9,
    IDLE_STATS_UPDATE_TIMEOUT = 10,
    CLIENT_CONNECTION_CHECK_TIMEOUT = 11,
    STARTUP_PROGRESS_TIMEOUT = 12,
    /// First user-definable timeout reason.
    USER_TIMEOUT = 13,
}

/// `MAX_TIMEOUTS` — `USER_TIMEOUT + 10`.
pub const MAX_TIMEOUTS: usize = TimeoutId::USER_TIMEOUT as usize + 10;

/// `TimeoutType` (`utils/timeout.h`).
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeoutType {
    TMPARAM_AFTER = 0,
    TMPARAM_AT = 1,
    TMPARAM_EVERY = 2,
}

/// `EnableTimeoutParams` (`utils/timeout.h`) — one entry for
/// `enable_timeouts`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnableTimeoutParams {
    pub id: TimeoutId,
    pub r#type: TimeoutType,
    /// Only used for `TMPARAM_AFTER`/`TMPARAM_EVERY`.
    pub delay_ms: i32,
    /// Only used for `TMPARAM_AT`.
    pub fin_time: TimestampTz,
}

/// `DisableTimeoutParams` (`utils/timeout.h`) — one entry for
/// `disable_timeouts`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DisableTimeoutParams {
    pub id: TimeoutId,
    /// Keep the indicator flag?
    pub keep_indicator: bool,
}
