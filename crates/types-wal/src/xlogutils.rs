//! Recovery-side vocabulary from `access/xlogutils.h`.

/// `HotStandbyState` (`access/xlogutils.h`) — how far along we are in hot
/// standby. Ordered: C compares with `>=` (`InHotStandby`).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(i32)]
pub enum HotStandbyState {
    STANDBY_DISABLED,
    STANDBY_INITIALIZED,
    STANDBY_SNAPSHOT_PENDING,
    STANDBY_SNAPSHOT_READY,
}

pub use HotStandbyState::*;
