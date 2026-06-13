//! `enum HotStandbyState` (`access/xlogutils.h`): the states a startup
//! process goes through, with respect to allowing connections during
//! recovery. Once `STANDBY_SNAPSHOT_READY` is reached, snapshots can be
//! taken and read-only queries can be run. The discriminants match the C
//! enum order (the `InHotStandby` test is a `>=` comparison).

pub type HotStandbyState = u32;

pub const STANDBY_DISABLED: HotStandbyState = 0;
pub const STANDBY_INITIALIZED: HotStandbyState = 1;
pub const STANDBY_SNAPSHOT_PENDING: HotStandbyState = 2;
pub const STANDBY_SNAPSHOT_READY: HotStandbyState = 3;

/// `#define InHotStandby (standbyState >= STANDBY_SNAPSHOT_PENDING)`
#[inline]
pub fn in_hot_standby(standby_state: HotStandbyState) -> bool {
    standby_state >= STANDBY_SNAPSHOT_PENDING
}
