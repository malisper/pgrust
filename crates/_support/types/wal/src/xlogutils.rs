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

/// `typedef enum { ... } XLogRedoAction` (`access/xlogutils.h`) — result codes
/// for `XLogReadBufferForRedo[Extended]`. Discriminants follow the C enum
/// declaration order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum XLogRedoAction {
    /// `BLK_NEEDS_REDO` — changes from the WAL record need to be applied.
    BlkNeedsRedo = 0,
    /// `BLK_DONE` — block is already up-to-date.
    BlkDone = 1,
    /// `BLK_RESTORED` — block was restored from a full-page image.
    BlkRestored = 2,
    /// `BLK_NOTFOUND` — block was not found (and hence does not need replay).
    BlkNotFound = 3,
}
