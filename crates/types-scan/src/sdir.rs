//! `ScanDirection` (`access/sdir.h`).

/// `enum ScanDirection` (`access/sdir.h:24`).
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ScanDirection {
    BackwardScanDirection = -1,
    NoMovementScanDirection = 0,
    ForwardScanDirection = 1,
}

pub use ScanDirection::*;

/// `ScanDirectionIsForward(direction)` (sdir.h).
pub const fn ScanDirectionIsForward(direction: ScanDirection) -> bool {
    matches!(direction, ScanDirection::ForwardScanDirection)
}

/// `ScanDirectionIsBackward(direction)` (sdir.h).
pub const fn ScanDirectionIsBackward(direction: ScanDirection) -> bool {
    matches!(direction, ScanDirection::BackwardScanDirection)
}
