//! `ScanDirection` (`access/sdir.h`).

/// `ScanDirection` (`access/sdir.h`) — a real C enum with explicit values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ScanDirection {
    BackwardScanDirection = -1,
    NoMovementScanDirection = 0,
    ForwardScanDirection = 1,
}

pub use ScanDirection::{BackwardScanDirection, ForwardScanDirection, NoMovementScanDirection};

/// `ScanDirectionIsValid(direction)` (sdir.h).
pub const fn ScanDirectionIsValid(direction: ScanDirection) -> bool {
    matches!(
        direction,
        BackwardScanDirection | NoMovementScanDirection | ForwardScanDirection
    )
}

/// `ScanDirectionIsForward(direction)` (sdir.h).
pub const fn ScanDirectionIsForward(direction: ScanDirection) -> bool {
    matches!(direction, ForwardScanDirection)
}

/// `ScanDirectionIsBackward(direction)` (sdir.h).
pub const fn ScanDirectionIsBackward(direction: ScanDirection) -> bool {
    matches!(direction, BackwardScanDirection)
}

/// `ScanDirectionIsNoMovement(direction)` (sdir.h).
pub const fn ScanDirectionIsNoMovement(direction: ScanDirection) -> bool {
    matches!(direction, NoMovementScanDirection)
}
