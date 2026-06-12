//! `ScanDirection` (`access/sdir.h`).

pub type ScanDirection = i32;

pub const BackwardScanDirection: ScanDirection = -1;
pub const NoMovementScanDirection: ScanDirection = 0;
pub const ForwardScanDirection: ScanDirection = 1;
