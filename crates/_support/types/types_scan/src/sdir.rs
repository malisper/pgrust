// C values -1/0/1 preserved: ScanDirectionCombine is integer multiplication.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScanDirection {
    BackwardScanDirection = -1,
    NoMovementScanDirection = 0,
    ForwardScanDirection = 1,
}

pub use ScanDirection::{BackwardScanDirection, ForwardScanDirection, NoMovementScanDirection};

pub const fn ScanDirectionCombine(a: ScanDirection, b: ScanDirection) -> ScanDirection {
    match (a as i32) * (b as i32) {
        -1 => BackwardScanDirection,
        1 => ForwardScanDirection,
        _ => NoMovementScanDirection,
    }
}

pub const fn ScanDirectionIsValid(direction: ScanDirection) -> bool {
    matches!(
        direction,
        BackwardScanDirection | NoMovementScanDirection | ForwardScanDirection
    )
}

pub const fn ScanDirectionIsForward(direction: ScanDirection) -> bool {
    matches!(direction, ForwardScanDirection)
}

pub const fn ScanDirectionIsBackward(direction: ScanDirection) -> bool {
    matches!(direction, BackwardScanDirection)
}

pub const fn ScanDirectionIsNoMovement(direction: ScanDirection) -> bool {
    matches!(direction, NoMovementScanDirection)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combine_and_predicates() {
        assert_eq!(
            ScanDirectionCombine(ForwardScanDirection, BackwardScanDirection),
            BackwardScanDirection
        );
        assert_eq!(
            ScanDirectionCombine(BackwardScanDirection, BackwardScanDirection),
            ForwardScanDirection
        );
        assert_eq!(
            ScanDirectionCombine(NoMovementScanDirection, ForwardScanDirection),
            NoMovementScanDirection
        );
        assert!(ScanDirectionIsForward(ForwardScanDirection));
        assert!(ScanDirectionIsBackward(BackwardScanDirection));
        assert!(ScanDirectionIsNoMovement(NoMovementScanDirection));
        assert!(ScanDirectionIsValid(BackwardScanDirection));
        assert_eq!(BackwardScanDirection as i32, -1);
        assert_eq!(NoMovementScanDirection as i32, 0);
        assert_eq!(ForwardScanDirection as i32, 1);
    }
}
