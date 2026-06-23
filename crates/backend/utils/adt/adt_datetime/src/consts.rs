//! Crate-local date/time constants that are not part of the shared ABI.
//!
//! The ABI-relevant struct field codes and DTK_* token codes live in
//! `types::datetime`; this module holds the derived convenience masks and the
//! string-token defaults from `utils/datetime.h` that only the decode / encode
//! code paths need.

use ::types_datetime::{DTK_M, HOUR, MICROSECOND, MILLISECOND, MINUTE, SECOND};

// ---------------------------------------------------------------------------
// Convenience bit-mask combinations. (utils/datetime.h)
// ---------------------------------------------------------------------------

/// A second, plus any fractional component.
pub const DTK_ALL_SECS_M: i32 = DTK_M(SECOND) | DTK_M(MILLISECOND) | DTK_M(MICROSECOND);
/// YEAR | MONTH | DAY.
pub const DTK_DATE_M: i32 =
    DTK_M(::types_datetime::YEAR) | DTK_M(::types_datetime::MONTH) | DTK_M(::types_datetime::DAY);
/// HOUR | MINUTE | all seconds.
pub const DTK_TIME_M: i32 = DTK_M(HOUR) | DTK_M(MINUTE) | DTK_ALL_SECS_M;

// ---------------------------------------------------------------------------
// Default output time-quantity strings. (utils/datetime.h)
// ---------------------------------------------------------------------------

pub const DAGO: &str = "ago";
pub const DCURRENT: &str = "current";
pub const EPOCH: &str = "epoch";
pub const INVALID: &str = "invalid";
pub const EARLY: &str = "-infinity";
pub const LATE: &str = "infinity";
pub const NOW: &str = "now";
pub const TODAY: &str = "today";
pub const TOMORROW: &str = "tomorrow";
pub const YESTERDAY: &str = "yesterday";
pub const ZULU: &str = "zulu";

pub const DMICROSEC: &str = "usecond";
pub const DMILLISEC: &str = "msecond";
pub const DSECOND: &str = "second";
pub const DMINUTE: &str = "minute";
pub const DHOUR: &str = "hour";
pub const DDAY: &str = "day";
pub const DWEEK: &str = "week";
pub const DMONTH: &str = "month";
pub const DQUARTER: &str = "quarter";
pub const DYEAR: &str = "year";
pub const DDECADE: &str = "decade";
pub const DCENTURY: &str = "century";
pub const DMILLENNIUM: &str = "millennium";
pub const DA_D: &str = "ad";
pub const DB_C: &str = "bc";
pub const DTIMEZONE: &str = "timezone";

#[cfg(test)]
mod tests {
    use super::*;
    use ::types_datetime::{DAY, MONTH, YEAR};

    #[test]
    fn date_mask_matches_field_bits() {
        assert_eq!(DTK_DATE_M, (1 << YEAR) | (1 << MONTH) | (1 << DAY));
    }

    #[test]
    fn all_secs_mask_matches_field_bits() {
        assert_eq!(
            DTK_ALL_SECS_M,
            (1 << SECOND) | (1 << MILLISECOND) | (1 << MICROSECOND)
        );
    }
}
