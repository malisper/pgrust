//! Seam declarations for the `backend-utils-adt-timestamp` unit
//! (`utils/adt/timestamp.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::TimestampTz;

seam_core::seam!(
    /// `GetCurrentTimestamp()` (`utils/adt/timestamp.c`).
    pub fn get_current_timestamp() -> TimestampTz
);

seam_core::seam!(
    /// `void TimestampDifference(TimestampTz start, TimestampTz stop,
    /// long *secs, int *microsecs)` (`timestamp.c`) — returns `(secs, usecs)`
    /// of `stop - start`, clamped to 0 when negative.
    pub fn timestamp_difference(start_time: TimestampTz, stop_time: TimestampTz) -> (i64, i32)
);

seam_core::seam!(
    /// `bool TimestampDifferenceExceedsSeconds(TimestampTz start,
    /// TimestampTz stop, int threshold_sec)` (`timestamp.c`).
    pub fn timestamp_difference_exceeds_seconds(
        start_time: TimestampTz,
        stop_time: TimestampTz,
        threshold_sec: i32,
    ) -> bool
);
