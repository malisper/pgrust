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
    /// `TimestampDifference(start_time, stop_time, *secs, *microsecs)` —
    /// the difference split into seconds and microseconds, clamped to zero
    /// when `stop_time <= start_time`.
    pub fn timestamp_difference(start_time: TimestampTz, stop_time: TimestampTz) -> (i64, i32)
);

seam_core::seam!(
    /// `TimestampDifferenceExceeds(start_time, stop_time, msec)`.
    pub fn timestamp_difference_exceeds(
        start_time: TimestampTz,
        stop_time: TimestampTz,
        msec: i32,
    ) -> bool
);

seam_core::seam!(
    /// `TimestampDifferenceMilliseconds(start_time, stop_time)` — the
    /// difference in milliseconds, clamped to the `[0, INT_MAX]` range.
    pub fn timestamp_difference_milliseconds(
        start_time: TimestampTz,
        stop_time: TimestampTz
    ) -> i64
);

seam_core::seam!(
    /// `timestamptz_to_str(ts)` — render a timestamptz to its display string
    /// (C returns a static buffer; the seam returns an owned copy).
    pub fn timestamptz_to_str(ts: TimestampTz) -> String
);
