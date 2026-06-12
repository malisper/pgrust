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
    /// `TimestampDifference(start_time, stop_time, *secs, *microsecs)`
    /// (`utils/adt/timestamp.c`) — elapsed time between two timestamps,
    /// clamped to zero if `stop_time <= start_time`. The C out-parameters
    /// `(long *secs, int *microsecs)` are the returned tuple.
    pub fn timestamp_difference(start_time: TimestampTz, stop_time: TimestampTz) -> (i64, i32)
);
