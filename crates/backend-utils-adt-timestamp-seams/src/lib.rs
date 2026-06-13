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
    /// `timestamptz_to_str(t)` (timestamp.c): format a timestamp with ISO
    /// date style and the session timezone. The C writes a static buffer and
    /// never errors ("(timestamp out of range)" on conversion failure); the
    /// owned copy lands in `mcx`, so OOM of the copy is the only `Err`.
    pub fn timestamptz_to_str<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        t: TimestampTz,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);
