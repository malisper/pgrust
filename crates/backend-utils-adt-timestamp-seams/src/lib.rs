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
    /// Syntax-only timestamp parse used by `check_recovery_target_time`
    /// (xlogrecovery.c:4948): `ParseDateTime` + `DecodeDateTime` and a final
    /// `tm2timestamp` range check, returning `true` when the string parses to a
    /// `DTK_DATE` timestamp in range. The time-zone-dependent final parse is
    /// deferred to `timestamptz_in` at assign time. Owned by `timestamp.c`.
    pub fn parse_recovery_target_time(newval: String) -> bool
);

seam_core::seam!(
    /// `void TimestampDifference(TimestampTz start, TimestampTz stop,
    /// long *secs, int *microsecs)` (`timestamp.c`) — returns `(secs, usecs)`
    /// of `stop - start`, clamped to 0 when negative.
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

seam_core::seam!(
    /// `bool TimestampDifferenceExceedsSeconds(TimestampTz start,
    /// TimestampTz stop, int threshold_sec)` (`timestamp.c`).
    pub fn timestamp_difference_exceeds_seconds(
        start_time: TimestampTz,
        stop_time: TimestampTz,
        threshold_sec: i32,
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
    /// `JsonEncodeDateTime(buf, value, typid, tzp)` (json.c:309) — encode a
    /// date/time `Datum` into ISO format (forcing XSD date style), returning the
    /// formatted string. `tzp`, if `Some`, is the time-zone offset in seconds
    /// for `timestamptz`. The body is entirely the
    /// `backend/utils/adt/{date,time,timestamp}.c` field conversions
    /// (`j2date`/`time2tm`/`timetz2tm`/`timestamp2tm`) plus the `Encode*`
    /// routines, so the whole operation is owned by the datetime subsystem.
    /// `Err` carries the C `DTERR_*` → `ereport(ERROR, "... out of range")`.
    ///
    /// Datum-unification: `value` is the canonical unified value
    /// (`types_tuple::Datum<'mcx>`), passed by reference. A datetime is a
    /// by-value word (`ByVal`), so the owner reads it via the `Datum`
    /// conversion methods.
    pub fn json_encode_datetime<'mcx>(
        value: &types_tuple::Datum<'mcx>,
        typid: types_core::Oid,
        tzp: Option<i32>,
    ) -> types_error::PgResult<String>
);
