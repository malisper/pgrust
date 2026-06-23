//! Seam declarations for the `backend-utils-adt-timestamp` unit
//! (`utils/adt/timestamp.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::TimestampTz;
use types_datetime::{fsec_t, pg_itm, Interval, TimeADT, TimeTzADT, Timestamp, Timestamp2TmResult};
use pgtime::pg_tm;

seam_core::seam!(
    /// `GetCurrentTimestamp()` (`utils/adt/timestamp.c`).
    pub fn get_current_timestamp() -> TimestampTz
);

seam_core::seam!(
    /// `TimestampTimestampTzRequiresRewrite()` (`utils/adt/timestamp.c:6435`):
    /// returns `false` when the current `TimeZone` GUC makes
    /// `timestamp_timestamptz` / `timestamptz_timestamp` no-ops (session
    /// timezone offset is 0), so an ALTER COLUMN TYPE between the two needs no
    /// heap rewrite. Read by `ATColumnChangeRequiresRewrite` (tablecmds.c).
    pub fn timestamp_timestamptz_requires_rewrite() -> bool
);

seam_core::seam!(
    /// `timestamptz_to_time_t(t)` (`utils/adt/timestamp.c`): convert a
    /// `TimestampTz` (microseconds since the PG epoch, UTC) to a `pg_time_t`
    /// (seconds since the Unix epoch). Pure arithmetic; never errors. Used by
    /// `InitProcessGlobals` to set `MyStartTime` from `MyStartTimestamp`.
    pub fn timestamptz_to_time_t(t: TimestampTz) -> types_core::pg_time_t
);

seam_core::seam!(
    /// `timestamptz_pl_interval(timestamp, span)` (`utils/adt/timestamp.c`):
    /// add an [`Interval`] to a [`TimestampTz`], the SQL `timestamptz + interval`
    /// operator. uuid.c reaches it via `DirectFunctionCall2` from
    /// `uuidv7(interval)`. Owned by `timestamp.c`; `Err` carries its
    /// out-of-range `ereport(ERROR)`.
    pub fn timestamptz_pl_interval(
        timestamp: TimestampTz,
        span: Interval,
    ) -> types_error::PgResult<TimestampTz>
);

seam_core::seam!(
    /// `interval_lerp(lo, hi, pct)` (`utils/adt/orderedsetaggs.c`): linearly
    /// interpolate between two `interval` values — `lo + pct*(hi - lo)` — built
    /// from the `interval_mi`/`interval_mul`/`interval_pl` arithmetic owned by
    /// `timestamp.c` (C: `DirectFunctionCall2(interval_mi, hi, lo)` etc.). The
    /// ordered-set `percentile_cont(interval)` finalfn reaches it this way. `Err`
    /// carries the `interval out of range` `ereport(ERROR)` the arithmetic raises.
    pub fn interval_lerp(
        lo: Interval,
        hi: Interval,
        pct: f64,
    ) -> types_error::PgResult<Interval>
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
    /// `DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in, ...))` — the full
    /// `timestamptz_in` conversion of `recovery_target_time_string`, run by
    /// `validateRecoveryParameters` (xlogrecovery.c:1168). Distinct from the
    /// syntax-only `parse_recovery_target_time`: this returns the actual
    /// `TimestampTz` (time-zone-dependent), `Err` carrying the
    /// `ereport(ERROR)` for an unparsable/out-of-range value. Owned by
    /// `timestamp.c`.
    pub fn recovery_target_timestamptz_in(
        newval: String,
    ) -> types_error::PgResult<TimestampTz>
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

seam_core::seam!(
    /// `timestamp2tm(dt, &tzp, tm, &fsec, &tzn, attimezone)` (timestamp.c):
    /// break a `Timestamp`/`TimestampTz` down into `tm`/`fsec` (+ zone when
    /// `want_tz`). C returns `0`/`-1`; `Err(())` is the `-1` the caller maps to
    /// "timestamp out of range".
    pub fn timestamp2tm(dt: Timestamp, want_tz: bool) -> Result<Timestamp2TmResult, ()>
);

seam_core::seam!(
    /// `tm2timestamp(tm, fsec, tzp, &result)` (timestamp.c): assemble a
    /// `Timestamp` from broken-down time. `tz` is `Some` for the timestamptz
    /// case. `Err(())` is the C `-1` out-of-range return.
    pub fn tm2timestamp(tm: &pg_tm, fsec: fsec_t, tz: Option<i32>) -> Result<Timestamp, ()>
);

seam_core::seam!(
    /// `interval2itm(span, itm)` (timestamp.c): expand an `Interval` into the
    /// broken-down `pg_itm` form the DCH interval formatter consumes.
    pub fn interval2itm(span: Interval) -> pg_itm
);

seam_core::seam!(
    /// `tm2time(tm, fsec, &result)` (date.c): assemble a `TimeADT` from
    /// broken-down time.
    pub fn tm2time(tm: &pg_tm, fsec: fsec_t) -> TimeADT
);

seam_core::seam!(
    /// `tm2timetz(tm, fsec, tz, &result)` (date.c): assemble a `TimeTzADT`.
    pub fn tm2timetz(tm: &pg_tm, fsec: fsec_t, tz: i32) -> TimeTzADT
);

seam_core::seam!(
    /// `AdjustTimestampForTypmod(&time, typmod, NULL)` (timestamp.c): round a
    /// `Timestamp` to the typmod's sub-second precision. `Err` carries the C
    /// `ereport(ERROR, "timestamp out of range")`.
    pub fn adjust_timestamp_for_typmod(value: Timestamp, typmod: i32) -> types_error::PgResult<Timestamp>
);

seam_core::seam!(
    /// `AdjustTimeForTypmod(&time, typmod)` (date.c): round a `TimeADT` to the
    /// typmod's sub-second precision. Infallible in C.
    pub fn adjust_time_for_typmod(time: TimeADT, typmod: i32) -> TimeADT
);
