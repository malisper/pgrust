use types_core::TimestampTz;
use types_error::PgResult;

seam_core::seam!(
    pub fn get_current_timestamp() -> TimestampTz
);

/// Flat pg_tm snapshot (adt_datetime's pg_tm here would cycle) + fsec/tz.
#[derive(Clone, Copy, Debug, Default)]
pub struct CurrentTimeUsec {
    pub tm_sec: i32,
    pub tm_min: i32,
    pub tm_hour: i32,
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
    pub tm_wday: i32,
    pub tm_yday: i32,
    pub tm_isdst: i32,
    pub tm_gmtoff: i64,
    pub tm_zone: Option<&'static str>,
    pub fsec: i32,
    pub tz: i32,
}

seam_core::seam!(
    pub fn get_current_datetime() -> PgResult<CurrentTimeUsec>
);

seam_core::seam!(
    pub fn get_current_time_usec() -> PgResult<CurrentTimeUsec>
);

// timestamp.c timestamptz_to_str: elog/debug formatting of a TimestampTz.
seam_core::seam!(
    pub fn timestamptz_to_str(t: TimestampTz) -> String
);
