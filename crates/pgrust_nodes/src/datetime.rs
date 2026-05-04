pub const POSTGRES_EPOCH_JDATE: i32 = 2_451_545;
pub const USECS_PER_SEC: i64 = 1_000_000;
pub const USECS_PER_MINUTE: i64 = 60 * USECS_PER_SEC;
pub const USECS_PER_HOUR: i64 = 60 * USECS_PER_MINUTE;
pub const USECS_PER_DAY: i64 = 24 * USECS_PER_HOUR;
pub const SECS_PER_DAY: i32 = 86_400;
pub const MAX_TIME_PRECISION: i32 = 6;
pub const DATEVAL_NOBEGIN: i32 = i32::MIN;
pub const DATEVAL_NOEND: i32 = i32::MAX;
pub const TIMESTAMP_NOBEGIN: i64 = i64::MIN;
pub const TIMESTAMP_NOEND: i64 = i64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateADT(pub i32);

impl DateADT {
    pub const fn is_finite(self) -> bool {
        self.0 != DATEVAL_NOBEGIN && self.0 != DATEVAL_NOEND
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimeADT(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeTzADT {
    pub time: TimeADT,
    pub offset_seconds: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampADT(pub i64);

impl TimestampADT {
    pub const fn is_finite(self) -> bool {
        self.0 != TIMESTAMP_NOBEGIN && self.0 != TIMESTAMP_NOEND
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampTzADT(pub i64);

impl TimestampTzADT {
    pub const fn is_finite(self) -> bool {
        self.0 != TIMESTAMP_NOBEGIN && self.0 != TIMESTAMP_NOEND
    }
}
