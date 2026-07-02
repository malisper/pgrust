//! datetime.c core: ParseDateTime/DecodeDateTime/DecodeTimeOnly and helpers,
//! the datetkn keyword tables, and EncodeDateOnly/TimeOnly/DateTime.
//! Zero-allocation by construction: fields are borrowed slices of a
//! caller-owned workbuf (C's stack `workbuf[]`), outputs write into a
//! caller-owned `MAXDATELEN` buffer.

pub mod calendar;
pub mod consts;
pub mod decode;
pub mod encode;
pub mod errors;
pub mod settings;
pub mod tables;
pub mod tz;

pub use calendar::{date2j, isleap, j2date, j2day, DAYS, DAY_TAB, MONTHS};
pub use consts::*;
pub use decode::{
    datebsearch, dt2time, time_overflows, CheckDateTokenTables, ClearTimeZoneAbbrevCache,
    DecodeDate, DecodeDateTime, DecodeNumber, DecodeNumberField, DecodeSpecial, DecodeTime,
    DecodeTimeOnly, DecodeTimezone, DecodeTimezoneAbbrev, DecodeUnits, ParseDateTime,
    ParseFraction, ParseFractionalSecond, ValidateDate,
};
pub use encode::{
    AppendSeconds, EncodeDateOnly, EncodeDateTime, EncodeTimeOnly, EncodeTimezone,
};
pub use errors::DateTimeParseError;
pub use settings::{
    date_order, date_style, interval_style, set_date_order, set_date_style, set_interval_style,
};
pub use tables::{DATETKTBL, DELTATKTBL};
