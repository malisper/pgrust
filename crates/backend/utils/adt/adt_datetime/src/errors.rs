#![allow(non_snake_case)]

use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_CONFIG_FILE_ERROR,
    ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_INTERVAL_FIELD_OVERFLOW,
    ERRCODE_INVALID_DATETIME_FORMAT, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
};

use crate::consts::*;

fn lossy(b: Option<&[u8]>) -> String {
    String::from_utf8_lossy(b.unwrap_or_default()).into_owned()
}

#[cold]
pub fn DateTimeParseError(
    dterr: i32,
    extra: Option<&DateTimeErrorExtra<'_>>,
    str_: &str,
    datatype: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<()> {
    let err = match dterr {
        DTERR_FIELD_OVERFLOW => {
            PgError::error(format!("date/time field value out of range: \"{str_}\""))
                .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW)
        }
        DTERR_MD_FIELD_OVERFLOW => {
            PgError::error(format!("date/time field value out of range: \"{str_}\""))
                .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW)
                .with_hint("Perhaps you need a different \"DateStyle\" setting.")
        }
        DTERR_INTERVAL_OVERFLOW => {
            PgError::error(format!("interval field value out of range: \"{str_}\""))
                .with_sqlstate(ERRCODE_INTERVAL_FIELD_OVERFLOW)
        }
        DTERR_TZDISP_OVERFLOW => {
            PgError::error(format!("time zone displacement out of range: \"{str_}\""))
                .with_sqlstate(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE)
        }
        DTERR_BAD_TIMEZONE => {
            let zone = lossy(extra.and_then(|e| e.dtee_timezone));
            PgError::error(format!("time zone \"{zone}\" not recognized"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        }
        DTERR_BAD_ZONE_ABBREV => {
            let zone = lossy(extra.and_then(|e| e.dtee_timezone));
            let abbr = lossy(extra.and_then(|e| e.dtee_abbrev));
            PgError::error(format!("time zone \"{zone}\" not recognized"))
                .with_sqlstate(ERRCODE_CONFIG_FILE_ERROR)
                .with_detail(format!(
                    "This time zone name appears in the configuration file for time zone abbreviation \"{abbr}\"."
                ))
        }
        _ => PgError::error(format!("invalid input syntax for type {datatype}: \"{str_}\""))
            .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
    };
    ereturn(escontext, (), err)
}
