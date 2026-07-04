use crate::maps::local_tables::{ISO88592_2_WIN1250, WIN1250_2_ISO88592};
use crate::{
    latin2mic, latin2mic_with_table, local2local, mic2latin, mic2latin_with_table, ConvArgs,
    LC_ISO8859_2,
};
use datum::Datum;
use mbutils::check_encoding_conversion_args;
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use wchar::{PG_LATIN2, PG_MULE_INTERNAL, PG_WIN1250};

pub fn fc_latin2_to_mic(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(
        a.src_encoding,
        a.dest_encoding,
        a.len,
        PG_LATIN2,
        PG_MULE_INTERNAL,
    )?;
    let n = unsafe { latin2mic(a.src(), a.dest, LC_ISO8859_2, PG_LATIN2, a.no_error)? };
    Ok(Datum::from_i32(n))
}

pub fn fc_mic_to_latin2(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(
        a.src_encoding,
        a.dest_encoding,
        a.len,
        PG_MULE_INTERNAL,
        PG_LATIN2,
    )?;
    let n = unsafe { mic2latin(a.src(), a.dest, LC_ISO8859_2, PG_LATIN2, a.no_error)? };
    Ok(Datum::from_i32(n))
}

pub fn fc_win1250_to_mic(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(
        a.src_encoding,
        a.dest_encoding,
        a.len,
        PG_WIN1250,
        PG_MULE_INTERNAL,
    )?;
    let n = unsafe {
        latin2mic_with_table(
            a.src(),
            a.dest,
            LC_ISO8859_2,
            PG_WIN1250,
            &WIN1250_2_ISO88592,
            a.no_error,
        )?
    };
    Ok(Datum::from_i32(n))
}

pub fn fc_mic_to_win1250(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(
        a.src_encoding,
        a.dest_encoding,
        a.len,
        PG_MULE_INTERNAL,
        PG_WIN1250,
    )?;
    let n = unsafe {
        mic2latin_with_table(
            a.src(),
            a.dest,
            LC_ISO8859_2,
            PG_WIN1250,
            &ISO88592_2_WIN1250,
            a.no_error,
        )?
    };
    Ok(Datum::from_i32(n))
}

pub fn fc_latin2_to_win1250(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, PG_LATIN2, PG_WIN1250)?;
    let n = unsafe {
        local2local(
            a.src(),
            a.dest,
            PG_LATIN2,
            PG_WIN1250,
            &ISO88592_2_WIN1250,
            a.no_error,
        )?
    };
    Ok(Datum::from_i32(n))
}

pub fn fc_win1250_to_latin2(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, PG_WIN1250, PG_LATIN2)?;
    let n = unsafe {
        local2local(
            a.src(),
            a.dest,
            PG_WIN1250,
            PG_LATIN2,
            &WIN1250_2_ISO88592,
            a.no_error,
        )?
    };
    Ok(Datum::from_i32(n))
}
