use crate::maps::local_tables::{
    ISO2KOI, ISO2WIN1251, ISO2WIN866, KOI2ISO, KOI2WIN1251, KOI2WIN866, WIN12512ISO, WIN12512KOI,
    WIN12512WIN866, WIN8662ISO, WIN8662KOI, WIN8662WIN1251,
};
use crate::{
    latin2mic, latin2mic_with_table, local2local, mic2latin, mic2latin_with_table, ConvArgs,
    LC_KOI8_R,
};
use datum::Datum;
use mbutils::check_encoding_conversion_args;
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use wchar::{PG_ISO_8859_5, PG_KOI8R, PG_MULE_INTERNAL, PG_WIN1251, PG_WIN866};

pub fn fc_koi8r_to_mic(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(
        a.src_encoding,
        a.dest_encoding,
        a.len,
        PG_KOI8R,
        PG_MULE_INTERNAL,
    )?;
    let n = unsafe { latin2mic(a.src(), a.dest, LC_KOI8_R, PG_KOI8R, a.no_error)? };
    Ok(Datum::from_i32(n))
}

pub fn fc_mic_to_koi8r(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { ConvArgs::from(fcinfo) };
    check_encoding_conversion_args(
        a.src_encoding,
        a.dest_encoding,
        a.len,
        PG_MULE_INTERNAL,
        PG_KOI8R,
    )?;
    let n = unsafe { mic2latin(a.src(), a.dest, LC_KOI8_R, PG_KOI8R, a.no_error)? };
    Ok(Datum::from_i32(n))
}

macro_rules! with_table_pair {
    ($to_mic:ident, $from_mic:ident, $enc:expr, $to_tab:expr, $from_tab:expr) => {
        pub fn $to_mic(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = unsafe { ConvArgs::from(fcinfo) };
            check_encoding_conversion_args(
                a.src_encoding,
                a.dest_encoding,
                a.len,
                $enc,
                PG_MULE_INTERNAL,
            )?;
            let n = unsafe {
                latin2mic_with_table(a.src(), a.dest, LC_KOI8_R, $enc, $to_tab, a.no_error)?
            };
            Ok(Datum::from_i32(n))
        }

        pub fn $from_mic(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = unsafe { ConvArgs::from(fcinfo) };
            check_encoding_conversion_args(
                a.src_encoding,
                a.dest_encoding,
                a.len,
                PG_MULE_INTERNAL,
                $enc,
            )?;
            let n = unsafe {
                mic2latin_with_table(a.src(), a.dest, LC_KOI8_R, $enc, $from_tab, a.no_error)?
            };
            Ok(Datum::from_i32(n))
        }
    };
}

with_table_pair!(fc_iso_to_mic, fc_mic_to_iso, PG_ISO_8859_5, &ISO2KOI, &KOI2ISO);
with_table_pair!(
    fc_win1251_to_mic,
    fc_mic_to_win1251,
    PG_WIN1251,
    &WIN12512KOI,
    &KOI2WIN1251
);
with_table_pair!(
    fc_win866_to_mic,
    fc_mic_to_win866,
    PG_WIN866,
    &WIN8662KOI,
    &KOI2WIN866
);

macro_rules! local_pair {
    ($name:ident, $src:expr, $dst:expr, $tab:expr) => {
        pub fn $name(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = unsafe { ConvArgs::from(fcinfo) };
            check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, $src, $dst)?;
            let n = unsafe { local2local(a.src(), a.dest, $src, $dst, $tab, a.no_error)? };
            Ok(Datum::from_i32(n))
        }
    };
}

local_pair!(fc_koi8r_to_win1251, PG_KOI8R, PG_WIN1251, &KOI2WIN1251);
local_pair!(fc_win1251_to_koi8r, PG_WIN1251, PG_KOI8R, &WIN12512KOI);
local_pair!(fc_koi8r_to_win866, PG_KOI8R, PG_WIN866, &KOI2WIN866);
local_pair!(fc_win866_to_koi8r, PG_WIN866, PG_KOI8R, &WIN8662KOI);
local_pair!(fc_win866_to_win1251, PG_WIN866, PG_WIN1251, &WIN8662WIN1251);
local_pair!(fc_win1251_to_win866, PG_WIN1251, PG_WIN866, &WIN12512WIN866);
local_pair!(fc_iso_to_koi8r, PG_ISO_8859_5, PG_KOI8R, &ISO2KOI);
local_pair!(fc_koi8r_to_iso, PG_KOI8R, PG_ISO_8859_5, &KOI2ISO);
local_pair!(fc_iso_to_win1251, PG_ISO_8859_5, PG_WIN1251, &ISO2WIN1251);
local_pair!(fc_win1251_to_iso, PG_WIN1251, PG_ISO_8859_5, &WIN12512ISO);
local_pair!(fc_iso_to_win866, PG_ISO_8859_5, PG_WIN866, &ISO2WIN866);
local_pair!(fc_win866_to_iso, PG_WIN866, PG_ISO_8859_5, &WIN8662ISO);
