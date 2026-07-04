use crate::{latin2mic, mic2latin, ConvArgs, LC_ISO8859_1, LC_ISO8859_3, LC_ISO8859_4};
use datum::Datum;
use mbutils::check_encoding_conversion_args;
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use wchar::{PG_LATIN1, PG_LATIN3, PG_LATIN4, PG_MULE_INTERNAL};

macro_rules! latin_mic_pair {
    ($to_mic:ident, $from_mic:ident, $enc:expr, $lc:expr) => {
        pub fn $to_mic(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = unsafe { ConvArgs::from(fcinfo) };
            check_encoding_conversion_args(
                a.src_encoding,
                a.dest_encoding,
                a.len,
                $enc,
                PG_MULE_INTERNAL,
            )?;
            let n = unsafe { latin2mic(a.src(), a.dest, $lc, $enc, a.no_error)? };
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
            let n = unsafe { mic2latin(a.src(), a.dest, $lc, $enc, a.no_error)? };
            Ok(Datum::from_i32(n))
        }
    };
}

latin_mic_pair!(fc_latin1_to_mic, fc_mic_to_latin1, PG_LATIN1, LC_ISO8859_1);
latin_mic_pair!(fc_latin3_to_mic, fc_mic_to_latin3, PG_LATIN3, LC_ISO8859_3);
latin_mic_pair!(fc_latin4_to_mic, fc_mic_to_latin4, PG_LATIN4, LC_ISO8859_4);
