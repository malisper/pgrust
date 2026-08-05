use datum::{Datum, Varlena, VARHDRSZ};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
};
use wchar::pg_enc;

// C returns namein(DatabaseEncoding->name): a NAMEDATALEN block.
pub fn fc_getdatabaseencoding(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let name = crate::GetDatabaseEncodingName();
    let mut buf = [0u8; 64];
    buf[..name.len()].copy_from_slice(name.as_bytes());
    byref_result(fcinfo.result_mcx(), &buf)
}

fn name_str(name: &[u8; 64]) -> &str {
    let n = name.iter().position(|&b| b == 0).unwrap_or(64);
    core::str::from_utf8(&name[..n]).unwrap_or("")
}

#[track_caller]
#[cold]
fn invalid_encoding_name(which: &str, name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("invalid {which} encoding name \"{name}\""))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

fn convert_common(
    fcinfo: &mut Fcinfo,
    string_arg: usize,
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
) -> PgResult<Datum> {
    let string = unsafe { fcinfo.arg_varlena_packed(string_arg)? };
    let src = string.data();
    crate::pg_verify_mbstr(src_encoding, src, false)?;
    let mcx = fcinfo.result_mcx();
    match crate::pg_do_encoding_conversion(mcx, src, src_encoding, dest_encoding)? {
        None => Ok(fcinfo.arg(string_arg)),
        Some(out) => {
            let mut image = ::mcx::vec_with_capacity_in(mcx, VARHDRSZ + out.len())?;
            ::mcx::vec_append_bytes(&mut image, &[0u8; VARHDRSZ])?;
            ::mcx::vec_append_bytes(&mut image, &out)?;
            Ok(varlena_result(Varlena::from_image(image)))
        }
    }
}

// BYTEA convert(BYTEA string, NAME src_encoding_name, NAME dest_encoding_name)
pub fn fc_pg_convert(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let src_name = name_str(unsafe { fcinfo.arg_name(1) });
    let src_encoding = crate::pg_char_to_encoding(src_name);
    if src_encoding < 0 {
        return Err(invalid_encoding_name("source", src_name));
    }
    let dest_name = name_str(unsafe { fcinfo.arg_name(2) });
    let dest_encoding = crate::pg_char_to_encoding(dest_name);
    if dest_encoding < 0 {
        return Err(invalid_encoding_name("destination", dest_name));
    }
    convert_common(fcinfo, 0, src_encoding, dest_encoding)
}

// TEXT convert_from(BYTEA string, NAME src_encoding_name); dest = DB encoding.
pub fn fc_pg_convert_from(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let src_name = name_str(unsafe { fcinfo.arg_name(1) });
    let src_encoding = crate::pg_char_to_encoding(src_name);
    if src_encoding < 0 {
        return Err(invalid_encoding_name("source", src_name));
    }
    convert_common(fcinfo, 0, src_encoding, crate::GetDatabaseEncoding())
}

// BYTEA convert_to(TEXT string, NAME dest_encoding_name); src = DB encoding.
pub fn fc_pg_convert_to(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let dest_name = name_str(unsafe { fcinfo.arg_name(1) });
    let dest_encoding = crate::pg_char_to_encoding(dest_name);
    if dest_encoding < 0 {
        return Err(invalid_encoding_name("destination", dest_name));
    }
    convert_common(fcinfo, 0, crate::GetDatabaseEncoding(), dest_encoding)
}

// pg_encoding_max_length_sql (mbutils.c): NULL for an invalid encoding number.
pub fn fc_pg_encoding_max_length(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let encoding = fcinfo.arg_i32(0);
    if wchar::pg_valid_encoding(encoding) {
        Ok(Datum::from_i32(wchar::pg_encoding_max_length(encoding)))
    } else {
        Ok(fcinfo.return_null())
    }
}

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending.
pub const MBUTILS_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 1039,
        name: "getdatabaseencoding",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_getdatabaseencoding,
    },
    FmgrBuiltin {
        foid: 1714,
        name: "pg_convert_from",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_pg_convert_from,
    },
    FmgrBuiltin {
        foid: 1717,
        name: "pg_convert_to",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_pg_convert_to,
    },
    FmgrBuiltin {
        foid: 1813,
        name: "pg_convert",
        nargs: 3,
        strict: true,
        retset: false,
        func: fc_pg_convert,
    },
    FmgrBuiltin {
        foid: 2319,
        name: "pg_encoding_max_length",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_encoding_max_length,
    },
];
