//! ascii.c: to_ascii_default/enc/encname + ascii_safe_strlcpy.

use ::datum::{Datum, Varlena};
use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_UNDEFINED_OBJECT,
};
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use ::wchar::{pg_enc, pg_valid_encoding, PG_LATIN1, PG_LATIN2, PG_LATIN9, PG_WIN1250};

const RANGE_128: u8 = 128;
const RANGE_160: u8 = 160;

const LATIN1_MAP: &[u8; 96] =
    b"  cL Y  \"Ca  -R     'u .,      ?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
const LATIN2_MAP: &[u8; 96] =
    b" A L LS \"SSTZ-ZZ a,l'ls ,sstz\"zzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt.";
const LATIN9_MAP: &[u8; 96] =
    b"  cL YS sCa  -R     Zu .z   EeY?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
const WIN1250_MAP: &[u8; 128] =
    b"  ' \"    %S<STZZ `'\"\".--  s>stzz   L A  \"CS  -RZ  ,l'u .,as L\"lzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt ";

// pub: proofs/name-ascii Kani harness dual-executes this core against the
// verbatim C pg_to_ascii (proofs must call shipped code, never a copy).
pub fn pg_to_ascii(src: &[u8], dest: &mut [u8], enc: pg_enc) -> PgResult<()> {
    let (ascii, range): (&[u8], u8) = if enc == PG_LATIN1 {
        (LATIN1_MAP, RANGE_160)
    } else if enc == PG_LATIN2 {
        (LATIN2_MAP, RANGE_160)
    } else if enc == PG_LATIN9 {
        (LATIN9_MAP, RANGE_160)
    } else if enc == PG_WIN1250 {
        (WIN1250_MAP, RANGE_128)
    } else {
        return Err(conversion_not_supported(enc));
    };
    for (d, &x) in dest.iter_mut().zip(src) {
        *d = if x < 128 {
            x
        } else if x < range {
            b' '
        } else {
            ascii[(x - range) as usize]
        };
    }
    Ok(())
}

fn encode_to_ascii<'mcx>(mcx: Mcx<'mcx>, data: &[u8], enc: pg_enc) -> PgResult<Datum> {
    let mut out = varlena::image_with_header(mcx, data.len())?;
    let start = out.len();
    out.resize(start + data.len(), 0);
    pg_to_ascii(data, &mut out[start..], enc)?;
    Ok(varlena_result(Varlena::from_image(out)))
}

pub fn fc_to_ascii_encname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args (text, name); strict fn.
    let (data, name) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_name(1)) };
    let nul = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    let encname = core::str::from_utf8(&name[..nul]).unwrap_or("");
    let enc = mbutils::pg_char_to_encoding(encname);
    if enc < 0 {
        return Err(invalid_encoding_name(encname));
    }
    encode_to_ascii(fcinfo.result_mcx(), data.data(), enc)
}

pub fn fc_to_ascii_enc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is text; strict fn.
    let data = unsafe { fcinfo.arg_varlena_packed(0)? };
    let enc = fcinfo.arg_i32(1);
    if !pg_valid_encoding(enc) {
        return Err(invalid_encoding_code(enc));
    }
    encode_to_ascii(fcinfo.result_mcx(), data.data(), enc)
}

pub fn fc_to_ascii_default(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is text; strict fn.
    let data = unsafe { fcinfo.arg_varlena_packed(0)? };
    let enc = mbutils::GetDatabaseEncoding();
    encode_to_ascii(fcinfo.result_mcx(), data.data(), enc)
}

/// strlcpy-shaped: never errors (postmaster-reachable in C).
pub fn ascii_safe_strlcpy(dest: &mut [u8], src: &[u8]) {
    if dest.is_empty() {
        return;
    }
    let mut i = 0;
    while i + 1 < dest.len() && i < src.len() && src[i] != 0 {
        let ch = src[i];
        dest[i] = if (32..=127).contains(&ch) || ch == b'\n' || ch == b'\r' || ch == b'\t' {
            ch
        } else {
            b'?'
        };
        i += 1;
    }
    dest[i] = 0;
}

#[track_caller]
#[cold]
#[inline(never)]
fn conversion_not_supported(enc: pg_enc) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "encoding conversion from {} to ASCII not supported",
            mbutils::pg_encoding_to_char(enc)
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_encoding_name(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{name} is not a valid encoding name"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_encoding_code(enc: i32) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{enc} is not a valid encoding code"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const ASCII_BUILTINS: &[FmgrBuiltin] = &[
    b(1845, "to_ascii_default", 1, fc_to_ascii_default),
    b(1846, "to_ascii_enc", 2, fc_to_ascii_enc),
    b(1847, "to_ascii_encname", 2, fc_to_ascii_encname),
];

#[cfg(test)]
mod tests;
