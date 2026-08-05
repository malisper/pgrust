//! cryptohashfuncs.c: MD5 (hex text) + SHA-2 (bytea digest) halves.

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

fn md5_common(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text/bytea varlena; strict fn.
    let input = unsafe { fcinfo.arg_varlena_packed(0)? };
    let hexsum = pg_md5::pg_md5_hash(input.data());
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(varlena::cstring_to_text(mcx, &hexsum)?))
}

pub fn fc_md5_text(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    md5_common(fcinfo)
}

pub fn fc_md5_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    md5_common(fcinfo)
}

// bytea shares text's varlena image; only the type OID differs.
fn sha_common(
    fcinfo: &mut Fcinfo,
    digest: fn(&[u8]) -> Vec<u8>,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena; strict fn.
    let input = unsafe { fcinfo.arg_varlena_packed(0)? };
    let d = digest(input.data());
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(varlena::cstring_to_text(mcx, &d)?))
}

pub fn fc_sha224_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    sha_common(fcinfo, |d| pg_sha2::sha224(d).to_vec())
}

pub fn fc_sha256_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    sha_common(fcinfo, |d| pg_sha2::sha256(d).to_vec())
}

pub fn fc_sha384_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    sha_common(fcinfo, |d| pg_sha2::sha384(d).to_vec())
}

pub fn fc_sha512_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    sha_common(fcinfo, |d| pg_sha2::sha512(d).to_vec())
}

// pg_crc.c's SQL builtins ride here (the crc32c crate owns the kernels).
pub fn fc_crc32_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena; strict fn.
    let input = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i64(crc32c::traditional_crc32(input.data()) as i64))
}

pub fn fc_crc32c_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null bytea varlena; strict fn.
    let input = unsafe { fcinfo.arg_varlena_packed(0)? };
    let crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, input.data()));
    Ok(Datum::from_i64(crc as i64))
}

const fn b(foid: Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 1, strict: true, retset: false, func }
}

pub const CRYPTOHASH_BUILTINS: &[FmgrBuiltin] = &[
    b(2311, "md5_text", fc_md5_text),
    b(2321, "md5_bytea", fc_md5_bytea),
    b(3419, "sha224_bytea", fc_sha224_bytea),
    b(3420, "sha256_bytea", fc_sha256_bytea),
    b(3421, "sha384_bytea", fc_sha384_bytea),
    b(3422, "sha512_bytea", fc_sha512_bytea),
    b(6364, "crc32_bytea", fc_crc32_bytea),
    b(6365, "crc32c_bytea", fc_crc32c_bytea),
];
