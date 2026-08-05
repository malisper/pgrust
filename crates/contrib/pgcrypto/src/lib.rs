
mod cipher;
mod crypt;
mod hashing;
mod pgp;

use datum::Datum;
use elog::ereport;
use types_error::{ErrorLocation, PgError, PgResult, NOTICE,
    ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

const LIBRARY: &str = "pgcrypto";

fn bytea_result(fcinfo: &mut Fcinfo, bytes: &[u8]) -> PgResult<Datum> {
    let img = varlena::cstring_to_text(fcinfo.result_mcx(), bytes)?;
    Ok(types_fmgr::varlena_result(img))
}

fn px_err(msg: String) -> Box<PgError> {
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE).into()
}

fn crypt_err(e: crypt::CryptError) -> Box<PgError> {
    match e {
        crypt::CryptError::Unsupported(what) => PgError::error(format!("pgcrypto: {what} not yet ported"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .into(),
        // C's genuine-NULL path is pgcrypto.c:234: px_crypt returned NULL and
        // pg_crypt raises 39000 (ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION)
        // — EXECUTED on stock 18.3 (crypt('foox','$2$') => ERROR 39000),
        // captured twice 2026-08-01. Every other crypt/gen_salt message
        // ("invalid salt", "gen_salt: ...") is 22023 in C, which px_err gives.
        crypt::CryptError::Message(m) if m == "crypt(3) returned NULL" => PgError::error(m)
            .with_sqlstate(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION)
            .into(),
        crypt::CryptError::Message(m) => px_err(m),
        // Interrupts / C-parity ereports pass through with their own SQLSTATE.
        crypt::CryptError::Pg(e) => e,
    }
}

fn fc_pg_digest(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 bytea/text (implicit-cast), arg1 text, both non-null.
    let (data, name) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let name = String::from_utf8_lossy(name.data()).into_owned();
    let out = hashing::digest(&name, data.data()).map_err(px_err)?;
    bytea_result(fcinfo, &out)
}

fn fc_pg_hmac(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 data, arg1 key (bytea/text), arg2 type text.
    let (data, key, name) = unsafe {
        (
            fcinfo.arg_varlena_packed(0)?,
            fcinfo.arg_varlena_packed(1)?,
            fcinfo.arg_varlena_packed(2)?,
        )
    };
    let name = String::from_utf8_lossy(name.data()).into_owned();
    let out = hashing::hmac(&name, key.data(), data.data()).map_err(px_err)?;
    bytea_result(fcinfo, &out)
}

fn check_builtin_crypto() -> PgResult<()> {
    let mode = guc::GetConfigOption("pgcrypto.builtin_crypto_enabled", true, false)?;
    if matches!(mode.as_deref(), Some("off")) {
        return Err(PgError::error("use of built-in crypto functions is disabled").into());
    }
    Ok(())
}

fn fc_pg_gen_salt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    check_builtin_crypto()?;
    // SAFETY: strict fn — arg0 text.
    let ty = unsafe { fcinfo.arg_varlena_packed(0)? };
    let ty = String::from_utf8_lossy(ty.data()).into_owned();
    let s = crypt::gen_salt(&ty, 0).map_err(crypt_err)?;
    bytea_result(fcinfo, s.as_bytes())
}

fn fc_pg_gen_salt_rounds(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    check_builtin_crypto()?;
    // SAFETY: strict fn — arg0 text, arg1 int4.
    let ty = unsafe { fcinfo.arg_varlena_packed(0)? };
    let ty = String::from_utf8_lossy(ty.data()).into_owned();
    let rounds = fcinfo.arg_i32(1);
    let s = crypt::gen_salt(&ty, rounds).map_err(crypt_err)?;
    bytea_result(fcinfo, s.as_bytes())
}

fn fc_pg_crypt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    check_builtin_crypto()?;
    // SAFETY: strict fn — arg0 password text, arg1 salt text.
    let (pw, salt) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    // D21: C's pg_crypt goes text_to_cstring -> px_crypt -> cstring_to_text
    // with NO encoding validation anywhere — password, salt, and result are
    // raw bytes (the result even echoes setting-prefix bytes verbatim).
    // Laundering through from_utf8_lossy collapsed distinct non-UTF-8
    // passwords onto U+FFFD and rewrote C's result bytes.
    let s = crypt::crypt(pw.data(), salt.data()).map_err(crypt_err)?;
    bytea_result(fcinfo, &s)
}

fn cipher_err(op: &str, e: cipher::CipherError) -> Box<PgError> {
    let msg = match e {
        cipher::CipherError::NoCipher(spec) => format!("Cannot use \"{spec}\": No such cipher algorithm"),
        cipher::CipherError::EncryptFailed => format!("{op} error: Encryption failed"),
        cipher::CipherError::DecryptFailed => format!("{op} error: Decryption failed"),
    };
    px_err(msg)
}

macro_rules! fc_cipher {
    ($fname:ident, $core:ident, $op:literal, $with_iv:literal) => {
        fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: strict fn — bytea/bytea[/bytea] then text spec, all non-null.
            let out = if $with_iv {
                let (data, key, iv, spec) = unsafe {
                    (
                        fcinfo.arg_varlena_packed(0)?,
                        fcinfo.arg_varlena_packed(1)?,
                        fcinfo.arg_varlena_packed(2)?,
                        fcinfo.arg_varlena_packed(3)?,
                    )
                };
                let spec = String::from_utf8_lossy(spec.data()).into_owned();
                cipher::$core(&spec, key.data(), iv.data(), data.data())
            } else {
                let (data, key, spec) = unsafe {
                    (
                        fcinfo.arg_varlena_packed(0)?,
                        fcinfo.arg_varlena_packed(1)?,
                        fcinfo.arg_varlena_packed(2)?,
                    )
                };
                let spec = String::from_utf8_lossy(spec.data()).into_owned();
                cipher::$core(&spec, key.data(), &[], data.data())
            };
            let out = out.map_err(|e| cipher_err($op, e))?;
            bytea_result(fcinfo, &out)
        }
    };
}

fc_cipher!(fc_pg_encrypt, encrypt, "encrypt", false);
fc_cipher!(fc_pg_decrypt, decrypt, "decrypt", false);
fc_cipher!(fc_pg_encrypt_iv, encrypt, "encrypt_iv", true);
fc_cipher!(fc_pg_decrypt_iv, decrypt, "decrypt_iv", true);

fn fc_pg_random_bytes(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let len = fcinfo.arg_i32(0);
    if !(1..=1024).contains(&len) {
        return Err(PgError::error("Length not in range")
            .with_sqlstate(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION)
            .into());
    }
    let mut buf = vec![0u8; len as usize];
    if !pg_strong_random::pg_strong_random(&mut buf) {
        return Err(px_err("Failed to generate random data".to_string()));
    }
    bytea_result(fcinfo, &buf)
}

fn fc_pg_random_uuid(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let uuid = adt_uuid::gen_random_uuid()?;
    types_fmgr::byref_result(fcinfo.result_mcx(), &uuid)
}

fn fc_pg_check_fipsmode(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(false))
}


#[track_caller]
fn here(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report OUR source site (call site via track_caller).
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn pgp_notice(msg: &str) {
    let _ = ereport(NOTICE).errmsg(msg.to_string()).finish(here("pgp_decrypt"));
}

// pgcrypto px_THROW text is rendered verbatim (byte-identical to C's ERROR).
//
// SQLSTATE follows px_THROW_ERROR (px.c:94-109) exactly: every px error is
// ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION (39000) EXCEPT PXE_NO_RANDOM,
// which C raises as ERRCODE_INTERNAL_ERROR (XX000). Stamping 22023 here — as
// this did until the p1-pgcryptofam differential caught it on the pg_dearmor
// arm — mis-states the code on every px-throwing path (dearmor,
// pgp_armor_headers, and the four pgp sym/pub wrappers).
fn px_msg(msg: &str) -> Box<PgError> {
    let sqlstate = if msg == PXE_NO_RANDOM_MSG {
        ERRCODE_INTERNAL_ERROR
    } else {
        ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION
    };
    PgError::error(msg.to_string()).with_sqlstate(sqlstate).into()
}

/// C's `PXE_NO_RANDOM` message (px.c:96-101) — the one px error whose
/// SQLSTATE is XX000 rather than 39000.
const PXE_NO_RANDOM_MSG: &str = "could not generate a random number";

fn opt_arg_bytes(fcinfo: &Fcinfo, i: usize) -> PgResult<Option<Vec<u8>>> {
    if i >= fcinfo.nargs() || fcinfo.args[i].isnull {
        return Ok(None);
    }
    // SAFETY: arg present and non-null per the guard above.
    let img = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(Some(img.data().to_vec()))
}

fn pgp_sym_encrypt(fcinfo: &mut Fcinfo, is_text: bool) -> PgResult<Datum> {
    // SAFETY: strict on arg0/arg1 (data, key); arg2 (args) optional/nullable.
    let (data, key) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let (data, key) = (data.data().to_vec(), key.data().to_vec());
    let args = opt_arg_bytes(fcinfo, 2)?;
    let out = pgp::sym_encrypt(&data, &key, args.as_deref(), is_text).map_err(|e| px_msg(&e))?;
    bytea_result(fcinfo, &out)
}

fn pgp_sym_decrypt(fcinfo: &mut Fcinfo, need_text: bool) -> PgResult<Datum> {
    // SAFETY: strict on arg0/arg1 (data, key); arg2 (args) optional/nullable.
    let (data, key) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let (data, key) = (data.data().to_vec(), key.data().to_vec());
    let args = opt_arg_bytes(fcinfo, 2)?;
    match pgp::sym_decrypt(&data, &key, args.as_deref(), need_text) {
        Ok(out) => {
            for n in &out.notices {
                pgp_notice(n);
            }
            if need_text {
                mbutils::pg_verifymbstr(&out.plaintext, false)?;
            }
            bytea_result(fcinfo, &out.plaintext)
        }
        Err(e) => {
            for n in &e.notices {
                pgp_notice(n);
            }
            Err(px_msg(&e.message))
        }
    }
}

fn pgp_pub_encrypt(fcinfo: &mut Fcinfo, is_text: bool) -> PgResult<Datum> {
    // SAFETY: strict on arg0/arg1 (data, key); arg2 (args) optional/nullable.
    let (data, key) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let (data, key) = (data.data().to_vec(), key.data().to_vec());
    let args = opt_arg_bytes(fcinfo, 2)?;
    let out = pgp::pub_encrypt(&data, &key, args.as_deref(), is_text).map_err(|e| px_msg(&e))?;
    bytea_result(fcinfo, &out)
}

fn pgp_pub_decrypt(fcinfo: &mut Fcinfo, need_text: bool) -> PgResult<Datum> {
    // SAFETY: strict on arg0/arg1 (msg, seckey); arg2 psw, arg3 args optional.
    let (data, key) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let (data, key) = (data.data().to_vec(), key.data().to_vec());
    let psw = opt_arg_bytes(fcinfo, 2)?;
    let args = opt_arg_bytes(fcinfo, 3)?;
    match pgp::pub_decrypt(&data, &key, psw.as_deref(), args.as_deref(), need_text) {
        Ok(out) => {
            for n in &out.notices {
                pgp_notice(n);
            }
            if need_text {
                mbutils::pg_verifymbstr(&out.plaintext, false)?;
            }
            bytea_result(fcinfo, &out.plaintext)
        }
        Err(e) => {
            for n in &e.notices {
                pgp_notice(n);
            }
            Err(px_msg(&e.message))
        }
    }
}

fn fc_pgp_sym_encrypt_text(f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    let _ = f;
    pgp_sym_encrypt(fc, true)
}
fn fc_pgp_sym_encrypt_bytea(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    pgp_sym_encrypt(fc, false)
}
fn fc_pgp_sym_decrypt_text(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    pgp_sym_decrypt(fc, true)
}
fn fc_pgp_sym_decrypt_bytea(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    pgp_sym_decrypt(fc, false)
}
fn fc_pgp_pub_encrypt_text(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    pgp_pub_encrypt(fc, true)
}
fn fc_pgp_pub_encrypt_bytea(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    pgp_pub_encrypt(fc, false)
}
fn fc_pgp_pub_decrypt_text(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    pgp_pub_decrypt(fc, true)
}
fn fc_pgp_pub_decrypt_bytea(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    pgp_pub_decrypt(fc, false)
}

fn fc_pgp_key_id_w(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 bytea.
    let data = unsafe { fcinfo.arg_varlena_packed(0)? };
    let s = pgp::key_id(data.data()).map_err(px_msg)?;
    bytea_result(fcinfo, s.as_bytes())
}

fn fc_pg_armor(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 bytea; arg1/arg2 text[] when present.
    let data = unsafe { fcinfo.arg_varlena_packed(0)? }.data().to_vec();
    let (keys, values) = if fcinfo.nargs() == 3 {
        let scratch = mcx::MemoryContext::new("pgp_armor headers");
        // SAFETY: strict fn — args non-null.
        let ki = unsafe { array_image(fcinfo, 1)? };
        let vi = unsafe { array_image(fcinfo, 2)? };
        parse_key_value_arrays(scratch.mcx(), &ki, &vi)?
    } else {
        (Vec::new(), Vec::new())
    };
    let out = pgp::armor::armor_encode(&data, &keys, &values);
    bytea_result(fcinfo, &out)
}

fn fc_pg_dearmor(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 text.
    let data = unsafe { fcinfo.arg_varlena_packed(0)? };
    let out = pgp::armor::armor_decode(data.data()).map_err(|()| px_msg(pgp::armor::CORRUPT_ARMOR))?;
    bytea_result(fcinfo, &out)
}

fn fc_pgp_armor_headers(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 text.
    let data = unsafe { fcinfo.arg_varlena_packed(0)? }.data().to_vec();
    let headers =
        pgp::armor::extract_armor_headers(&data).map_err(|()| px_msg(pgp::armor::CORRUPT_ARMOR))?;
    let flinfo = flinfo.expect("pgp_armor_headers: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    for (k, v) in &headers {
        let kd = types_fmgr::varlena_result(varlena::cstring_to_text(mcx, k)?);
        let vd = types_fmgr::varlena_result(varlena::cstring_to_text(mcx, v)?);
        srf.putvalues(&[kd, vd], &[false, false])?;
    }
    Ok(srf.finish(fcinfo))
}

/// Header-ful (4B varlena header) image of a varlena arg — arrays keep their
/// dims header, which `arrayfuncs` reads at fixed offsets. Short-header
/// images are re-expanded to the 4B form C's PG_GETARG_ARRAYTYPE_P delivers.
///
/// Safety: strict-fn contract — arg `i` non-null.
unsafe fn array_image(fcinfo: &Fcinfo, i: usize) -> PgResult<Vec<u8>> {
    // SAFETY: forwarded caller contract.
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    if v.is_short() {
        let d = v.data();
        let total = 4 + d.len();
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&types_tuple::varatt::set_varsize_4b_word(total as u32).to_ne_bytes());
        buf.extend_from_slice(d);
        Ok(buf)
    } else {
        Ok(v.image().to_vec())
    }
}

/// Payload bytes of a non-null text element datum inside an array image.
fn text_elem_bytes(d: Datum) -> Vec<u8> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null text element datum inside the array image.
    unsafe {
        let total = types_tuple::varatt::varsize_any(p);
        let hdr = if types_tuple::varatt::varatt_is_1b(p) { 1 } else { 4 };
        core::slice::from_raw_parts(p.add(hdr), total - hdr).to_vec()
    }
}

fn armor_header_err(msg: &str, sqlstate: types_error::SqlState) -> Box<PgError> {
    PgError::error(msg.to_string()).with_sqlstate(sqlstate).into()
}

// C pg_is_ascii (high bit clear on every byte).
fn all_ascii(b: &[u8]) -> bool {
    b.iter().all(|&c| c < 0x80)
}

/// C parse_key_value_arrays (pgp-pgsql.c): converts the key/value text[]
/// pair into byte vectors, applying C's exact checks in C's exact order —
/// dimensions, count, then per pair: key null / non-ASCII / `": "` /
/// newline, value null / non-ASCII / newline. Without the content checks an
/// attacker-controlled header value containing `\n` injects a forged armor
/// header line (lane p1-pgcrypto, D8).
#[allow(clippy::type_complexity)]
fn parse_key_value_arrays(
    mcx: mcx::Mcx<'_>,
    key_image: &[u8],
    val_image: &[u8],
) -> PgResult<(Vec<Vec<u8>>, Vec<Vec<u8>>)> {
    let nkdims = arrayfuncs::foundation::arr_ndim(key_image);
    let nvdims = arrayfuncs::foundation::arr_ndim(val_image);
    if nkdims > 1 || nkdims != nvdims {
        return Err(armor_header_err(
            "wrong number of array subscripts",
            ERRCODE_ARRAY_SUBSCRIPT_ERROR,
        ));
    }
    if nkdims == 0 {
        return Ok((Vec::new(), Vec::new()));
    }

    let (key_datums, key_nulls) =
        arrayfuncs::construct::deconstruct_array_builtin(mcx, key_image, types_core::TEXTOID, true)?;
    let (val_datums, val_nulls) =
        arrayfuncs::construct::deconstruct_array_builtin(mcx, val_image, types_core::TEXTOID, true)?;

    if key_datums.len() != val_datums.len() {
        return Err(armor_header_err(
            "mismatched array dimensions",
            ERRCODE_ARRAY_SUBSCRIPT_ERROR,
        ));
    }

    let n = key_datums.len();
    let mut keys = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);
    for i in 0..n {
        // Check that the key doesn't contain anything funny.
        if key_nulls[i] {
            return Err(armor_header_err(
                "null value not allowed for header key",
                ERRCODE_NULL_VALUE_NOT_ALLOWED,
            ));
        }
        let k = text_elem_bytes(key_datums[i]);
        if !all_ascii(&k) {
            return Err(armor_header_err(
                "header key must not contain non-ASCII characters",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        if k.windows(2).any(|w| w == b": ") {
            return Err(armor_header_err(
                "header key must not contain \": \"",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        if k.contains(&b'\n') {
            return Err(armor_header_err(
                "header key must not contain newlines",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        keys.push(k);

        // And the same for the value.
        if val_nulls[i] {
            return Err(armor_header_err(
                "null value not allowed for header value",
                ERRCODE_NULL_VALUE_NOT_ALLOWED,
            ));
        }
        let v = text_elem_bytes(val_datums[i]);
        if !all_ascii(&v) {
            return Err(armor_header_err(
                "header value must not contain non-ASCII characters",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        if v.contains(&b'\n') {
            return Err(armor_header_err(
                "header value must not contain newlines",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        values.push(v);
    }
    Ok((keys, values))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "pg_digest" => fc_pg_digest,
        "pg_hmac" => fc_pg_hmac,
        "pg_gen_salt" => fc_pg_gen_salt,
        "pg_gen_salt_rounds" => fc_pg_gen_salt_rounds,
        "pg_crypt" => fc_pg_crypt,
        "pg_encrypt" => fc_pg_encrypt,
        "pg_decrypt" => fc_pg_decrypt,
        "pg_encrypt_iv" => fc_pg_encrypt_iv,
        "pg_decrypt_iv" => fc_pg_decrypt_iv,
        "pg_random_bytes" => fc_pg_random_bytes,
        "pg_random_uuid" => fc_pg_random_uuid,
        "pg_check_fipsmode" => fc_pg_check_fipsmode,
        "pg_armor" => fc_pg_armor,
        "pg_dearmor" => fc_pg_dearmor,
        "pgp_armor_headers" => fc_pgp_armor_headers,
        "pgp_key_id_w" => fc_pgp_key_id_w,
        "pgp_sym_encrypt_bytea" => fc_pgp_sym_encrypt_bytea,
        "pgp_sym_encrypt_text" => fc_pgp_sym_encrypt_text,
        "pgp_sym_decrypt_bytea" => fc_pgp_sym_decrypt_bytea,
        "pgp_sym_decrypt_text" => fc_pgp_sym_decrypt_text,
        "pgp_pub_encrypt_bytea" => fc_pgp_pub_encrypt_bytea,
        "pgp_pub_encrypt_text" => fc_pgp_pub_encrypt_text,
        "pgp_pub_decrypt_bytea" => fc_pgp_pub_decrypt_bytea,
        "pgp_pub_decrypt_text" => fc_pgp_pub_decrypt_text,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}

#[cfg(test)]
mod armor_header_tests {
    //! C parse_key_value_arrays parity: every message/SQLSTATE below was
    //! EXECUTED against stock PostgreSQL 18.3 (pg-stock183, 2026-08-01).
    use super::*;
    use arrayfuncs::foundation::TYPALIGN_INT;

    // Build a header-ful 1-D (or md) text[] image from optional elements.
    fn text_array<'m>(
        mcx: mcx::Mcx<'m>,
        elems: &[Option<&[u8]>],
        ndims: i32,
        dims: &[i32],
    ) -> Vec<u8> {
        if elems.is_empty() {
            return arrayfuncs::construct::construct_empty_array(mcx, types_core::TEXTOID)
                .unwrap()
                .to_vec();
        }
        let mut datums = Vec::new();
        let mut nulls = Vec::new();
        let mut keep = Vec::new();
        for e in elems {
            match e {
                Some(b) => {
                    let v = varlena::cstring_to_text(mcx, b).unwrap();
                    let d = types_fmgr::varlena_result(v);
                    keep.push(d);
                    datums.push(d);
                    nulls.push(false);
                }
                None => {
                    datums.push(Datum::null());
                    nulls.push(true);
                }
            }
        }
        let lbs = vec![1i32; ndims as usize];
        arrayfuncs::construct::construct_md_array(
            mcx,
            &datums,
            Some(&nulls),
            ndims,
            dims,
            &lbs,
            types_core::TEXTOID,
            -1,
            false,
            TYPALIGN_INT,
        )
        .unwrap()
        .to_vec()
    }

    fn expect_err(
        keys: &[Option<&[u8]>],
        vals: &[Option<&[u8]>],
        msg: &str,
        sqlstate: types_error::SqlState,
    ) {
        let ctx = mcx::MemoryContext::new("armor test");
        let n = keys.len() as i32;
        let m = vals.len() as i32;
        let ki = text_array(ctx.mcx(), keys, 1, &[n]);
        let vi = text_array(ctx.mcx(), vals, 1, &[m]);
        match parse_key_value_arrays(ctx.mcx(), &ki, &vi) {
            Err(e) => {
                assert_eq!(e.message, msg);
                assert_eq!(e.sqlstate, sqlstate);
            }
            Ok(_) => panic!("expected error {msg:?}, got Ok"),
        }
    }

    // D8, check 1/5: key non-ASCII.
    #[test]
    fn key_non_ascii_rejected() {
        expect_err(
            &[Some("k\u{e9}y".as_bytes())],
            &[Some(b"v")],
            "header key must not contain non-ASCII characters",
            ERRCODE_INVALID_PARAMETER_VALUE,
        );
    }

    // D8, check 2/5: key containing ": ".
    #[test]
    fn key_colon_space_rejected() {
        expect_err(
            &[Some(b"k: k")],
            &[Some(b"v")],
            "header key must not contain \": \"",
            ERRCODE_INVALID_PARAMETER_VALUE,
        );
    }

    // D8, check 3/5: key containing a newline.
    #[test]
    fn key_newline_rejected() {
        expect_err(
            &[Some(b"k\nk")],
            &[Some(b"v")],
            "header key must not contain newlines",
            ERRCODE_INVALID_PARAMETER_VALUE,
        );
    }

    // D8, check 4/5: value non-ASCII.
    #[test]
    fn value_non_ascii_rejected() {
        expect_err(
            &[Some(b"k")],
            &[Some("v\u{e9}".as_bytes())],
            "header value must not contain non-ASCII characters",
            ERRCODE_INVALID_PARAMETER_VALUE,
        );
    }

    // D8, check 5/5: value containing a newline — the header-injection vector.
    #[test]
    fn value_newline_rejected() {
        expect_err(
            &[Some(b"k")],
            &[Some(b"v\nInjected: forged")],
            "header value must not contain newlines",
            ERRCODE_INVALID_PARAMETER_VALUE,
        );
    }

    // C allows ": " in the VALUE (only the key check has it) — 18.3 emits
    // "k: v: v".
    #[test]
    fn value_colon_space_allowed() {
        let ctx = mcx::MemoryContext::new("armor test");
        let ki = text_array(ctx.mcx(), &[Some(b"k")], 1, &[1]);
        let vi = text_array(ctx.mcx(), &[Some(b"v: v")], 1, &[1]);
        let (keys, values) = parse_key_value_arrays(ctx.mcx(), &ki, &vi).unwrap();
        assert_eq!(keys, vec![b"k".to_vec()]);
        assert_eq!(values, vec![b"v: v".to_vec()]);
    }

    #[test]
    fn null_key_rejected() {
        expect_err(
            &[Some(b"k"), None],
            &[Some(b"v"), Some(b"v")],
            "null value not allowed for header key",
            ERRCODE_NULL_VALUE_NOT_ALLOWED,
        );
    }

    #[test]
    fn null_value_rejected() {
        expect_err(
            &[Some(b"k"), Some(b"k")],
            &[Some(b"v"), None],
            "null value not allowed for header value",
            ERRCODE_NULL_VALUE_NOT_ALLOWED,
        );
    }

    #[test]
    fn count_mismatch_rejected() {
        expect_err(
            &[Some(b"k"), Some(b"k")],
            &[Some(b"v")],
            "mismatched array dimensions",
            ERRCODE_ARRAY_SUBSCRIPT_ERROR,
        );
    }

    #[test]
    fn multidim_rejected() {
        let ctx = mcx::MemoryContext::new("armor test");
        let ki = text_array(ctx.mcx(), &[Some(b"k")], 2, &[1, 1]);
        let vi = text_array(ctx.mcx(), &[Some(b"v")], 2, &[1, 1]);
        match parse_key_value_arrays(ctx.mcx(), &ki, &vi) {
            Err(e) => {
                assert_eq!(e.message, "wrong number of array subscripts");
                assert_eq!(e.sqlstate, ERRCODE_ARRAY_SUBSCRIPT_ERROR);
            }
            Ok(_) => panic!("expected error"),
        }
    }

    // D10's ORDERING half, previously unwitnessed. C validates each key/value
    // PAIR interleaved (pgp-pgsql.c:790-834: key null/ASCII/": "/newline, then
    // value null/ASCII/newline, then i+1) rather than all keys before any
    // value. With a NULL value at index 0 and a NULL key at index 1, C reaches
    // the VALUE check at i=0 first and reports the value error. Validating all
    // keys first would report the key error instead.
    #[test]
    fn pairs_are_validated_interleaved_not_keys_first() {
        expect_err(
            &[Some(b"k"), None],
            &[None, Some(b"v")],
            "null value not allowed for header value",
            ERRCODE_NULL_VALUE_NOT_ALLOWED,
        );
        // Same shape one level down: a bad key at index 1 must not preempt a
        // bad VALUE at index 0.
        expect_err(
            &[Some(b"k"), Some(b"k\nx")],
            &[Some(b"v\nx"), Some(b"v")],
            "header value must not contain newlines",
            ERRCODE_INVALID_PARAMETER_VALUE,
        );
        // And within one pair the KEY is checked before the value.
        expect_err(
            &[Some(b"k\nx")],
            &[Some(b"v\nx")],
            "header key must not contain newlines",
            ERRCODE_INVALID_PARAMETER_VALUE,
        );
    }

    // D9's asymmetric half: C's dimension test is `nkdims > 1 || nkdims !=
    // nvdims` (pgp-pgsql.c:772), which an EMPTY array (ndim 0) paired with a
    // 1-element array fails — "wrong number of array subscripts", NOT the
    // count-mismatch message. The all-empty case below returns 0 headers.
    #[test]
    fn empty_array_against_nonempty_is_a_subscript_error() {
        let ctx = mcx::MemoryContext::new("armor test");
        let empty = text_array(ctx.mcx(), &[], 0, &[]);
        let one = text_array(ctx.mcx(), &[Some(b"a")], 1, &[1]);
        for (ki, vi) in [(&empty, &one), (&one, &empty)] {
            match parse_key_value_arrays(ctx.mcx(), ki, vi) {
                Err(e) => {
                    assert_eq!(e.message, "wrong number of array subscripts");
                    assert_eq!(e.sqlstate, ERRCODE_ARRAY_SUBSCRIPT_ERROR);
                }
                Ok(_) => panic!("expected a subscript error"),
            }
        }
    }

    #[test]
    fn empty_arrays_yield_no_headers() {
        let ctx = mcx::MemoryContext::new("armor test");
        let ki = text_array(ctx.mcx(), &[], 0, &[]);
        let vi = text_array(ctx.mcx(), &[], 0, &[]);
        let (keys, values) = parse_key_value_arrays(ctx.mcx(), &ki, &vi).unwrap();
        assert!(keys.is_empty() && values.is_empty());
    }

    // Sanity: the accepted pair flows into armor_encode as one header line.
    #[test]
    fn accepted_headers_render() {
        let ctx = mcx::MemoryContext::new("armor test");
        let ki = text_array(ctx.mcx(), &[Some(b"Comment")], 1, &[1]);
        let vi = text_array(ctx.mcx(), &[Some(b"pgcrypto")], 1, &[1]);
        let (keys, values) = parse_key_value_arrays(ctx.mcx(), &ki, &vi).unwrap();
        let out = pgp::armor::armor_encode(b"x", &keys, &values);
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("Comment: pgcrypto\n"), "{s}");
    }

    // Task #105 witness for the short-varlena re-expansion arm of
    // array_image: a small text[] stored in a heap tuple arrives packed
    // (1-byte header) — the packed form is reproduced here by re-headering
    // the canonical image exactly the way heap storage does for
    // VARATT_CAN_MAKE_SHORT values — and C's PG_GETARG_ARRAYTYPE_P delivers
    // the re-expanded 4B form whose ndim/dims sit at the fixed offsets
    // arrayfuncs reads. Both arms of array_image must return that image
    // byte-for-byte.
    #[test]
    fn short_varlena_array_arg_reexpands_to_4b_image() {
        let ctx = mcx::MemoryContext::new("armor test");
        let img4 = text_array(ctx.mcx(), &[Some(b"k")], 1, &[1]);
        // Heap packed form: little-endian 1B header (len << 1) | 1, then the
        // identical payload bytes.
        let payload = &img4[4..];
        let short_total = 1 + payload.len();
        assert!(short_total <= 0x7F, "fixture must stay short-form eligible");
        let mut short = Vec::with_capacity(short_total);
        short.push(((short_total as u8) << 1) | 1);
        short.extend_from_slice(payload);

        let mut fci = types_fmgr::LocalFcinfo::<2>::fresh(0);
        fci.set_arg(0, Datum::from_usize(short.as_ptr() as usize));
        fci.set_arg(1, Datum::from_usize(img4.as_ptr() as usize));
        // SAFETY: both args are live, non-null varlena images owned by this
        // frame (the strict-fn contract array_image forwards).
        let re = unsafe { array_image(&fci, 0) }.unwrap();
        let pass = unsafe { array_image(&fci, 1) }.unwrap();
        assert_eq!(re, img4, "short arg must re-expand to the 4B image");
        assert_eq!(pass, img4, "4B arg is a passthrough");
        // The fixed-offset reads parse_key_value_arrays depends on.
        assert_eq!(arrayfuncs::foundation::arr_ndim(&re), 1);
        assert_eq!(arrayfuncs::foundation::arr_dim(&re, 0), 1);
    }

    // D15 (crypt SQLSTATE map): C's genuine-NULL path (pgcrypto.c:234) is
    // 39000, NOT the 22023 px_err stamps on everything else. EXECUTED on
    // stock 18.3 twice (2026-08-01): crypt('foox','$2$') => ERROR 39000
    // "crypt(3) returned NULL"; crypt('foox','') => ERROR 22023 "invalid
    // salt"; gen_salt bounds errors are 22023 ("gen_salt: %s", 22023).
    #[test]
    fn crypt_err_maps_null_path_to_39000_and_rest_to_22023() {
        assert_eq!(
            crypt_err(crypt::CryptError::Message("crypt(3) returned NULL".to_string())).sqlstate,
            ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION
        );
        assert_eq!(
            crypt_err(crypt::CryptError::Message("invalid salt".to_string())).sqlstate,
            ERRCODE_INVALID_PARAMETER_VALUE
        );
        assert_eq!(
            crypt_err(crypt::CryptError::Message(
                "gen_salt: Incorrect number of rounds".to_string()
            ))
            .sqlstate,
            ERRCODE_INVALID_PARAMETER_VALUE
        );
    }
}
