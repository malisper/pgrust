
mod cipher;
mod crypt;
mod hashing;
mod pgp;

use datum::Datum;
use elog::ereport;
use types_error::{ErrorLocation, PgError, PgResult, NOTICE,
    ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE};
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
        crypt::CryptError::Message(m) => px_err(m),
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
    let pw = String::from_utf8_lossy(pw.data()).into_owned();
    let salt = String::from_utf8_lossy(salt.data()).into_owned();
    let s = crypt::crypt(&pw, &salt).map_err(crypt_err)?;
    bytea_result(fcinfo, s.as_bytes())
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
fn px_msg(msg: &str) -> Box<PgError> {
    px_err(msg.to_string())
}

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
        let ki = unsafe { fcinfo.arg_varlena_packed(1)? }.data().to_vec();
        let vi = unsafe { fcinfo.arg_varlena_packed(2)? }.data().to_vec();
        let k = deconstruct_nonnull_text(scratch.mcx(), &ki)?;
        let v = deconstruct_nonnull_text(scratch.mcx(), &vi)?;
        if k.len() != v.len() {
            return Err(px_err("pgp_armor: number of keys and values must be equal".to_string()));
        }
        (k, v)
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

fn deconstruct_nonnull_text(mcx: mcx::Mcx<'_>, image: &[u8]) -> PgResult<Vec<Vec<u8>>> {
    let (elems, nulls) =
        arrayfuncs::construct::deconstruct_array_builtin(mcx, image, types_core::TEXTOID, true)?;
    let mut out = Vec::with_capacity(elems.len());
    for (d, &isnull) in elems.iter().zip(nulls.iter()) {
        if isnull {
            return Err(px_err("pgp_armor: null value not allowed in header".to_string()));
        }
        let p = d.as_usize() as *const u8;
        // SAFETY: non-null text element datum inside the array image.
        let bytes = unsafe {
            let total = types_tuple::varatt::varsize_any(p);
            let hdr = if types_tuple::varatt::varatt_is_1b(p) { 1 } else { 4 };
            core::slice::from_raw_parts(p.add(hdr), total - hdr).to_vec()
        };
        out.push(bytes);
    }
    Ok(out)
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
