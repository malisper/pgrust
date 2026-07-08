//! `contrib/pgcrypto` — Rust builtin resolved through the dfmgr registry.
//! Ported: digest/hmac (in-repo hashes), gen_random_bytes/uuid, fips_mode,
//! crypt/gen_salt md5-crypt ($1$), encrypt/decrypt(+_iv) (RustCrypto ciphers).
//! Loud feature-not-supported stubs (so CREATE EXTENSION still installs):
//! crypt/gen_salt des/bcrypt/xdes + sha-crypt, and the full PGP suite.

mod cipher;
mod crypt;
mod hashing;

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION,
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

fn fc_pg_gen_salt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 text.
    let ty = unsafe { fcinfo.arg_varlena_packed(0)? };
    let ty = String::from_utf8_lossy(ty.data()).into_owned();
    let s = crypt::gen_salt(&ty, 0).map_err(crypt_err)?;
    bytea_result(fcinfo, s.as_bytes())
}

fn fc_pg_gen_salt_rounds(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 text, arg1 int4.
    let ty = unsafe { fcinfo.arg_varlena_packed(0)? };
    let ty = String::from_utf8_lossy(ty.data()).into_owned();
    let rounds = fcinfo.arg_i32(1);
    let s = crypt::gen_salt(&ty, rounds).map_err(crypt_err)?;
    bytea_result(fcinfo, s.as_bytes())
}

fn fc_pg_crypt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
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

macro_rules! unported {
    ($($fname:ident => $what:literal;)*) => {$(
        fn $fname(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Err(PgError::error(concat!("pgcrypto: ", $what, " not yet ported"))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .into())
        }
    )*};
}

unported! {
    fc_pg_armor => "armor() (pgp-armor.c)";
    fc_pg_dearmor => "dearmor() (pgp-armor.c)";
    fc_pgp_armor_headers => "pgp_armor_headers() (pgp-armor.c)";
    fc_pgp_key_id_w => "pgp_key_id() (pgp.c)";
    fc_pgp_sym_encrypt_bytea => "pgp_sym_encrypt_bytea() (pgp-pgsql.c)";
    fc_pgp_sym_encrypt_text => "pgp_sym_encrypt_text() (pgp-pgsql.c)";
    fc_pgp_sym_decrypt_bytea => "pgp_sym_decrypt_bytea() (pgp-pgsql.c)";
    fc_pgp_sym_decrypt_text => "pgp_sym_decrypt_text() (pgp-pgsql.c)";
    fc_pgp_pub_encrypt_bytea => "pgp_pub_encrypt_bytea() (pgp-pgsql.c)";
    fc_pgp_pub_encrypt_text => "pgp_pub_encrypt_text() (pgp-pgsql.c)";
    fc_pgp_pub_decrypt_bytea => "pgp_pub_decrypt_bytea() (pgp-pgsql.c)";
    fc_pgp_pub_decrypt_text => "pgp_pub_decrypt_text() (pgp-pgsql.c)";
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
