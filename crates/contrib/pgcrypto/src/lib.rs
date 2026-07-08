//! `contrib/pgcrypto` — ported as a Rust builtin (no `.so`; symbols resolve
//! through the dfmgr in-process registry so CREATE EXTENSION pgcrypto
//! validates and installs).
//!
//! SCOPE (2026-07-08 contrib-ports increment 1): digest()/hmac() over the
//! in-repo reference hashes (pg_md5/pg_sha1/pg_sha2), gen_random_bytes,
//! gen_random_uuid. The cipher (encrypt/decrypt), crypt/gen_salt, and full
//! PGP surfaces are NOT yet ported — their symbols resolve to loud stubs so
//! CREATE EXTENSION still succeeds (a caller of an unported function gets the
//! stub's error, exactly as an unresolved C symbol would fail the call).
//! Those depend on third-party symmetric-cipher/bignum crates (fabled used
//! RustCrypto aes/blowfish/des/cast5 + num-bigint + miniz_oxide); adding them
//! is a follow-up increment.

mod hashing;

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

const LIBRARY: &str = "pgcrypto";

// bytea and text share the varlena image layout; cstring_to_text builds a
// 4-byte-header image over arbitrary bytes.
fn bytea_result(fcinfo: &mut Fcinfo, bytes: &[u8]) -> PgResult<Datum> {
    let img = varlena::cstring_to_text(fcinfo.result_mcx(), bytes)?;
    Ok(types_fmgr::varlena_result(img))
}

fn px_err(msg: String) -> Box<PgError> {
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE).into()
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

// C: return gen_random_uuid(fcinfo) — the core v4 generator.
fn fc_pg_random_uuid(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let uuid = adt_uuid::gen_random_uuid()?;
    types_fmgr::byref_result(fcinfo.result_mcx(), &uuid)
}

// pgcrypto 1.4's fips_mode(): CheckFIPSMode(). No OpenSSL FIPS provider is
// active in this build, so always false (C's non-FIPS path).
fn fc_pg_check_fipsmode(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(false))
}

// Unported surfaces (cipher / crypt / PGP): resolve so CREATE EXTENSION
// validates; a call raises feature-not-supported naming the C source, exactly
// like a missing symbol would fail the call.
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
    fc_pg_crypt => "crypt() (crypt-*.c)";
    fc_pg_gen_salt => "gen_salt() (crypt-gensalt.c)";
    fc_pg_gen_salt_rounds => "gen_salt(rounds) (crypt-gensalt.c)";
    fc_pg_encrypt => "encrypt() (px cipher)";
    fc_pg_decrypt => "decrypt() (px cipher)";
    fc_pg_encrypt_iv => "encrypt_iv() (px cipher)";
    fc_pg_decrypt_iv => "decrypt_iv() (px cipher)";
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
        "pg_random_bytes" => fc_pg_random_bytes,
        "pg_random_uuid" => fc_pg_random_uuid,
        "pg_check_fipsmode" => fc_pg_check_fipsmode,
        "pg_crypt" => fc_pg_crypt,
        "pg_gen_salt" => fc_pg_gen_salt,
        "pg_gen_salt_rounds" => fc_pg_gen_salt_rounds,
        "pg_encrypt" => fc_pg_encrypt,
        "pg_decrypt" => fc_pg_decrypt,
        "pg_encrypt_iv" => fc_pg_encrypt_iv,
        "pg_decrypt_iv" => fc_pg_decrypt_iv,
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
        // pgcrypto.c's PG_MODULE_MAGIC has no _PG_init (the builtin_crypto_enabled
        // GUC is registered in _PG_init in 18; unported surface uses no GUC).
        pg_init: None,
    });
}
