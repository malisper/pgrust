#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `contrib/pgcrypto` — the pgcrypto cryptographic-functions extension, ported
//! to a Rust BUILTIN (no `.so` loading; the Rust backend exposes no C ABI).
//!
//! Registered with the dynamic-loader unit's ported-library registry exactly
//! like `pg_prewarm`/`pg_stat_statements`: the SQL emitted by
//! `pgcrypto--1.3.sql` (`CREATE FUNCTION ... LANGUAGE C AS
//! 'MODULE_PATHNAME','<symbol>'`) resolves through the in-process registry, so
//! `CREATE EXTENSION pgcrypto` validates and installs the catalog objects.
//!
//! ## Provider strategy
//!
//! * **digest / hmac** use PG's own in-tree reference hashes via the ported
//!   `cryptohash` crate (`pg_cryptohash_*`), so the output is byte-identical to
//!   the C (non-OpenSSL) build. HMAC is RFC 2104 over that dispatcher.
//! * **encrypt / decrypt (+ _iv)** use the RustCrypto block ciphers (`aes`,
//!   `blowfish`, `des`, `cast5`) with `cbc`/`ecb` modes, matching OpenSSL's
//!   ECB/CBC + PKCS#5/#7 padding semantics that pgcrypto's `px_combo_*` expose.
//! * **gen_random_bytes** uses the ported `pg_strong_random` (`/dev/urandom`).
//! * **crypt / gen_salt** implement the bounded md5-crypt + bcrypt + des-crypt +
//!   xdes salt generators / verifiers.
//! * **gen_random_uuid** mirrors core's v4 UUID (random bytes + version/variant
//!   bits), formatted as a 36-char text (the catalog declares a `uuid` return,
//!   but the value crosses the by-ref boundary as the canonical text image).
//! * The **PGP suite** (`pgp_*`, `armor`/`dearmor`) is registered as
//!   graceful-`ERROR` stubs (large self-contained subsystem; see module docs and
//!   the port report). `CREATE EXTENSION` still succeeds because the symbols
//!   resolve.

use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};
use ::types_error::{PgError, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use ::utils_error::ereport;

mod cipher;
mod crypt;
mod gucs;
mod hashing;

/// The simple (suffix-free, directory-free) name of the loadable module —
/// `$libdir/pgcrypto` reduces to this for the registry.
const LIBRARY: &str = "pgcrypto";

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// `PGFunction` crosses (`invoke_pgfunction`'s `catch_unwind`), which downcasts
/// the panic payload back to the structured [`PgError`].
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

/// pgcrypto's `px_strerror`-style "ERROR: <message>" with no SQLSTATE detail;
/// pgcrypto raises these with `ereport(ERROR, errmsg("%s", px_strerror(err)))`,
/// which carries the default `ERRCODE_INTERNAL_ERROR` but only the bare message
/// shows in the regress output. We use a generic invalid-parameter errcode.
fn px_error(msg: &str) -> ! {
    raise(
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(msg)
            .into_error(),
    );
}

// ===========================================================================
// fmgr argument / result helpers (mirrors backend-test-regress)
// ===========================================================================

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image.
fn varlena_payload(image: &[u8]) -> &[u8] {
    match image.first() {
        // VARATT_IS_1B && !VARATT_IS_1B_E: short 1-byte header (skip 1 byte).
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        // 4-byte uncompressed header (skip VARHDRSZ).
        Some(_) if image.len() >= ::datum::varlena::VARHDRSZ => {
            &image[::datum::varlena::VARHDRSZ..]
        }
        _ => &[],
    }
}

/// `PG_GETARG_TEXT_PP(i)` / `PG_GETARG_BYTEA_PP(i)` — a `text`/`bytea` arg's
/// `VARDATA_ANY` payload bytes, owned (the cipher/hash code keeps slices alive
/// across allocations).
fn arg_bytes(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Vec<u8> {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .unwrap_or_else(|| px_error("pgcrypto: by-ref arg missing from by-ref lane"));
    varlena_payload(image).to_vec()
}

/// A `text` arg decoded to a `String` (the C `text_to_cstring`).
fn arg_text_string(fcinfo: &FunctionCallInfoBaseData, i: usize) -> String {
    String::from_utf8_lossy(&arg_bytes(fcinfo, i)).into_owned()
}

/// `PG_GETARG_INT32(i)`.
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .unwrap_or_else(|| px_error("pgcrypto: missing int4 arg"))
        .value
        .as_i32()
}

/// Build a header-ful varlena (`text`/`bytea`) image from its payload bytes
/// (C: `SET_VARSIZE(result, len + VARHDRSZ)`).
fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + ::datum::varlena::VARHDRSZ;
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&::datum::varlena::set_varsize_4b(total));
    image.extend_from_slice(payload);
    image
}

/// `PG_RETURN_TEXT_P` / `PG_RETURN_BYTEA_P` — write a header-ful varlena result.
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, payload: &[u8]) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Varlena(varlena_image(payload)));
    Datum::from_usize(0)
}

/// `PG_RETURN_TEXT_P` over UTF-8 text.
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, s: &str) -> Datum {
    ret_varlena(fcinfo, s.as_bytes())
}

// ===========================================================================
// digest / hmac
// ===========================================================================

/// `pg_digest(data, type)` — both the `text` and `bytea` first-arg variants.
/// arg0 = data (bytea/text payload), arg1 = digest name (text). Returns bytea.
fn fc_pg_digest(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let name = arg_text_string(fcinfo, 1);
    match hashing::digest(&name, &data) {
        Ok(out) => ret_varlena(fcinfo, &out),
        Err(e) => px_error(&e),
    }
}

/// `pg_hmac(data, key, type)` — text/text/text and bytea/bytea/text variants.
/// arg0 = data, arg1 = key, arg2 = digest name. Returns bytea.
fn fc_pg_hmac(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let key = arg_bytes(fcinfo, 1);
    let name = arg_text_string(fcinfo, 2);
    match hashing::hmac(&name, &key, &data) {
        Ok(out) => ret_varlena(fcinfo, &out),
        Err(e) => px_error(&e),
    }
}

// ===========================================================================
// gen_salt / crypt
// ===========================================================================

/// `pg_gen_salt(salt_type)` — VOLATILE STRICT, returns text.
fn fc_pg_gen_salt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // px_gen_salt → CheckBuiltinCryptoMode() (px-crypt.c).
    if let Err(e) = gucs::check_builtin_crypto_mode() {
        px_error(&e);
    }
    let salt_type = arg_text_string(fcinfo, 0);
    match crypt::gen_salt(&salt_type, 0) {
        Ok(s) => ret_text(fcinfo, &s),
        Err(e) => px_error(&e),
    }
}

/// `pg_gen_salt_rounds(salt_type, rounds)` — VOLATILE STRICT, returns text.
fn fc_pg_gen_salt_rounds(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if let Err(e) = gucs::check_builtin_crypto_mode() {
        px_error(&e);
    }
    let salt_type = arg_text_string(fcinfo, 0);
    let rounds = arg_int32(fcinfo, 1);
    match crypt::gen_salt(&salt_type, rounds) {
        Ok(s) => ret_text(fcinfo, &s),
        Err(e) => px_error(&e),
    }
}

/// `pg_crypt(password, salt)` — IMMUTABLE STRICT, returns text.
fn fc_pg_crypt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // px_crypt → CheckBuiltinCryptoMode() (px-crypt.c).
    if let Err(e) = gucs::check_builtin_crypto_mode() {
        px_error(&e);
    }
    let password = arg_text_string(fcinfo, 0);
    let salt = arg_text_string(fcinfo, 1);
    match crypt::crypt(&password, &salt) {
        Ok(s) => ret_text(fcinfo, &s),
        Err(e) => px_error(&e),
    }
}

// ===========================================================================
// encrypt / decrypt (+ iv)
// ===========================================================================

/// Map a [`cipher::CipherError`] to pgcrypto's exact error text. `op` is the
/// SQL function name (`encrypt` / `decrypt` / `encrypt_iv` / `decrypt_iv`); a
/// missing cipher uses the `Cannot use "%s"` form from `find_provider`, while an
/// operation failure uses `<op> error: <px_strerror>`.
fn cipher_err(op: &str, e: cipher::CipherError) -> ! {
    match e {
        cipher::CipherError::NoCipher(spec) => {
            px_error(&format!("Cannot use \"{spec}\": No such cipher algorithm"))
        }
        cipher::CipherError::EncryptFailed => {
            px_error(&format!("{op} error: Encryption failed"))
        }
        cipher::CipherError::DecryptFailed => {
            px_error(&format!("{op} error: Decryption failed"))
        }
    }
}

fn fc_pg_encrypt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let key = arg_bytes(fcinfo, 1);
    let spec = arg_text_string(fcinfo, 2);
    match cipher::encrypt(&spec, &key, &[], &data) {
        Ok(out) => ret_varlena(fcinfo, &out),
        Err(e) => cipher_err("encrypt", e),
    }
}

fn fc_pg_decrypt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let key = arg_bytes(fcinfo, 1);
    let spec = arg_text_string(fcinfo, 2);
    match cipher::decrypt(&spec, &key, &[], &data) {
        Ok(out) => ret_varlena(fcinfo, &out),
        Err(e) => cipher_err("decrypt", e),
    }
}

fn fc_pg_encrypt_iv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let key = arg_bytes(fcinfo, 1);
    let iv = arg_bytes(fcinfo, 2);
    let spec = arg_text_string(fcinfo, 3);
    match cipher::encrypt(&spec, &key, &iv, &data) {
        Ok(out) => ret_varlena(fcinfo, &out),
        Err(e) => cipher_err("encrypt_iv", e),
    }
}

fn fc_pg_decrypt_iv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let key = arg_bytes(fcinfo, 1);
    let iv = arg_bytes(fcinfo, 2);
    let spec = arg_text_string(fcinfo, 3);
    match cipher::decrypt(&spec, &key, &iv, &data) {
        Ok(out) => ret_varlena(fcinfo, &out),
        Err(e) => cipher_err("decrypt_iv", e),
    }
}

// ===========================================================================
// gen_random_bytes / gen_random_uuid
// ===========================================================================

/// `pg_random_bytes(count)` — VOLATILE STRICT, returns bytea. count in 1..=1024.
fn fc_pg_random_bytes(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let len = arg_int32(fcinfo, 0);
    if !(1..=1024).contains(&len) {
        px_error("Length not in range");
    }
    let mut buf = vec![0u8; len as usize];
    if !pg_strong_random::pg_strong_random(&mut buf) {
        px_error("Failed to generate a random number");
    }
    ret_varlena(fcinfo, &buf)
}

/// `pg_random_uuid()` — VOLATILE, returns uuid. The value crosses the by-ref
/// boundary as the 16-byte raw image (a `uuid` is a fixed-length-by-ref type).
fn fc_pg_random_uuid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut buf = [0u8; 16];
    if !pg_strong_random::pg_strong_random(&mut buf) {
        px_error("Failed to generate a random number");
    }
    // Version 4 (random) + RFC 4122 variant.
    buf[6] = (buf[6] & 0x0f) | 0x40;
    buf[8] = (buf[8] & 0x3f) | 0x80;
    fcinfo.isnull = false;
    // A `uuid` is a 16-byte fixed-length-by-ref type: no varlena header.
    fcinfo.set_ref_result(RefPayload::Varlena(buf.to_vec()));
    Datum::from_usize(0)
}

// ===========================================================================
// PGP suite — graceful-ERROR stubs (large self-contained subsystem, unported)
// ===========================================================================

macro_rules! pgp_stub {
    ($name:ident, $sym:literal) => {
        fn $name(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            px_error(concat!(
                "pgcrypto: ",
                $sym,
                " (PGP/armor subsystem) is not ported in this build"
            ));
        }
    };
}

pgp_stub!(fc_pgp_sym_encrypt_text, "pgp_sym_encrypt_text");
pgp_stub!(fc_pgp_sym_encrypt_bytea, "pgp_sym_encrypt_bytea");
pgp_stub!(fc_pgp_sym_decrypt_text, "pgp_sym_decrypt_text");
pgp_stub!(fc_pgp_sym_decrypt_bytea, "pgp_sym_decrypt_bytea");
pgp_stub!(fc_pgp_pub_encrypt_text, "pgp_pub_encrypt_text");
pgp_stub!(fc_pgp_pub_encrypt_bytea, "pgp_pub_encrypt_bytea");
pgp_stub!(fc_pgp_pub_decrypt_text, "pgp_pub_decrypt_text");
pgp_stub!(fc_pgp_pub_decrypt_bytea, "pgp_pub_decrypt_bytea");
pgp_stub!(fc_pgp_key_id_w, "pgp_key_id_w");
pgp_stub!(fc_pg_armor, "pg_armor");
pgp_stub!(fc_pg_dearmor, "pg_dearmor");
pgp_stub!(fc_pgp_armor_headers, "pgp_armor_headers");

/// `pg_check_fipsmode()` — FIPS mode probe (1.3--1.4). Always false here.
fn fc_pg_check_fipsmode(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.isnull = false;
    Datum::from_bool(false)
}

// ===========================================================================
// Builtin-library registration
// ===========================================================================

fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        "pg_digest" => Some(fc_pg_digest),
        "pg_hmac" => Some(fc_pg_hmac),
        "pg_gen_salt" => Some(fc_pg_gen_salt),
        "pg_gen_salt_rounds" => Some(fc_pg_gen_salt_rounds),
        "pg_crypt" => Some(fc_pg_crypt),
        "pg_encrypt" => Some(fc_pg_encrypt),
        "pg_decrypt" => Some(fc_pg_decrypt),
        "pg_encrypt_iv" => Some(fc_pg_encrypt_iv),
        "pg_decrypt_iv" => Some(fc_pg_decrypt_iv),
        "pg_random_bytes" => Some(fc_pg_random_bytes),
        "pg_random_uuid" => Some(fc_pg_random_uuid),
        "pg_check_fipsmode" => Some(fc_pg_check_fipsmode),
        // PGP suite (stubs)
        "pgp_sym_encrypt_text" => Some(fc_pgp_sym_encrypt_text),
        "pgp_sym_encrypt_bytea" => Some(fc_pgp_sym_encrypt_bytea),
        "pgp_sym_decrypt_text" => Some(fc_pgp_sym_decrypt_text),
        "pgp_sym_decrypt_bytea" => Some(fc_pgp_sym_decrypt_bytea),
        "pgp_pub_encrypt_text" => Some(fc_pgp_pub_encrypt_text),
        "pgp_pub_encrypt_bytea" => Some(fc_pgp_pub_encrypt_bytea),
        "pgp_pub_decrypt_text" => Some(fc_pgp_pub_decrypt_text),
        "pgp_pub_decrypt_bytea" => Some(fc_pgp_pub_decrypt_bytea),
        "pgp_key_id_w" => Some(fc_pgp_key_id_w),
        "pg_armor" => Some(fc_pg_armor),
        "pg_dearmor" => Some(fc_pg_dearmor),
        "pgp_armor_headers" => Some(fc_pgp_armor_headers),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        api_version: 1,
    })
}

/// `_PG_init` (pgcrypto.c) — registers the `pgcrypto.builtin_crypto_enabled`
/// custom GUC. Invoked once when the module is first loaded.
fn pg_init() -> ::types_error::PgResult<()> {
    gucs::register();
    Ok(())
}

/// Install this unit's inward seams: register the `pgcrypto` module with the
/// dynamic-loader unit's ported-library registry.
pub fn init_seams() {
    dfmgr_seams::register_builtin_library(dfmgr_seams::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: Some(pg_init),
    });
}
