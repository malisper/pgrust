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
//! * The **PGP suite** (`pgp_*`, `armor`/`dearmor`) is fully ported: ASCII
//!   armor, symmetric and public-key (RSA/ElGamal) encrypt/decrypt, compression,
//!   S2K, CFB, MDC, and key-id extraction (see the `pgp` module).

use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::mat_srf::{self, MatCell};
use ::fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};
use ::types_error::{
    ErrorLocation, PgError, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERROR, NOTICE,
};
use ::utils_error::ereport;

mod cipher;
mod crypt;
mod gucs;
mod hashing;
mod pgp;

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

/// The FULL varlena image (header included) of a by-ref arg — used for array
/// args, which `deconstruct_text_array_nullable` parses with the header in place.
fn arg_raw_image(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Vec<u8> {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .unwrap_or_else(|| px_error("pgcrypto: by-ref arg missing from by-ref lane"))
        .to_vec()
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
// PGP suite — armor/dearmor, symmetric encrypt/decrypt, key-id (ported).
// Public-key (RSA/ElGamal) encrypt/decrypt remain graceful-ERROR stubs.
// ===========================================================================

/// `ErrorLocation` for ereport in the PGP glue.
fn here(func: &str) -> ErrorLocation {
    ErrorLocation::new("pgcrypto/pgp", 0, func)
}

/// Emit an `ereport(NOTICE)` line (used by the `expect-*` decrypt checks).
fn pgp_notice(msg: &str) {
    let _ = ereport(NOTICE).errmsg(msg).finish(here("pgp_decrypt"));
}

/// `pgp_pub_encrypt_text` / `pgp_pub_encrypt_bytea`. arg0=data, arg1=pubkey
/// (dearmored), optional arg2=args.
fn pgp_pub_encrypt(fcinfo: &mut FunctionCallInfoBaseData, is_text: bool) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let key = arg_bytes(fcinfo, 1);
    let args = opt_arg_bytes(fcinfo, 2);
    match pgp::pub_encrypt(&data, &key, args.as_deref(), is_text) {
        Ok(out) => ret_varlena(fcinfo, &out),
        Err(e) => px_error(&e),
    }
}

fn fc_pgp_pub_encrypt_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    pgp_pub_encrypt(fcinfo, true)
}

fn fc_pgp_pub_encrypt_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    pgp_pub_encrypt(fcinfo, false)
}

/// `pgp_pub_decrypt_text` / `pgp_pub_decrypt_bytea`. arg0=msg(bytea),
/// arg1=seckey (dearmored), optional arg2=password, optional arg3=args.
fn pgp_pub_decrypt(fcinfo: &mut FunctionCallInfoBaseData, need_text: bool) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let key = arg_bytes(fcinfo, 1);
    let psw = opt_arg_bytes(fcinfo, 2);
    let args = opt_arg_bytes(fcinfo, 3);
    match pgp::pub_decrypt(&data, &key, psw.as_deref(), args.as_deref(), need_text) {
        Ok(out) => {
            for n in &out.notices {
                pgp_notice(n);
            }
            if need_text {
                if let Err(e) = ::mbutils_seams::pg_verifymbstr::call(&out.plaintext, false) {
                    raise(e);
                }
            }
            ret_varlena(fcinfo, &out.plaintext)
        }
        Err(e) => {
            for n in &e.notices {
                pgp_notice(n);
            }
            px_error(&e.message);
        }
    }
}

fn fc_pgp_pub_decrypt_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    pgp_pub_decrypt(fcinfo, true)
}

fn fc_pgp_pub_decrypt_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    pgp_pub_decrypt(fcinfo, false)
}

/// Optional args arg (the 3rd arg of `pgp_sym_*`): `None` when absent.
fn opt_arg_bytes(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Option<Vec<u8>> {
    if i < fcinfo.nargs() && fcinfo.arg(i).map(|a| !a.isnull).unwrap_or(false) {
        Some(arg_bytes(fcinfo, i))
    } else {
        None
    }
}

/// `pgp_sym_encrypt_text` / `pgp_sym_encrypt_bytea`. arg0=data, arg1=key,
/// optional arg2=args. `is_text` selects the literal-data text mode.
fn pgp_sym_encrypt(fcinfo: &mut FunctionCallInfoBaseData, is_text: bool) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let key = arg_bytes(fcinfo, 1);
    let args = opt_arg_bytes(fcinfo, 2);
    match pgp::sym_encrypt(&data, &key, args.as_deref(), is_text) {
        Ok(out) => ret_varlena(fcinfo, &out),
        Err(e) => px_error(&e),
    }
}

fn fc_pgp_sym_encrypt_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    pgp_sym_encrypt(fcinfo, true)
}

fn fc_pgp_sym_encrypt_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    pgp_sym_encrypt(fcinfo, false)
}

/// `pgp_sym_decrypt_text` / `pgp_sym_decrypt_bytea`. arg0=data(bytea),
/// arg1=key, optional arg2=args.
fn pgp_sym_decrypt(fcinfo: &mut FunctionCallInfoBaseData, need_text: bool) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let key = arg_bytes(fcinfo, 1);
    let args = opt_arg_bytes(fcinfo, 2);
    match pgp::sym_decrypt(&data, &key, args.as_deref(), need_text) {
        Ok(out) => {
            for n in &out.notices {
                pgp_notice(n);
            }
            // pgp_sym_decrypt_text runs pg_verifymbstr over the result.
            if need_text {
                if let Err(e) = ::mbutils_seams::pg_verifymbstr::call(&out.plaintext, false) {
                    raise(e);
                }
            }
            ret_varlena(fcinfo, &out.plaintext)
        }
        Err(e) => {
            for n in &e.notices {
                pgp_notice(n);
            }
            px_error(&e.message);
        }
    }
}

fn fc_pgp_sym_decrypt_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    pgp_sym_decrypt(fcinfo, true)
}

fn fc_pgp_sym_decrypt_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    pgp_sym_decrypt(fcinfo, false)
}

/// `pgp_key_id_w(bytea) -> text` — the 16-hex / SYMKEY / ANYKEY key id.
fn fc_pgp_key_id_w(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    match pgp::key_id(&data) {
        Ok(s) => ret_text(fcinfo, &s),
        Err(e) => px_error(e),
    }
}

/// `pg_armor(bytea [, text[], text[]]) -> text`.
fn fc_pg_armor(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let (keys, values) = if fcinfo.nargs() == 3 {
        match parse_key_value_arrays(fcinfo) {
            Ok(kv) => kv,
            Err(e) => raise(e),
        }
    } else {
        (Vec::new(), Vec::new())
    };
    let out = pgp::armor::armor_encode(&data, &keys, &values);
    ret_varlena(fcinfo, &out)
}

/// `pg_dearmor(text) -> bytea`.
fn fc_pg_dearmor(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    match pgp::armor::armor_decode(&data) {
        Ok(out) => ret_varlena(fcinfo, &out),
        Err(()) => px_error(pgp::armor::CORRUPT_ARMOR),
    }
}

/// `pgp_armor_headers(text) -> setof (key text, value text)` — materialize SRF.
fn fc_pgp_armor_headers(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytes(fcinfo, 0);
    let headers = match pgp::armor::extract_armor_headers(&data) {
        Ok(h) => h,
        Err(()) => px_error(pgp::armor::CORRUPT_ARMOR),
    };
    mat_srf::with_top(|sink| {
        if let Some(sink) = sink {
            sink.materialized = true;
            for (k, v) in &headers {
                sink.rows.push(vec![text_cell(k), text_cell(v)]);
            }
        } else {
            raise(PgError::error(
                "set-valued function called in context that cannot accept a set",
            ));
        }
    });
    Datum::null()
}

/// Build a header-ful `text` MatCell from payload bytes.
fn text_cell(bytes: &[u8]) -> MatCell {
    MatCell {
        value: 0,
        ref_payload: Some(RefPayload::Varlena(varlena_image(bytes))),
        isnull: false,
    }
}

/// `parse_key_value_arrays` — deconstruct the two `text[]` args into validated
/// key/value byte vectors. Errors mirror pgp-pgsql.c exactly.
fn parse_key_value_arrays(
    fcinfo: &FunctionCallInfoBaseData,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>), PgError> {
    let key_img = arg_raw_image(fcinfo, 1);
    let val_img = arg_raw_image(fcinfo, 2);

    let nkdims = array_ndim(&key_img);
    let nvdims = array_ndim(&val_img);
    if nkdims > 1 || nkdims != nvdims {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .errmsg("wrong number of array subscripts")
            .into_error());
    }
    if nkdims == 0 {
        return Ok((Vec::new(), Vec::new()));
    }

    let keys = deconstruct_text_array(&key_img)?;
    let values = deconstruct_text_array(&val_img)?;
    if keys.len() != values.len() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .errmsg("mismatched array dimensions")
            .into_error());
    }

    let mut out_keys = Vec::with_capacity(keys.len());
    let mut out_vals = Vec::with_capacity(values.len());
    for i in 0..keys.len() {
        let k = keys[i].as_ref().ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .errmsg("null value not allowed for header key")
                .into_error()
        })?;
        if !k.iter().all(|&b| b.is_ascii()) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("header key must not contain non-ASCII characters")
                .into_error());
        }
        if find_sub(k, b": ") {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("header key must not contain \": \"")
                .into_error());
        }
        if k.contains(&b'\n') {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("header key must not contain newlines")
                .into_error());
        }
        let v = values[i].as_ref().ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .errmsg("null value not allowed for header value")
                .into_error()
        })?;
        if !v.iter().all(|&b| b.is_ascii()) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("header value must not contain non-ASCII characters")
                .into_error());
        }
        if v.contains(&b'\n') {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("header value must not contain newlines")
                .into_error());
        }
        out_keys.push(k.clone());
        out_vals.push(v.clone());
    }
    Ok((out_keys, out_vals))
}

fn find_sub(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// `ARR_NDIM` from the full ArrayType varlena image: skip the varlena header,
/// then the first 4 bytes of the ArrayType struct are `ndim`.
fn array_ndim(image: &[u8]) -> i32 {
    let payload = varlena_payload(image);
    if payload.len() < 4 {
        return 0;
    }
    i32::from_ne_bytes([payload[0], payload[1], payload[2], payload[3]])
}

/// Deconstruct a 1-D `text[]` image (full varlena) into nullable byte vectors.
fn deconstruct_text_array(image: &[u8]) -> Result<Vec<Option<Vec<u8>>>, PgError> {
    let scratch = ::mcx::MemoryContext::new("pgcrypto text[] arg");
    let mcx = scratch.mcx();
    let v = ::arrayfuncs::construct::deconstruct_text_array_nullable(mcx, image)?;
    Ok(v.iter()
        .map(|o| o.as_ref().map(|s| s.as_str().as_bytes().to_vec()))
        .collect())
}

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
