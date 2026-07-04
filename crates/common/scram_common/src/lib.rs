// Specialized to SHA-256: C threads pg_cryptohash_type/key_length through
// every function but scram_build_secret asserts PG_SHA256 and no other hash
// is reachable in 18.3. Crypto legs are infallible here (C's -1/errstr arms
// are OOM/EVP-failure only); PgResult carries CHECK_FOR_INTERRUPTS and OOM.

use mcx::{Mcx, PgString};
use pg_b64::{pg_b64_enc_len, pg_b64_encode};
use pg_hmac::{hmac_sha256, PgHmacCtx, Sha256};
use pg_sha2::PG_SHA256_DIGEST_LENGTH;
use types_error::PgResult;

pub const SCRAM_SHA_256_NAME: &str = "SCRAM-SHA-256";
pub const SCRAM_SHA_256_PLUS_NAME: &str = "SCRAM-SHA-256-PLUS";
pub const SCRAM_SHA_256_KEY_LEN: usize = PG_SHA256_DIGEST_LENGTH;
pub const SCRAM_MAX_KEY_LEN: usize = SCRAM_SHA_256_KEY_LEN;
pub const SCRAM_RAW_NONCE_LEN: usize = 18;
pub const SCRAM_DEFAULT_SALT_LEN: usize = 16;
pub const SCRAM_SHA_256_DEFAULT_ITERATIONS: i32 = 4096;

pub fn scram_salted_password(
    password: &[u8],
    salt: &[u8],
    iterations: i32,
) -> PgResult<[u8; SCRAM_SHA_256_KEY_LEN]> {
    let one = 1u32.to_be_bytes();

    let mut ctx = PgHmacCtx::<Sha256>::init(password);
    ctx.update(salt);
    ctx.update(&one);
    let mut ui_prev = ctx.finalize();

    let mut result = ui_prev;
    let mut i = 1;
    while i < iterations {
        // Interruptible: scram_iterations may be set very large.
        postgres_seams::check_for_interrupts::call()?;

        let ui = hmac_sha256(password, &ui_prev);
        for j in 0..SCRAM_SHA_256_KEY_LEN {
            result[j] ^= ui[j];
        }
        ui_prev = ui;
        i += 1;
    }

    Ok(result)
}

pub fn scram_h(input: &[u8; SCRAM_SHA_256_KEY_LEN]) -> [u8; SCRAM_SHA_256_KEY_LEN] {
    pg_sha2::sha256(input)
}

pub fn scram_client_key(
    salted_password: &[u8; SCRAM_SHA_256_KEY_LEN],
) -> [u8; SCRAM_SHA_256_KEY_LEN] {
    hmac_sha256(salted_password, b"Client Key")
}

pub fn scram_server_key(
    salted_password: &[u8; SCRAM_SHA_256_KEY_LEN],
) -> [u8; SCRAM_SHA_256_KEY_LEN] {
    hmac_sha256(salted_password, b"Server Key")
}

pub fn scram_build_secret<'mcx>(
    mcx: Mcx<'mcx>,
    salt: &[u8],
    iterations: i32,
    password: &[u8],
) -> PgResult<PgString<'mcx>> {
    assert!(iterations > 0);

    let salted_password = scram_salted_password(password, salt, iterations)?;
    let stored_key = scram_h(&scram_client_key(&salted_password));
    let server_key = scram_server_key(&salted_password);

    let encoded_salt = b64(salt);
    let encoded_stored = b64(&stored_key);
    let encoded_server = b64(&server_key);

    // Cold DDL path: one std String temp for the format, then one mcx copy.
    let secret = format!(
        "{SCRAM_SHA_256_NAME}${iterations}:{encoded_salt}${encoded_stored}:{encoded_server}"
    );
    PgString::from_str_in(&secret, mcx)
}

fn b64(src: &[u8]) -> String {
    let cap = pg_b64_enc_len(src.len() as i32);
    let mut dst = vec![0u8; cap as usize];
    let n = pg_b64_encode(src, src.len() as i32, &mut dst, cap);
    assert!(n >= 0);
    dst.truncate(n as usize);
    String::from_utf8(dst).unwrap()
}

#[cfg(test)]
mod tests;
