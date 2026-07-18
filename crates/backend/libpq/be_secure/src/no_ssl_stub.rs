// C parity: !USE_SSL build shape (be-secure.c with the #ifdef USE_SSL arms
// compiled out; be-secure-openssl.c absent). In C these entry points do not
// exist; every caller is gated on LoadedSSL / port->ssl_in_use, which can
// never become true without SSL support — postmaster answers 'N' to
// SSLRequest and secure_initialize is only reached when EnableSSL, whose GUC
// check would already have refused. Reaching an unreachable arm reports a
// clean ERROR (no panics on ported paths).

use types_error::{PgError, PgResult, ERROR};

pub struct TlsOpen {
    pub r: i32,
    pub raw_remaining: usize,
    pub alpn_used: bool,
    pub peer_cn: Option<String>,
    pub peer_dn: Option<String>,
    pub peer_cert_valid: bool,
}

pub struct TlsIo {
    pub n: isize,
    pub errno: i32,
    pub waitfor: u32,
}

#[cold]
fn no_ssl_err<T>() -> PgResult<T> {
    // guc.c check_ssl message in !USE_SSL builds.
    Err(Box::new(PgError::new(
        ERROR,
        "SSL is not supported by this build",
    )))
}

pub fn be_tls_init(_is_server_start: bool) -> PgResult<i32> {
    no_ssl_err()
}

pub fn be_tls_destroy() {}

pub fn ssl_loaded_verify_locations() -> bool {
    false
}

pub fn be_tls_open_server(_sock: i32, _raw_buf: Vec<u8>) -> PgResult<TlsOpen> {
    no_ssl_err()
}

pub fn be_tls_close() {}

pub fn be_tls_read(_buf: &mut [u8]) -> PgResult<TlsIo> {
    no_ssl_err()
}

pub fn be_tls_write(_buf: &[u8]) -> PgResult<TlsIo> {
    no_ssl_err()
}

pub fn be_tls_get_cipher_bits() -> i32 {
    0
}

pub fn be_tls_get_version() -> Option<String> {
    None
}

pub fn be_tls_get_cipher() -> Option<String> {
    None
}

pub fn be_tls_get_certificate_hash() -> PgResult<Option<Vec<u8>>> {
    Ok(None)
}
