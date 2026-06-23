//! OpenSSL provider for `backend-libpq-be-secure-openssl-ffi-seams`.
//!
//! This is the `--with-ssl=openssl` provider: it binds the *same* system
//! libssl + libcrypto PostgreSQL binds and installs every outward OpenSSL FFI
//! seam declared in `backend-libpq-be-secure-openssl-ffi-seams` (the ported
//! pure-logic owner `backend-libpq-be-secure-openssl` already implements the
//! accept loop, the `SSL_get_error` classification, the GUC validation, etc.;
//! only the libssl/libcrypto calls cross the seam, and they land here).
//!
//! The seam handle model uses opaque `u64` tokens (`0 == NULL`): this provider
//! mints and interprets them as the corresponding OpenSSL pointers. The four
//! real OpenSSL C callbacks (the BIO read/write, the passphrase callback, the
//! certificate-verification callback, and the handshake info callback) are
//! `extern "C" fn`s defined here that gather facts from libssl and delegate the
//! pure logic back into the owner crate (or, for the BIO read/write, back
//! through PostgreSQL's socket layer via the owner's inbound bridge seams).
//!
//! Feature-gated as `ssl-openssl`. With the feature OFF (`#ifdef USE_SSL`
//! false), [`init_seams`] installs nothing and the outward seams stay
//! loud-panicking, exactly as `be-secure.c`'s dispatch never routes into the
//! TLS arm.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

#[cfg(feature = "ssl-openssl")]
mod provider;

/// Install all OpenSSL FFI seams (binds libssl + libcrypto). Call once at
/// startup when the build uses `--with-ssl=openssl`. With the `ssl-openssl`
/// feature off this is a no-op (the seams stay panicking — faithful USE_SSL
/// off).
pub fn init_seams() {
    #[cfg(feature = "ssl-openssl")]
    provider::install();
}
