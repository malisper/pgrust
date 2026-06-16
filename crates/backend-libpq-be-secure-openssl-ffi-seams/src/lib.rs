//! OpenSSL C-library FFI seams for `src/backend/libpq/be-secure-openssl.c`.
//!
//! `be-secure-openssl.c` is fundamentally an OpenSSL FFI shim: the pure
//! control-flow logic (the accept loop, the `SSL_get_error` classification,
//! ALPN + embedded-NUL checks, the protocol-version GUC validation, every
//! `ereport`) is ported natively in `backend-libpq-be-secure-openssl`, while
//! the actual libssl/libcrypto calls (`SSL_CTX_new`, `SSL_accept`, `SSL_read`,
//! `X509_NAME_print_ex`, the BIO machinery, …) cross this seam crate.
//!
//! This is the sanctioned external-library pattern used elsewhere in the tree
//! (`backend-utils-adt-xml-libxml-seams`, `backend-utils-adt-pg-locale-icu`):
//! the C calls into a system library that has no Rust translation unit to own
//! it. There is **no** `...-openssl-ffi` owner crate in this repo (PostgreSQL's
//! TLS engine is OpenSSL, an external C library), so every seam here
//! **loud-panics until a provider binds it**. This faithfully mirrors the C
//! file's `#ifdef USE_SSL` gating: in the repo build `USE_SSL` is false, so
//! `be-secure.c`'s dispatch never routes into the TLS arm and these seams are
//! never reached at run time. Flipping the build to use TLS requires binding a
//! real OpenSSL provider to these slots (the work `--with-ssl=openssl` would
//! do); until then a call panics loudly rather than fabricating a handshake.
//!
//! The seam-install guard exempts this crate (no name-matched owner), exactly
//! like the libxml seams: these are outward dependency seams on an unported
//! external substrate, not an inward contract any repo crate owns.
//!
//! ## Handle model
//!
//! OpenSSL objects are referenced through opaque `u64` tokens minted and
//! interpreted by the (future) provider:
//!   * [`SslCtx`]  — `SSL_CTX *`
//!   * [`Ssl`]     — `SSL *` (the per-connection object)
//!   * [`X509`]    — `X509 *` (a peer/server certificate)
//!   * [`X509Name`]— `X509_NAME *`
//! A `0` token is the C `NULL` pointer. No certificate or key material crosses
//! a seam except as already-decoded bytes/strings; the heavy ASN.1 work stays
//! inside libcrypto behind the seam.

#![allow(non_snake_case)]

/// `SSL_CTX *` — the server-wide SSL context. `0` == `NULL`.
pub type SslCtx = u64;
/// `SSL *` — a per-connection SSL object. `0` == `NULL`.
pub type Ssl = u64;
/// `X509 *` — a certificate handle. `0` == `NULL`.
pub type X509 = u64;
/// `X509_NAME *` — a certificate subject/issuer name handle. `0` == `NULL`.
pub type X509Name = u64;

/// Which file-scope passphrase callback `default_openssl_tls_init` selects
/// (the C registers a C function pointer; we register a tag the provider maps
/// to the corresponding ported callback in `backend-libpq-be-secure-openssl`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PasswdCb {
    /// `ssl_external_passwd_cb` — run `ssl_passphrase_command`.
    External,
    /// `dummy_ssl_passwd_cb` — return an empty passphrase (guaranteed failure).
    Dummy,
}

/// Outcome of one `SSL_read`/`SSL_write` after `SSL_get_error` classification
/// at the FFI boundary: the raw byte count and the OpenSSL error class. The
/// pure-logic crate maps these to `errno`/`waitfor`/`ereport` exactly as the C
/// `switch (err)` does.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SslIoResult {
    /// `n` — `SSL_read`/`SSL_write` return value (`> 0` bytes, `<= 0` trouble).
    pub n: isize,
    /// `err` — `SSL_get_error(ssl, n)` (one of the `SSL_ERROR_*` constants).
    pub err: i32,
    /// `errno` — the process `errno` immediately after the call (the C reads
    /// it directly on the `SSL_ERROR_SYSCALL` path).
    pub sys_errno: i32,
    /// `ecode` — `ERR_get_error()` (`0` when there was no queued error).
    pub ecode: u64,
}

/// Outcome of `SSL_accept` after `SSL_get_error`, mirroring the open-server
/// loop's `r`/`err`/`ecode`/`errno` locals.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SslAcceptResult {
    /// `r` — `SSL_accept` return (`1` success, `<= 0` not-yet/failed).
    pub r: i32,
    /// `err` — `SSL_get_error(ssl, r)`.
    pub err: i32,
    /// `ecode` — `ERR_get_error()` after the failing call.
    pub ecode: u64,
    /// `errno` — process `errno` after the call (`SSL_ERROR_SYSCALL` path).
    pub sys_errno: i32,
}

/* ========================================================================= *
 *  SSL_ERROR_* (openssl/ssl.h) and SSL_R_* reason codes — needed by the
 *  pure-logic classification `switch`es in the consumer crate. Defined here
 *  alongside the FFI surface that produces them.
 * ========================================================================= */
pub const SSL_ERROR_NONE: i32 = 0;
pub const SSL_ERROR_SSL: i32 = 1;
pub const SSL_ERROR_WANT_READ: i32 = 2;
pub const SSL_ERROR_WANT_WRITE: i32 = 3;
pub const SSL_ERROR_WANT_X509_LOOKUP: i32 = 4;
pub const SSL_ERROR_SYSCALL: i32 = 5;
pub const SSL_ERROR_ZERO_RETURN: i32 = 6;
pub const SSL_ERROR_WANT_CONNECT: i32 = 7;
pub const SSL_ERROR_WANT_ACCEPT: i32 = 8;

/* ========================================================================= *
 *  Context lifecycle + configuration (be_tls_init)
 * ========================================================================= */

seam_core::seam!(
    /// `SSL_CTX_new(SSLv23_method())` — create a server SSL context that can
    /// negotiate the highest mutually supported protocol. Returns `0` (NULL)
    /// on allocation failure.
    pub fn ssl_ctx_new_server() -> SslCtx
);

seam_core::seam!(
    /// `SSL_CTX_free(ctx)` — release a context (used both on the error path and
    /// to replace the previous active context). A no-op on `0`.
    pub fn ssl_ctx_free(ctx: SslCtx)
);

seam_core::seam!(
    /// `SSL_CTX_set_mode(ctx, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER)` — disable
    /// the moving-write-buffer sanity check (the only mode the C file sets).
    pub fn ssl_ctx_set_mode_accept_moving_write_buffer(ctx: SslCtx)
);

seam_core::seam!(
    /// `SSL_CTX_set_default_passwd_cb(ctx, cb)` — install the file-scope
    /// passphrase callback selected by `default_openssl_tls_init`.
    pub fn ssl_ctx_set_default_passwd_cb(ctx: SslCtx, cb: PasswdCb)
);

seam_core::seam!(
    /// `SSL_CTX_use_certificate_chain_file(ctx, file)` — load the server cert
    /// chain. Returns the OpenSSL int (`1` == success).
    pub fn ssl_ctx_use_certificate_chain_file(ctx: SslCtx, file: &str) -> i32
);

seam_core::seam!(
    /// `SSL_CTX_use_PrivateKey_file(ctx, file, SSL_FILETYPE_PEM)` — load the
    /// private key (PEM). Returns the OpenSSL int (`1` == success). May invoke
    /// the registered passphrase callback (which sets `dummy_ssl_passwd_cb`'s
    /// flag, queried via [`dummy_ssl_passwd_cb_called`]).
    pub fn ssl_ctx_use_private_key_file_pem(ctx: SslCtx, file: &str) -> i32
);

seam_core::seam!(
    /// Whether the dummy passphrase callback ran during the most recent
    /// `SSL_CTX_use_PrivateKey_file` (`dummy_ssl_passwd_cb_called`). Reset by
    /// [`reset_dummy_ssl_passwd_cb_called`] before the key load.
    pub fn dummy_ssl_passwd_cb_called() -> bool
);

seam_core::seam!(
    /// Reset the `dummy_ssl_passwd_cb_called` flag to false (the C
    /// `dummy_ssl_passwd_cb_called = false;` before the private-key load).
    pub fn reset_dummy_ssl_passwd_cb_called()
);

seam_core::seam!(
    /// `SSL_CTX_check_private_key(ctx)` — verify the key matches the cert.
    /// Returns the OpenSSL int (`1` == success).
    pub fn ssl_ctx_check_private_key(ctx: SslCtx) -> i32
);

seam_core::seam!(
    /// `SSL_CTX_set_min_proto_version(ctx, ver)` — returns the OpenSSL int
    /// (non-zero == success). `ver` is the OpenSSL protocol-version constant
    /// produced by `ssl_protocol_version_to_openssl`.
    pub fn ssl_ctx_set_min_proto_version(ctx: SslCtx, ver: i32) -> i32
);

seam_core::seam!(
    /// `SSL_CTX_set_max_proto_version(ctx, ver)` — non-zero == success.
    pub fn ssl_ctx_set_max_proto_version(ctx: SslCtx, ver: i32) -> i32
);

seam_core::seam!(
    /// `SSL_CTX_set_num_tickets(ctx, 0)` + `SSL_CTX_set_options(ctx,
    /// SSL_OP_NO_TICKET)` — disallow TLS session tickets. The C `#ifdef
    /// HAVE_SSL_CTX_SET_NUM_TICKETS` guard always holds for OpenSSL >= 1.1.1.
    pub fn ssl_ctx_disallow_tickets(ctx: SslCtx)
);

seam_core::seam!(
    /// `SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF)` — disallow
    /// SSL session caching.
    pub fn ssl_ctx_disable_session_cache(ctx: SslCtx)
);

seam_core::seam!(
    /// `SSL_CTX_set_options(ctx, SSL_OP_NO_COMPRESSION)` — disallow SSL
    /// compression.
    pub fn ssl_ctx_disallow_compression(ctx: SslCtx)
);

seam_core::seam!(
    /// `SSL_CTX_set_options(ctx, SSL_OP_NO_RENEGOTIATION |
    /// SSL_OP_NO_CLIENT_RENEGOTIATION)` — disallow SSL renegotiation (the
    /// `#ifdef`-guarded options, always present on the supported builds).
    pub fn ssl_ctx_disallow_renegotiation(ctx: SslCtx)
);

seam_core::seam!(
    /// `SSL_CTX_set_options(ctx, SSL_OP_SINGLE_DH_USE)` — set before loading DH
    /// parameters (from `initialize_dh`).
    pub fn ssl_ctx_set_single_dh_use(ctx: SslCtx)
);

seam_core::seam!(
    /// `SSL_CTX_set_tmp_dh(ctx, dh)` where `dh` is loaded from the
    /// DBA-supplied file (`ssl_dh_params_file`, if any) or the hardcoded
    /// `FILE_DH2048` buffer, with all of `load_dh_file`'s `DH_check` validation
    /// performed inside the provider. `dh_params_file` is `None` to use the
    /// hardcoded fallback. Returns `Ok(true)` on success; `Ok(false)` after the
    /// provider could not load/validate the parameters (the consumer emits the
    /// `ereport`). The DH validation `ereport`s themselves (invalid prime, etc.)
    /// originate inside the provider as it owns the libcrypto `DH_check`.
    pub fn ssl_ctx_setup_dh(ctx: SslCtx, dh_params_file: Option<&str>, is_server_start: bool) -> bool
);

seam_core::seam!(
    /// `SSL_CTX_set1_groups_list(ctx, SSLECDHCurve)` (`initialize_ecdh`) —
    /// returns the OpenSSL int (`1` == success). `0` triggers the
    /// `ssl_groups` error in the consumer.
    pub fn ssl_ctx_set_groups_list(ctx: SslCtx, groups: &str) -> i32
);

seam_core::seam!(
    /// `SSL_CTX_set_cipher_list(ctx, SSLCipherList)` — TLSv1.2-and-below cipher
    /// list. Returns the OpenSSL int (`1` == success).
    pub fn ssl_ctx_set_cipher_list(ctx: SslCtx, ciphers: &str) -> i32
);

seam_core::seam!(
    /// `SSL_CTX_set_ciphersuites(ctx, SSLCipherSuites)` — TLSv1.3 cipher
    /// suites (only when the GUC is non-empty). Returns `1` == success.
    pub fn ssl_ctx_set_ciphersuites(ctx: SslCtx, suites: &str) -> i32
);

seam_core::seam!(
    /// `SSL_CTX_set_options(ctx, SSL_OP_CIPHER_SERVER_PREFERENCE)` — let the
    /// server choose the cipher order (`SSLPreferServerCiphers`).
    pub fn ssl_ctx_set_cipher_server_preference(ctx: SslCtx)
);

seam_core::seam!(
    /// `SSL_CTX_load_verify_locations(ctx, ssl_ca_file, NULL)` followed by
    /// `SSL_load_client_CA_file(ssl_ca_file)` and
    /// `SSL_CTX_set_client_CA_list(ctx, root_cert_list)`. Returns `true` when
    /// both load steps succeed (so the CA store is loaded and the client-CA
    /// list installed); `false` when either fails (the consumer emits the
    /// "could not load root certificate file" `ereport`).
    pub fn ssl_ctx_load_ca(ctx: SslCtx, ca_file: &str) -> bool
);

seam_core::seam!(
    /// `SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE,
    /// verify_cb)` — request (but don't require) a client cert, installing the
    /// `verify_cb` certificate-verification callback.
    pub fn ssl_ctx_set_verify_peer(ctx: SslCtx)
);

seam_core::seam!(
    /// CRL setup: `cvstore = SSL_CTX_get_cert_store(ctx);
    /// X509_STORE_load_locations(cvstore, crl_file?, crl_dir?)` and on success
    /// `X509_STORE_set_flags(cvstore, X509_V_FLAG_CRL_CHECK |
    /// X509_V_FLAG_CRL_CHECK_ALL)`. `crl_file`/`crl_dir` are `None` for the
    /// empty-GUC case. Returns: `Some(true)` load succeeded (flags set);
    /// `Some(false)` load failed (consumer emits the right ereport per which
    /// GUC was set); `None` when `SSL_CTX_get_cert_store` returned NULL (C's
    /// `if (cvstore)` false arm: nothing happens).
    pub fn ssl_ctx_setup_crl(ctx: SslCtx, crl_file: Option<&str>, crl_dir: Option<&str>) -> Option<bool>
);

seam_core::seam!(
    /// `SSL_CTX_set_info_callback(ctx, info_cb)` — install the handshake
    /// info-logging callback.
    pub fn ssl_ctx_set_info_callback(ctx: SslCtx)
);

seam_core::seam!(
    /// `SSL_CTX_set_alpn_select_cb(ctx, alpn_cb, port)` — install the ALPN
    /// selection callback. `port_token` identifies the connection for the
    /// callback (the C passes `port`).
    pub fn ssl_ctx_set_alpn_select_cb(ctx: SslCtx, port_token: u64)
);

/* ========================================================================= *
 *  Active-context management (be_tls_init success / be_tls_destroy)
 * ========================================================================= */

seam_core::seam!(
    /// Read the current active `SSL_context` token (`0` == NULL). Used by
    /// `be_tls_init` (replace existing), `be_tls_open_server` (must be set),
    /// and `be_tls_destroy`.
    pub fn get_active_ssl_context() -> SslCtx
);

seam_core::seam!(
    /// Set the active `SSL_context` to `ctx` (`SSL_context = context;`), or to
    /// `0` for `be_tls_destroy`'s `SSL_context = NULL;`.
    pub fn set_active_ssl_context(ctx: SslCtx)
);

/* ========================================================================= *
 *  Per-connection SSL object (be_tls_open_server / be_tls_close)
 * ========================================================================= */

seam_core::seam!(
    /// `SSL_new(SSL_context)` — create the per-connection SSL object. `0`
    /// (NULL) on failure.
    pub fn ssl_new(ctx: SslCtx) -> Ssl
);

seam_core::seam!(
    /// Set up the custom `port_bio` BIO (`ssl_set_port_bio(port)`): create the
    /// BIO with `port_bio_method`, attach `port_token`, and `SSL_set_bio`. The
    /// BIO's read/write route back into `secure_raw_read`/`secure_raw_write`
    /// through the provider. Returns the C int (`1` == success).
    pub fn ssl_set_port_bio(ssl: Ssl, port_token: u64) -> i32
);

seam_core::seam!(
    /// `errno = 0; ERR_clear_error(); r = SSL_accept(ssl); err =
    /// SSL_get_error(ssl, r); ecode = ERR_get_error()` — one accept step with
    /// the queue cleared first and the error classified after.
    pub fn ssl_accept(ssl: Ssl) -> SslAcceptResult
);

seam_core::seam!(
    /// `ERR_GET_REASON(ecode)` — extract the reason code from a packed OpenSSL
    /// error, for the protocol-version-hint classification in the accept loop.
    pub fn err_get_reason(ecode: u64) -> i32
);

seam_core::seam!(
    /// `SSL_get0_alpn_selected(ssl, &selected, &len)` — the ALPN protocol the
    /// handshake selected, as owned bytes (`None` == `selected == NULL`, i.e.
    /// ALPN not used).
    pub fn ssl_get0_alpn_selected(ssl: Ssl) -> Option<Vec<u8>>
);

seam_core::seam!(
    /// `SSL_get_peer_certificate(ssl)` — the client certificate, or `0` (NULL)
    /// if the client presented none.
    pub fn ssl_get_peer_certificate(ssl: Ssl) -> X509
);

seam_core::seam!(
    /// `X509_get_subject_name(cert)` — the subject name handle.
    pub fn x509_get_subject_name(cert: X509) -> X509Name
);

seam_core::seam!(
    /// `X509_NAME_get_text_by_NID(name, NID_commonName, ...)` — the certificate
    /// Common Name as owned bytes, or `None` when the NID is absent
    /// (`len == -1`). The provider returns the exact CN bytes (no truncation),
    /// so the consumer can perform the embedded-NUL length check.
    pub fn x509_name_get_common_name(name: X509Name) -> Option<Vec<u8>>
);

seam_core::seam!(
    /// `X509_NAME_print_ex(bio, name, 0, XN_FLAG_RFC2253)` into a memory BIO,
    /// then `BIO_get_mem_ptr` — the RFC2253-formatted Distinguished Name as
    /// owned bytes. `None` mirrors the C BIO-allocation/print failure paths
    /// (the consumer returns `-1` without an ereport, matching the C).
    pub fn x509_name_print_rfc2253(name: X509Name) -> Option<Vec<u8>>
);

seam_core::seam!(
    /// `X509_free(cert)` — release a certificate handle (`be_tls_close`'s
    /// `port->peer`). A no-op on `0`.
    pub fn x509_free(cert: X509)
);

seam_core::seam!(
    /// `SSL_shutdown(ssl); SSL_free(ssl)` — shut down and free the
    /// per-connection SSL object (`be_tls_close`). A no-op on `0`.
    pub fn ssl_shutdown_and_free(ssl: Ssl)
);

/* ========================================================================= *
 *  I/O (be_tls_read / be_tls_write)
 * ========================================================================= */

seam_core::seam!(
    /// `errno = 0; ERR_clear_error(); n = SSL_read(ssl, ptr, len); err =
    /// SSL_get_error(ssl, n); ecode = ...` — read up to `len` bytes. On
    /// `n > 0`, the decrypted bytes are returned alongside the result for the
    /// consumer to copy into the caller buffer.
    pub fn ssl_read(ssl: Ssl, len: usize) -> (SslIoResult, Vec<u8>)
);

seam_core::seam!(
    /// `errno = 0; ERR_clear_error(); n = SSL_write(ssl, ptr, len); err =
    /// SSL_get_error(ssl, n); ecode = ...` — write `buf`.
    pub fn ssl_write(ssl: Ssl, buf: &[u8]) -> SslIoResult
);

/* ========================================================================= *
 *  Error-string helpers (SSLerrmessage)
 * ========================================================================= */

seam_core::seam!(
    /// `ERR_get_error()` — pop the next error off the thread's queue (`0` when
    /// empty). Used by the consumer's stand-alone `SSLerrmessage(ERR_get_error())`
    /// reports (context creation, cert load, etc.).
    pub fn err_get_error() -> u64
);

seam_core::seam!(
    /// `SSLerrmessage(ecode)` core lookup: `ERR_reason_error_string(ecode)`,
    /// with the OpenSSL-3 `ERR_SYSTEM_ERROR` `strerror` fallback and the
    /// numeric `"SSL error code %lu"` last resort all performed inside the
    /// provider (they need the live libcrypto error tables). `ecode == 0`
    /// ("no SSL error reported") is handled by the consumer before calling, so
    /// this is only invoked for nonzero codes and always yields a non-NULL
    /// string.
    pub fn ssl_err_reason_string(ecode: u64) -> String
);

/* ========================================================================= *
 *  Accessors (be_tls_get_*)
 * ========================================================================= */

seam_core::seam!(
    /// `SSL_get_version(ssl)` — negotiated protocol version string, or `None`
    /// when `port->ssl` is NULL.
    pub fn ssl_get_version(ssl: Ssl) -> Option<String>
);

seam_core::seam!(
    /// `SSL_get_cipher(ssl)` — negotiated cipher name string, or `None` when
    /// `port->ssl` is NULL.
    pub fn ssl_get_cipher(ssl: Ssl) -> Option<String>
);

seam_core::seam!(
    /// `SSL_get_cipher_bits(ssl, &bits)` — effective bits of the negotiated
    /// cipher. The consumer returns `0` when `port->ssl` is NULL (so this is
    /// only called for a live `ssl`).
    pub fn ssl_get_cipher_bits(ssl: Ssl) -> i32
);
