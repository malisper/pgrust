//! Port of `src/backend/libpq/be-secure-openssl.c` — the backend's
//! server-side OpenSSL TLS engine.
//!
//! This is the `#ifdef USE_SSL` backend that `be-secure.c` dispatches the TLS
//! arm into. The C file is an OpenSSL FFI shim: all the *control-flow* logic
//! (the accept loop, the `SSL_get_error` classification, the protocol-version
//! GUC validation, ALPN + embedded-NUL handling, every `ereport`) is ported
//! natively here, while the actual libssl/libcrypto calls cross the
//! [`backend_libpq_be_secure_openssl_ffi_seams`] FFI boundary. That seam crate
//! has no Rust owner (OpenSSL is an external C library); its seams loud-panic
//! until a provider binds them, faithfully mirroring the C `#ifdef USE_SSL`
//! gating. In the repo build `USE_SSL` is false, so `be-secure.c` never routes
//! into these functions at run time.
//!
//! Per-connection OpenSSL objects (`port->ssl`, `port->peer`) are not fields of
//! [`types_net::Port`]; the FFI provider owns them, keyed by the connection's
//! socket (the `port_token`). This crate keeps a thread-local `sock -> Ssl`
//! map so the `be_tls_get_*` accessors can locate the live `SSL *` (the C reads
//! `port->ssl` directly).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::collections::HashMap;

use backend_utils_error::ereport;
use backend_utils_misc_guc_tables::vars;
use types_error::{
    ErrorLocation, PgResult, COMMERROR, ERRCODE_CONFIG_FILE_ERROR, ERRCODE_PROTOCOL_VIOLATION,
    FATAL, LOG,
};
use mcx::{Mcx, PgString};
use types_net::Port;

use backend_libpq_be_secure_openssl_ffi_seams as ffi;
use ffi::{
    PasswdCb, Ssl, SslCtx, X509, SSL_ERROR_NONE, SSL_ERROR_SSL, SSL_ERROR_SYSCALL,
    SSL_ERROR_WANT_READ, SSL_ERROR_WANT_WRITE, SSL_ERROR_ZERO_RETURN,
};

const SRCFILE: &str = "be-secure-openssl.c";

fn errloc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRCFILE, 0, funcname)
}

/* ========================================================================= *
 *  Constants
 * ========================================================================= */

/// `enum ssl_protocol_versions` (libpq/libpq.h).
pub const PG_TLS_ANY: i32 = 0;
pub const PG_TLS1_VERSION: i32 = 1;
pub const PG_TLS1_1_VERSION: i32 = 2;
pub const PG_TLS1_2_VERSION: i32 = 3;
pub const PG_TLS1_3_VERSION: i32 = 4;

/// OpenSSL protocol-version constants (`openssl/tls1.h`). The repo target uses
/// OpenSSL >= 1.1.1, so TLS1/1.1/1.2/1.3 are all available.
const TLS1_VERSION: i32 = 0x0301;
const TLS1_1_VERSION: i32 = 0x0302;
const TLS1_2_VERSION: i32 = 0x0303;
const TLS1_3_VERSION: i32 = 0x0304;

/// `MIN_OPENSSL_TLS_VERSION` / `MAX_OPENSSL_TLS_VERSION` (common/openssl.h)
/// for an OpenSSL >= 1.1.1 build.
const MIN_OPENSSL_TLS_VERSION: &str = "TLSv1";
const MAX_OPENSSL_TLS_VERSION: &str = "TLSv1.3";

/// `PG_ALPN_PROTOCOL` (libpq/pqcomm.h).
const PG_ALPN_PROTOCOL: &[u8] = b"postgresql";

/// SSL_R_* reason codes (openssl/sslerr.h) that earn a protocol-version hint
/// in the accept loop's `SSL_ERROR_SSL` arm.
const SSL_R_NO_PROTOCOLS_AVAILABLE: i32 = 181;
const SSL_R_UNSUPPORTED_PROTOCOL: i32 = 258;
const SSL_R_BAD_PROTOCOL_VERSION_NUMBER: i32 = 182;
const SSL_R_UNKNOWN_PROTOCOL: i32 = 252;
const SSL_R_UNKNOWN_SSL_VERSION: i32 = 254;
const SSL_R_UNSUPPORTED_SSL_VERSION: i32 = 259;
const SSL_R_WRONG_SSL_VERSION: i32 = 266;
const SSL_R_WRONG_VERSION_NUMBER: i32 = 267;
const SSL_R_TLSV1_ALERT_PROTOCOL_VERSION: i32 = 1070;
const SSL_R_VERSION_TOO_HIGH: i32 = 274;
const SSL_R_VERSION_TOO_LOW: i32 = 396;

/* ========================================================================= *
 *  errno values used by the read/write classification.
 *  `be_tls_read`/`be_tls_write` return their classified errno to be-secure.c.
 * ========================================================================= */
#[cfg(target_os = "macos")]
const EWOULDBLOCK: i32 = 35;
#[cfg(not(target_os = "macos"))]
const EWOULDBLOCK: i32 = 11;
const ECONNRESET: i32 = libc_econnreset();

const fn libc_econnreset() -> i32 {
    #[cfg(target_os = "macos")]
    {
        54
    }
    #[cfg(not(target_os = "macos"))]
    {
        104
    }
}

/* ========================================================================= *
 *  File-scope state (the C `static` globals).
 * ========================================================================= */

thread_local! {
    /// `static bool ssl_is_server_start;` — used by `ssl_external_passwd_cb`.
    static SSL_IS_SERVER_START: RefCell<bool> = const { RefCell::new(false) };
    /// `static const char *cert_errdetail;` — set by `verify_cb`, consumed by
    /// the accept loop's `SSL_ERROR_SSL` arm.
    static CERT_ERRDETAIL: RefCell<Option<String>> = const { RefCell::new(None) };
    /// The provider-owned `SSL *` per connection, keyed by socket (the C
    /// `port->ssl`). Installed by `be_tls_open_server`, cleared by
    /// `be_tls_close`.
    static PORT_SSL: RefCell<HashMap<i32, Ssl>> = RefCell::new(HashMap::new());
}

fn port_ssl(sock: i32) -> Ssl {
    PORT_SSL.with(|m| m.borrow().get(&sock).copied().unwrap_or(0))
}
fn set_port_ssl(sock: i32, ssl: Ssl) {
    PORT_SSL.with(|m| {
        m.borrow_mut().insert(sock, ssl);
    });
}
fn clear_port_ssl(sock: i32) {
    PORT_SSL.with(|m| {
        m.borrow_mut().remove(&sock);
    });
}

/* ========================================================================= *
 *  Result carrier types for the be-secure.c dispatch boundary.
 * ========================================================================= */

/// What `be_tls_open_server` copies into the `Port` on a completed handshake.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsOpenResult {
    pub ssl_in_use: bool,
    pub alpn_used: bool,
    pub peer_cn: Option<String>,
    pub peer_dn: Option<String>,
    pub peer_cert_valid: bool,
}

/// The `(n, waitfor)` outcome of `be_tls_read`/`be_tls_write`, plus the errno
/// the C function would have left in the process `errno` (be-secure.c reads it
/// for the would-block test). On `n > 0` reads, `data` carries the decrypted
/// bytes for the caller to copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsIo {
    pub n: isize,
    pub waitfor: i32,
    pub errno: i32,
}

/* ========================================================================= *
 *  Public interface
 * ========================================================================= */

/// `int be_tls_init(bool isServerStart)`.
///
/// Returns `Ok(loaded_ca)` (whether the CA store was loaded into the active
/// context, i.e. the `ssl_loaded_verify_locations` flag) on success, or
/// `Err(())` on the C `goto error` path (the FATAL/LOG `ereport` is already
/// emitted). A `FATAL` (server start) report propagates as the surrounding
/// `PgResult` error.
pub fn be_tls_init(min_version: i32, max_version: i32, is_server_start: bool) -> PgResult<Result<bool, ()>> {
    let level = if is_server_start { FATAL } else { LOG };

    // context = SSL_CTX_new(SSLv23_method());
    let context: SslCtx = ffi::ssl_ctx_new_server::call();
    if context == 0 {
        let ecode = ffi::err_get_error::call();
        ereport(level)
            .errmsg(format!("could not create SSL context: {}", ssl_errmessage(ecode)))
            .finish(errloc("be_tls_init"))?;
        return Ok(Err(())); // goto error (context is 0; nothing to free)
    }

    // A closure-free `goto error`: any failure below frees `context` and
    // returns. We thread it as an inner fallible block.
    let result = be_tls_init_inner(context, min_version, max_version, is_server_start, level);
    match result {
        Ok(Ok(loaded_ca)) => Ok(Ok(loaded_ca)),
        Ok(Err(())) => {
            // error: if (context) SSL_CTX_free(context); return -1;
            ffi::ssl_ctx_free::call(context);
            Ok(Err(()))
        }
        Err(e) => {
            // A FATAL/ERROR report unwinds; still free the working context.
            ffi::ssl_ctx_free::call(context);
            Err(e)
        }
    }
}

fn be_tls_init_inner(
    context: SslCtx,
    ssl_min_protocol_version: i32,
    ssl_max_protocol_version: i32,
    is_server_start: bool,
    level: backend_utils_error::ErrorLevel,
) -> PgResult<Result<bool, ()>> {
    // The file-scope GUC strings (read once; `read()` yields `Option<String>`,
    // an empty/NULL GUC behaves like the C empty `char[0]` test).
    let ssl_cert_file = vars::ssl_cert_file.read().unwrap_or_default();
    let ssl_key_file = vars::ssl_key_file.read().unwrap_or_default();
    let ssl_ca_file = vars::ssl_ca_file.read().unwrap_or_default();
    let ssl_crl_file = vars::ssl_crl_file.read().unwrap_or_default();
    let ssl_crl_dir = vars::ssl_crl_dir.read().unwrap_or_default();
    let ssl_cipher_suites = vars::SSLCipherSuites.read().unwrap_or_default();
    let ssl_cipher_list = vars::SSLCipherList.read().unwrap_or_default();
    let ssl_ecdh_curve = vars::SSLECDHCurve.read().unwrap_or_default();
    let ssl_prefer_server_ciphers = vars::SSLPreferServerCiphers.read();

    macro_rules! goto_error {
        ($b:expr) => {{
            $b.finish(errloc("be_tls_init"))?;
            return Ok(Err(()));
        }};
    }

    let mut ssl_ver_min: i32 = -1;
    let mut ssl_ver_max: i32 = -1;

    // SSL_CTX_set_mode(context, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    ffi::ssl_ctx_set_mode_accept_moving_write_buffer::call(context);

    // (*openssl_tls_init_hook)(context, isServerStart);
    default_openssl_tls_init(context, is_server_start);

    // ssl_is_server_start = isServerStart;
    SSL_IS_SERVER_START.with(|c| *c.borrow_mut() = is_server_start);

    // SSL_CTX_use_certificate_chain_file(context, ssl_cert_file)
    if ffi::ssl_ctx_use_certificate_chain_file::call(context, &ssl_cert_file) != 1 {
        let ecode = ffi::err_get_error::call();
        goto_error!(ereport(level).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(format!(
            "could not load server certificate file \"{ssl_cert_file}\": {}",
            ssl_errmessage(ecode)
        )));
    }

    // if (!check_ssl_key_file_permissions(ssl_key_file, isServerStart)) goto error;
    if !backend_libpq_be_secure_common_seams::check_ssl_key_file_permissions::call(
        &ssl_key_file,
        is_server_start,
    )? {
        return Ok(Err(()));
    }

    // dummy_ssl_passwd_cb_called = false;
    ffi::reset_dummy_ssl_passwd_cb_called::call();

    // SSL_CTX_use_PrivateKey_file(context, ssl_key_file, SSL_FILETYPE_PEM)
    if ffi::ssl_ctx_use_private_key_file_pem::call(context, &ssl_key_file) != 1 {
        if ffi::dummy_ssl_passwd_cb_called::call() {
            goto_error!(ereport(level).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(format!(
                "private key file \"{ssl_key_file}\" cannot be reloaded because it requires a passphrase"
            )));
        } else {
            let ecode = ffi::err_get_error::call();
            goto_error!(ereport(level).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(format!(
                "could not load private key file \"{ssl_key_file}\": {}",
                ssl_errmessage(ecode)
            )));
        }
    }

    // SSL_CTX_check_private_key(context)
    if ffi::ssl_ctx_check_private_key::call(context) != 1 {
        let ecode = ffi::err_get_error::call();
        goto_error!(ereport(level).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(format!(
            "check of private key failed: {}",
            ssl_errmessage(ecode)
        )));
    }

    // Protocol-version range.
    if ssl_min_protocol_version != 0 {
        ssl_ver_min = ssl_protocol_version_to_openssl(ssl_min_protocol_version);
        if ssl_ver_min == -1 {
            let val = get_config_option("ssl_min_protocol_version");
            goto_error!(ereport(level).errmsg(format!(
                "\"{}\" setting \"{}\" not supported by this build",
                "ssl_min_protocol_version", val
            )));
        }
        if ffi::ssl_ctx_set_min_proto_version::call(context, ssl_ver_min) == 0 {
            goto_error!(ereport(level).errmsg("could not set minimum SSL protocol version"));
        }
    }

    if ssl_max_protocol_version != 0 {
        ssl_ver_max = ssl_protocol_version_to_openssl(ssl_max_protocol_version);
        if ssl_ver_max == -1 {
            let val = get_config_option("ssl_max_protocol_version");
            goto_error!(ereport(level).errmsg(format!(
                "\"{}\" setting \"{}\" not supported by this build",
                "ssl_max_protocol_version", val
            )));
        }
        if ffi::ssl_ctx_set_max_proto_version::call(context, ssl_ver_max) == 0 {
            goto_error!(ereport(level).errmsg("could not set maximum SSL protocol version"));
        }
    }

    // Check compatibility of min/max protocols.
    if ssl_min_protocol_version != 0 && ssl_max_protocol_version != 0 && ssl_ver_min > ssl_ver_max {
        goto_error!(ereport(level)
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg("could not set SSL protocol version range")
            .errdetail(format!(
                "\"{}\" cannot be higher than \"{}\"",
                "ssl_min_protocol_version", "ssl_max_protocol_version"
            )));
    }

    // Disallow tickets / caching / compression / renegotiation.
    ffi::ssl_ctx_disallow_tickets::call(context);
    ffi::ssl_ctx_disable_session_cache::call(context);
    ffi::ssl_ctx_disallow_compression::call(context);
    ffi::ssl_ctx_disallow_renegotiation::call(context);

    // set up ephemeral DH and ECDH keys
    if !initialize_dh(context, &ssl_dh_params_file(), is_server_start)? {
        return Ok(Err(()));
    }
    if !initialize_ecdh(context, &ssl_ecdh_curve, is_server_start)? {
        return Ok(Err(()));
    }

    // SSL_CTX_set_cipher_list(context, SSLCipherList)
    if ffi::ssl_ctx_set_cipher_list::call(context, &ssl_cipher_list) != 1 {
        goto_error!(ereport(level).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(
            "could not set the TLSv1.2 cipher list (no valid ciphers available)"
        ));
    }

    // TLSv1.3 cipher suites (only when the GUC is non-empty).
    if !ssl_cipher_suites.is_empty() {
        if ffi::ssl_ctx_set_ciphersuites::call(context, &ssl_cipher_suites) != 1 {
            goto_error!(ereport(level).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(
                "could not set the TLSv1.3 cipher suites (no valid ciphers available)"
            ));
        }
    }

    // if (SSLPreferServerCiphers) SSL_CTX_set_options(...);
    if ssl_prefer_server_ciphers {
        ffi::ssl_ctx_set_cipher_server_preference::call(context);
    }

    // Load CA store, so we can verify client certificates if needed.
    if !ssl_ca_file.is_empty() {
        if !ffi::ssl_ctx_load_ca::call(context, &ssl_ca_file) {
            let ecode = ffi::err_get_error::call();
            goto_error!(ereport(level).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(format!(
                "could not load root certificate file \"{ssl_ca_file}\": {}",
                ssl_errmessage(ecode)
            )));
        }
        // Always ask for SSL client cert, but don't fail if it's not presented.
        ffi::ssl_ctx_set_verify_peer::call(context);
    }

    // Load the Certificate Revocation List (CRL).
    if !ssl_crl_file.is_empty() || !ssl_crl_dir.is_empty() {
        let crl_file = if !ssl_crl_file.is_empty() { Some(ssl_crl_file.as_str()) } else { None };
        let crl_dir = if !ssl_crl_dir.is_empty() { Some(ssl_crl_dir.as_str()) } else { None };
        match ffi::ssl_ctx_setup_crl::call(context, crl_file, crl_dir) {
            // cvstore was NULL: C does nothing.
            None => {}
            // X509_STORE_load_locations == 1: flags already set inside provider.
            Some(true) => {}
            // load failed: pick the right ereport per which GUC was set.
            Some(false) => {
                let ecode = ffi::err_get_error::call();
                if ssl_crl_dir.is_empty() {
                    goto_error!(ereport(level).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(format!(
                        "could not load SSL certificate revocation list file \"{ssl_crl_file}\": {}",
                        ssl_errmessage(ecode)
                    )));
                } else if ssl_crl_file.is_empty() {
                    goto_error!(ereport(level).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(format!(
                        "could not load SSL certificate revocation list directory \"{ssl_crl_dir}\": {}",
                        ssl_errmessage(ecode)
                    )));
                } else {
                    goto_error!(ereport(level).errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(format!(
                        "could not load SSL certificate revocation list file \"{ssl_crl_file}\" or directory \"{ssl_crl_dir}\": {}",
                        ssl_errmessage(ecode)
                    )));
                }
            }
        }
    }

    // Success! Replace any existing SSL_context.
    let prev = ffi::get_active_ssl_context::call();
    if prev != 0 {
        ffi::ssl_ctx_free::call(prev);
    }
    ffi::set_active_ssl_context::call(context);

    // ssl_loaded_verify_locations = (ssl_ca_file[0] != 0)
    let loaded_verify = !ssl_ca_file.is_empty();
    Ok(Ok(loaded_verify))
}

/// `void be_tls_destroy(void)`. Returns whether the active context was cleared
/// (so be-secure.c can clear its `ssl_loaded_verify_locations` mirror).
pub fn be_tls_destroy() -> bool {
    let ctx = ffi::get_active_ssl_context::call();
    if ctx != 0 {
        ffi::ssl_ctx_free::call(ctx);
    }
    ffi::set_active_ssl_context::call(0);
    // ssl_loaded_verify_locations = false;
    true
}

/// `int be_tls_open_server(Port *port)`.
///
/// Runs the server-side handshake and returns the negotiated facts. `wait` is
/// the accept loop's between-step sleep (be-secure.c's `WaitLatchOrSocket(NULL,
/// waitfor, port->sock, 0, WAIT_EVENT_SSL_OPEN_SERVER)`), invoked with the
/// `waitfor` bitmask. Returns `Ok(Ok(result))` on a completed handshake or
/// `Ok(Err(()))` on failure (the COMMERROR `ereport` already emitted; C's
/// `return -1`).
pub fn be_tls_open_server(
    port: &mut Port,
    mut wait: impl FnMut(u32),
) -> PgResult<Result<TlsOpenResult, ()>> {
    let sock = port.sock;

    // Assert(!port->ssl); Assert(!port->peer);
    debug_assert_eq!(port_ssl(sock), 0);

    // if (!SSL_context) { ereport(COMMERROR, ...); return -1; }
    let context = ffi::get_active_ssl_context::call();
    if context == 0 {
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("could not initialize SSL connection: SSL context not set up")
            .finish(errloc("be_tls_open_server"))?;
        return Ok(Err(()));
    }

    // SSL_CTX_set_info_callback(SSL_context, info_cb);
    ffi::ssl_ctx_set_info_callback::call(context);
    // SSL_CTX_set_alpn_select_cb(SSL_context, alpn_cb, port);
    ffi::ssl_ctx_set_alpn_select_cb::call(context, sock as u64);

    // if (!(port->ssl = SSL_new(SSL_context))) { ereport; return -1; }
    let ssl = ffi::ssl_new::call(context);
    if ssl == 0 {
        let ecode = ffi::err_get_error::call();
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!("could not initialize SSL connection: {}", ssl_errmessage(ecode)))
            .finish(errloc("be_tls_open_server"))?;
        return Ok(Err(()));
    }
    set_port_ssl(sock, ssl);

    // if (!ssl_set_port_bio(port)) { ereport; return -1; }
    if ffi::ssl_set_port_bio::call(ssl, sock as u64) == 0 {
        let ecode = ffi::err_get_error::call();
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!("could not set SSL socket: {}", ssl_errmessage(ecode)))
            .finish(errloc("be_tls_open_server"))?;
        return Ok(Err(()));
    }
    // port->ssl_in_use = true;  (recorded in the result; be-secure.c sets it)
    let mut result = TlsOpenResult {
        ssl_in_use: true,
        ..Default::default()
    };

    // aloop:
    loop {
        let acc = ffi::ssl_accept::call(ssl);
        if acc.r <= 0 {
            let err = acc.err;
            let ecode = acc.ecode;
            match err {
                SSL_ERROR_WANT_READ | SSL_ERROR_WANT_WRITE => {
                    // not allowed during connection establishment
                    debug_assert!(!port.noblock);
                    let waitfor = if err == SSL_ERROR_WANT_READ {
                        types_storage::waiteventset::WL_SOCKET_READABLE
                            | types_storage::waiteventset::WL_EXIT_ON_PM_DEATH
                    } else {
                        types_storage::waiteventset::WL_SOCKET_WRITEABLE
                            | types_storage::waiteventset::WL_EXIT_ON_PM_DEATH
                    };
                    wait(waitfor);
                    continue; // goto aloop;
                }
                SSL_ERROR_SYSCALL => {
                    if acc.r < 0 && acc.sys_errno != 0 {
                        ereport(COMMERROR)
                            .with_saved_errno(acc.sys_errno)
                            .errcode_for_socket_access()
                            .errmsg("could not accept SSL connection: %m")
                            .finish(errloc("be_tls_open_server"))?;
                    } else {
                        ereport(COMMERROR)
                            .errcode(ERRCODE_PROTOCOL_VIOLATION)
                            .errmsg("could not accept SSL connection: EOF detected")
                            .finish(errloc("be_tls_open_server"))?;
                    }
                }
                SSL_ERROR_SSL => {
                    let give_proto_hint = matches!(
                        ffi::err_get_reason::call(ecode),
                        x if x == SSL_R_NO_PROTOCOLS_AVAILABLE
                            || x == SSL_R_UNSUPPORTED_PROTOCOL
                            || x == SSL_R_BAD_PROTOCOL_VERSION_NUMBER
                            || x == SSL_R_UNKNOWN_PROTOCOL
                            || x == SSL_R_UNKNOWN_SSL_VERSION
                            || x == SSL_R_UNSUPPORTED_SSL_VERSION
                            || x == SSL_R_WRONG_SSL_VERSION
                            || x == SSL_R_WRONG_VERSION_NUMBER
                            || x == SSL_R_TLSV1_ALERT_PROTOCOL_VERSION
                            || x == SSL_R_VERSION_TOO_HIGH
                            || x == SSL_R_VERSION_TOO_LOW
                    );
                    let cert_detail = CERT_ERRDETAIL.with(|c| c.borrow().clone());
                    let mut b = ereport(COMMERROR)
                        .errcode(ERRCODE_PROTOCOL_VIOLATION)
                        .errmsg(format!("could not accept SSL connection: {}", ssl_errmessage(ecode)));
                    if let Some(detail) = cert_detail {
                        b = b.errdetail_internal(detail);
                    }
                    if give_proto_hint {
                        let lo = if ssl_min_protocol_version_guc() != 0 {
                            ssl_protocol_version_to_string(ssl_min_protocol_version_guc()).to_string()
                        } else {
                            MIN_OPENSSL_TLS_VERSION.to_string()
                        };
                        let hi = if ssl_max_protocol_version_guc() != 0 {
                            ssl_protocol_version_to_string(ssl_max_protocol_version_guc()).to_string()
                        } else {
                            MAX_OPENSSL_TLS_VERSION.to_string()
                        };
                        b = b.errhint(format!(
                            "This may indicate that the client does not support any SSL protocol version between {lo} and {hi}."
                        ));
                    }
                    b.finish(errloc("be_tls_open_server"))?;
                    CERT_ERRDETAIL.with(|c| *c.borrow_mut() = None);
                }
                SSL_ERROR_ZERO_RETURN => {
                    ereport(COMMERROR)
                        .errcode(ERRCODE_PROTOCOL_VIOLATION)
                        .errmsg("could not accept SSL connection: EOF detected")
                        .finish(errloc("be_tls_open_server"))?;
                }
                _ => {
                    ereport(COMMERROR)
                        .errcode(ERRCODE_PROTOCOL_VIOLATION)
                        .errmsg(format!("unrecognized SSL error code: {err}"))
                        .finish(errloc("be_tls_open_server"))?;
                }
            }
            return Ok(Err(()));
        }
        // r > 0: handshake completed.
        break;
    }

    // Get the protocol selected by ALPN.
    result.alpn_used = false;
    if let Some(selected) = ffi::ssl_get0_alpn_selected::call(ssl) {
        if selected.len() == PG_ALPN_PROTOCOL.len() && selected == PG_ALPN_PROTOCOL {
            result.alpn_used = true;
        } else {
            // shouldn't happen
            ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("received SSL connection request with unexpected ALPN protocol")
                .finish(errloc("be_tls_open_server"))?;
        }
    }

    // Get client certificate, if available.
    let peer = ffi::ssl_get_peer_certificate::call(ssl);
    result.peer_cn = None;
    result.peer_dn = None;
    result.peer_cert_valid = false;
    if peer != 0 {
        let x509name = ffi::x509_get_subject_name::call(peer);

        // Common Name.
        if let Some(cn_bytes) = ffi::x509_name_get_common_name::call(x509name) {
            // Reject embedded NULs (CVE-2009-4034): len != strlen(peer_cn).
            if cn_bytes.contains(&0) {
                ereport(COMMERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("SSL certificate's common name contains embedded null")
                    .finish(errloc("be_tls_open_server"))?;
                ffi::x509_free::call(peer);
                return Ok(Err(()));
            }
            result.peer_cn = Some(String::from_utf8_lossy(&cn_bytes).into_owned());
        }

        // Distinguished Name (RFC2253).
        match ffi::x509_name_print_rfc2253::call(x509name) {
            None => {
                // BIO alloc/print failure: C returns -1 (no ereport).
                ffi::x509_free::call(peer);
                return Ok(Err(()));
            }
            Some(dn_bytes) => {
                if dn_bytes.contains(&0) {
                    ereport(COMMERROR)
                        .errcode(ERRCODE_PROTOCOL_VIOLATION)
                        .errmsg("SSL certificate's distinguished name contains embedded null")
                        .finish(errloc("be_tls_open_server"))?;
                    ffi::x509_free::call(peer);
                    return Ok(Err(()));
                }
                result.peer_dn = Some(String::from_utf8_lossy(&dn_bytes).into_owned());
            }
        }

        result.peer_cert_valid = true;
        // The peer cert handle is freed by be_tls_close (port->peer). Stash it
        // alongside the ssl so close can release it; we key it on the socket.
        set_peer_cert(sock, peer);
    }

    Ok(Ok(result))
}

/// `void be_tls_close(Port *port)`.
pub fn be_tls_close(port: &mut Port) {
    let sock = port.sock;
    let ssl = port_ssl(sock);
    if ssl != 0 {
        ffi::ssl_shutdown_and_free::call(ssl);
        clear_port_ssl(sock);
        port.ssl_in_use = false;
    }
    let peer = take_peer_cert(sock);
    if peer != 0 {
        ffi::x509_free::call(peer);
    }
    if port.peer_cn.is_some() {
        port.peer_cn = None;
    }
    if port.peer_dn.is_some() {
        port.peer_dn = None;
    }
}

/// `ssize_t be_tls_read(Port *port, void *ptr, size_t len, int *waitfor)`.
pub fn be_tls_read(port: &mut Port, len: usize) -> (TlsIo, Vec<u8>) {
    let ssl = port_ssl(port.sock);
    let (res, data) = ffi::ssl_read::call(ssl, len);
    let mut n = res.n;
    let mut errno = 0;
    let mut waitfor = 0;
    match res.err {
        SSL_ERROR_NONE => { /* a-ok */ }
        SSL_ERROR_WANT_READ => {
            waitfor = types_storage::waiteventset::WL_SOCKET_READABLE as i32;
            errno = EWOULDBLOCK;
            n = -1;
        }
        SSL_ERROR_WANT_WRITE => {
            waitfor = types_storage::waiteventset::WL_SOCKET_WRITEABLE as i32;
            errno = EWOULDBLOCK;
            n = -1;
        }
        SSL_ERROR_SYSCALL => {
            errno = res.sys_errno;
            if n != -1 || errno == 0 {
                errno = ECONNRESET;
                n = -1;
            }
        }
        SSL_ERROR_SSL => {
            let _ = ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!("SSL error: {}", ssl_errmessage(res.ecode)))
                .finish(errloc("be_tls_read"));
            errno = ECONNRESET;
            n = -1;
        }
        SSL_ERROR_ZERO_RETURN => {
            // connection was cleanly shut down by peer
            n = 0;
        }
        other => {
            let _ = ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!("unrecognized SSL error code: {other}"))
                .finish(errloc("be_tls_read"));
            errno = ECONNRESET;
            n = -1;
        }
    }
    (TlsIo { n, waitfor, errno }, data)
}

/// `ssize_t be_tls_write(Port *port, const void *ptr, size_t len, int *waitfor)`.
pub fn be_tls_write(port: &mut Port, buf: &[u8]) -> TlsIo {
    let ssl = port_ssl(port.sock);
    let res = ffi::ssl_write::call(ssl, buf);
    let mut n = res.n;
    let mut errno = 0;
    let mut waitfor = 0;
    match res.err {
        SSL_ERROR_NONE => { /* a-ok */ }
        SSL_ERROR_WANT_READ => {
            waitfor = types_storage::waiteventset::WL_SOCKET_READABLE as i32;
            errno = EWOULDBLOCK;
            n = -1;
        }
        SSL_ERROR_WANT_WRITE => {
            waitfor = types_storage::waiteventset::WL_SOCKET_WRITEABLE as i32;
            errno = EWOULDBLOCK;
            n = -1;
        }
        SSL_ERROR_SYSCALL => {
            errno = res.sys_errno;
            if n != -1 || errno == 0 {
                errno = ECONNRESET;
                n = -1;
            }
        }
        SSL_ERROR_SSL => {
            let _ = ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!("SSL error: {}", ssl_errmessage(res.ecode)))
                .finish(errloc("be_tls_write"));
            errno = ECONNRESET;
            n = -1;
        }
        SSL_ERROR_ZERO_RETURN => {
            // the SSL connection was closed
            errno = ECONNRESET;
            n = -1;
        }
        other => {
            let _ = ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!("unrecognized SSL error code: {other}"))
                .finish(errloc("be_tls_write"));
            errno = ECONNRESET;
            n = -1;
        }
    }
    TlsIo { n, waitfor, errno }
}

/* ========================================================================= *
 *  Peer-cert handle stash (the C `port->peer`, not a field of types_net::Port)
 * ========================================================================= */

thread_local! {
    static PORT_PEER: RefCell<HashMap<i32, X509>> = RefCell::new(HashMap::new());
}
fn set_peer_cert(sock: i32, peer: X509) {
    PORT_PEER.with(|m| {
        m.borrow_mut().insert(sock, peer);
    });
}
fn take_peer_cert(sock: i32) -> X509 {
    PORT_PEER.with(|m| m.borrow_mut().remove(&sock).unwrap_or(0))
}

/* ========================================================================= *
 *  Internal functions: callbacks (the C `static` callbacks, ported as the
 *  pure-logic the OpenSSL provider would invoke).
 * ========================================================================= */

/// `default_openssl_tls_init(SSL_CTX *context, bool isServerStart)`.
fn default_openssl_tls_init(context: SslCtx, is_server_start: bool) {
    let ssl_passphrase_command = vars::ssl_passphrase_command.read().unwrap_or_default();
    let supports_reload = vars::ssl_passphrase_command_supports_reload.read();
    if is_server_start {
        if !ssl_passphrase_command.is_empty() {
            ffi::ssl_ctx_set_default_passwd_cb::call(context, PasswdCb::External);
        }
    } else if !ssl_passphrase_command.is_empty() && supports_reload {
        ffi::ssl_ctx_set_default_passwd_cb::call(context, PasswdCb::External);
    } else {
        // If reloading and no external command, override OpenSSL's default
        // handling so we don't prompt for a passphrase in a running server.
        ffi::ssl_ctx_set_default_passwd_cb::call(context, PasswdCb::Dummy);
    }
}

/// `ssl_external_passwd_cb(char *buf, int size, int rwflag, void *userdata)`.
/// Invoked by OpenSSL's private-key load; runs `ssl_passphrase_command`.
/// Returns the passphrase bytes (the C return value is its length). The
/// provider copies them into OpenSSL's `buf` of capacity `size`.
pub fn ssl_external_passwd_cb<'mcx>(mcx: Mcx<'mcx>, size: i32) -> PgResult<mcx::PgVec<'mcx, u8>> {
    const PROMPT: &str = "Enter PEM pass phrase:";
    let is_server_start = SSL_IS_SERVER_START.with(|c| *c.borrow());
    // run_ssl_passphrase_command(prompt, ssl_is_server_start, buf, size)
    backend_libpq_be_secure_common_seams::run_ssl_passphrase_command::call(
        mcx,
        PROMPT,
        is_server_start,
        size,
    )
}

/// `info_cb(const SSL *ssl, int type, int args)` — copy SSL info messages into
/// the log. `desc` is `SSL_state_string_long(ssl)`, supplied by the provider.
pub fn info_cb(type_: i32, args: i32, desc: &str) {
    use types_error::DEBUG4;
    // SSL_CB_* constants (openssl/ssl.h).
    const SSL_CB_HANDSHAKE_START: i32 = 0x10;
    const SSL_CB_HANDSHAKE_DONE: i32 = 0x20;
    const SSL_CB_ACCEPT_LOOP: i32 = 0x2001;
    const SSL_CB_ACCEPT_EXIT: i32 = 0x2002;
    const SSL_CB_CONNECT_LOOP: i32 = 0x1001;
    const SSL_CB_CONNECT_EXIT: i32 = 0x1002;
    const SSL_CB_READ_ALERT: i32 = 0x4004;
    const SSL_CB_WRITE_ALERT: i32 = 0x4008;
    let msg = match type_ {
        SSL_CB_HANDSHAKE_START => Some(format!("SSL: handshake start: \"{desc}\"")),
        SSL_CB_HANDSHAKE_DONE => Some(format!("SSL: handshake done: \"{desc}\"")),
        SSL_CB_ACCEPT_LOOP => Some(format!("SSL: accept loop: \"{desc}\"")),
        SSL_CB_ACCEPT_EXIT => Some(format!("SSL: accept exit ({args}): \"{desc}\"")),
        SSL_CB_CONNECT_LOOP => Some(format!("SSL: connect loop: \"{desc}\"")),
        SSL_CB_CONNECT_EXIT => Some(format!("SSL: connect exit ({args}): \"{desc}\"")),
        SSL_CB_READ_ALERT => Some(format!("SSL: read alert (0x{args:04x}): \"{desc}\"")),
        SSL_CB_WRITE_ALERT => Some(format!("SSL: write alert (0x{args:04x}): \"{desc}\"")),
        _ => None,
    };
    if let Some(m) = msg {
        let _ = ereport(DEBUG4).errmsg_internal(m).finish(errloc("info_cb"));
    }
}

/// ALPN protocol vector `PG_ALPN_PROTOCOL_VECTOR` (libpq/pqcomm.h): a single
/// length-prefixed entry `{ 10, 'p','o','s','t','g','r','e','s','q','l' }`.
pub const ALPN_PROTOS: &[u8] = &[
    10, b'p', b'o', b's', b't', b'g', b'r', b'e', b's', b'q', b'l',
];

/// `verify_cb(int ok, X509_STORE_CTX *ctx)` — certificate verification
/// callback. On a verification failure it assembles the detail message and
/// stashes it in `cert_errdetail` (read by the accept loop). The provider
/// passes the verification facts already extracted from the `X509_STORE_CTX`
/// (depth, the error string, and, if a current cert is present, its prepared
/// subject/issuer/serial). Returns `ok` unchanged.
pub fn verify_cb(
    ok: bool,
    depth: i32,
    errstring: &str,
    cert: Option<VerifyCertInfo>,
) -> bool {
    if ok {
        return ok;
    }
    let mut s = format!("Client certificate verification failed at depth {depth}: {errstring}.");
    if let Some(c) = cert {
        // subject/issuer already X509_NAME_to_cstring'd by the provider;
        // prepare_cert_name truncates + sanitizes here.
        let sub_prepared = prepare_cert_name(&c.subject);
        let iss_prepared = prepare_cert_name(&c.issuer);
        let serialno = c.serial.unwrap_or_else(|| "unknown".to_string());
        s.push('\n');
        s.push_str(&format!(
            "Failed certificate data (unverified): subject \"{sub_prepared}\", serial number {serialno}, issuer \"{iss_prepared}\"."
        ));
    }
    CERT_ERRDETAIL.with(|c| *c.borrow_mut() = Some(s));
    ok
}

/// The verification-failure cert facts the provider extracts for [`verify_cb`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyCertInfo {
    /// `X509_NAME_to_cstring(X509_get_subject_name(cert))`.
    pub subject: String,
    /// `X509_NAME_to_cstring(X509_get_issuer_name(cert))`.
    pub issuer: String,
    /// `BN_bn2dec(ASN1_INTEGER_to_BN(X509_get_serialNumber(cert)))`; `None`
    /// when OpenSSL returned NULL (the C `serialno ? serialno : _("unknown")`).
    pub serial: Option<String>,
}

/// `prepare_cert_name(char *name)` — truncate an over-long cert name and
/// sanitize unprintable ASCII. Keeps the most-specific (trailing) portion.
fn prepare_cert_name(name: &str) -> String {
    const MAXLEN: usize = 71;
    let bytes = name.as_bytes();
    let namelen = bytes.len();
    let truncated: Vec<u8> = if namelen > MAXLEN {
        // Keep the end; overwrite the first 3 kept bytes with '.'.
        let mut t = bytes[namelen - MAXLEN..].to_vec();
        if t.len() >= 3 {
            t[0] = b'.';
            t[1] = b'.';
            t[2] = b'.';
        }
        t
    } else {
        bytes.to_vec()
    };
    // pg_clean_ascii(truncated, 0): copy bytes up to the first NUL, escaping
    // any byte < 32 or > 126 as "\xXX". (common/string.c pg_clean_ascii.)
    pg_clean_ascii(&truncated)
}

/// `pg_clean_ascii(const char *str, int alloc_flags)` (common/string.c),
/// inlined for the no-mcx `verify_cb` path. Iterates the NUL-terminated input
/// (stops at the first `\0`), passing through printable ASCII (32..=126) and
/// escaping every other byte as `\xXX`.
fn pg_clean_ascii(bytes: &[u8]) -> String {
    let mut out = String::new();
    for &b in bytes {
        if b == 0 {
            break;
        }
        if b < 32 || b > 126 {
            out.push_str(&format!("\\x{b:02x}"));
        } else {
            out.push(b as char);
        }
    }
    out
}

/* ========================================================================= *
 *  Protocol-version mapping helpers.
 * ========================================================================= */

/// `ssl_protocol_version_to_openssl(int v)`.
fn ssl_protocol_version_to_openssl(v: i32) -> i32 {
    match v {
        PG_TLS_ANY => 0,
        PG_TLS1_VERSION => TLS1_VERSION,
        PG_TLS1_1_VERSION => TLS1_1_VERSION,
        PG_TLS1_2_VERSION => TLS1_2_VERSION,
        PG_TLS1_3_VERSION => TLS1_3_VERSION,
        _ => -1,
    }
}

/// `ssl_protocol_version_to_string(int v)`.
fn ssl_protocol_version_to_string(v: i32) -> &'static str {
    match v {
        PG_TLS_ANY => "any",
        PG_TLS1_VERSION => "TLSv1",
        PG_TLS1_1_VERSION => "TLSv1.1",
        PG_TLS1_2_VERSION => "TLSv1.2",
        PG_TLS1_3_VERSION => "TLSv1.3",
        _ => "(unrecognized)",
    }
}

/* ========================================================================= *
 *  DH / ECDH setup (the libcrypto DH work lives behind the FFI seam).
 * ========================================================================= */

/// `initialize_dh(SSL_CTX *context, bool isServerStart)`. The `load_dh_file`
/// validation and `load_dh_buffer` fallback all happen inside the provider
/// (they need libcrypto's `DH_check`); the provider reports success/failure and
/// this function emits the "could not load DH parameters" report on failure.
fn initialize_dh(context: SslCtx, dh_params_file: &str, is_server_start: bool) -> PgResult<bool> {
    let level = if is_server_start { FATAL } else { LOG };
    // SSL_CTX_set_options(context, SSL_OP_SINGLE_DH_USE);
    ffi::ssl_ctx_set_single_dh_use::call(context);

    let file = if !dh_params_file.is_empty() { Some(dh_params_file) } else { None };
    if !ffi::ssl_ctx_setup_dh::call(context, file, is_server_start) {
        // The provider performed load_dh_file/load_dh_buffer + DH_check +
        // SSL_CTX_set_tmp_dh, emitting any specific DH ereport itself; the
        // catch-all "could not load DH parameters" report is here.
        ereport(level)
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg("DH: could not load DH parameters")
            .finish(errloc("initialize_dh"))?;
        return Ok(false);
    }
    Ok(true)
}

/// `initialize_ecdh(SSL_CTX *context, bool isServerStart)`.
fn initialize_ecdh(context: SslCtx, ssl_ecdh_curve: &str, is_server_start: bool) -> PgResult<bool> {
    let level = if is_server_start { FATAL } else { LOG };
    // SSL_CTX_set1_groups_list(context, SSLECDHCurve)
    if ffi::ssl_ctx_set_groups_list::call(context, ssl_ecdh_curve) != 1 {
        let ecode = ffi::err_get_error::call();
        ereport(level)
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(format!(
                "could not set group names specified in ssl_groups: {}",
                ssl_errmessage_ext(ecode, "No valid groups found")
            ))
            .errhint("Ensure that each group name is spelled correctly and supported by the installed version of OpenSSL.")
            .finish(errloc("initialize_ecdh"))?;
        return Ok(false);
    }
    Ok(true)
}

/* ========================================================================= *
 *  SSLerrmessage / SSLerrmessageExt
 * ========================================================================= */

/// `SSLerrmessage(unsigned long ecode)`. `ecode == 0` yields "no SSL error
/// reported"; otherwise the provider performs `ERR_reason_error_string` (with
/// its OpenSSL-3 errno + numeric fallbacks), always returning a non-NULL
/// string.
fn ssl_errmessage(ecode: u64) -> String {
    if ecode == 0 {
        return "no SSL error reported".to_string();
    }
    ffi::ssl_err_reason_string::call(ecode)
}

/// `SSLerrmessageExt(unsigned long ecode, const char *replacement)`.
fn ssl_errmessage_ext(ecode: u64, replacement: &str) -> String {
    if ecode == 0 {
        replacement.to_string()
    } else {
        ssl_errmessage(ecode)
    }
}

/* ========================================================================= *
 *  be_tls_get_* accessors (consumed by postinit / backend-startup via the
 *  be-secure seam contract).
 * ========================================================================= */

/// `int be_tls_get_cipher_bits(Port *port)`.
pub fn be_tls_get_cipher_bits(port: &mut Port) -> i32 {
    let ssl = port_ssl(port.sock);
    if ssl != 0 {
        ffi::ssl_get_cipher_bits::call(ssl)
    } else {
        0
    }
}

/// `const char *be_tls_get_version(Port *port)`. C returns NULL when
/// `port->ssl` is NULL; the seam contract returns an (empty) `PgString` — the
/// caller (`PerformAuthentication`) only formats it into a log line.
pub fn be_tls_get_version<'mcx>(mcx: Mcx<'mcx>, port: &mut Port) -> PgResult<PgString<'mcx>> {
    let ssl = port_ssl(port.sock);
    let s = if ssl != 0 {
        ffi::ssl_get_version::call(ssl).unwrap_or_default()
    } else {
        String::new()
    };
    PgString::from_str_in(&s, mcx)
}

/// `const char *be_tls_get_cipher(Port *port)`.
pub fn be_tls_get_cipher<'mcx>(mcx: Mcx<'mcx>, port: &mut Port) -> PgResult<PgString<'mcx>> {
    let ssl = port_ssl(port.sock);
    let s = if ssl != 0 {
        ffi::ssl_get_cipher::call(ssl).unwrap_or_default()
    } else {
        String::new()
    };
    PgString::from_str_in(&s, mcx)
}

/* ========================================================================= *
 *  File-scope GUC reads used by the open-server protocol-hint path.
 * ========================================================================= */

fn ssl_min_protocol_version_guc() -> i32 {
    vars::ssl_min_protocol_version.read()
}
fn ssl_max_protocol_version_guc() -> i32 {
    vars::ssl_max_protocol_version.read()
}
fn ssl_dh_params_file() -> String {
    vars::ssl_dh_params_file.read().unwrap_or_default()
}

/// `GetConfigOption(name, false, false)` for the "not supported by this build"
/// reports. C passes the value into `errmsg`; a missing value would have been
/// caught upstream, so an absent return is rendered as the empty string.
fn get_config_option(name: &str) -> String {
    backend_utils_misc_guc_seams::get_config_option::call(name.to_string(), false, false)
        .unwrap_or_default()
}

/* ========================================================================= *
 *  Seam installation.
 * ========================================================================= */

/// Install the `be_tls_get_*` accessors this unit owns in the be-secure seam
/// contract (consumed by postinit / backend-startup). `be_tls_get_cipher_bits`
/// returns the C int directly; `version`/`cipher` allocate in the caller's
/// mcx and so are higher-ranked seams.
pub fn init_seams() {
    backend_libpq_be_secure_seams::be_tls_get_version::set(be_tls_get_version);
    backend_libpq_be_secure_seams::be_tls_get_cipher::set(be_tls_get_cipher);
    backend_libpq_be_secure_seams::be_tls_get_cipher_bits::set(be_tls_get_cipher_bits);
}
