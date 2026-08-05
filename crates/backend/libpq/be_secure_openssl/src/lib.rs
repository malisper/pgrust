// be-secure-openssl.c on the `openssl` crate. The SSL_CTX is process-global
// (C postmaster forks; here backends are threads sharing the postmaster's
// context — SSL_CTX is refcounted and thread-safe for SSL_new). The per-
// connection SSL/peer live in a backend thread-local, not on Port.
//
// C divergences (openssl-crate API shape):
// - info/ALPN callbacks are installed at be_tls_init on the shared context,
//   not per-connection in be_tls_open_server (a shared ctx must not be
//   mutated per backend under threads); alpn_cb's Port userdata was unused.
// - openssl_tls_init_hook is not overridable (no shared_preload_libraries
//   hook surface yet); the default hook body is inlined.
// - The custom BIO is rust-openssl's Read/Write bridge over PortStream;
//   EINTR retries inside PortStream instead of via BIO retry flags (the
//   socket is nonblocking, so recv/send never block in EINTR-able ways).
// - X509_NAME_get_text_by_NID / print_ex results are not passed through
//   pg_any_to_server: at handshake time the server encoding is the boot
//   default, where that call is a no-op.

#![allow(clippy::result_large_err)]

use std::cell::{Cell, RefCell};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use elog::ereport;
use foreign_types::ForeignTypeRef;
use openssl::dh::Dh;
use openssl::error::ErrorStack;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::Params;
use openssl::ssl::{
    select_next_proto, AlpnError, ErrorCode, HandshakeError, Ssl, SslContext, SslContextBuilder,
    SslMethod, SslMode, SslOptions, SslSessionCacheMode, SslStream, SslVerifyMode, SslVersion,
};
use openssl::x509::verify::X509VerifyFlags;
use openssl::x509::{X509Name, X509NameRef, X509Ref, X509};
use types_error::{
    ErrorLocation, PgResult, COMMERROR, DEBUG2, DEBUG4, ERRCODE_CONFIG_FILE_ERROR,
    ERRCODE_PROTOCOL_VIOLATION, ERROR, FATAL, LOG,
};
use types_storage::waiteventset::{WL_LATCH_SET, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE};

use guc_tables::consts::{
    PG_TLS1_1_VERSION, PG_TLS1_2_VERSION, PG_TLS1_3_VERSION, PG_TLS1_VERSION, PG_TLS_ANY,
};
use guc_tables::vars;

mod cffi;

const WAIT_EVENT_SSL_OPEN_SERVER: u32 = 0x0600_0000 | 5;

const PG_ALPN_PROTOCOL: &[u8] = b"postgresql";
const ALPN_PROTOS: &[u8] = b"\x0apostgresql";

const MIN_OPENSSL_TLS_VERSION: &str = "TLSv1";
const MAX_OPENSSL_TLS_VERSION: &str = "TLSv1.3";

// libpq-be.h FILE_DH2048 (RFC 3526 2048-bit MODP group).
const FILE_DH2048: &[u8] = b"-----BEGIN DH PARAMETERS-----\n\
MIIBCAKCAQEA///////////JD9qiIWjCNMTGYouA3BzRKQJOCIpnzHQCC76mOxOb\n\
IlFKCHmONATd75UZs806QxswKwpt8l8UN0/hNW1tUcJF5IW1dmJefsb0TELppjft\n\
awv/XLb0Brft7jhr+1qJn6WunyQRfEsf5kkoZlHs5Fs9wgB8uKFjvwWY2kg2HFXT\n\
mmkWP6j9JM9fg2VdI9yjrZYcYvNWIIVSu57VKQdwlpZtZww1Tkq8mATxdGwIyhgh\n\
fDKQXkYuNs474553LBgOhgObJ4Oi7Aeij7XFXfBvTFLJ3ivL9pVYFxg5lUl86pVq\n\
5RXSJhiY+gUQFXKOWoqsqmj//////////wIBAg==\n\
-----END DH PARAMETERS-----\n";

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

static SSL_CONTEXT: RwLock<Option<SslContext>> = RwLock::new(None);
static SSL_LOADED_VERIFY_LOCATIONS: AtomicBool = AtomicBool::new(false);
static DUMMY_SSL_PASSWD_CB_CALLED: AtomicBool = AtomicBool::new(false);
static SSL_IS_SERVER_START: AtomicBool = AtomicBool::new(false);

thread_local! {
    static CONN: RefCell<Option<TlsConn>> = const { RefCell::new(None) };
    static CERT_ERRDETAIL: RefCell<Option<String>> = const { RefCell::new(None) };
    static PASSPHRASE_ERR: Cell<Option<Box<types_error::PgError>>> = const { Cell::new(None) };
    static PASSPHRASE_PANIC: Cell<Option<Box<dyn std::any::Any + Send>>> = const { Cell::new(None) };
}

struct TlsConn {
    stream: SslStream<PortStream>,
    peer: Option<X509>,
}

pub struct PortStream {
    sock: i32,
    raw_buf: Vec<u8>,
    raw_consumed: usize,
}

impl PortStream {
    fn raw_remaining(&self) -> usize {
        self.raw_buf.len() - self.raw_consumed
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn errno() -> i32 {
    unsafe { *libc::__error() }
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
fn errno() -> i32 {
    unsafe { *libc::__errno_location() }
}

impl Read for PortStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let rem = self.raw_remaining();
        if rem > 0 {
            let n = rem.min(buf.len());
            buf[..n].copy_from_slice(&self.raw_buf[self.raw_consumed..self.raw_consumed + n]);
            self.raw_consumed += n;
            return Ok(n);
        }
        loop {
            // SAFETY: buf is valid writable memory of buf.len() bytes.
            let n = unsafe { libc::recv(self.sock, buf.as_mut_ptr().cast(), buf.len(), 0) };
            if n >= 0 {
                return Ok(n as usize);
            }
            let e = errno();
            if e == libc::EINTR {
                continue;
            }
            return Err(std::io::Error::from_raw_os_error(e));
        }
    }
}

impl Write for PortStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            // SAFETY: buf is valid readable memory of buf.len() bytes.
            let n = unsafe { libc::send(self.sock, buf.as_ptr().cast(), buf.len(), 0) };
            if n >= 0 {
                return Ok(n as usize);
            }
            let e = errno();
            if e == libc::EINTR {
                continue;
            }
            return Err(std::io::Error::from_raw_os_error(e));
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn ssl_errmessage(stack: Option<&ErrorStack>) -> String {
    let Some(err) = stack.and_then(|s| s.errors().first()) else {
        return "no SSL error reported".to_string();
    };
    if let Some(reason) = err.reason() {
        return reason.to_string();
    }
    let code = err.code();
    // OpenSSL 3 stops mapping system errnos; ERR_SYSTEM_ERROR is the high bit
    // and ERR_GET_REASON is then code & ERR_SYSTEM_MASK (0x7fffffff).
    if code & 0x8000_0000 != 0 {
        let e = (code & 0x7fff_ffff) as i32;
        // SAFETY: strerror returns a valid static/thread-local C string.
        let p = unsafe { libc::strerror(e) };
        if !p.is_null() {
            return unsafe { std::ffi::CStr::from_ptr(p) }
                .to_string_lossy()
                .into_owned();
        }
    }
    format!("SSL error code {code}")
}

fn ssl_errmessage_ext(stack: Option<&ErrorStack>, replacement: &str) -> String {
    if stack.map_or(true, |s| s.errors().is_empty()) {
        replacement.to_string()
    } else {
        ssl_errmessage(stack)
    }
}

fn guc_str(var: &'static guc_tables::GucStringVar) -> String {
    var.read().unwrap_or_default()
}

// ereport(isServerStart ? FATAL : LOG, ...) arms: Err propagates the FATAL,
// Ok(()) means the LOG was emitted and the caller returns -1.
struct InitCx {
    is_server_start: bool,
}

impl InitCx {
    fn level(&self) -> types_error::ErrorLevel {
        if self.is_server_start {
            FATAL
        } else {
            LOG
        }
    }
}

unsafe extern "C" fn ssl_external_passwd_cb(
    buf: *mut libc::c_char,
    size: libc::c_int,
    rwflag: libc::c_int,
    _userdata: *mut libc::c_void,
) -> libc::c_int {
    debug_assert_eq!(rwflag, 0);
    let prompt = "Enter PEM pass phrase:";
    let is_server_start = SSL_IS_SERVER_START.load(Ordering::Relaxed);
    // SAFETY: OpenSSL hands a writable buffer of `size` bytes.
    let slice = unsafe { std::slice::from_raw_parts_mut(buf.cast::<u8>(), size as usize) };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        be_secure_common::run_ssl_passphrase_command(prompt, is_server_start, slice)
    }));
    match result {
        Ok(Ok(len)) => len as libc::c_int,
        Ok(Err(e)) => {
            PASSPHRASE_ERR.set(Some(e));
            if size > 0 {
                slice[0] = 0;
            }
            0
        }
        Err(p) => {
            // Panics can't unwind over the OpenSSL FFI frame; re-raised by
            // be_tls_init.
            PASSPHRASE_PANIC.set(Some(p));
            if size > 0 {
                slice[0] = 0;
            }
            0
        }
    }
}

unsafe extern "C" fn dummy_ssl_passwd_cb(
    buf: *mut libc::c_char,
    size: libc::c_int,
    _rwflag: libc::c_int,
    _userdata: *mut libc::c_void,
) -> libc::c_int {
    DUMMY_SSL_PASSWD_CB_CALLED.store(true, Ordering::Relaxed);
    debug_assert!(size > 0);
    // SAFETY: OpenSSL hands a writable buffer of `size` bytes.
    unsafe { *buf = 0 };
    0
}

unsafe extern "C" fn info_cb(ssl: *const openssl_sys::SSL, type_: libc::c_int, args: libc::c_int) {
    const SSL_CB_LOOP: i32 = 0x01;
    const SSL_CB_EXIT: i32 = 0x02;
    const SSL_CB_READ: i32 = 0x04;
    const SSL_CB_WRITE: i32 = 0x08;
    const SSL_CB_ALERT: i32 = 0x4000;
    const SSL_ST_CONNECT: i32 = 0x1000;
    const SSL_ST_ACCEPT: i32 = 0x2000;
    const SSL_CB_HANDSHAKE_START: i32 = 0x10;
    const SSL_CB_HANDSHAKE_DONE: i32 = 0x20;

    let desc = {
        // SAFETY: SSL_state_string_long returns a static C string.
        let p = unsafe { cffi::SSL_state_string_long(ssl) };
        if p.is_null() {
            String::new()
        } else {
            unsafe { std::ffi::CStr::from_ptr(p) }
                .to_string_lossy()
                .into_owned()
        }
    };

    let msg = match type_ {
        SSL_CB_HANDSHAKE_START => Some(format!("SSL: handshake start: \"{desc}\"")),
        SSL_CB_HANDSHAKE_DONE => Some(format!("SSL: handshake done: \"{desc}\"")),
        t if t == SSL_ST_ACCEPT | SSL_CB_LOOP => Some(format!("SSL: accept loop: \"{desc}\"")),
        t if t == SSL_ST_ACCEPT | SSL_CB_EXIT => {
            Some(format!("SSL: accept exit ({args}): \"{desc}\""))
        }
        t if t == SSL_ST_CONNECT | SSL_CB_LOOP => Some(format!("SSL: connect loop: \"{desc}\"")),
        t if t == SSL_ST_CONNECT | SSL_CB_EXIT => {
            Some(format!("SSL: connect exit ({args}): \"{desc}\""))
        }
        t if t == SSL_CB_ALERT | SSL_CB_READ => {
            Some(format!("SSL: read alert (0x{args:04x}): \"{desc}\""))
        }
        t if t == SSL_CB_ALERT | SSL_CB_WRITE => {
            Some(format!("SSL: write alert (0x{args:04x}): \"{desc}\""))
        }
        _ => None,
    };
    if let Some(msg) = msg {
        let _ = std::panic::catch_unwind(|| {
            let _ = ereport(DEBUG4).errmsg_internal(msg).finish(loc("info_cb"));
        });
    }
}

fn prepare_cert_name(name: &str) -> String {
    const MAXLEN: usize = 71;
    let bytes = name.as_bytes();
    let truncated = if bytes.len() > MAXLEN {
        let mut t = bytes[bytes.len() - MAXLEN..].to_vec();
        t[0] = b'.';
        t[1] = b'.';
        t[2] = b'.';
        String::from_utf8_lossy(&t).into_owned()
    } else {
        name.to_string()
    };
    pg_string::pg_clean_ascii(&truncated, 0).unwrap_or_default()
}

fn verify_cb(ok: bool, ctx: &mut openssl::x509::X509StoreContextRef) -> bool {
    if ok {
        return true;
    }

    let depth = ctx.error_depth();
    let errstring = ctx.error().error_string();
    let mut str = format!("Client certificate verification failed at depth {depth}: {errstring}.");

    if let Some(cert) = ctx.current_cert() {
        let subject = x509_name_to_cstring(cert.subject_name());
        let sub_prepared = prepare_cert_name(&subject);
        let issuer = x509_name_to_cstring(cert.issuer_name());
        let iss_prepared = prepare_cert_name(&issuer);
        let serialno = cert
            .serial_number()
            .to_bn()
            .ok()
            .and_then(|b| b.to_dec_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "unknown".to_string());
        str.push('\n');
        str.push_str(&format!(
            "Failed certificate data (unverified): subject \"{sub_prepared}\", serial number {serialno}, issuer \"{iss_prepared}\"."
        ));
    }

    CERT_ERRDETAIL.with(|d| *d.borrow_mut() = Some(str));
    false
}

// X509_NAME_to_cstring: "/SN=value" segments via ASN1_STRING_print_ex.
fn x509_name_to_cstring(name: &X509NameRef) -> String {
    cffi::x509_name_slash_format(name.as_ptr()).unwrap_or_default()
}

fn load_dh_file(cx: &InitCx, filename: &str) -> PgResult<Option<Dh<Params>>> {
    let bytes = match std::fs::read(filename) {
        Ok(b) => b,
        Err(e) => {
            ereport(cx.level())
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not open DH parameters file \"{filename}\": %m"
                ))
                .finish(loc("load_dh_file"))?;
            return Ok(None);
        }
    };

    let dh = match Dh::params_from_pem(&bytes) {
        Ok(dh) => dh,
        Err(st) => {
            ereport(cx.level())
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(format!(
                    "could not load DH parameters file: {}",
                    ssl_errmessage(Some(&st))
                ))
                .finish(loc("load_dh_file"))?;
            return Ok(None);
        }
    };

    let mut codes: libc::c_int = 0;
    // SAFETY: dh is a live DH; codes is writable.
    if unsafe { openssl_sys::DH_check(dh.as_ptr() as *const _, &mut codes) } == 0 {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(format!(
                "invalid DH parameters: {}",
                ssl_errmessage(Some(&ErrorStack::get()))
            ))
            .finish(loc("load_dh_file"))?;
        return Ok(None);
    }
    const DH_CHECK_P_NOT_PRIME: i32 = 0x01;
    const DH_CHECK_P_NOT_SAFE_PRIME: i32 = 0x02;
    const DH_NOT_SUITABLE_GENERATOR: i32 = 0x08;
    if codes & DH_CHECK_P_NOT_PRIME != 0 {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg("invalid DH parameters: p is not prime")
            .finish(loc("load_dh_file"))?;
        return Ok(None);
    }
    if codes & DH_NOT_SUITABLE_GENERATOR != 0 && codes & DH_CHECK_P_NOT_SAFE_PRIME != 0 {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg("invalid DH parameters: neither suitable generator or safe prime")
            .finish(loc("load_dh_file"))?;
        return Ok(None);
    }

    Ok(Some(dh))
}

fn load_dh_buffer() -> PgResult<Option<Dh<Params>>> {
    match Dh::params_from_pem(FILE_DH2048) {
        Ok(dh) => Ok(Some(dh)),
        Err(st) => {
            ereport(DEBUG2)
                .errmsg_internal(format!("DH load buffer: {}", ssl_errmessage(Some(&st))))
                .finish(loc("load_dh_buffer"))?;
            Ok(None)
        }
    }
}

fn initialize_dh(cx: &InitCx, ctx: &mut SslContextBuilder) -> PgResult<bool> {
    ctx.set_options(SslOptions::SINGLE_DH_USE);

    let dh_file = guc_str(&vars::ssl_dh_params_file);
    let mut dh = None;
    if !dh_file.is_empty() {
        dh = load_dh_file(cx, &dh_file)?;
        if dh.is_none() && cx.is_server_start {
            return Ok(false);
        }
    }
    if dh.is_none() {
        dh = load_dh_buffer()?;
    }
    let Some(dh) = dh else {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg("DH: could not load DH parameters")
            .finish(loc("initialize_dh"))?;
        return Ok(false);
    };

    if let Err(st) = ctx.set_tmp_dh(&dh) {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(format!(
                "DH: could not set DH parameters: {}",
                ssl_errmessage(Some(&st))
            ))
            .finish(loc("initialize_dh"))?;
        return Ok(false);
    }
    Ok(true)
}

fn initialize_ecdh(cx: &InitCx, ctx: &mut SslContextBuilder) -> PgResult<bool> {
    let groups = guc_str(&vars::SSLECDHCurve);
    if let Err(st) = ctx.set_groups_list(&groups) {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(format!(
                "could not set group names specified in ssl_groups: {}",
                ssl_errmessage_ext(Some(&st), "No valid groups found")
            ))
            .errhint("Ensure that each group name is spelled correctly and supported by the installed version of OpenSSL.")
            .finish(loc("initialize_ecdh"))?;
        return Ok(false);
    }
    Ok(true)
}

fn ssl_protocol_version_to_openssl(v: i32) -> Option<Option<SslVersion>> {
    match v {
        x if x == PG_TLS_ANY => Some(None),
        x if x == PG_TLS1_VERSION => Some(Some(SslVersion::TLS1)),
        x if x == PG_TLS1_1_VERSION => Some(Some(SslVersion::TLS1_1)),
        x if x == PG_TLS1_2_VERSION => Some(Some(SslVersion::TLS1_2)),
        x if x == PG_TLS1_3_VERSION => Some(Some(SslVersion::TLS1_3)),
        _ => None,
    }
}

fn ssl_protocol_version_to_string(v: i32) -> &'static str {
    match v {
        x if x == PG_TLS_ANY => "any",
        x if x == PG_TLS1_VERSION => "TLSv1",
        x if x == PG_TLS1_1_VERSION => "TLSv1.1",
        x if x == PG_TLS1_2_VERSION => "TLSv1.2",
        x if x == PG_TLS1_3_VERSION => "TLSv1.3",
        _ => "(unrecognized)",
    }
}

fn default_openssl_tls_init(ctx: &mut SslContextBuilder, is_server_start: bool) {
    let has_command = !guc_str(&vars::ssl_passphrase_command).is_empty();
    let supports_reload = vars::ssl_passphrase_command_supports_reload.read();
    let ptr = ctx.as_ptr();
    if is_server_start {
        if has_command {
            // SAFETY: ptr is the live builder's SSL_CTX; cb is 'static.
            unsafe { cffi::SSL_CTX_set_default_passwd_cb(ptr, Some(ssl_external_passwd_cb)) };
        }
    } else if has_command && supports_reload {
        // SAFETY: as above.
        unsafe { cffi::SSL_CTX_set_default_passwd_cb(ptr, Some(ssl_external_passwd_cb)) };
    } else {
        // SAFETY: as above.
        unsafe { cffi::SSL_CTX_set_default_passwd_cb(ptr, Some(dummy_ssl_passwd_cb)) };
    }
}

pub fn be_tls_init(is_server_start: bool) -> PgResult<i32> {
    let cx = InitCx { is_server_start };

    let mut ctx = match SslContextBuilder::new(SslMethod::tls()) {
        Ok(c) => c,
        Err(st) => {
            ereport(cx.level())
                .errmsg(format!(
                    "could not create SSL context: {}",
                    ssl_errmessage(Some(&st))
                ))
                .finish(loc("be_tls_init"))?;
            return Ok(-1);
        }
    };

    ctx.set_mode(SslMode::ACCEPT_MOVING_WRITE_BUFFER);

    default_openssl_tls_init(&mut ctx, is_server_start);
    SSL_IS_SERVER_START.store(is_server_start, Ordering::Relaxed);
    PASSPHRASE_ERR.set(None);
    PASSPHRASE_PANIC.set(None);

    let ssl_cert_file = guc_str(&vars::ssl_cert_file);
    if let Err(st) = ctx.set_certificate_chain_file(&ssl_cert_file) {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(format!(
                "could not load server certificate file \"{ssl_cert_file}\": {}",
                ssl_errmessage(Some(&st))
            ))
            .finish(loc("be_tls_init"))?;
        return Ok(-1);
    }

    let ssl_key_file = guc_str(&vars::ssl_key_file);
    if !be_secure_common::check_ssl_key_file_permissions(&ssl_key_file, is_server_start)? {
        return Ok(-1);
    }

    DUMMY_SSL_PASSWD_CB_CALLED.store(false, Ordering::Relaxed);

    if let Err(st) = ctx.set_private_key_file(&ssl_key_file, openssl::ssl::SslFiletype::PEM) {
        if let Some(p) = PASSPHRASE_PANIC.take() {
            std::panic::resume_unwind(p);
        }
        if let Some(e) = PASSPHRASE_ERR.take() {
            // The passphrase command's own ereport(ERROR) surfaced inside the
            // OpenSSL callback in C; re-raise it here (can't unwind over FFI).
            return Err(e);
        }
        if DUMMY_SSL_PASSWD_CB_CALLED.load(Ordering::Relaxed) {
            ereport(cx.level())
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(format!(
                    "private key file \"{ssl_key_file}\" cannot be reloaded because it requires a passphrase"
                ))
                .finish(loc("be_tls_init"))?;
        } else {
            ereport(cx.level())
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(format!(
                    "could not load private key file \"{ssl_key_file}\": {}",
                    ssl_errmessage(Some(&st))
                ))
                .finish(loc("be_tls_init"))?;
        }
        return Ok(-1);
    }

    if let Err(st) = ctx.check_private_key() {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(format!(
                "check of private key failed: {}",
                ssl_errmessage(Some(&st))
            ))
            .finish(loc("be_tls_init"))?;
        return Ok(-1);
    }

    let ssl_min = vars::ssl_min_protocol_version.read();
    let ssl_max = vars::ssl_max_protocol_version.read();

    if ssl_min != 0 {
        let Some(ver) = ssl_protocol_version_to_openssl(ssl_min) else {
            ereport(cx.level())
                .errmsg(format!(
                    "\"ssl_min_protocol_version\" setting \"{}\" not supported by this build",
                    ssl_protocol_version_to_string(ssl_min)
                ))
                .finish(loc("be_tls_init"))?;
            return Ok(-1);
        };
        if ctx.set_min_proto_version(ver).is_err() {
            ereport(cx.level())
                .errmsg("could not set minimum SSL protocol version")
                .finish(loc("be_tls_init"))?;
            return Ok(-1);
        }
    }

    if ssl_max != 0 {
        let Some(ver) = ssl_protocol_version_to_openssl(ssl_max) else {
            ereport(cx.level())
                .errmsg(format!(
                    "\"ssl_max_protocol_version\" setting \"{}\" not supported by this build",
                    ssl_protocol_version_to_string(ssl_max)
                ))
                .finish(loc("be_tls_init"))?;
            return Ok(-1);
        };
        if ctx.set_max_proto_version(ver).is_err() {
            ereport(cx.level())
                .errmsg("could not set maximum SSL protocol version")
                .finish(loc("be_tls_init"))?;
            return Ok(-1);
        }
    }

    if ssl_min != 0 && ssl_max != 0 && ssl_min > ssl_max {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg("could not set SSL protocol version range")
            .errdetail(
                "\"ssl_min_protocol_version\" cannot be higher than \"ssl_max_protocol_version\"",
            )
            .finish(loc("be_tls_init"))?;
        return Ok(-1);
    }

    let _ = ctx.set_num_tickets(0);
    ctx.set_options(SslOptions::NO_TICKET);
    ctx.set_session_cache_mode(SslSessionCacheMode::OFF);
    ctx.set_options(SslOptions::NO_COMPRESSION);
    ctx.set_options(SslOptions::NO_RENEGOTIATION);

    if !initialize_dh(&cx, &mut ctx)? {
        return Ok(-1);
    }
    if !initialize_ecdh(&cx, &mut ctx)? {
        return Ok(-1);
    }

    let cipher_list = guc_str(&vars::SSLCipherList);
    if ctx.set_cipher_list(&cipher_list).is_err() {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg("could not set the TLSv1.2 cipher list (no valid ciphers available)")
            .finish(loc("be_tls_init"))?;
        return Ok(-1);
    }

    let cipher_suites = guc_str(&vars::SSLCipherSuites);
    if !cipher_suites.is_empty() && ctx.set_ciphersuites(&cipher_suites).is_err() {
        ereport(cx.level())
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg("could not set the TLSv1.3 cipher suites (no valid ciphers available)")
            .finish(loc("be_tls_init"))?;
        return Ok(-1);
    }

    if vars::SSLPreferServerCiphers.read() {
        ctx.set_options(SslOptions::CIPHER_SERVER_PREFERENCE);
    }

    let ssl_ca_file = guc_str(&vars::ssl_ca_file);
    if !ssl_ca_file.is_empty() {
        let loaded = ctx.set_ca_file(&ssl_ca_file);
        let root_cert_list = X509Name::load_client_ca_file(&ssl_ca_file);
        match (loaded, root_cert_list) {
            (Ok(()), Ok(list)) => {
                ctx.set_client_ca_list(list);
                ctx.set_verify_callback(
                    SslVerifyMode::PEER | SslVerifyMode::CLIENT_ONCE,
                    verify_cb,
                );
            }
            (r, l) => {
                let st = r.err().or_else(|| l.err());
                ereport(cx.level())
                    .errcode(ERRCODE_CONFIG_FILE_ERROR)
                    .errmsg(format!(
                        "could not load root certificate file \"{ssl_ca_file}\": {}",
                        ssl_errmessage(st.as_ref())
                    ))
                    .finish(loc("be_tls_init"))?;
                return Ok(-1);
            }
        }
    }

    let ssl_crl_file = guc_str(&vars::ssl_crl_file);
    let ssl_crl_dir = guc_str(&vars::ssl_crl_dir);
    if !ssl_crl_file.is_empty() || !ssl_crl_dir.is_empty() {
        let cfile = std::ffi::CString::new(ssl_crl_file.as_str()).unwrap();
        let cdir = std::ffi::CString::new(ssl_crl_dir.as_str()).unwrap();
        let store = ctx.cert_store_mut();
        // SAFETY: store is the builder's live X509_STORE; the CStrings outlive
        // the call.
        let rc = unsafe {
            cffi::X509_STORE_load_locations(
                store.as_ptr(),
                if ssl_crl_file.is_empty() {
                    std::ptr::null()
                } else {
                    cfile.as_ptr()
                },
                if ssl_crl_dir.is_empty() {
                    std::ptr::null()
                } else {
                    cdir.as_ptr()
                },
            )
        };
        if rc == 1 {
            let _ = store.set_flags(X509VerifyFlags::CRL_CHECK | X509VerifyFlags::CRL_CHECK_ALL);
        } else if ssl_crl_dir.is_empty() {
            ereport(cx.level())
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(format!(
                    "could not load SSL certificate revocation list file \"{ssl_crl_file}\": {}",
                    ssl_errmessage(Some(&ErrorStack::get()))
                ))
                .finish(loc("be_tls_init"))?;
            return Ok(-1);
        } else if ssl_crl_file.is_empty() {
            ereport(cx.level())
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(format!(
                    "could not load SSL certificate revocation list directory \"{ssl_crl_dir}\": {}",
                    ssl_errmessage(Some(&ErrorStack::get()))
                ))
                .finish(loc("be_tls_init"))?;
            return Ok(-1);
        } else {
            ereport(cx.level())
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(format!(
                    "could not load SSL certificate revocation list file \"{ssl_crl_file}\" or directory \"{ssl_crl_dir}\": {}",
                    ssl_errmessage(Some(&ErrorStack::get()))
                ))
                .finish(loc("be_tls_init"))?;
            return Ok(-1);
        }
    }

    // SAFETY: live SSL_CTX; static callback.
    unsafe { cffi::SSL_CTX_set_info_callback(ctx.as_ptr(), Some(info_cb)) };
    ctx.set_alpn_select_callback(|_ssl, client| {
        select_next_proto(ALPN_PROTOS, client).ok_or(AlpnError::ALERT_FATAL)
    });

    *SSL_CONTEXT.write().unwrap() = Some(ctx.build());
    SSL_LOADED_VERIFY_LOCATIONS.store(!ssl_ca_file.is_empty(), Ordering::Relaxed);

    Ok(0)
}

pub fn be_tls_destroy() {
    *SSL_CONTEXT.write().unwrap() = None;
    SSL_LOADED_VERIFY_LOCATIONS.store(false, Ordering::Relaxed);
}

pub fn ssl_loaded_verify_locations() -> bool {
    SSL_LOADED_VERIFY_LOCATIONS.load(Ordering::Relaxed)
}

pub struct TlsOpen {
    pub r: i32,
    pub raw_remaining: usize,
    pub alpn_used: bool,
    pub peer_cn: Option<String>,
    pub peer_dn: Option<String>,
    pub peer_cert_valid: bool,
}

impl TlsOpen {
    fn failed(raw_remaining: usize) -> Self {
        TlsOpen {
            r: -1,
            raw_remaining,
            alpn_used: false,
            peer_cn: None,
            peer_dn: None,
            peer_cert_valid: false,
        }
    }
}

const SSL_R_NO_PROTOCOLS_AVAILABLE: u64 = 191;
const SSL_R_UNSUPPORTED_PROTOCOL: u64 = 258;
const SSL_R_BAD_PROTOCOL_VERSION_NUMBER: u64 = 116;
const SSL_R_UNKNOWN_PROTOCOL: u64 = 252;
const SSL_R_UNKNOWN_SSL_VERSION: u64 = 254;
const SSL_R_UNSUPPORTED_SSL_VERSION: u64 = 259;
const SSL_R_WRONG_SSL_VERSION: u64 = 266;
const SSL_R_WRONG_VERSION_NUMBER: u64 = 267;
const SSL_R_TLSV1_ALERT_PROTOCOL_VERSION: u64 = 1070;
const SSL_R_VERSION_TOO_HIGH: u64 = 271;
const SSL_R_VERSION_TOO_LOW: u64 = 396;

fn accept_failure_report(err: &openssl::ssl::Error) -> PgResult<()> {
    match err.code() {
        ErrorCode::SYSCALL => {
            if let Some(io) = err.io_error() {
                if let Some(e) = io.raw_os_error() {
                    if e != 0 {
                        return ereport(COMMERROR)
                            .with_saved_errno(e)
                            .errcode_for_socket_access()
                            .errmsg("could not accept SSL connection: %m")
                            .finish(loc("be_tls_open_server"));
                    }
                }
            }
            ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("could not accept SSL connection: EOF detected")
                .finish(loc("be_tls_open_server"))
        }
        ErrorCode::SSL => {
            let stack = err.ssl_error();
            let reason = stack
                .and_then(|s| s.errors().first())
                .map(|e| e.code() & 0x7f_ffff)
                .unwrap_or(0);
            let give_proto_hint = matches!(
                reason,
                SSL_R_NO_PROTOCOLS_AVAILABLE
                    | SSL_R_UNSUPPORTED_PROTOCOL
                    | SSL_R_BAD_PROTOCOL_VERSION_NUMBER
                    | SSL_R_UNKNOWN_PROTOCOL
                    | SSL_R_UNKNOWN_SSL_VERSION
                    | SSL_R_UNSUPPORTED_SSL_VERSION
                    | SSL_R_WRONG_SSL_VERSION
                    | SSL_R_WRONG_VERSION_NUMBER
                    | SSL_R_TLSV1_ALERT_PROTOCOL_VERSION
                    | SSL_R_VERSION_TOO_HIGH
                    | SSL_R_VERSION_TOO_LOW
            );
            let mut b = ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!(
                    "could not accept SSL connection: {}",
                    ssl_errmessage(stack)
                ));
            if let Some(detail) = CERT_ERRDETAIL.with(|d| d.borrow_mut().take()) {
                b = b.errdetail_internal(detail);
            }
            if give_proto_hint {
                let ssl_min = vars::ssl_min_protocol_version.read();
                let ssl_max = vars::ssl_max_protocol_version.read();
                b = b.errhint(format!(
                    "This may indicate that the client does not support any SSL protocol version between {} and {}.",
                    if ssl_min != 0 {
                        ssl_protocol_version_to_string(ssl_min)
                    } else {
                        MIN_OPENSSL_TLS_VERSION
                    },
                    if ssl_max != 0 {
                        ssl_protocol_version_to_string(ssl_max)
                    } else {
                        MAX_OPENSSL_TLS_VERSION
                    },
                ));
            }
            b.finish(loc("be_tls_open_server"))
        }
        ErrorCode::ZERO_RETURN => ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("could not accept SSL connection: EOF detected")
            .finish(loc("be_tls_open_server")),
        code => ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!("unrecognized SSL error code: {}", code.as_raw()))
            .finish(loc("be_tls_open_server")),
    }
}

pub fn be_tls_open_server(sock: i32, raw_buf: Vec<u8>) -> PgResult<TlsOpen> {
    assert!(CONN.with(|c| c.borrow().is_none()));

    let ctx = SSL_CONTEXT.read().unwrap().clone();
    let Some(ctx) = ctx else {
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("could not initialize SSL connection: SSL context not set up")
            .finish(loc("be_tls_open_server"))?;
        return Ok(TlsOpen::failed(raw_buf.len()));
    };

    let ssl = match Ssl::new(&ctx) {
        Ok(s) => s,
        Err(st) => {
            ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!(
                    "could not initialize SSL connection: {}",
                    ssl_errmessage(Some(&st))
                ))
                .finish(loc("be_tls_open_server"))?;
            return Ok(TlsOpen::failed(raw_buf.len()));
        }
    };

    let stream = PortStream {
        sock,
        raw_buf,
        raw_consumed: 0,
    };
    // C sets port->ssl_in_use before SSL_accept; the cell routes secure_read/
    // write (and socket_close's secure_close) through the TLS arms from here.
    pqcomm::set_ssl_in_use(true);

    clear_err_queue();
    let mut result = ssl.accept(stream);
    let stream = loop {
        match result {
            Ok(s) => break s,
            Err(HandshakeError::WouldBlock(mid)) => {
                let waitfor = if mid.error().code() == ErrorCode::WANT_READ {
                    WL_SOCKET_READABLE
                } else {
                    WL_SOCKET_WRITEABLE
                };
                pqcomm::pq_modify_fe_be_wait_set_socket(waitfor)?;
                let events = pqcomm::pq_wait_event_set_wait_fe_be(-1, WAIT_EVENT_SSL_OPEN_SERVER)?;
                // C waits with WaitLatchOrSocket(NULL latch, ...): the latch is
                // not in its set. FeBeWaitSet has it, so a set latch must be
                // consumed or this wait returns immediately forever (busy
                // spin); no interrupt processing is needed pre-auth, as C notes.
                if events & WL_LATCH_SET != 0 {
                    latch_seams::reset_latch_my_latch::call();
                }
                clear_err_queue();
                result = mid.handshake();
            }
            Err(HandshakeError::Failure(mid)) => {
                let remaining = mid.get_ref().raw_remaining();
                accept_failure_report(mid.error())?;
                return Ok(TlsOpen::failed(remaining));
            }
            Err(HandshakeError::SetupFailure(st)) => {
                ereport(COMMERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!(
                        "could not set SSL socket: {}",
                        ssl_errmessage(Some(&st))
                    ))
                    .finish(loc("be_tls_open_server"))?;
                return Ok(TlsOpen::failed(0));
            }
        }
    };

    let raw_remaining = stream.get_ref().raw_remaining();

    let mut alpn_used = false;
    if let Some(selected) = stream.ssl().selected_alpn_protocol() {
        if selected == PG_ALPN_PROTOCOL {
            alpn_used = true;
        } else {
            ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("received SSL connection request with unexpected ALPN protocol")
                .finish(loc("be_tls_open_server"))?;
        }
    }

    let peer = stream.ssl().peer_certificate();

    let mut peer_cn = None;
    let mut peer_dn = None;
    let mut peer_cert_valid = false;
    if let Some(peer_cert) = &peer {
        match extract_peer_cn(peer_cert)? {
            Ok(cn) => peer_cn = cn,
            Err(()) => {
                CONN.with(|c| *c.borrow_mut() = Some(TlsConn { stream, peer }));
                return Ok(TlsOpen::failed(raw_remaining));
            }
        }

        let Some(dn_bytes) = cffi::x509_name_print_rfc2253(peer_cert.subject_name().as_ptr())
        else {
            CONN.with(|c| *c.borrow_mut() = Some(TlsConn { stream, peer }));
            return Ok(TlsOpen::failed(raw_remaining));
        };
        if dn_bytes.contains(&0) {
            ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("SSL certificate's distinguished name contains embedded null")
                .finish(loc("be_tls_open_server"))?;
            CONN.with(|c| *c.borrow_mut() = Some(TlsConn { stream, peer }));
            return Ok(TlsOpen::failed(raw_remaining));
        }
        peer_dn = Some(String::from_utf8_lossy(&dn_bytes).into_owned());
        peer_cert_valid = true;
    }

    CONN.with(|c| *c.borrow_mut() = Some(TlsConn { stream, peer }));

    Ok(TlsOpen {
        r: 0,
        raw_remaining,
        alpn_used,
        peer_cn,
        peer_dn,
        peer_cert_valid,
    })
}

// X509_NAME_get_text_by_NID(NID_commonName) with the CVE-2009-4034
// embedded-null check. Ok(Err(())) is the C `return -1` connection abort.
#[allow(clippy::result_unit_err)]
fn extract_peer_cn(peer: &X509Ref) -> PgResult<Result<Option<String>, ()>> {
    const NID_COMMON_NAME: libc::c_int = 13;
    let name = peer.subject_name().as_ptr();
    // SAFETY: name is the live subject X509_NAME; NULL buf probes the length.
    let len =
        unsafe { cffi::X509_NAME_get_text_by_NID(name, NID_COMMON_NAME, std::ptr::null_mut(), 0) };
    if len == -1 {
        return Ok(Ok(None));
    }
    let mut buf = vec![0u8; len as usize + 1];
    // SAFETY: buf holds len+1 writable bytes as the API requires.
    let r = unsafe {
        cffi::X509_NAME_get_text_by_NID(name, NID_COMMON_NAME, buf.as_mut_ptr().cast(), len + 1)
    };
    if r != len {
        return Ok(Err(()));
    }
    buf.truncate(len as usize);
    if buf.contains(&0) {
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("SSL certificate's common name contains embedded null")
            .finish(loc("be_tls_open_server"))?;
        return Ok(Err(()));
    }
    Ok(Ok(Some(String::from_utf8_lossy(&buf).into_owned())))
}

pub fn be_tls_close() {
    CONN.with(|c| {
        if let Some(mut conn) = c.borrow_mut().take() {
            let _ = conn.stream.shutdown();
        }
    });
}

pub struct TlsIo {
    pub n: isize,
    pub errno: i32,
    pub waitfor: u32,
}

fn classify_io(res: Result<usize, openssl::ssl::Error>, is_read: bool) -> PgResult<TlsIo> {
    match res {
        Ok(n) => Ok(TlsIo {
            n: n as isize,
            errno: 0,
            waitfor: 0,
        }),
        Err(err) => match err.code() {
            ErrorCode::WANT_READ => Ok(TlsIo {
                n: -1,
                errno: libc::EWOULDBLOCK,
                waitfor: WL_SOCKET_READABLE,
            }),
            ErrorCode::WANT_WRITE => Ok(TlsIo {
                n: -1,
                errno: libc::EWOULDBLOCK,
                waitfor: WL_SOCKET_WRITEABLE,
            }),
            ErrorCode::SYSCALL => {
                let e = err
                    .io_error()
                    .and_then(|io| io.raw_os_error())
                    .filter(|&e| e != 0)
                    .unwrap_or(libc::ECONNRESET);
                Ok(TlsIo {
                    n: -1,
                    errno: e,
                    waitfor: 0,
                })
            }
            ErrorCode::SSL => {
                ereport(COMMERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!("SSL error: {}", ssl_errmessage(err.ssl_error())))
                    .finish(loc(if is_read {
                        "be_tls_read"
                    } else {
                        "be_tls_write"
                    }))?;
                Ok(TlsIo {
                    n: -1,
                    errno: libc::ECONNRESET,
                    waitfor: 0,
                })
            }
            ErrorCode::ZERO_RETURN => {
                if is_read {
                    Ok(TlsIo {
                        n: 0,
                        errno: 0,
                        waitfor: 0,
                    })
                } else {
                    Ok(TlsIo {
                        n: -1,
                        errno: libc::ECONNRESET,
                        waitfor: 0,
                    })
                }
            }
            code => {
                ereport(COMMERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!("unrecognized SSL error code: {}", code.as_raw()))
                    .finish(loc(if is_read {
                        "be_tls_read"
                    } else {
                        "be_tls_write"
                    }))?;
                Ok(TlsIo {
                    n: -1,
                    errno: libc::ECONNRESET,
                    waitfor: 0,
                })
            }
        },
    }
}

// C clears the thread-local OpenSSL error queue before every SSL_accept /
// SSL_read / SSL_write (be-secure-openssl.c: "Prepare to call SSL_get_error()
// by clearing thread's OpenSSL error queue"): a stale error pushed by some
// OTHER libcrypto caller in this backend (e.g. sslinfo's OBJ_txt2nid on an
// invalid field name) would otherwise make SSL_get_error misclassify the next
// I/O — including turning a clean WANT_READ into a fatal "SSL error" that
// kills the connection.
fn clear_err_queue() {
    // SAFETY: clears this thread's OpenSSL error queue; no preconditions.
    unsafe { openssl_sys::ERR_clear_error() }
}

pub fn be_tls_read(buf: &mut [u8]) -> PgResult<TlsIo> {
    CONN.with(|c| {
        let mut conn = c.borrow_mut();
        let conn = conn
            .as_mut()
            .expect("be_tls_read: no TLS connection on this backend");
        clear_err_queue();
        classify_io(conn.stream.ssl_read(buf), true)
    })
}

pub fn be_tls_write(buf: &[u8]) -> PgResult<TlsIo> {
    CONN.with(|c| {
        let mut conn = c.borrow_mut();
        let conn = conn
            .as_mut()
            .expect("be_tls_write: no TLS connection on this backend");
        clear_err_queue();
        classify_io(conn.stream.ssl_write(buf), false)
    })
}

pub fn be_tls_get_cipher_bits() -> i32 {
    CONN.with(|c| {
        c.borrow()
            .as_ref()
            .and_then(|conn| {
                conn.stream
                    .ssl()
                    .current_cipher()
                    .map(|ci| ci.bits().secret)
            })
            .unwrap_or(0)
    })
}

pub fn be_tls_get_version() -> Option<String> {
    CONN.with(|c| {
        c.borrow()
            .as_ref()
            .map(|conn| conn.stream.ssl().version_str().to_string())
    })
}

pub fn be_tls_get_cipher() -> Option<String> {
    CONN.with(|c| {
        c.borrow().as_ref().and_then(|conn| {
            conn.stream
                .ssl()
                .current_cipher()
                .map(|ci| ci.name().to_string())
        })
    })
}

// C returns "" for these when there is no peer certificate; None maps to ""
// at the pgstat consumer.
pub fn be_tls_get_peer_subject_name() -> Option<String> {
    CONN.with(|c| {
        c.borrow()
            .as_ref()
            .and_then(|conn| conn.peer.as_ref())
            .map(|peer| x509_name_to_cstring(peer.subject_name()))
    })
}

pub fn be_tls_get_peer_issuer_name() -> Option<String> {
    CONN.with(|c| {
        c.borrow()
            .as_ref()
            .and_then(|conn| conn.peer.as_ref())
            .map(|peer| x509_name_to_cstring(peer.issuer_name()))
    })
}

pub fn be_tls_get_peer_serial() -> Option<String> {
    CONN.with(|c| {
        c.borrow()
            .as_ref()
            .and_then(|conn| conn.peer.as_ref())
            .and_then(|peer| peer.serial_number().to_bn().ok())
            .and_then(|b| b.to_dec_str().ok().map(|s| s.to_string()))
    })
}

// contrib/sslinfo accessors. C's sslinfo.c reads MyProcPort->peer and walks
// the X509 with libcrypto directly; the pgrust peer lives in this backend's
// CONN, so the walks are exposed as be_tls_*-style getters here.

pub use cffi::{DnFieldLookup, X509ExtensionInfo, X509ExtensionsError};

/// `MyProcPort->peer != NULL` (sslinfo.c ssl_issuer_field's only guard).
pub fn be_tls_has_peer_certificate() -> bool {
    CONN.with(|c| c.borrow().as_ref().is_some_and(|conn| conn.peer.is_some()))
}

/// sslinfo.c X509_NAME_field_to_text over the peer's subject (or issuer)
/// name. None when there is no TLS connection or no peer certificate.
pub fn be_tls_get_peer_dn_field(field: &std::ffi::CStr, issuer: bool) -> Option<DnFieldLookup> {
    CONN.with(|c| {
        c.borrow()
            .as_ref()
            .and_then(|conn| conn.peer.as_ref())
            .map(|peer| {
                let name = if issuer {
                    peer.issuer_name()
                } else {
                    peer.subject_name()
                };
                cffi::x509_name_field_utf8(name.as_ptr(), field)
            })
    })
}

/// sslinfo.c ssl_extension_info's extension walk; no connection or no peer
/// certificate yields no rows (C: `max_calls = cert != NULL ?
/// X509_get_ext_count(cert) : 0`).
pub fn be_tls_get_peer_extensions() -> Result<Vec<X509ExtensionInfo>, X509ExtensionsError> {
    CONN.with(
        |c| match c.borrow().as_ref().and_then(|conn| conn.peer.as_ref()) {
            Some(peer) => cffi::x509_extensions(peer.as_ptr()),
            None => Ok(Vec::new()),
        },
    )
}

const NID_MD5: libc::c_int = 4;
const NID_SHA1: libc::c_int = 64;

pub fn be_tls_get_certificate_hash() -> PgResult<Option<Vec<u8>>> {
    CONN.with(|c| {
        let conn = c.borrow();
        let Some(conn) = conn.as_ref() else {
            return Ok(None);
        };
        let Some(server_cert) = conn.stream.ssl().certificate() else {
            return Ok(None);
        };

        let mut algo_nid: libc::c_int = 0;
        // SAFETY: live X509; only mdnid is requested.
        let ok = unsafe {
            cffi::X509_get_signature_info(
                server_cert.as_ptr(),
                &mut algo_nid,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return ereport(ERROR)
                .errmsg_internal("could not determine server certificate signature algorithm")
                .finish(loc("be_tls_get_certificate_hash"))
                .map(|()| None);
        }

        // RFC 5929: MD5 and SHA-1 signatures hash with SHA-256.
        let algo_type = match algo_nid {
            NID_MD5 | NID_SHA1 => MessageDigest::sha256(),
            nid => match MessageDigest::from_nid(Nid::from_raw(nid)) {
                Some(md) => md,
                None => {
                    return ereport(ERROR)
                        .errmsg_internal(format!(
                            "could not find digest for NID {}",
                            cffi::nid_short_name(nid).unwrap_or_else(|| nid.to_string())
                        ))
                        .finish(loc("be_tls_get_certificate_hash"))
                        .map(|()| None);
                }
            },
        };

        match server_cert.digest(algo_type) {
            Ok(hash) => Ok(Some(hash.as_ref().to_vec())),
            Err(_) => ereport(ERROR)
                .errmsg_internal("could not generate server certificate hash")
                .finish(loc("be_tls_get_certificate_hash"))
                .map(|()| None),
        }
    })
}
