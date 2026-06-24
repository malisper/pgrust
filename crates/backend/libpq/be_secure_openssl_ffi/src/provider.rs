//! The OpenSSL binding + seam installs (only compiled with `ssl-openssl`).
#![allow(non_upper_case_globals)] // mirror OpenSSL header symbol names (NID_commonName, …)

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use core::ffi::{c_char, c_int, c_long, c_uchar, c_uint, c_void};
use std::ffi::{CStr, CString};

use be_secure_openssl as owner;
use be_secure_openssl_ffi_seams as seams;
use seams::{PasswdCb, SslAcceptResult, SslIoResult, X509Name};

/* ===================================================================== *
 *  Opaque OpenSSL types. Only pointers cross the FFI; no layouts matter.
 * ===================================================================== */
#[repr(C)]
struct SSL_CTX {
    _p: [u8; 0],
}
#[repr(C)]
struct SSL {
    _p: [u8; 0],
}
#[repr(C)]
struct SSL_METHOD {
    _p: [u8; 0],
}
#[repr(C)]
struct SSL_CIPHER {
    _p: [u8; 0],
}
#[repr(C)]
struct BIO {
    _p: [u8; 0],
}
#[repr(C)]
struct BIO_METHOD {
    _p: [u8; 0],
}
#[repr(C)]
struct X509 {
    _p: [u8; 0],
}
#[repr(C)]
struct X509_NAME {
    _p: [u8; 0],
}
#[repr(C)]
struct X509_NAME_ENTRY {
    _p: [u8; 0],
}
#[repr(C)]
struct X509_STORE {
    _p: [u8; 0],
}
#[repr(C)]
struct X509_STORE_CTX {
    _p: [u8; 0],
}
#[repr(C)]
struct ASN1_OBJECT {
    _p: [u8; 0],
}
#[repr(C)]
struct ASN1_STRING {
    _p: [u8; 0],
}
#[repr(C)]
struct DH {
    _p: [u8; 0],
}
#[repr(C)]
struct stack_st_X509_NAME {
    _p: [u8; 0],
}

type pem_password_cb =
    unsafe extern "C" fn(buf: *mut c_char, size: c_int, rwflag: c_int, u: *mut c_void) -> c_int;
type info_callback_fn = unsafe extern "C" fn(ssl: *const SSL, type_: c_int, val: c_int);
type verify_callback_fn = unsafe extern "C" fn(ok: c_int, ctx: *mut X509_STORE_CTX) -> c_int;
type alpn_select_cb_fn = unsafe extern "C" fn(
    ssl: *mut SSL,
    out: *mut *const c_uchar,
    outlen: *mut c_uchar,
    in_: *const c_uchar,
    inlen: c_uint,
    arg: *mut c_void,
) -> c_int;
type bio_write_fn = unsafe extern "C" fn(b: *mut BIO, buf: *const c_char, len: c_int) -> c_int;
type bio_read_fn = unsafe extern "C" fn(b: *mut BIO, buf: *mut c_char, len: c_int) -> c_int;
type bio_ctrl_fn =
    unsafe extern "C" fn(b: *mut BIO, cmd: c_int, larg: c_long, parg: *mut c_void) -> c_long;

/* ===================================================================== *
 *  libssl / libcrypto symbols (the same set c2rust emitted; narrowed to
 *  what the ported owner actually invokes through its seams).
 * ===================================================================== */
extern "C" {
    fn TLS_method() -> *const SSL_METHOD;
    fn SSL_CTX_new(meth: *const SSL_METHOD) -> *mut SSL_CTX;
    fn SSL_CTX_free(ctx: *mut SSL_CTX);
    fn SSL_CTX_ctrl(ctx: *mut SSL_CTX, cmd: c_int, larg: c_long, parg: *mut c_void) -> c_long;
    fn SSL_CTX_set_options(ctx: *mut SSL_CTX, op: u64) -> u64;
    fn SSL_CTX_set_default_passwd_cb(ctx: *mut SSL_CTX, cb: Option<pem_password_cb>);
    fn SSL_CTX_use_certificate_chain_file(ctx: *mut SSL_CTX, file: *const c_char) -> c_int;
    fn SSL_CTX_use_PrivateKey_file(ctx: *mut SSL_CTX, file: *const c_char, type_0: c_int) -> c_int;
    fn SSL_CTX_check_private_key(ctx: *const SSL_CTX) -> c_int;
    fn SSL_CTX_set_cipher_list(ctx: *mut SSL_CTX, str_0: *const c_char) -> c_int;
    fn SSL_CTX_set_ciphersuites(ctx: *mut SSL_CTX, str_0: *const c_char) -> c_int;
    fn SSL_CTX_get_cert_store(ctx: *const SSL_CTX) -> *mut X509_STORE;
    fn SSL_CTX_load_verify_locations(
        ctx: *mut SSL_CTX,
        ca_file: *const c_char,
        ca_path: *const c_char,
    ) -> c_int;
    fn SSL_CTX_set_client_CA_list(ctx: *mut SSL_CTX, list: *mut stack_st_X509_NAME);
    fn SSL_CTX_set_verify(ctx: *mut SSL_CTX, mode: c_int, cb: Option<verify_callback_fn>);
    fn SSL_CTX_set_info_callback(ctx: *mut SSL_CTX, cb: Option<info_callback_fn>);
    fn SSL_CTX_set_alpn_select_cb(ctx: *mut SSL_CTX, cb: Option<alpn_select_cb_fn>, arg: *mut c_void);
    fn SSL_load_client_CA_file(file: *const c_char) -> *mut stack_st_X509_NAME;

    fn SSL_new(ctx: *mut SSL_CTX) -> *mut SSL;
    fn SSL_free(ssl: *mut SSL);
    fn SSL_set_bio(s: *mut SSL, rbio: *mut BIO, wbio: *mut BIO);
    fn SSL_accept(ssl: *mut SSL) -> c_int;
    fn SSL_read(ssl: *mut SSL, buf: *mut c_void, num: c_int) -> c_int;
    fn SSL_write(ssl: *mut SSL, buf: *const c_void, num: c_int) -> c_int;
    fn SSL_shutdown(s: *mut SSL) -> c_int;
    fn SSL_get_error(s: *const SSL, ret_code: c_int) -> c_int;
    fn SSL_get_version(s: *const SSL) -> *const c_char;
    fn SSL_state_string_long(s: *const SSL) -> *const c_char;
    fn SSL_get1_peer_certificate(s: *const SSL) -> *mut X509;
    fn SSL_get0_alpn_selected(ssl: *const SSL, data: *mut *const c_uchar, len: *mut c_uint);
    fn SSL_get_current_cipher(s: *const SSL) -> *const SSL_CIPHER;
    fn SSL_CIPHER_get_name(c: *const SSL_CIPHER) -> *const c_char;
    fn SSL_CIPHER_get_bits(c: *const SSL_CIPHER, alg_bits: *mut c_int) -> c_int;
    fn SSL_select_next_proto(
        out: *mut *mut c_uchar,
        outlen: *mut c_uchar,
        server: *const c_uchar,
        server_len: c_uint,
        client: *const c_uchar,
        client_len: c_uint,
    ) -> c_int;

    fn BIO_new(type_0: *const BIO_METHOD) -> *mut BIO;
    fn BIO_free(a: *mut BIO) -> c_int;
    fn BIO_s_mem() -> *const BIO_METHOD;
    fn BIO_new_mem_buf(buf: *const c_void, len: c_int) -> *mut BIO;
    fn BIO_ctrl(b: *mut BIO, cmd: c_int, larg: c_long, parg: *mut c_void) -> c_long;
    fn BIO_set_data(a: *mut BIO, ptr: *mut c_void);
    fn BIO_get_data(a: *mut BIO) -> *mut c_void;
    fn BIO_set_init(a: *mut BIO, init: c_int);
    fn BIO_set_flags(b: *mut BIO, flags: c_int);
    fn BIO_clear_flags(b: *mut BIO, flags: c_int);
    fn BIO_get_new_index() -> c_int;
    fn BIO_meth_new(type_0: c_int, name: *const c_char) -> *mut BIO_METHOD;
    fn BIO_meth_set_write(biom: *mut BIO_METHOD, write: Option<bio_write_fn>) -> c_int;
    fn BIO_meth_set_read(biom: *mut BIO_METHOD, read: Option<bio_read_fn>) -> c_int;
    fn BIO_meth_set_ctrl(biom: *mut BIO_METHOD, ctrl: Option<bio_ctrl_fn>) -> c_int;

    fn X509_free(a: *mut X509);
    fn X509_get_subject_name(a: *const X509) -> *mut X509_NAME;
    fn X509_get_issuer_name(a: *const X509) -> *mut X509_NAME;
    fn X509_NAME_get_text_by_NID(
        name: *const X509_NAME,
        nid: c_int,
        buf: *mut c_char,
        len: c_int,
    ) -> c_int;
    fn X509_NAME_print_ex(out: *mut BIO, nm: *const X509_NAME, indent: c_int, flags: u64) -> c_int;
    fn X509_NAME_entry_count(name: *const X509_NAME) -> c_int;
    fn X509_NAME_get_entry(name: *const X509_NAME, loc: c_int) -> *mut X509_NAME_ENTRY;
    fn X509_NAME_ENTRY_get_object(ne: *const X509_NAME_ENTRY) -> *mut ASN1_OBJECT;
    fn X509_NAME_ENTRY_get_data(ne: *const X509_NAME_ENTRY) -> *mut ASN1_STRING;
    fn X509_STORE_set_flags(ctx: *mut X509_STORE, flags: u64) -> c_int;
    fn X509_STORE_load_locations(
        ctx: *mut X509_STORE,
        file: *const c_char,
        dir: *const c_char,
    ) -> c_int;

    fn ASN1_STRING_print_ex(out: *mut BIO, str_0: *const ASN1_STRING, flags: u64) -> c_int;
    fn OBJ_obj2nid(o: *const ASN1_OBJECT) -> c_int;
    fn OBJ_nid2sn(n: c_int) -> *const c_char;
    fn OBJ_nid2ln(n: c_int) -> *const c_char;

    fn DH_free(dh: *mut DH);
    fn DH_check(dh: *const DH, codes: *mut c_int) -> c_int;
    fn PEM_read_bio_DHparams(
        bp: *mut BIO,
        x: *mut *mut DH,
        cb: Option<pem_password_cb>,
        u: *mut c_void,
    ) -> *mut DH;

    fn BIO_printf(bio: *mut BIO, format: *const c_char, ...) -> c_int;
    fn BIO_write(b: *mut BIO, data: *const c_void, dlen: c_int) -> c_int;

    fn ERR_get_error() -> c_ulong_t;
    fn ERR_clear_error();
    fn ERR_reason_error_string(e: c_ulong_t) -> *const c_char;
}

#[allow(non_camel_case_types)]
type c_ulong_t = core::ffi::c_ulong;

/* ===================================================================== *
 *  OpenSSL constants (stable ABI integer values from the public headers).
 * ===================================================================== */
const SSL_FILETYPE_PEM: c_int = 1;
const SSL_VERIFY_PEER: c_int = 0x01;
const SSL_VERIFY_CLIENT_ONCE: c_int = 0x04;

// SSL_CTRL_* commands used through SSL_CTX_ctrl (openssl/ssl.h).
const SSL_CTRL_SET_TMP_DH: c_int = 3;
const SSL_CTRL_MODE: c_int = 33;
const SSL_CTRL_SET_SESS_CACHE_MODE: c_int = 44;
const SSL_CTRL_SET_MIN_PROTO_VERSION: c_int = 123;
const SSL_CTRL_SET_MAX_PROTO_VERSION: c_int = 124;
const SSL_CTRL_SET_NUM_TICKETS: c_int = 127;
const SSL_CTRL_SET_GROUPS_LIST: c_int = 92;

const SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER: c_long = 0x00000002;
const SSL_SESS_CACHE_OFF: c_long = 0x0000;

// SSL_OP_* options (openssl/ssl.h, OpenSSL 3.x u64 values).
const SSL_OP_NO_TICKET: u64 = 0x0000000000004000;
const SSL_OP_NO_COMPRESSION: u64 = 0x0000000000020000;
const SSL_OP_NO_RENEGOTIATION: u64 = 0x0000000040000000;
const SSL_OP_NO_CLIENT_RENEGOTIATION: u64 = 0x0000000020000000;
const SSL_OP_SINGLE_DH_USE: u64 = 0x0000000000100000;
const SSL_OP_CIPHER_SERVER_PREFERENCE: u64 = 0x0000000000400000;

// X509 verification flags (openssl/x509_vfy.h).
const X509_V_FLAG_CRL_CHECK: u64 = 0x4;
const X509_V_FLAG_CRL_CHECK_ALL: u64 = 0x8;

// BIO control / flags (openssl/bio.h).
const BIO_CTRL_FLUSH: c_int = 11;
const BIO_FLAGS_SHOULD_RETRY: c_int = 0x08;
const BIO_FLAGS_READ: c_int = 0x01;
const BIO_FLAGS_WRITE: c_int = 0x02;
const BIO_CTRL_GET_MEM_DATA: c_int = 3;

// X509_NAME_print_ex / ASN1_STRING_print_ex flags (openssl/asn1.h).
const XN_FLAG_RFC2253: u64 = 0x002CF1F3; // (ASN1_STRFLGS_RFC2253 | ESC_CTRL | ESC_MSB-cleared) per header
const ASN1_STRFLGS_RFC2253: u64 = 0x0871F;
const ASN1_STRFLGS_ESC_MSB: u64 = 4;
const ASN1_STRFLGS_UTF8_CONVERT: u64 = 0x10;

// NID for commonName (openssl/obj_mac.h).
const NID_commonName: c_int = 13;
const NID_undef: c_int = 0;

// SSL_select_next_proto outcome + ALPN callback return values.
const OPENSSL_NPN_NEGOTIATED: c_int = 1;
const SSL_TLSEXT_ERR_OK: c_int = 0;
const SSL_TLSEXT_ERR_ALERT_FATAL: c_int = 2;
const SSL_TLSEXT_ERR_NOACK: c_int = 3;

/// `PG_ALPN_PROTOCOL_VECTOR` — length-prefixed `"postgresql"` (pqcomm.h).
const ALPN_PROTOS: &[u8] = b"\x0apostgresql";

/* ===================================================================== *
 *  Token <-> pointer mapping. `0 == NULL`.
 * ===================================================================== */
#[inline]
fn tok<T>(p: *mut T) -> u64 {
    p as u64
}
#[inline]
fn ptr_ctx(t: u64) -> *mut SSL_CTX {
    t as *mut SSL_CTX
}
#[inline]
fn ptr_ssl(t: u64) -> *mut SSL {
    t as *mut SSL
}
#[inline]
fn ptr_x509(t: u64) -> *mut X509 {
    t as *mut X509
}
#[inline]
fn ptr_name(t: u64) -> *mut X509_NAME {
    t as *mut X509_NAME
}

// The active server-wide `SSL_CTX*` (the C `static SSL_CTX *SSL_context`),
// the passphrase-callback selection, the dummy-cb flag, and the custom port
// BIO method. Per-backend (set once in the postmaster before fork in practice).
thread_local! {
    static SSL_CONTEXT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Which passphrase callback `default_openssl_tls_init` selected.
    static PASSWD_CB: std::cell::Cell<Option<PasswdCb>> = const { std::cell::Cell::new(None) };
    /// `static bool dummy_ssl_passwd_cb_called;`
    static DUMMY_CB_CALLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// The custom port BIO method (`static BIO_METHOD *port_bio_method_ptr`).
    static PORT_BIO_METHOD: std::cell::Cell<*mut BIO_METHOD> =
        const { std::cell::Cell::new(core::ptr::null_mut()) };
}

/* ===================================================================== *
 *  Real OpenSSL C callbacks delegating into the ported owner.
 * ===================================================================== */

/// `info_cb(const SSL *ssl, int type, int args)`.
unsafe extern "C" fn info_cb(ssl: *const SSL, type_: c_int, args: c_int) {
    let desc_ptr = SSL_state_string_long(ssl);
    let desc = if desc_ptr.is_null() {
        String::new()
    } else {
        CStr::from_ptr(desc_ptr).to_string_lossy().into_owned()
    };
    owner::info_cb(type_, args, &desc);
}

/// `verify_cb(int ok, X509_STORE_CTX *ctx)`.
unsafe extern "C" fn verify_cb(ok: c_int, ctx: *mut X509_STORE_CTX) -> c_int {
    if ok != 0 {
        return ok;
    }
    let depth = X509_STORE_CTX_get_error_depth(ctx);
    let errcode = X509_STORE_CTX_get_error(ctx);
    let errstring_ptr = X509_verify_cert_error_string(errcode as c_long);
    let errstring = if errstring_ptr.is_null() {
        String::new()
    } else {
        CStr::from_ptr(errstring_ptr).to_string_lossy().into_owned()
    };

    let cert = X509_STORE_CTX_get_current_cert(ctx);
    let info = if cert.is_null() {
        None
    } else {
        let subject = x509_name_to_cstring(X509_get_subject_name(cert));
        let issuer = x509_name_to_cstring(X509_get_issuer_name(cert));
        let serial = serial_to_dec(cert);
        Some(owner::VerifyCertInfo {
            subject,
            issuer,
            serial,
        })
    };
    owner::verify_cb(ok != 0, depth, &errstring, info) as c_int
}

/// `ssl_external_passwd_cb` / `dummy_ssl_passwd_cb`. The C return value is the
/// passphrase length; on error it returns -1 / 0.
unsafe extern "C" fn passwd_cb(
    buf: *mut c_char,
    size: c_int,
    _rwflag: c_int,
    _u: *mut c_void,
) -> c_int {
    let which = PASSWD_CB.with(|c| c.get());
    match which {
        Some(PasswdCb::Dummy) | None => {
            // dummy_ssl_passwd_cb: set flag, return empty passphrase.
            DUMMY_CB_CALLED.with(|c| c.set(true));
            if size > 0 {
                *buf = 0;
            }
            0
        }
        Some(PasswdCb::External) => {
            // ssl_external_passwd_cb: run ssl_passphrase_command into buf.
            let ctx = mcx::MemoryContext::new("ssl_external_passwd_cb");
            let ret = match owner::ssl_external_passwd_cb(ctx.mcx(), size) {
                Ok(pass) => {
                    let bytes = pass.as_slice();
                    let n = bytes.len().min((size.max(0)) as usize);
                    core::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, n);
                    n as c_int
                }
                Err(_) => -1,
            };
            ret
        }
    }
}

/// `alpn_cb(ssl, out, outlen, in, inlen, userdata)`.
unsafe extern "C" fn alpn_cb(
    _ssl: *mut SSL,
    out: *mut *const c_uchar,
    outlen: *mut c_uchar,
    in_: *const c_uchar,
    inlen: c_uint,
    _arg: *mut c_void,
) -> c_int {
    let retval = SSL_select_next_proto(
        out as *mut *mut c_uchar,
        outlen,
        ALPN_PROTOS.as_ptr(),
        ALPN_PROTOS.len() as c_uint,
        in_,
        inlen,
    );
    if (*out).is_null() || (*outlen as usize) > ALPN_PROTOS.len() || *outlen == 0 {
        return SSL_TLSEXT_ERR_NOACK; // can't happen
    }
    if retval == OPENSSL_NPN_NEGOTIATED {
        SSL_TLSEXT_ERR_OK
    } else {
        SSL_TLSEXT_ERR_ALERT_FATAL
    }
}

/* ===================================================================== *
 *  Custom port BIO (port_bio_read / port_bio_write / port_bio_ctrl).
 * ===================================================================== */

unsafe extern "C" fn port_bio_read(h: *mut BIO, buf: *mut c_char, size: c_int) -> c_int {
    let token = BIO_get_data(h) as u64;
    BIO_clear_flags(h, BIO_FLAGS_READ | BIO_FLAGS_SHOULD_RETRY);
    let (res, bytes) = seams::port_bio_read::call(token, size.max(0) as usize);
    if res <= 0 {
        // Set retry flag if it was a non-blocking would-block (errno EAGAIN).
        let e = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if res < 0 && (e == libc::EAGAIN || e == libc::EWOULDBLOCK || e == libc::EINTR) {
            BIO_set_flags(h, BIO_FLAGS_READ | BIO_FLAGS_SHOULD_RETRY);
        }
        return res as c_int;
    }
    let n = bytes.len().min(size.max(0) as usize);
    core::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, n);
    n as c_int
}

unsafe extern "C" fn port_bio_write(h: *mut BIO, buf: *const c_char, size: c_int) -> c_int {
    let token = BIO_get_data(h) as u64;
    BIO_clear_flags(h, BIO_FLAGS_WRITE | BIO_FLAGS_SHOULD_RETRY);
    let slice = core::slice::from_raw_parts(buf as *const u8, size.max(0) as usize);
    let res = seams::port_bio_write::call(token, slice);
    if res <= 0 {
        let e = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if res < 0 && (e == libc::EAGAIN || e == libc::EWOULDBLOCK || e == libc::EINTR) {
            BIO_set_flags(h, BIO_FLAGS_WRITE | BIO_FLAGS_SHOULD_RETRY);
        }
    }
    res as c_int
}

unsafe extern "C" fn port_bio_ctrl(
    _h: *mut BIO,
    cmd: c_int,
    _num: c_long,
    _ptr: *mut c_void,
) -> c_long {
    // Mirror the C: only BIO_CTRL_FLUSH returns success; everything else 0.
    if cmd == BIO_CTRL_FLUSH {
        1
    } else {
        0
    }
}

/// `port_bio_method()` — lazily build the custom BIO method.
unsafe fn port_bio_method() -> *mut BIO_METHOD {
    let existing = PORT_BIO_METHOD.with(|c| c.get());
    if !existing.is_null() {
        return existing;
    }
    let idx = BIO_get_new_index();
    let name = CString::new("PostgreSQL backend socket").unwrap();
    let m = BIO_meth_new(idx, name.as_ptr());
    if m.is_null() {
        return core::ptr::null_mut();
    }
    if BIO_meth_set_write(m, Some(port_bio_write)) == 0
        || BIO_meth_set_read(m, Some(port_bio_read)) == 0
        || BIO_meth_set_ctrl(m, Some(port_bio_ctrl)) == 0
    {
        // BIO_meth_free not bound; on this near-impossible path leak the method
        // rather than risk an unbound symbol — matches the C error return.
        PORT_BIO_METHOD.with(|c| c.set(core::ptr::null_mut()));
        return core::ptr::null_mut();
    }
    PORT_BIO_METHOD.with(|c| c.set(m));
    m
}

/* ===================================================================== *
 *  X509_NAME_to_cstring (RFC2253 DN as a Rust String), used by verify_cb
 *  and x509_name_print_rfc2253.
 * ===================================================================== */
unsafe fn x509_name_to_cstring(name: *mut X509_NAME) -> String {
    let membuf = BIO_new(BIO_s_mem());
    if membuf.is_null() {
        return String::new();
    }
    let count = X509_NAME_entry_count(name);
    for i in 0..count {
        let e = X509_NAME_get_entry(name, i);
        let nid = OBJ_obj2nid(X509_NAME_ENTRY_get_object(e));
        if nid == NID_undef {
            continue;
        }
        let v = X509_NAME_ENTRY_get_data(e);
        let mut field = OBJ_nid2sn(nid);
        if field.is_null() {
            field = OBJ_nid2ln(nid);
        }
        if field.is_null() {
            continue;
        }
        let fmt = CString::new("/%s=").unwrap();
        BIO_printf(membuf, fmt.as_ptr(), field);
        ASN1_STRING_print_ex(
            membuf,
            v,
            (ASN1_STRFLGS_RFC2253 & !ASN1_STRFLGS_ESC_MSB) | ASN1_STRFLGS_UTF8_CONVERT,
        );
    }
    let nullterm: c_char = 0;
    BIO_write(membuf, &nullterm as *const c_char as *const c_void, 1);
    let mut sp: *mut c_char = core::ptr::null_mut();
    let size = BIO_ctrl(
        membuf,
        BIO_CTRL_GET_MEM_DATA,
        0,
        &mut sp as *mut *mut c_char as *mut c_void,
    ) as usize;
    let out = if sp.is_null() || size == 0 {
        String::new()
    } else {
        let bytes = core::slice::from_raw_parts(sp as *const u8, size.saturating_sub(1));
        String::from_utf8_lossy(bytes).into_owned()
    };
    BIO_free(membuf);
    out
}

/// `BN_bn2dec(ASN1_INTEGER_to_BN(X509_get_serialNumber(cert)))` as a String.
/// We bind the minimal BN/ASN1 path; on any NULL we return None (the C
/// `serialno ? serialno : "unknown"`).
unsafe fn serial_to_dec(cert: *mut X509) -> Option<String> {
    extern "C" {
        fn X509_get_serialNumber(x: *mut X509) -> *mut c_void; // ASN1_INTEGER*
        fn ASN1_INTEGER_to_BN(ai: *const c_void, bn: *mut c_void) -> *mut c_void; // BIGNUM*
        fn BN_bn2dec(a: *const c_void) -> *mut c_char;
        fn BN_free(a: *mut c_void);
        // `OPENSSL_free` is a header macro for `CRYPTO_free(ptr, file, line)`;
        // bind the real exported symbol.
        fn CRYPTO_free(p: *mut c_void, file: *const c_char, line: c_int);
    }
    let sn = X509_get_serialNumber(cert);
    if sn.is_null() {
        return None;
    }
    let b = ASN1_INTEGER_to_BN(sn, core::ptr::null_mut());
    if b.is_null() {
        return None;
    }
    let dec = BN_bn2dec(b);
    let res = if dec.is_null() {
        None
    } else {
        Some(CStr::from_ptr(dec).to_string_lossy().into_owned())
    };
    if !dec.is_null() {
        CRYPTO_free(dec as *mut c_void, core::ptr::null(), 0);
    }
    BN_free(b);
    res
}

/* X509_STORE_CTX accessors + verify-error string (used by verify_cb). */
extern "C" {
    fn X509_STORE_CTX_get_error(ctx: *const X509_STORE_CTX) -> c_int;
    fn X509_STORE_CTX_get_error_depth(ctx: *const X509_STORE_CTX) -> c_int;
    fn X509_STORE_CTX_get_current_cert(ctx: *const X509_STORE_CTX) -> *mut X509;
    fn X509_verify_cert_error_string(n: c_long) -> *const c_char;
}

/* ===================================================================== *
 *  DH parameter loading (load_dh_file / load_dh_buffer + DH_check).
 * ===================================================================== */
const FILE_DH2048: &str = "-----BEGIN DH PARAMETERS-----\n\
MIIBCAKCAQEA///////////JD9qiIWjCNMTGYouA3BzRKQJOCIpnzHQCC76mOxOb\n\
IlFKCHmONATd75UZs806QxswKwpt8l8UN0/hNW1tUcJF5IW1dmJefsb0TELppjft\n\
awv/XLb0Brft7jhr+1qJn6WunyQRfEsf5kkoZlHs5Fs9wgB8uKFjvwWY2kg2HFXT\n\
mmkWP6j9JM9fg2VdI9yjrZYcYvNWIIVSu57VKQdwlpZtZww1Tkq8mATxdGwIyhgh\n\
fDKQXkYuNs474553LBgOhgObJ4Oi7Aeij7XFXfBvTFLJ3ivL9pVYFxg5lUl86pVq\n\
5RXSJhiY+gUQFXKOWoqsqmj//////////wIBAg==\n\
-----END DH PARAMETERS-----\n";

// DH_check codes (openssl/dh.h).
const DH_CHECK_P_NOT_PRIME: c_int = 0x01;
const DH_NOT_SUITABLE_GENERATOR: c_int = 0x08;
const DH_CHECK_P_NOT_SAFE_PRIME: c_int = 0x02;

/// Load + validate DH params from a buffer (the hardcoded fallback) or a file.
/// Returns a `*mut DH` (NULL on any failure/validation rejection). The validation
/// rejections are logged by the owner via the returned bool; here we only
/// produce/free the DH and run DH_check.
unsafe fn load_dh_buffer(buf: &str) -> *mut DH {
    let bio = BIO_new_mem_buf(buf.as_ptr() as *const c_void, buf.len() as c_int);
    if bio.is_null() {
        return core::ptr::null_mut();
    }
    let dh = PEM_read_bio_DHparams(
        bio,
        core::ptr::null_mut(),
        None,
        core::ptr::null_mut(),
    );
    BIO_free(bio);
    dh
}

unsafe fn load_dh_file(path: &str) -> *mut DH {
    let c = match CString::new(path) {
        Ok(c) => c,
        Err(_) => return core::ptr::null_mut(),
    };
    extern "C" {
        fn fopen(path: *const c_char, mode: *const c_char) -> *mut c_void;
        fn fclose(f: *mut c_void) -> c_int;
    }
    let mode = CString::new("r").unwrap();
    let f = fopen(c.as_ptr(), mode.as_ptr());
    if f.is_null() {
        return core::ptr::null_mut();
    }
    extern "C" {
        fn PEM_read_DHparams(
            fp: *mut c_void,
            x: *mut *mut DH,
            cb: Option<pem_password_cb>,
            u: *mut c_void,
        ) -> *mut DH;
    }
    let dh = PEM_read_DHparams(f, core::ptr::null_mut(), None, core::ptr::null_mut());
    fclose(f);
    dh
}

/// Run DH_check, freeing+nulling the DH on a hard rejection. Returns true if the
/// DH passed validation.
unsafe fn dh_check_ok(dh: *mut DH) -> bool {
    if dh.is_null() {
        return false;
    }
    let mut codes: c_int = 0;
    if DH_check(dh, &mut codes) == 0 {
        return false;
    }
    if (codes & DH_CHECK_P_NOT_PRIME) != 0 {
        return false;
    }
    if (codes & DH_NOT_SUITABLE_GENERATOR) != 0 && (codes & DH_CHECK_P_NOT_SAFE_PRIME) != 0 {
        return false;
    }
    true
}

/* ===================================================================== *
 *  Seam installs.
 * ===================================================================== */
pub fn install() {
    // ---- context lifecycle / configuration ----
    seams::ssl_ctx_new_server::set(|| unsafe {
        let m = TLS_method();
        tok(SSL_CTX_new(m))
    });
    seams::ssl_ctx_free::set(|ctx| unsafe {
        if ctx != 0 {
            SSL_CTX_free(ptr_ctx(ctx));
        }
    });
    seams::ssl_ctx_set_mode_accept_moving_write_buffer::set(|ctx| unsafe {
        SSL_CTX_ctrl(
            ptr_ctx(ctx),
            SSL_CTRL_MODE,
            SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER,
            core::ptr::null_mut(),
        );
    });
    seams::ssl_ctx_set_default_passwd_cb::set(|ctx, cb| unsafe {
        PASSWD_CB.with(|c| c.set(Some(cb)));
        SSL_CTX_set_default_passwd_cb(ptr_ctx(ctx), Some(passwd_cb));
    });
    seams::ssl_ctx_use_certificate_chain_file::set(|ctx, file| unsafe {
        let c = match CString::new(file) {
            Ok(c) => c,
            Err(_) => return 0,
        };
        SSL_CTX_use_certificate_chain_file(ptr_ctx(ctx), c.as_ptr())
    });
    seams::ssl_ctx_use_private_key_file_pem::set(|ctx, file| unsafe {
        let c = match CString::new(file) {
            Ok(c) => c,
            Err(_) => return 0,
        };
        SSL_CTX_use_PrivateKey_file(ptr_ctx(ctx), c.as_ptr(), SSL_FILETYPE_PEM)
    });
    seams::dummy_ssl_passwd_cb_called::set(|| DUMMY_CB_CALLED.with(|c| c.get()));
    seams::reset_dummy_ssl_passwd_cb_called::set(|| DUMMY_CB_CALLED.with(|c| c.set(false)));
    seams::ssl_ctx_check_private_key::set(|ctx| unsafe {
        SSL_CTX_check_private_key(ptr_ctx(ctx))
    });
    seams::ssl_ctx_set_min_proto_version::set(|ctx, ver| unsafe {
        SSL_CTX_ctrl(
            ptr_ctx(ctx),
            SSL_CTRL_SET_MIN_PROTO_VERSION,
            ver as c_long,
            core::ptr::null_mut(),
        ) as i32
    });
    seams::ssl_ctx_set_max_proto_version::set(|ctx, ver| unsafe {
        SSL_CTX_ctrl(
            ptr_ctx(ctx),
            SSL_CTRL_SET_MAX_PROTO_VERSION,
            ver as c_long,
            core::ptr::null_mut(),
        ) as i32
    });
    seams::ssl_ctx_disallow_tickets::set(|ctx| unsafe {
        SSL_CTX_ctrl(
            ptr_ctx(ctx),
            SSL_CTRL_SET_NUM_TICKETS,
            0,
            core::ptr::null_mut(),
        );
        SSL_CTX_set_options(ptr_ctx(ctx), SSL_OP_NO_TICKET);
    });
    seams::ssl_ctx_disable_session_cache::set(|ctx| unsafe {
        SSL_CTX_ctrl(
            ptr_ctx(ctx),
            SSL_CTRL_SET_SESS_CACHE_MODE,
            SSL_SESS_CACHE_OFF,
            core::ptr::null_mut(),
        );
    });
    seams::ssl_ctx_disallow_compression::set(|ctx| unsafe {
        SSL_CTX_set_options(ptr_ctx(ctx), SSL_OP_NO_COMPRESSION);
    });
    seams::ssl_ctx_disallow_renegotiation::set(|ctx| unsafe {
        SSL_CTX_set_options(
            ptr_ctx(ctx),
            SSL_OP_NO_RENEGOTIATION | SSL_OP_NO_CLIENT_RENEGOTIATION,
        );
    });
    seams::ssl_ctx_set_single_dh_use::set(|ctx| unsafe {
        SSL_CTX_set_options(ptr_ctx(ctx), SSL_OP_SINGLE_DH_USE);
    });
    seams::ssl_ctx_setup_dh::set(|ctx, dh_params_file, _is_server_start| unsafe {
        let dh = match dh_params_file {
            Some(f) => load_dh_file(f),
            None => load_dh_buffer(FILE_DH2048),
        };
        if dh.is_null() {
            return false;
        }
        if !dh_check_ok(dh) {
            DH_free(dh);
            return false;
        }
        let rc = SSL_CTX_ctrl(
            ptr_ctx(ctx),
            SSL_CTRL_SET_TMP_DH,
            0,
            dh as *mut c_void,
        );
        DH_free(dh);
        rc != 0
    });
    seams::ssl_ctx_set_groups_list::set(|ctx, groups| unsafe {
        let c = match CString::new(groups) {
            Ok(c) => c,
            Err(_) => return 0,
        };
        SSL_CTX_ctrl(
            ptr_ctx(ctx),
            SSL_CTRL_SET_GROUPS_LIST,
            0,
            c.as_ptr() as *mut c_void,
        ) as i32
    });
    seams::ssl_ctx_set_cipher_list::set(|ctx, ciphers| unsafe {
        let c = match CString::new(ciphers) {
            Ok(c) => c,
            Err(_) => return 0,
        };
        SSL_CTX_set_cipher_list(ptr_ctx(ctx), c.as_ptr())
    });
    seams::ssl_ctx_set_ciphersuites::set(|ctx, suites| unsafe {
        let c = match CString::new(suites) {
            Ok(c) => c,
            Err(_) => return 0,
        };
        SSL_CTX_set_ciphersuites(ptr_ctx(ctx), c.as_ptr())
    });
    seams::ssl_ctx_set_cipher_server_preference::set(|ctx| unsafe {
        SSL_CTX_set_options(ptr_ctx(ctx), SSL_OP_CIPHER_SERVER_PREFERENCE);
    });
    seams::ssl_ctx_load_ca::set(|ctx, ca_file| unsafe {
        let c = match CString::new(ca_file) {
            Ok(c) => c,
            Err(_) => return false,
        };
        if SSL_CTX_load_verify_locations(ptr_ctx(ctx), c.as_ptr(), core::ptr::null()) != 1 {
            return false;
        }
        let root_list = SSL_load_client_CA_file(c.as_ptr());
        if root_list.is_null() {
            return false;
        }
        SSL_CTX_set_client_CA_list(ptr_ctx(ctx), root_list);
        true
    });
    seams::ssl_ctx_set_verify_peer::set(|ctx| unsafe {
        SSL_CTX_set_verify(
            ptr_ctx(ctx),
            SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE,
            Some(verify_cb),
        );
    });
    seams::ssl_ctx_setup_crl::set(|ctx, crl_file, crl_dir| unsafe {
        let cvstore = SSL_CTX_get_cert_store(ptr_ctx(ctx));
        if cvstore.is_null() {
            return None;
        }
        let cf = crl_file.and_then(|s| CString::new(s).ok());
        let cd = crl_dir.and_then(|s| CString::new(s).ok());
        let cf_ptr = cf.as_ref().map_or(core::ptr::null(), |c| c.as_ptr());
        let cd_ptr = cd.as_ref().map_or(core::ptr::null(), |c| c.as_ptr());
        if X509_STORE_load_locations(cvstore, cf_ptr, cd_ptr) == 1 {
            X509_STORE_set_flags(cvstore, X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL);
            Some(true)
        } else {
            Some(false)
        }
    });
    seams::ssl_ctx_set_info_callback::set(|ctx| unsafe {
        SSL_CTX_set_info_callback(ptr_ctx(ctx), Some(info_cb));
    });
    seams::ssl_ctx_set_alpn_select_cb::set(|ctx, port_token| unsafe {
        SSL_CTX_set_alpn_select_cb(ptr_ctx(ctx), Some(alpn_cb), port_token as *mut c_void);
    });

    // ---- active-context management ----
    seams::get_active_ssl_context::set(|| SSL_CONTEXT.with(|c| c.get()));
    seams::set_active_ssl_context::set(|ctx| SSL_CONTEXT.with(|c| c.set(ctx)));

    // ---- per-connection SSL object ----
    seams::ssl_new::set(|ctx| unsafe { tok(SSL_new(ptr_ctx(ctx))) });
    seams::ssl_set_port_bio::set(|ssl, port_token| unsafe {
        let bm = port_bio_method();
        if bm.is_null() {
            return 0;
        }
        let bio = BIO_new(bm);
        if bio.is_null() {
            return 0;
        }
        BIO_set_data(bio, port_token as *mut c_void);
        BIO_set_init(bio, 1);
        SSL_set_bio(ptr_ssl(ssl), bio, bio);
        1
    });
    seams::ssl_accept::set(|ssl| unsafe {
        set_errno(0);
        ERR_clear_error();
        let r = SSL_accept(ptr_ssl(ssl));
        let err = SSL_get_error(ptr_ssl(ssl), r);
        let ecode = ERR_get_error() as u64;
        let sys_errno = get_errno();
        SslAcceptResult {
            r,
            err,
            ecode,
            sys_errno,
        }
    });
    seams::err_get_reason::set(|ecode| (ecode & 0xFFF) as i32); // ERR_GET_REASON
    seams::ssl_get0_alpn_selected::set(|ssl| unsafe {
        let mut data: *const c_uchar = core::ptr::null();
        let mut len: c_uint = 0;
        SSL_get0_alpn_selected(ptr_ssl(ssl), &mut data, &mut len);
        if data.is_null() {
            None
        } else {
            Some(core::slice::from_raw_parts(data, len as usize).to_vec())
        }
    });
    seams::ssl_get_peer_certificate::set(|ssl| unsafe {
        tok(SSL_get1_peer_certificate(ptr_ssl(ssl)))
    });
    seams::x509_get_subject_name::set(|cert| unsafe {
        tok(X509_get_subject_name(ptr_x509(cert)))
    });
    seams::x509_name_get_common_name::set(|name: X509Name| unsafe {
        // X509_NAME_get_text_by_NID(name, NID_commonName, NULL, 0) -> len.
        let len = X509_NAME_get_text_by_NID(ptr_name(name), NID_commonName, core::ptr::null_mut(), 0);
        if len == -1 {
            return None;
        }
        let mut buf = vec![0u8; (len as usize) + 1];
        X509_NAME_get_text_by_NID(
            ptr_name(name),
            NID_commonName,
            buf.as_mut_ptr() as *mut c_char,
            buf.len() as c_int,
        );
        buf.truncate(len as usize);
        Some(buf)
    });
    seams::x509_name_print_rfc2253::set(|name: X509Name| unsafe {
        let bio = BIO_new(BIO_s_mem());
        if bio.is_null() {
            return None;
        }
        if X509_NAME_print_ex(bio, ptr_name(name), 0, XN_FLAG_RFC2253) == -1 {
            BIO_free(bio);
            return None;
        }
        let mut sp: *mut c_char = core::ptr::null_mut();
        let size = BIO_ctrl(
            bio,
            BIO_CTRL_GET_MEM_DATA,
            0,
            &mut sp as *mut *mut c_char as *mut c_void,
        ) as usize;
        let out = if sp.is_null() {
            None
        } else {
            Some(core::slice::from_raw_parts(sp as *const u8, size).to_vec())
        };
        BIO_free(bio);
        out
    });
    seams::x509_free::set(|cert| unsafe {
        if cert != 0 {
            X509_free(ptr_x509(cert));
        }
    });
    seams::ssl_shutdown_and_free::set(|ssl| unsafe {
        if ssl != 0 {
            SSL_shutdown(ptr_ssl(ssl));
            SSL_free(ptr_ssl(ssl));
        }
    });

    // ---- I/O ----
    seams::ssl_read::set(|ssl, len| unsafe {
        set_errno(0);
        ERR_clear_error();
        let mut buf = vec![0u8; len];
        let n = SSL_read(ptr_ssl(ssl), buf.as_mut_ptr() as *mut c_void, len as c_int);
        let err = SSL_get_error(ptr_ssl(ssl), n);
        let ecode = ERR_get_error() as u64;
        let sys_errno = get_errno();
        let data = if n > 0 {
            buf.truncate(n as usize);
            buf
        } else {
            Vec::new()
        };
        (
            SslIoResult {
                n: n as isize,
                err,
                sys_errno,
                ecode,
            },
            data,
        )
    });
    seams::ssl_write::set(|ssl, buf: &[u8]| unsafe {
        set_errno(0);
        ERR_clear_error();
        let n = SSL_write(ptr_ssl(ssl), buf.as_ptr() as *const c_void, buf.len() as c_int);
        let err = SSL_get_error(ptr_ssl(ssl), n);
        let ecode = ERR_get_error() as u64;
        let sys_errno = get_errno();
        SslIoResult {
            n: n as isize,
            err,
            sys_errno,
            ecode,
        }
    });

    // ---- error-string helpers ----
    seams::err_get_error::set(|| unsafe { ERR_get_error() as u64 });
    seams::ssl_err_reason_string::set(|ecode| unsafe {
        let s = ERR_reason_error_string(ecode as c_ulong_t);
        if s.is_null() {
            // ERR_SYSTEM_ERROR / numeric fallback (mirrors SSLerrmessageExt).
            format!("SSL error code {ecode}")
        } else {
            CStr::from_ptr(s).to_string_lossy().into_owned()
        }
    });

    // ---- accessors ----
    seams::ssl_get_version::set(|ssl| unsafe {
        let p = SSL_get_version(ptr_ssl(ssl));
        if p.is_null() {
            None
        } else {
            Some(CStr::from_ptr(p).to_string_lossy().into_owned())
        }
    });
    seams::ssl_get_cipher::set(|ssl| unsafe {
        let c = SSL_get_current_cipher(ptr_ssl(ssl));
        if c.is_null() {
            return None;
        }
        let p = SSL_CIPHER_get_name(c);
        if p.is_null() {
            None
        } else {
            Some(CStr::from_ptr(p).to_string_lossy().into_owned())
        }
    });
    seams::ssl_get_cipher_bits::set(|ssl| unsafe {
        let c = SSL_get_current_cipher(ptr_ssl(ssl));
        if c.is_null() {
            return 0;
        }
        SSL_CIPHER_get_bits(c, core::ptr::null_mut())
    });
}

/* errno helpers (the C reads/writes the process errno directly). */
fn get_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}
fn set_errno(v: i32) {
    extern "C" {
        fn __error() -> *mut c_int;
    }
    // macOS/BSD: errno is `*__error()`. (Linux glibc uses `__errno_location`.)
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    unsafe {
        *__error() = v;
    }
    #[cfg(target_os = "linux")]
    unsafe {
        extern "C" {
            fn __errno_location() -> *mut c_int;
        }
        *__errno_location() = v;
    }
    let _ = v;
}
