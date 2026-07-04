// libcrypto/libssl entry points the `openssl` crate does not surface; symbols
// resolve against the vendored OpenSSL that openssl-sys links.

use libc::{c_char, c_int, c_long, c_ulong, c_void};
use openssl_sys as ossl;

// Opaque; openssl-sys does not declare it.
pub enum X509_NAME_ENTRY {}

extern "C" {
    pub fn X509_NAME_get_text_by_NID(
        name: *mut ossl::X509_NAME,
        nid: c_int,
        buf: *mut c_char,
        len: c_int,
    ) -> c_int;
    fn X509_NAME_print_ex(
        out: *mut ossl::BIO,
        nm: *mut ossl::X509_NAME,
        indent: c_int,
        flags: c_ulong,
    ) -> c_int;
    fn X509_NAME_entry_count(name: *const ossl::X509_NAME) -> c_int;
    fn X509_NAME_get_entry(
        name: *const ossl::X509_NAME,
        loc: c_int,
    ) -> *mut X509_NAME_ENTRY;
    fn X509_NAME_ENTRY_get_object(ne: *const X509_NAME_ENTRY) -> *mut ossl::ASN1_OBJECT;
    fn X509_NAME_ENTRY_get_data(ne: *const X509_NAME_ENTRY) -> *mut ossl::ASN1_STRING;
    fn OBJ_obj2nid(o: *const ossl::ASN1_OBJECT) -> c_int;
    fn OBJ_nid2sn(n: c_int) -> *const c_char;
    fn OBJ_nid2ln(n: c_int) -> *const c_char;
    fn ASN1_STRING_print_ex(
        out: *mut ossl::BIO,
        str_: *const ossl::ASN1_STRING,
        flags: c_ulong,
    ) -> c_int;
    pub fn SSL_CTX_set_default_passwd_cb(
        ctx: *mut ossl::SSL_CTX,
        cb: Option<unsafe extern "C" fn(*mut c_char, c_int, c_int, *mut c_void) -> c_int>,
    );
    pub fn SSL_CTX_set_info_callback(
        ctx: *mut ossl::SSL_CTX,
        cb: Option<unsafe extern "C" fn(*const ossl::SSL, c_int, c_int)>,
    );
    pub fn SSL_state_string_long(ssl: *const ossl::SSL) -> *const c_char;
    pub fn X509_STORE_load_locations(
        store: *mut ossl::X509_STORE,
        file: *const c_char,
        dir: *const c_char,
    ) -> c_int;
    pub fn X509_get_signature_info(
        x: *mut ossl::X509,
        mdnid: *mut c_int,
        pknid: *mut c_int,
        secbits: *mut c_int,
        flags: *mut u32,
    ) -> c_int;
    fn BIO_new(t: *const ossl::BIO_METHOD) -> *mut ossl::BIO;
    fn BIO_s_mem() -> *const ossl::BIO_METHOD;
    fn BIO_write(b: *mut ossl::BIO, data: *const c_void, dlen: c_int) -> c_int;
    fn BIO_ctrl(b: *mut ossl::BIO, cmd: c_int, larg: c_long, parg: *mut c_void) -> c_long;
    fn BIO_free(b: *mut ossl::BIO) -> c_int;
}

const BIO_CTRL_INFO: c_int = 3;

// XN_FLAG_RFC2253 (x509v3 header composition; verified against openssl 3.x).
const XN_FLAG_RFC2253: c_ulong = 0x0111_0317;
// (ASN1_STRFLGS_RFC2253 & ~ASN1_STRFLGS_ESC_MSB) | ASN1_STRFLGS_UTF8_CONVERT.
const CSTRING_ASN1_FLAGS: c_ulong = 0x0313;

struct MemBio(*mut ossl::BIO);

impl MemBio {
    fn new() -> Option<MemBio> {
        // SAFETY: standard memory-BIO construction; freed by Drop.
        let b = unsafe { BIO_new(BIO_s_mem()) };
        if b.is_null() {
            None
        } else {
            Some(MemBio(b))
        }
    }

    fn contents(&self) -> Vec<u8> {
        let mut p: *mut c_char = std::ptr::null_mut();
        // SAFETY: BIO_CTRL_INFO on a mem BIO yields (len, data-ptr).
        let len = unsafe { BIO_ctrl(self.0, BIO_CTRL_INFO, 0, (&mut p as *mut *mut c_char).cast()) };
        if len <= 0 || p.is_null() {
            return Vec::new();
        }
        // SAFETY: p points at len readable bytes owned by the BIO.
        unsafe { std::slice::from_raw_parts(p.cast::<u8>(), len as usize) }.to_vec()
    }
}

impl Drop for MemBio {
    fn drop(&mut self) {
        // SAFETY: self.0 is a live BIO created by MemBio::new.
        unsafe { BIO_free(self.0) };
    }
}

pub fn x509_name_print_rfc2253(name: *mut ossl::X509_NAME) -> Option<Vec<u8>> {
    let bio = MemBio::new()?;
    // SAFETY: name is a live X509_NAME borrowed from an X509.
    if unsafe { X509_NAME_print_ex(bio.0, name, 0, XN_FLAG_RFC2253) } == -1 {
        return None;
    }
    Some(bio.contents())
}

pub fn nid_short_name(nid: c_int) -> Option<String> {
    // SAFETY: OBJ_nid2sn returns a static C string or NULL.
    let p = unsafe { OBJ_nid2sn(nid) };
    if p.is_null() {
        return None;
    }
    Some(
        unsafe { std::ffi::CStr::from_ptr(p) }
            .to_string_lossy()
            .into_owned(),
    )
}

pub fn x509_name_slash_format(name: *mut ossl::X509_NAME) -> Option<String> {
    let bio = MemBio::new()?;
    // SAFETY: name is a live X509_NAME; entries/objects are borrowed from it
    // and only read within this loop.
    unsafe {
        let count = X509_NAME_entry_count(name);
        for i in 0..count {
            let e = X509_NAME_get_entry(name, i);
            let nid = OBJ_obj2nid(X509_NAME_ENTRY_get_object(e));
            if nid == 0 {
                return None;
            }
            let mut field = OBJ_nid2sn(nid);
            if field.is_null() {
                field = OBJ_nid2ln(nid);
            }
            if field.is_null() {
                return None;
            }
            let field_str = std::ffi::CStr::from_ptr(field);
            let prefix = format!("/{}=", field_str.to_string_lossy());
            BIO_write(bio.0, prefix.as_ptr().cast(), prefix.len() as c_int);
            let v = X509_NAME_ENTRY_get_data(e);
            ASN1_STRING_print_ex(bio.0, v, CSTRING_ASN1_FLAGS);
        }
    }
    Some(String::from_utf8_lossy(&bio.contents()).into_owned())
}
