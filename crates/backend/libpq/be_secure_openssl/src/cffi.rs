// libcrypto/libssl entry points the `openssl` crate does not surface; symbols
// resolve against the vendored OpenSSL that openssl-sys links.

use libc::{c_char, c_int, c_long, c_ulong, c_void};
use openssl_sys as ossl;

// Opaque; openssl-sys does not declare it.
pub enum X509_NAME_ENTRY {}
// Opaque; only passed between the libcrypto calls below.
pub enum X509_EXTENSION {}

extern "C" {
    pub fn X509_NAME_get_text_by_NID(
        name: *mut ossl::X509_NAME,
        nid: c_int,
        buf: *mut c_char,
        len: c_int,
    ) -> c_int;
    // contrib/sslinfo/sslinfo.c callees (X509_NAME_field_to_text /
    // ssl_extension_info).
    fn OBJ_txt2nid(s: *const c_char) -> c_int;
    fn X509_NAME_get_index_by_NID(name: *mut ossl::X509_NAME, nid: c_int, lastpos: c_int) -> c_int;
    fn X509_get_ext_count(x: *const ossl::X509) -> c_int;
    fn X509_get_ext(x: *const ossl::X509, loc: c_int) -> *mut X509_EXTENSION;
    fn X509_EXTENSION_get_object(ex: *mut X509_EXTENSION) -> *mut ossl::ASN1_OBJECT;
    fn X509_EXTENSION_get_critical(ex: *const X509_EXTENSION) -> c_int;
    fn X509V3_EXT_print(
        out: *mut ossl::BIO,
        ext: *mut X509_EXTENSION,
        flag: c_ulong,
        indent: c_int,
    ) -> c_int;
    fn X509_NAME_print_ex(
        out: *mut ossl::BIO,
        nm: *mut ossl::X509_NAME,
        indent: c_int,
        flags: c_ulong,
    ) -> c_int;
    fn X509_NAME_entry_count(name: *const ossl::X509_NAME) -> c_int;
    fn X509_NAME_get_entry(name: *const ossl::X509_NAME, loc: c_int) -> *mut X509_NAME_ENTRY;
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
        let len = unsafe {
            BIO_ctrl(
                self.0,
                BIO_CTRL_INFO,
                0,
                (&mut p as *mut *mut c_char).cast(),
            )
        };
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

/// sslinfo.c X509_NAME_field_to_text outcome; the caller (contrib sslinfo)
/// owns the ereport texts for the error arms.
pub enum DnFieldLookup {
    /// `OBJ_txt2nid` returned `NID_undef` (C: "invalid X.509 field name").
    InvalidFieldName,
    /// `X509_NAME_get_index_by_NID` found no entry (C: `return (Datum) 0`).
    NotPresent,
    /// `BIO_new` failed (C: "could not create OpenSSL BIO structure").
    BioFailure,
    /// `ASN1_STRING_print_ex` output — UTF-8, per sslinfo.c's
    /// `(ASN1_STRFLGS_RFC2253 & ~ASN1_STRFLGS_ESC_MSB) | ASN1_STRFLGS_UTF8_CONVERT`
    /// (the same composition as `CSTRING_ASN1_FLAGS`).
    Value(Vec<u8>),
}

// sslinfo.c X509_NAME_field_to_text + ASN1_STRING_to_text over one name.
pub fn x509_name_field_utf8(name: *mut ossl::X509_NAME, field: &std::ffi::CStr) -> DnFieldLookup {
    // SAFETY: field is NUL-terminated; OBJ_txt2nid only reads it.
    let nid = unsafe { OBJ_txt2nid(field.as_ptr()) };
    if nid == 0 {
        // NID_undef
        return DnFieldLookup::InvalidFieldName;
    }
    // SAFETY: name is a live X509_NAME borrowed from the peer X509.
    let index = unsafe { X509_NAME_get_index_by_NID(name, nid, -1) };
    if index < 0 {
        return DnFieldLookup::NotPresent;
    }
    let Some(bio) = MemBio::new() else {
        return DnFieldLookup::BioFailure;
    };
    // SAFETY: index is a valid entry position; entry/data are borrowed from
    // name and only read.
    unsafe {
        let data = X509_NAME_ENTRY_get_data(X509_NAME_get_entry(name, index));
        ASN1_STRING_print_ex(bio.0, data, CSTRING_ASN1_FLAGS);
    }
    DnFieldLookup::Value(bio.contents())
}

/// One row of sslinfo.c's ssl_extension_info SRF.
pub struct X509ExtensionInfo {
    /// `OBJ_nid2sn` short name.
    pub name: String,
    /// `X509V3_EXT_print(membuf, ext, 0, 0)` output
    /// (C: `cstring_to_text_with_len(buf, len)`, no encoding conversion).
    pub value: Vec<u8>,
    /// `X509_EXTENSION_get_critical`.
    pub critical: bool,
}

pub enum X509ExtensionsError {
    /// `BIO_new` failed (C: ERRCODE_OUT_OF_MEMORY).
    BioFailure,
    /// `OBJ_obj2nid` returned `NID_undef` at this position.
    UnknownExtension(i32),
    /// `X509V3_EXT_print` returned <= 0 at this position.
    PrintFailed(i32),
}

// sslinfo.c ssl_extension_info's per-call body, walked eagerly: the C SRF
// ereports out of the executor at the first bad extension, so collecting
// up-front is observation-equivalent.
pub fn x509_extensions(
    cert: *mut ossl::X509,
) -> Result<Vec<X509ExtensionInfo>, X509ExtensionsError> {
    // SAFETY: cert is the live peer X509 owned by the connection.
    let count = unsafe { X509_get_ext_count(cert) };
    let mut out = Vec::with_capacity(count.max(0) as usize);
    for i in 0..count {
        let bio = MemBio::new().ok_or(X509ExtensionsError::BioFailure)?;
        // SAFETY: i < X509_get_ext_count; ext and obj are borrowed from cert
        // and only read. OBJ_nid2sn on a known (non-undef) nid is non-NULL.
        let (name, critical) = unsafe {
            let ext = X509_get_ext(cert, i);
            let obj = X509_EXTENSION_get_object(ext);
            let nid = OBJ_obj2nid(obj);
            if nid == 0 {
                return Err(X509ExtensionsError::UnknownExtension(i));
            }
            if X509V3_EXT_print(bio.0, ext, 0, 0) <= 0 {
                return Err(X509ExtensionsError::PrintFailed(i));
            }
            (
                std::ffi::CStr::from_ptr(OBJ_nid2sn(nid))
                    .to_string_lossy()
                    .into_owned(),
                X509_EXTENSION_get_critical(ext) != 0,
            )
        };
        out.push(X509ExtensionInfo {
            name,
            value: bio.contents(),
            critical,
        });
    }
    Ok(out)
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

// The DN-formatting units below build X509_NAMEs directly (no TLS session)
// and pin the RFC 2253 escaping + entry ordering that sslinfo.c's
// ASN1_STRING_to_text / X509_NAME_to_cstring produce with
// `(ASN1_STRFLGS_RFC2253 & ~ASN1_STRFLGS_ESC_MSB) | ASN1_STRFLGS_UTF8_CONVERT`.
#[cfg(test)]
mod tests {
    use super::*;
    use foreign_types::ForeignTypeRef;
    use openssl::x509::{X509Name, X509NameBuilder};

    fn build_name(entries: &[(&str, &str)]) -> X509Name {
        let mut b = X509NameBuilder::new().unwrap();
        for (f, v) in entries {
            b.append_entry_by_text(f, v).unwrap();
        }
        b.build()
    }

    fn field(name: &X509Name, f: &str) -> DnFieldLookup {
        let c = std::ffi::CString::new(f).unwrap();
        x509_name_field_utf8(name.as_ptr(), &c)
    }

    fn field_value(name: &X509Name, f: &str) -> Vec<u8> {
        match field(name, f) {
            DnFieldLookup::Value(v) => v,
            DnFieldLookup::NotPresent => panic!("field {f} not present"),
            DnFieldLookup::InvalidFieldName => panic!("field {f} invalid"),
            DnFieldLookup::BioFailure => panic!("BIO failure"),
        }
    }

    #[test]
    fn dn_field_plain_value_passes_through() {
        let n = build_name(&[("CN", "pgrust")]);
        assert_eq!(field_value(&n, "CN"), b"pgrust");
    }

    #[test]
    fn dn_field_long_and_short_names_resolve_to_same_entry() {
        // OBJ_txt2nid accepts both 'CN' and 'commonName' (sslinfo docs use
        // both spellings).
        let n = build_name(&[("CN", "either-spelling")]);
        assert_eq!(field_value(&n, "commonName"), b"either-spelling");
        assert_eq!(field_value(&n, "CN"), b"either-spelling");
    }

    #[test]
    fn dn_field_rfc2253_specials_are_backslash_escaped() {
        // RFC 2253 special characters , + " \ < > ; each escape with a
        // backslash (ASN1_STRFLGS_ESC_2253).
        let n = build_name(&[("CN", "a,b+c\"d\\e<f>g;h")]);
        assert_eq!(field_value(&n, "CN"), b"a\\,b\\+c\\\"d\\\\e\\<f\\>g\\;h");
    }

    #[test]
    fn dn_field_edge_space_and_hash_escaped() {
        // Leading space, trailing space, and leading '#' escape; interior
        // spaces do not.
        let n = build_name(&[("CN", " padded value ")]);
        assert_eq!(field_value(&n, "CN"), b"\\ padded value\\ ");
        let n = build_name(&[("O", "#hash")]);
        assert_eq!(field_value(&n, "O"), b"\\#hash");
    }

    #[test]
    fn dn_field_utf8_multibyte_passes_unescaped() {
        // ~ASN1_STRFLGS_ESC_MSB + ASN1_STRFLGS_UTF8_CONVERT: non-ASCII comes
        // out as raw UTF-8 bytes, not \XX escapes.
        let n = build_name(&[("L", "Z\u{00fc}rich")]);
        assert_eq!(field_value(&n, "L"), "Z\u{00fc}rich".as_bytes());
    }

    #[test]
    fn dn_field_control_chars_hex_escaped() {
        // ASN1_STRFLGS_ESC_CTRL: control characters print as \XX hex.
        let n = build_name(&[("CN", "a\nb")]);
        assert_eq!(field_value(&n, "CN"), b"a\\0Ab");
    }

    #[test]
    fn dn_field_absent_and_invalid_names() {
        let n = build_name(&[("CN", "only-cn")]);
        assert!(matches!(field(&n, "O"), DnFieldLookup::NotPresent));
        assert!(matches!(
            field(&n, "not-a-field"),
            DnFieldLookup::InvalidFieldName
        ));
    }

    #[test]
    fn dn_field_duplicate_rdn_returns_first() {
        // X509_NAME_get_index_by_NID(name, nid, -1) finds the FIRST entry,
        // as sslinfo.c does.
        let n = build_name(&[("OU", "first"), ("OU", "second")]);
        assert_eq!(field_value(&n, "OU"), b"first");
    }

    #[test]
    fn slash_format_preserves_certificate_entry_order() {
        // X509_NAME_to_cstring walks entries 0..count in certificate order
        // ("/C=../ST=.." — NOT RFC 2253's reversed order), '/'-separating
        // short names, with the same per-value escaping.
        let n = build_name(&[
            ("C", "US"),
            ("ST", "CA"),
            ("O", "pgrust, Inc."),
            ("CN", "tester"),
        ]);
        assert_eq!(
            x509_name_slash_format(n.as_ptr()).unwrap(),
            "/C=US/ST=CA/O=pgrust\\, Inc./CN=tester"
        );
    }

    #[test]
    fn slash_format_utf8_value() {
        let n = build_name(&[("CN", "z\u{00fc}")]);
        assert_eq!(x509_name_slash_format(n.as_ptr()).unwrap(), "/CN=z\u{00fc}");
    }
}
