//! `contrib/sslinfo/sslinfo.c` — access to the current connection's client
//! SSL certificate information.
//!
//! C reads `MyProcPort->{ssl_in_use,peer_cert_valid}` and walks
//! `MyProcPort->peer` with libcrypto directly; here the Port flags come from
//! `init_small::globals` and the X.509 walks go through the
//! `be_secure_openssl::be_tls_*` accessors (the peer X509 lives in that
//! crate's per-backend connection state).
//!
//! C's ssl_extension_info is a value-per-call SRF; this port materializes
//! (the pg_walinspect precedent) — same rows, same first-error ereport.

#[cfg(not(target_family = "wasm"))]
use be_secure_openssl as tls;
#[cfg(target_family = "wasm")]
use tls_stub as tls;
use datum::Datum;

/// wasm arm: there is no OpenSSL and no SSL transport at all, so the no-SSL
/// answers (None / false / zero extensions) are the TRUE answers — every
/// entry point already guards on `ssl_in_use()` (always false here) or on
/// these accessors. Mirrors the be_tls_* surface of be_secure_openssl.
#[cfg(target_family = "wasm")]
#[allow(dead_code)]
mod tls_stub {
    /// Mirror of be_secure_openssl::DnFieldLookup.
    pub enum DnFieldLookup {
        InvalidFieldName,
        NotPresent,
        BioFailure,
        Value(Vec<u8>),
    }

    /// Mirror of be_secure_openssl::X509ExtensionInfo.
    pub struct X509ExtensionInfo {
        pub name: String,
        pub value: Vec<u8>,
        pub critical: bool,
    }

    /// Mirror of be_secure_openssl::X509ExtensionsError.
    pub enum X509ExtensionsError {
        BioFailure,
        UnknownExtension(i32),
        PrintFailed(i32),
    }

    pub fn be_tls_get_version() -> Option<String> {
        None
    }
    pub fn be_tls_get_cipher() -> Option<String> {
        None
    }
    pub fn be_tls_get_peer_subject_name() -> Option<String> {
        None
    }
    pub fn be_tls_get_peer_issuer_name() -> Option<String> {
        None
    }
    pub fn be_tls_get_peer_serial() -> Option<String> {
        None
    }
    pub fn be_tls_has_peer_certificate() -> bool {
        false
    }
    pub fn be_tls_get_peer_dn_field(
        _field: &std::ffi::CStr,
        _issuer: bool,
    ) -> Option<DnFieldLookup> {
        None
    }
    pub fn be_tls_get_peer_extensions() -> Result<Vec<X509ExtensionInfo>, X509ExtensionsError> {
        Ok(Vec::new())
    }
}
use init_small::globals as g;
use mcx::Mcx;
use tls::{DnFieldLookup, X509ExtensionsError};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OUT_OF_MEMORY,
};
use types_fmgr::{
    byref_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use wchar::PG_UTF8;

const LIBRARY: &str = "sslinfo";

// pg_config_manual.h; sslinfo.c's stack buffers for serial/subject/issuer.
const NAMEDATALEN: usize = 64;

// MyProcPort->ssl_in_use / MyProcPort->peer_cert_valid. C dereferences
// MyProcPort unconditionally (only reachable from a client backend, where it
// is always set); the guard keeps a portless caller at false instead of a
// panic.
fn ssl_in_use() -> bool {
    g::HaveMyProcPort() && g::WithMyProcPort(|p| p.ssl_in_use)
}

fn peer_cert_valid() -> bool {
    g::HaveMyProcPort() && g::WithMyProcPort(|p| p.peer_cert_valid)
}

// PG_RETURN_NULL()
fn null(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fcinfo.isnull = true;
    Ok(Datum::null())
}

fn text_datum(mcx: Mcx<'_>, s: &[u8]) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s)?))
}

#[track_caller]
#[cold]
fn bio_err() -> Box<PgError> {
    Box::new(
        PgError::error("could not create OpenSSL BIO structure")
            .with_sqlstate(ERRCODE_OUT_OF_MEMORY),
    )
}

// ssl_is_used
fn fc_ssl_is_used(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(ssl_in_use()))
}

// ssl_version
fn fc_ssl_version(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if !ssl_in_use() {
        return null(fcinfo);
    }
    match tls::be_tls_get_version() {
        Some(version) => text_datum(fcinfo.result_mcx(), version.as_bytes()),
        None => null(fcinfo),
    }
}

// ssl_cipher
fn fc_ssl_cipher(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if !ssl_in_use() {
        return null(fcinfo);
    }
    match tls::be_tls_get_cipher() {
        Some(cipher) => text_datum(fcinfo.result_mcx(), cipher.as_bytes()),
        None => null(fcinfo),
    }
}

// ssl_client_cert_present
fn fc_ssl_client_cert_present(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(peer_cert_valid()))
}

// ssl_client_serial: C fills a NAMEDATALEN buffer from be_tls_get_peer_serial
// (BN_bn2dec, <= 49 digits for a spec-legal 20-octet serial, so the strlcpy
// never truncates) and feeds it to numeric_in(str, 0, -1).
fn fc_ssl_client_serial(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if !ssl_in_use() || !peer_cert_valid() {
        return null(fcinfo);
    }
    // C: `if (!*decimal) PG_RETURN_NULL();`
    let Some(serial) = tls::be_tls_get_peer_serial().filter(|s| !s.is_empty()) else {
        return null(fcinfo);
    };
    match adt_numeric::numeric_in(&serial, -1, None)? {
        Some(img) => byref_result(fcinfo.result_mcx(), img.as_bytes()),
        None => unreachable!("hard numeric_in error escapes via Err"),
    }
}

// ASN1_STRING_to_text tail: pg_any_to_server(sp, size - 1, PG_UTF8), then
// cstring_to_text (strlen-bounded, hence the NUL cut).
fn asn1_bytes_to_text(fcinfo: &mut Fcinfo, bytes: &[u8]) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let converted = mbutils::pg_any_to_server(mcx, bytes, PG_UTF8)?;
    let s: &[u8] = match converted.as_ref() {
        Some(v) => v,
        None => bytes,
    };
    let cut = s.iter().position(|&b| b == 0).unwrap_or(s.len());
    let t = varlena::cstring_to_text(mcx, &s[..cut])?;
    Ok(varlena_result(t))
}

// X509_NAME_field_to_text over the already-guarded peer certificate.
fn dn_field(fcinfo: &mut Fcinfo, issuer: bool) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of these strict fns is a non-null text varlena.
    let field = unsafe { fcinfo.arg_varlena_packed(0)? };
    // text_to_cstring semantics: OBJ_txt2nid sees up to the first NUL.
    let bytes = field.data();
    let cut = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let field_bytes = bytes[..cut].to_vec();
    let cfield = std::ffi::CString::new(field_bytes.clone()).expect("NUL-free by construction");
    match tls::be_tls_get_peer_dn_field(&cfield, issuer) {
        // No peer: unreachable behind the callers' guards, NULL as in C.
        None | Some(DnFieldLookup::NotPresent) => null(fcinfo),
        Some(DnFieldLookup::InvalidFieldName) => Err(Box::new(
            PgError::error(format!(
                "invalid X.509 field name: \"{}\"",
                String::from_utf8_lossy(&field_bytes)
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        )),
        Some(DnFieldLookup::BioFailure) => Err(bio_err()),
        Some(DnFieldLookup::Value(v)) => asn1_bytes_to_text(fcinfo, &v),
    }
}

// ssl_client_dn_field
fn fc_ssl_client_dn_field(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if !ssl_in_use() || !peer_cert_valid() {
        return null(fcinfo);
    }
    dn_field(fcinfo, false)
}

// ssl_issuer_field: C's only guard is `if (!(MyProcPort->peer))`.
fn fc_ssl_issuer_field(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if !tls::be_tls_has_peer_certificate() {
        return null(fcinfo);
    }
    dn_field(fcinfo, true)
}

// ssl_client_dn / ssl_issuer_dn: C fills a NAMEDATALEN stack buffer via
// be_tls_get_peer_{subject,issuer}_name's strlcpy, so the DN text is
// truncated to NAMEDATALEN - 1 bytes (matching pg_stat_ssl's NAMEDATALEN
// columns, which the ssl TAP suite compares against).
fn whole_dn(fcinfo: &mut Fcinfo, name: Option<String>) -> PgResult<Datum> {
    let Some(name) = name.filter(|s| !s.is_empty()) else {
        // C: `if (!*subject) PG_RETURN_NULL();`
        return null(fcinfo);
    };
    let b = name.as_bytes();
    let b = &b[..b.len().min(NAMEDATALEN - 1)];
    text_datum(fcinfo.result_mcx(), b)
}

fn fc_ssl_client_dn(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if !ssl_in_use() || !peer_cert_valid() {
        return null(fcinfo);
    }
    whole_dn(fcinfo, tls::be_tls_get_peer_subject_name())
}

fn fc_ssl_issuer_dn(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if !ssl_in_use() || !peer_cert_valid() {
        return null(fcinfo);
    }
    whole_dn(fcinfo, tls::be_tls_get_peer_issuer_name())
}

// ssl_extension_info: SETOF (name text, value text, critical boolean).
fn fc_ssl_extension_info(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("ssl_extension_info: resolved FmgrInfo required");
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let exts = tls::be_tls_get_peer_extensions().map_err(|e| -> Box<PgError> {
        match e {
            X509ExtensionsError::BioFailure => bio_err(),
            X509ExtensionsError::UnknownExtension(pos) => Box::new(
                PgError::error(format!(
                    "unknown OpenSSL extension in certificate at position {pos}"
                ))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ),
            X509ExtensionsError::PrintFailed(pos) => Box::new(
                PgError::error(format!(
                    "could not print extension value in certificate at position {pos}"
                ))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ),
        }
    })?;

    for ext in exts {
        let values = [
            // C: CStringGetTextDatum(OBJ_nid2sn(nid))
            text_datum(mcx, ext.name.as_bytes())?,
            // C: cstring_to_text_with_len(buf, len) — raw bytes, no encoding
            // conversion.
            text_datum(mcx, &ext.value)?,
            Datum::from_bool(ext.critical),
        ];
        srf.putvalues(&values, &[false, false, false])?;
    }

    Ok(srf.finish(fcinfo))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "ssl_is_used" => fc_ssl_is_used,
        "ssl_version" => fc_ssl_version,
        "ssl_cipher" => fc_ssl_cipher,
        "ssl_client_cert_present" => fc_ssl_client_cert_present,
        "ssl_client_serial" => fc_ssl_client_serial,
        "ssl_client_dn_field" => fc_ssl_client_dn_field,
        "ssl_issuer_field" => fc_ssl_issuer_field,
        "ssl_client_dn" => fc_ssl_client_dn,
        "ssl_issuer_dn" => fc_ssl_issuer_dn,
        "ssl_extension_info" => fc_ssl_extension_info,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // sslinfo.c's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}
