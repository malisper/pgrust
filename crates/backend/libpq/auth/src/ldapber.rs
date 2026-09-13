//! Minimal LDAPv3 client (RFC 4511/4515 subset) speaking BER over TCP or
//! TLS — the in-tree replacement for the libldap calls auth.c makes:
//! ldap_initialize over an `ldap://` / `ldaps://` host list, simple bind,
//! search returning entry DNs, ldap_start_tls_s, unbind. TLS follows the
//! libldap C 18.6 links on Debian — OpenLDAP 2.5.13 tls2.c over the GnuTLS
//! backend tls_g.c — with OpenSSL as the crypto provider: the credentials
//! from the ldap.conf / LDAP* options (ldapconf; no trust store unless one
//! is configured), the peer verified AFTER the handshake per TLS_REQCERT
//! (tlsg_session_accept), the host-name check of tlsg_session_chkhost,
//! TLS_PEERKEY_HASH pinning, and ld_error carrying tlsg_session_errmsg's
//! text — `gnutls_strerror(rc)`, which for a failed verification (-1) and
//! a host-name / pin mismatch (LDAP_CONNECT_ERROR) is "(unknown error
//! code)". No SASL, referral chasing, or controls. Result codes use
//! libldap's convention: server result codes >= 0, client-side codes < 0
//! (ldap_err2string in hba).

#![cfg(not(target_family = "wasm"))]

use std::io::{Read, Write};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, ToSocketAddrs};

use foreign_types::ForeignTypeRef;
use openssl::nid::Nid;
use openssl::ssl::{
    HandshakeError, Ssl, SslContext, SslContextBuilder, SslFiletype, SslMethod, SslOptions,
    SslRef, SslStream, SslVerifyMode,
};
use openssl::x509::verify::X509VerifyFlags;
use openssl::x509::{X509Crl, X509VerifyResult};

use crate::ldapconf::{
    ldap_int_hostname, LdapOptions, LDAP_OPT_X_TLS_ALLOW, LDAP_OPT_X_TLS_DEMAND,
    LDAP_OPT_X_TLS_HARD, LDAP_OPT_X_TLS_NEVER, LDAP_OPT_X_TLS_TRY,
};

pub const LDAP_SUCCESS: i32 = 0;
pub const LDAP_SERVER_DOWN: i32 = -1;
pub const LDAP_LOCAL_ERROR: i32 = -2;
pub const LDAP_DECODING_ERROR: i32 = -4;
pub const LDAP_TIMEOUT: i32 = -5;
pub const LDAP_FILTER_ERROR: i32 = -7;
pub const LDAP_CONNECT_ERROR: i32 = -11;

/// LDAP_EXOP_START_TLS (RFC 4511).
pub const LDAP_EXOP_START_TLS: &[u8] = b"1.3.6.1.4.1.1466.20037";
const LDAP_PORT: u16 = 389;
const LDAPS_PORT: u16 = 636;

const LDAP_VERSION3: i64 = 3;

// Protocol op tags.
pub(crate) const TAG_BIND_REQUEST: u8 = 0x60;
pub(crate) const TAG_BIND_RESPONSE: u8 = 0x61;
pub(crate) const TAG_UNBIND_REQUEST: u8 = 0x42;
pub(crate) const TAG_SEARCH_REQUEST: u8 = 0x63;
pub(crate) const TAG_SEARCH_ENTRY: u8 = 0x64;
pub(crate) const TAG_SEARCH_DONE: u8 = 0x65;
pub(crate) const TAG_SEARCH_REFERENCE: u8 = 0x73;
pub(crate) const TAG_EXTENDED_REQUEST: u8 = 0x77;
pub(crate) const TAG_EXTENDED_RESPONSE: u8 = 0x78;

pub(crate) const TAG_SEQUENCE: u8 = 0x30;
pub(crate) const TAG_INTEGER: u8 = 0x02;
pub(crate) const TAG_ENUMERATED: u8 = 0x0a;
pub(crate) const TAG_OCTET_STRING: u8 = 0x04;
pub(crate) const TAG_BOOLEAN: u8 = 0x01;

// ---------- BER encoding ----------

pub(crate) fn put_len(out: &mut Vec<u8>, len: usize) {
    if len < 0x80 {
        out.push(len as u8);
    } else {
        let be = (len as u64).to_be_bytes();
        let first = be.iter().position(|&b| b != 0).unwrap_or(7);
        out.push(0x80 | (8 - first) as u8);
        out.extend_from_slice(&be[first..]);
    }
}

pub(crate) fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(content.len() + 6);
    out.push(tag);
    put_len(&mut out, content.len());
    out.extend_from_slice(content);
    out
}

pub(crate) fn ber_int(tag: u8, v: i64) -> Vec<u8> {
    let be = v.to_be_bytes();
    let mut i = 0;
    // Minimal two's-complement encoding.
    while i < 7 {
        let cur = be[i];
        let next_msb = be[i + 1] & 0x80;
        if (cur == 0x00 && next_msb == 0) || (cur == 0xff && next_msb != 0) {
            i += 1;
        } else {
            break;
        }
    }
    tlv(tag, &be[i..])
}

// ---------- BER decoding ----------

pub(crate) struct BerReader<'a> {
    buf: &'a [u8],
    pub(crate) pos: usize,
}

impl<'a> BerReader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    pub(crate) fn read_tlv(&mut self) -> Result<(u8, &'a [u8]), ()> {
        let tag = *self.buf.get(self.pos).ok_or(())?;
        self.pos += 1;
        let first = *self.buf.get(self.pos).ok_or(())?;
        self.pos += 1;
        let len = if first < 0x80 {
            first as usize
        } else {
            let n = (first & 0x7f) as usize;
            if n == 0 || n > 8 {
                return Err(());
            }
            let mut v: usize = 0;
            for _ in 0..n {
                v = v
                    .checked_mul(256)
                    .ok_or(())?
                    .checked_add(*self.buf.get(self.pos).ok_or(())? as usize)
                    .ok_or(())?;
                self.pos += 1;
            }
            v
        };
        let end = self.pos.checked_add(len).ok_or(())?;
        if end > self.buf.len() {
            return Err(());
        }
        let content = &self.buf[self.pos..end];
        self.pos = end;
        Ok((tag, content))
    }
}

pub(crate) fn decode_int(content: &[u8]) -> Result<i64, ()> {
    if content.is_empty() || content.len() > 8 {
        return Err(());
    }
    let mut v: i64 = if content[0] & 0x80 != 0 { -1 } else { 0 };
    for &b in content {
        v = (v << 8) | b as i64;
    }
    Ok(v)
}

// ---------- Search filters (RFC 4515 subset) ----------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    Eq(String, Vec<u8>),
    Ge(String, Vec<u8>),
    Le(String, Vec<u8>),
    Approx(String, Vec<u8>),
    Present(String),
    // initial, any*, final
    Substrings(String, Option<Vec<u8>>, Vec<Vec<u8>>, Option<Vec<u8>>),
    // RFC 4515 extensible match: attr [":dn"] [":" rule] ":=" value
    Extensible {
        attr: Option<String>,
        rule: Option<String>,
        value: Vec<u8>,
        dn_attrs: bool,
    },
}

fn encode_filter(f: &Filter) -> Vec<u8> {
    fn ava(tag: u8, attr: &str, val: &[u8]) -> Vec<u8> {
        let mut c = tlv(TAG_OCTET_STRING, attr.as_bytes());
        c.extend_from_slice(&tlv(TAG_OCTET_STRING, val));
        tlv(tag, &c)
    }
    match f {
        Filter::And(fs) | Filter::Or(fs) => {
            let tag = if matches!(f, Filter::And(_)) { 0xa0 } else { 0xa1 };
            let mut c = Vec::new();
            for sub in fs {
                c.extend_from_slice(&encode_filter(sub));
            }
            tlv(tag, &c)
        }
        Filter::Not(sub) => tlv(0xa2, &encode_filter(sub)),
        Filter::Eq(a, v) => ava(0xa3, a, v),
        Filter::Ge(a, v) => ava(0xa5, a, v),
        Filter::Le(a, v) => ava(0xa6, a, v),
        Filter::Approx(a, v) => ava(0xa8, a, v),
        Filter::Present(a) => tlv(0x87, a.as_bytes()),
        Filter::Extensible { attr, rule, value, dn_attrs } => {
            let mut c = Vec::new();
            if let Some(r) = rule {
                c.extend_from_slice(&tlv(0x81, r.as_bytes()));
            }
            if let Some(a) = attr {
                c.extend_from_slice(&tlv(0x82, a.as_bytes()));
            }
            c.extend_from_slice(&tlv(0x83, value));
            if *dn_attrs {
                c.extend_from_slice(&tlv(0x84, &[0xff]));
            }
            tlv(0xa9, &c)
        }
        Filter::Substrings(a, initial, any, fin) => {
            let mut subs = Vec::new();
            if let Some(i) = initial {
                subs.extend_from_slice(&tlv(0x80, i));
            }
            for m in any {
                subs.extend_from_slice(&tlv(0x81, m));
            }
            if let Some(fi) = fin {
                subs.extend_from_slice(&tlv(0x82, fi));
            }
            let mut c = tlv(TAG_OCTET_STRING, a.as_bytes());
            c.extend_from_slice(&tlv(TAG_SEQUENCE, &subs));
            tlv(0xa4, &c)
        }
    }
}

// RFC 4515 value unescape: backslash + two hex digits.
fn unescape_value(s: &str) -> Result<Vec<u8>, ()> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' {
            let h = char::from(*b.get(i + 1).ok_or(())?).to_digit(16).ok_or(())?;
            let l = char::from(*b.get(i + 2).ok_or(())?).to_digit(16).ok_or(())?;
            out.push(((h << 4) | l) as u8);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    Ok(out)
}

struct FilterParser<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> FilterParser<'a> {
    fn peek(&self) -> Option<u8> {
        self.b.get(self.pos).copied()
    }
    fn expect(&mut self, c: u8) -> Result<(), ()> {
        if self.peek() == Some(c) {
            self.pos += 1;
            Ok(())
        } else {
            Err(())
        }
    }
    fn parse(&mut self) -> Result<Filter, ()> {
        self.expect(b'(')?;
        let f = match self.peek().ok_or(())? {
            b'&' | b'|' => {
                let and = self.peek() == Some(b'&');
                self.pos += 1;
                let mut subs = Vec::new();
                while self.peek() == Some(b'(') {
                    subs.push(self.parse()?);
                }
                if subs.is_empty() {
                    return Err(());
                }
                if and {
                    Filter::And(subs)
                } else {
                    Filter::Or(subs)
                }
            }
            b'!' => {
                self.pos += 1;
                Filter::Not(Box::new(self.parse()?))
            }
            _ => self.parse_item()?,
        };
        self.expect(b')')?;
        Ok(f)
    }
    fn parse_item(&mut self) -> Result<Filter, ()> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == b'=' || c == b'>' || c == b'<' || c == b'~' || c == b')' || c == b'(' {
                break;
            }
            self.pos += 1;
        }
        let attr = std::str::from_utf8(&self.b[start..self.pos])
            .map_err(|_| ())?
            .to_string();
        if attr.is_empty() {
            return Err(());
        }
        let op = self.peek().ok_or(())?;
        let ge_le_approx = matches!(op, b'>' | b'<' | b'~');
        if ge_le_approx {
            self.pos += 1;
        }
        self.expect(b'=')?;
        let vstart = self.pos;
        while let Some(c) = self.peek() {
            if c == b')' {
                break;
            }
            if c == b'\\' {
                self.pos += 1; // escape consumes the next byte too
            }
            self.pos += 1;
        }
        let raw = std::str::from_utf8(&self.b[vstart..self.pos.min(self.b.len())])
            .map_err(|_| ())?;
        match op {
            b'>' => Ok(Filter::Ge(attr, unescape_value(raw)?)),
            b'<' => Ok(Filter::Le(attr, unescape_value(raw)?)),
            b'~' => Ok(Filter::Approx(attr, unescape_value(raw)?)),
            _ if attr.ends_with(':') => {
                let mut parts = attr[..attr.len() - 1].split(':');
                let attr = parts.next().unwrap_or("");
                let rest: Vec<&str> = parts.collect();
                let (dn_attrs, rule) = match rest.as_slice() {
                    [] => (false, None),
                    [d] if d.eq_ignore_ascii_case("dn") => (true, None),
                    [r] if !r.is_empty() => (false, Some((*r).to_string())),
                    [d, r] if d.eq_ignore_ascii_case("dn") && !r.is_empty() => {
                        (true, Some((*r).to_string()))
                    }
                    _ => return Err(()),
                };
                if attr.is_empty() && rule.is_none() {
                    return Err(());
                }
                Ok(Filter::Extensible {
                    attr: (!attr.is_empty()).then(|| attr.to_string()),
                    rule,
                    value: unescape_value(raw)?,
                    dn_attrs,
                })
            }
            _ => {
                if raw == "*" {
                    Ok(Filter::Present(attr))
                } else if raw.contains('*') {
                    let mut parts = raw.split('*');
                    let first = parts.next().unwrap_or("");
                    let mut rest: Vec<&str> = parts.collect();
                    let last = rest.pop().unwrap_or("");
                    let initial = if first.is_empty() {
                        None
                    } else {
                        Some(unescape_value(first)?)
                    };
                    let fin = if last.is_empty() {
                        None
                    } else {
                        Some(unescape_value(last)?)
                    };
                    let mut any = Vec::new();
                    for m in rest {
                        if !m.is_empty() {
                            any.push(unescape_value(m)?);
                        }
                    }
                    Ok(Filter::Substrings(attr, initial, any, fin))
                } else {
                    Ok(Filter::Eq(attr, unescape_value(raw)?))
                }
            }
        }
    }
}

/// libldap str2filter's contract for the filters auth.c builds: a
/// parenthesized RFC 4515 expression (a bare `attr=value` item is accepted
/// too, as OpenLDAP does). Err is CheckLDAPAuth's LDAP_FILTER_ERROR arm.
pub fn parse_search_filter(s: &str) -> Result<Filter, ()> {
    let mut p = FilterParser {
        b: s.as_bytes(),
        pos: 0,
    };
    let f = if p.peek() == Some(b'(') {
        p.parse()?
    } else {
        p.parse_item()?
    };
    if p.pos != p.b.len() {
        return Err(());
    }
    Ok(f)
}

// ---------- The connection ----------

enum Transport {
    Plain(TcpStream),
    Tls(Box<SslStream<TcpStream>>),
}

impl Read for Transport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Transport::Plain(s) => s.read(buf),
            Transport::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Transport::Plain(s) => s.write(buf),
            Transport::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Transport::Plain(s) => s.flush(),
            Transport::Tls(s) => s.flush(),
        }
    }
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

pub struct LdapConn {
    // ldap_initialize's URI list: (host, port) with one scheme for all.
    hosts: Vec<(String, i32)>,
    ldaps: bool,
    opts: LdapOptions,
    stream: Option<Transport>,
    // lconn_server->lud_host of the open connection.
    server_host: Option<String>,
    msgid: i64,
    // ld_error: the server's last diagnosticMessage, or libldap's own TLS
    // text (LDAP_OPT_DIAGNOSTIC_MESSAGE).
    diag: Option<String>,
}

impl LdapConn {
    /// ldap_initialize over `scheme://host:port` for every host, with the
    /// process's libldap options (ldap_int_initialize) — no I/O yet.
    pub fn new(hosts: Vec<(String, i32)>, ldaps: bool, opts: LdapOptions) -> Self {
        Self {
            hosts,
            ldaps,
            opts,
            stream: None,
            server_host: None,
            msgid: 0,
            diag: None,
        }
    }

    /// ldap_get_option(LDAP_OPT_DIAGNOSTIC_MESSAGE).
    pub fn diagnostic_message(&self) -> Option<&str> {
        self.diag.as_deref()
    }

    // os-ip.c ldap_connect_to_host: getaddrinfo(host, port) and the first
    // address that connects (a NETWORK_TIMEOUT bounds each attempt); the
    // TIMEOUT option bounds every later result wait on the socket.
    fn connect_to_host(&self, host: &str, port: i32) -> Result<TcpStream, ()> {
        let host = if host.is_empty() { "localhost" } else { host };
        let port = if port == 0 {
            if self.ldaps {
                LDAPS_PORT
            } else {
                LDAP_PORT
            }
        } else {
            u16::try_from(port).map_err(|_| ())?
        };
        let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs().map_err(|_| ())?.collect();
        for addr in addrs {
            let connected = match self.opts.tm_net {
                Some(tmo) => TcpStream::connect_timeout(&addr, tmo),
                None => TcpStream::connect(addr),
            };
            if let Ok(s) = connected {
                let _ = s.set_nodelay(true);
                let _ = s.set_read_timeout(self.opts.tm_api);
                return Ok(s);
            }
        }
        Err(())
    }

    // request.c ldap_new_connection + open.c ldap_int_open_connection: the
    // first URI that connects (for ldaps: connects AND completes TLS —
    // a failed handshake moves on to the next URI, its text staying in
    // ld_error); none -> LDAP_SERVER_DOWN.
    fn ensure_connected(&mut self) -> Result<(), i32> {
        if self.stream.is_some() {
            return Ok(());
        }
        let hosts = self.hosts.clone();
        for (host, port) in hosts {
            let Ok(tcp) = self.connect_to_host(&host, port) else { continue };
            if self.ldaps {
                match self.tls_start(tcp, &host) {
                    Ok(tls) => {
                        self.stream = Some(Transport::Tls(Box::new(tls)));
                        self.server_host = Some(host);
                        return Ok(());
                    }
                    Err(_) => continue,
                }
            }
            self.stream = Some(Transport::Plain(tcp));
            self.server_host = Some(host);
            return Ok(());
        }
        Err(LDAP_SERVER_DOWN)
    }

    // tls_g.c:184-380 tlsg_ctx_init (client side) from the global options:
    // TLS_CIPHER_SUITE as a GnuTLS priority string, the trust directory
    // (warn-only), the trust file (fatal when unreadable), no trust store
    // at all when none is configured, certificate + key only as a pair,
    // TLS_CRLFILE. Verification is NOT armed on the handshake: tls_g.c
    // verifies the peer afterwards (gnutls_verify below). Err = the context
    // could not be built (alloc_handle NULL in C: the connect fails as
    // LDAP_CONNECT_ERROR with no diagnostic).
    fn build_tls_ctx(&self) -> Result<SslContext, ()> {
        let o = &self.opts;
        let mut b = SslContextBuilder::new(SslMethod::tls_client()).map_err(|_| ())?;
        if let Some(suites) = &o.tls_ciphersuite {
            apply_gnutls_priority(&mut b, suites)?;
        }
        if let Some(dir) = &o.tls_cacertdir {
            // "warning: no certificate found in CA certificate directory":
            // only warn, no return
            let _ = b.load_verify_locations(None, Some(std::path::Path::new(dir)));
        }
        if let Some(file) = &o.tls_cacertfile {
            b.set_ca_file(file).map_err(|_| ())?;
        }
        match (&o.tls_certfile, &o.tls_keyfile) {
            (Some(cf), Some(kf)) => {
                b.set_private_key_file(kf, SslFiletype::PEM).map_err(|_| ())?;
                b.set_certificate_chain_file(cf).map_err(|_| ())?;
            }
            (None, None) => {}
            // "TLS: only one of certfile and keyfile specified"
            _ => return Err(()),
        }
        if let Some(crlfile) = &o.tls_crlfile {
            let pem = std::fs::read(crlfile).map_err(|_| ())?;
            let store = b.cert_store_mut();
            let mut any = false;
            for block in pem_blocks(&pem, b"-----END X509 CRL-----") {
                let crl = X509Crl::from_pem(block).map_err(|_| ())?;
                // SAFETY: both pointers come from live openssl-crate owners
                // for the duration of the call; X509_STORE_add_crl takes
                // its own reference.
                if unsafe { X509_STORE_add_crl(store.as_ptr(), crl.as_ptr()) } != 1 {
                    return Err(());
                }
                any = true;
            }
            if !any {
                return Err(());
            }
            store.set_flags(X509VerifyFlags::CRL_CHECK).map_err(|_| ())?;
        }
        b.set_verify(SslVerifyMode::NONE);
        Ok(b.build())
    }

    // tls_g.c:411-433 tlsg_session_accept, the post-handshake half: under
    // TLS_REQCERT != never the peer chain is verified (tlsg_cert_verify —
    // trust, revocation, validity period); no certificate passes under
    // try; any failure passes under allow. 0 or -1 (the gnutls code
    // errmsg later renders).
    fn gnutls_verify(&self, ssl: &SslRef) -> i32 {
        let reqcert = self.opts.tls_require_cert;
        if reqcert == LDAP_OPT_X_TLS_NEVER {
            return 0;
        }
        let has_cert = ssl.peer_certificate().is_some();
        if !has_cert && reqcert == LDAP_OPT_X_TLS_TRY {
            return 0;
        }
        let rc = if !has_cert || ssl.verify_result() != X509VerifyResult::OK {
            -1
        } else {
            0
        };
        if rc != 0 && reqcert == LDAP_OPT_X_TLS_ALLOW {
            return 0;
        }
        rc
    }

    // tls2.c ldap_int_tls_start + ldap_int_tls_connect on a fresh socket:
    // the handshake (SNI unless the host is numeric), tls_g.c's
    // post-handshake verification, then ldap_pvt_tls_check_hostname. Any
    // failure sets ld_error = tlsg_session_errmsg = gnutls_strerror(rc).
    // Err carries ld_errno (LDAP_CONNECT_ERROR unless check_hostname set
    // its own; LDAP_TIMEOUT when NETWORK_TIMEOUT expires).
    fn tls_start(&mut self, tcp: TcpStream, host: &str) -> Result<SslStream<TcpStream>, i32> {
        let host = if host.is_empty() { "localhost" } else { host };
        let ctx = self.build_tls_ctx().map_err(|()| LDAP_CONNECT_ERROR)?;
        let mut ssl = Ssl::new(&ctx).map_err(|_| LDAP_CONNECT_ERROR)?;
        let mut numeric = true;
        for c in host.bytes() {
            if c == b':' {
                break; // IPv6 address
            }
            if c == b'.' {
                continue;
            }
            if !c.is_ascii_digit() {
                numeric = false;
                break;
            }
        }
        if !numeric && ssl.set_hostname(host).is_err() {
            // gnutls_server_name_set failure (GNUTLS_E_INVALID_REQUEST)
            self.diag = Some(gnutls_strerror(GNUTLS_E_INVALID_REQUEST));
            return Err(LDAP_CONNECT_ERROR);
        }
        if let Some(tmo) = self.opts.tm_net {
            let _ = tcp.set_read_timeout(Some(tmo));
            let _ = tcp.set_write_timeout(Some(tmo));
        }
        let stream = match ssl.connect(tcp) {
            Ok(s) => s,
            Err(HandshakeError::Failure(mid)) => {
                let timed_out = mid.error().io_error().is_some_and(is_timeout);
                if timed_out && self.opts.tm_net.is_some() {
                    // ldap_int_tls_start's poll deadline: LDAP_TIMEOUT, no
                    // ld_error change
                    return Err(LDAP_TIMEOUT);
                }
                self.diag = Some(gnutls_strerror(gnutls_handshake_code(mid.error())));
                return Err(LDAP_CONNECT_ERROR);
            }
            Err(HandshakeError::SetupFailure(_)) | Err(HandshakeError::WouldBlock(_)) => {
                self.diag = Some(gnutls_strerror(GNUTLS_E_UNKNOWN));
                return Err(LDAP_CONNECT_ERROR);
            }
        };
        let _ = stream.get_ref().set_read_timeout(self.opts.tm_api);
        let _ = stream.get_ref().set_write_timeout(None);
        let rc = self.gnutls_verify(stream.ssl());
        if rc < 0 {
            self.diag = Some(gnutls_strerror(rc));
            return Err(LDAP_CONNECT_ERROR);
        }
        if let Err(rc) = self.check_hostname(stream.ssl(), host) {
            // tls2.c:409-423: the chkhost / pinning ld_error text is
            // replaced by ti_session_errmsg(rc) = gnutls_strerror(rc)
            self.diag = Some(gnutls_strerror(rc));
            return Err(rc);
        }
        Ok(stream)
    }

    // tls2.c:544-570 ldap_pvt_tls_check_hostname: the name check unless
    // TLS_REQCERT is never/allow, then TLS_PEERKEY_HASH pinning. Err = the
    // ld_errno C leaves (LDAP_CONNECT_ERROR; -1 for a pinning setup
    // failure).
    fn check_hostname(&self, ssl: &SslRef, host: &str) -> Result<(), i32> {
        if self.opts.tls_require_cert != LDAP_OPT_X_TLS_NEVER
            && self.opts.tls_require_cert != LDAP_OPT_X_TLS_ALLOW
        {
            session_chkhost(ssl, host, self.opts.tls_require_san)?;
        }
        if let Some((alg, pin)) = &self.opts.tls_pin {
            session_pinning(ssl, alg.as_deref(), pin)?;
        }
        Ok(())
    }

    /// ldap_start_tls_s (tls2.c:1304-1335): LDAP_LOCAL_ERROR if TLS is
    /// already in place, else the StartTLS extended operation (which opens
    /// the connection like any first operation) and, on LDAP_SUCCESS,
    /// ldap_int_tls_start on the default connection.
    pub fn start_tls(&mut self) -> i32 {
        if matches!(self.stream, Some(Transport::Tls(_))) {
            return LDAP_LOCAL_ERROR;
        }
        let op = tlv(TAG_EXTENDED_REQUEST, &tlv(0x80, LDAP_EXOP_START_TLS));
        let msgid = match self.send_op(&op) {
            Ok(id) => id,
            Err(rc) => return rc,
        };
        let (tag, body) = match self.read_op_for(msgid) {
            Ok(v) => v,
            Err(rc) => return rc,
        };
        if tag != TAG_EXTENDED_RESPONSE {
            return LDAP_DECODING_ERROR;
        }
        let rc = match self.parse_result(&body) {
            Ok(rc) => rc,
            Err(rc) => return rc,
        };
        if rc != LDAP_SUCCESS {
            return rc;
        }
        let host = self.server_host.clone().unwrap_or_default();
        match self.stream.take() {
            // HAS_TLS(sb): SSL_connect on the established session is a
            // no-op; only ldap_pvt_tls_check_hostname runs again.
            Some(Transport::Tls(tls)) => {
                let r = self.check_hostname(tls.ssl(), &host);
                self.stream = Some(Transport::Tls(tls));
                match r {
                    Ok(()) => LDAP_SUCCESS,
                    Err(rc) => {
                        self.diag = Some(gnutls_strerror(rc));
                        rc
                    }
                }
            }
            Some(Transport::Plain(tcp)) => match self.tls_start(tcp, &host) {
                Ok(tls) => {
                    self.stream = Some(Transport::Tls(Box::new(tls)));
                    LDAP_SUCCESS
                }
                Err(rc) => rc,
            },
            None => LDAP_SERVER_DOWN,
        }
    }

    fn send_op(&mut self, op: &[u8]) -> Result<i64, i32> {
        self.ensure_connected()?;
        self.msgid += 1;
        let msgid = self.msgid;
        let mut content = ber_int(TAG_INTEGER, msgid);
        content.extend_from_slice(op);
        let msg = tlv(TAG_SEQUENCE, &content);
        let stream = self.stream.as_mut().expect("connected");
        if stream.write_all(&msg).is_err() {
            self.stream = None;
            return Err(LDAP_SERVER_DOWN);
        }
        Ok(msgid)
    }

    // Read one complete BER element (an LDAPMessage) off the stream.
    fn read_message(&mut self) -> Result<Vec<u8>, i32> {
        let stream = self.stream.as_mut().ok_or(LDAP_SERVER_DOWN)?;
        let mut header = [0u8; 2];
        if let Err(e) = stream.read_exact(&mut header) {
            if is_timeout(&e) {
                return Err(LDAP_TIMEOUT);
            }
            self.stream = None;
            return Err(LDAP_SERVER_DOWN);
        }
        let mut msg = header.to_vec();
        let first = header[1];
        let content_len = if first < 0x80 {
            first as usize
        } else {
            let n = (first & 0x7f) as usize;
            if n == 0 || n > 8 {
                return Err(LDAP_DECODING_ERROR);
            }
            let mut lenbuf = vec![0u8; n];
            if let Err(e) = stream.read_exact(&mut lenbuf) {
                if is_timeout(&e) {
                    return Err(LDAP_TIMEOUT);
                }
                self.stream = None;
                return Err(LDAP_SERVER_DOWN);
            }
            msg.extend_from_slice(&lenbuf);
            let mut v: usize = 0;
            for b in lenbuf {
                v = v
                    .checked_mul(256)
                    .and_then(|x| x.checked_add(b as usize))
                    .ok_or(LDAP_DECODING_ERROR)?;
            }
            v
        };
        // Sanity ceiling to keep a malicious peer from ballooning memory.
        if content_len > 64 * 1024 * 1024 {
            return Err(LDAP_DECODING_ERROR);
        }
        let at = msg.len();
        msg.resize(at + content_len, 0);
        if let Err(e) = stream.read_exact(&mut msg[at..]) {
            if is_timeout(&e) {
                return Err(LDAP_TIMEOUT);
            }
            self.stream = None;
            return Err(LDAP_SERVER_DOWN);
        }
        Ok(msg)
    }

    // Read the next protocolOp for msgid; other message IDs are discarded
    // (nothing else is in flight on this synchronous connection).
    fn read_op_for(&mut self, msgid: i64) -> Result<(u8, Vec<u8>), i32> {
        loop {
            let msg = self.read_message()?;
            let mut r = BerReader::new(&msg);
            let (tag, content) = r.read_tlv().map_err(|()| LDAP_DECODING_ERROR)?;
            if tag != TAG_SEQUENCE {
                return Err(LDAP_DECODING_ERROR);
            }
            let mut r = BerReader::new(content);
            let (tag, id) = r.read_tlv().map_err(|()| LDAP_DECODING_ERROR)?;
            if tag != TAG_INTEGER {
                return Err(LDAP_DECODING_ERROR);
            }
            if decode_int(id).map_err(|()| LDAP_DECODING_ERROR)? != msgid {
                continue;
            }
            let (op_tag, op) = r.read_tlv().map_err(|()| LDAP_DECODING_ERROR)?;
            return Ok((op_tag, op.to_vec()));
        }
    }

    // Parse an LDAPResult body: resultCode, matchedDN, diagnosticMessage.
    fn parse_result(&mut self, body: &[u8]) -> Result<i32, i32> {
        let mut r = BerReader::new(body);
        let (tag, code) = r.read_tlv().map_err(|()| LDAP_DECODING_ERROR)?;
        if tag != TAG_ENUMERATED {
            return Err(LDAP_DECODING_ERROR);
        }
        let rc = decode_int(code).map_err(|()| LDAP_DECODING_ERROR)? as i32;
        let (_, _matched) = r.read_tlv().map_err(|()| LDAP_DECODING_ERROR)?;
        let (_, diag) = r.read_tlv().map_err(|()| LDAP_DECODING_ERROR)?;
        self.diag = Some(String::from_utf8_lossy(diag).into_owned());
        Ok(rc)
    }

    /// ldap_simple_bind_s.
    pub fn simple_bind(&mut self, dn: &str, passwd: &[u8]) -> i32 {
        let mut body = ber_int(TAG_INTEGER, LDAP_VERSION3);
        body.extend_from_slice(&tlv(TAG_OCTET_STRING, dn.as_bytes()));
        body.extend_from_slice(&tlv(0x80, passwd)); // simple auth (raw bytes)
        let op = tlv(TAG_BIND_REQUEST, &body);
        let msgid = match self.send_op(&op) {
            Ok(id) => id,
            Err(rc) => return rc,
        };
        let (tag, body) = match self.read_op_for(msgid) {
            Ok(v) => v,
            Err(rc) => return rc,
        };
        if tag != TAG_BIND_RESPONSE {
            return LDAP_DECODING_ERROR;
        }
        match self.parse_result(&body) {
            Ok(rc) => rc,
            Err(rc) => rc,
        }
    }

    /// ldap_search_s with attrsonly=0; returns the entry DNs. derefAliases,
    /// sizeLimit and timeLimit come from the DEREF / SIZELIMIT / TIMELIMIT
    /// options as ldap_search_ext's -1 defaults do.
    pub fn search(
        &mut self,
        base: &str,
        scope: i32,
        filter: &Filter,
        attrs: &[&str],
    ) -> Result<Vec<String>, i32> {
        let mut body = tlv(TAG_OCTET_STRING, base.as_bytes());
        body.extend_from_slice(&ber_int(TAG_ENUMERATED, scope as i64));
        body.extend_from_slice(&ber_int(TAG_ENUMERATED, self.opts.deref as i64));
        body.extend_from_slice(&ber_int(TAG_INTEGER, self.opts.sizelimit as i64));
        body.extend_from_slice(&ber_int(TAG_INTEGER, self.opts.timelimit as i64));
        body.extend_from_slice(&tlv(TAG_BOOLEAN, &[0x00])); // typesOnly
        body.extend_from_slice(&encode_filter(filter));
        let mut attrlist = Vec::new();
        for a in attrs {
            attrlist.extend_from_slice(&tlv(TAG_OCTET_STRING, a.as_bytes()));
        }
        body.extend_from_slice(&tlv(TAG_SEQUENCE, &attrlist));
        let op = tlv(TAG_SEARCH_REQUEST, &body);
        let msgid = self.send_op(&op)?;

        let mut entries = Vec::new();
        loop {
            let (tag, body) = self.read_op_for(msgid)?;
            match tag {
                TAG_SEARCH_ENTRY => {
                    let mut r = BerReader::new(&body);
                    let (t, dn) = r.read_tlv().map_err(|()| LDAP_DECODING_ERROR)?;
                    if t != TAG_OCTET_STRING {
                        return Err(LDAP_DECODING_ERROR);
                    }
                    entries.push(String::from_utf8_lossy(dn).into_owned());
                }
                TAG_SEARCH_REFERENCE => continue,
                TAG_SEARCH_DONE => {
                    let rc = self.parse_result(&body)?;
                    if rc != LDAP_SUCCESS {
                        return Err(rc);
                    }
                    return Ok(entries);
                }
                _ => return Err(LDAP_DECODING_ERROR),
            }
        }
    }

    /// ldap_unbind: fire the notification and drop the connection.
    pub fn unbind(&mut self) {
        if self.stream.is_some() {
            self.msgid += 1;
            let mut content = ber_int(TAG_INTEGER, self.msgid);
            content.extend_from_slice(&tlv(TAG_UNBIND_REQUEST, &[]));
            let msg = tlv(TAG_SEQUENCE, &content);
            if let Some(s) = self.stream.as_mut() {
                let _ = s.write_all(&msg);
            }
            self.stream = None;
        }
    }
}

// ---------- OpenLDAP's TLS pieces (tls_g.c on the OpenSSL provider) ----------

// libcrypto's X509_STORE_add_crl (the openssl crate wraps add_cert only).
extern "C" {
    fn X509_STORE_add_crl(store: *mut openssl_sys::X509_STORE, x: *mut openssl_sys::X509_CRL)
        -> libc::c_int;
}

// gnutls.h error codes tlsg_session_errmsg can render here.
const GNUTLS_E_FATAL_ALERT_RECEIVED: i32 = -12;
const GNUTLS_E_INVALID_REQUEST: i32 = -50;
const GNUTLS_E_PREMATURE_TERMINATION: i32 = -110;
// A code gnutls_strerror does not know (and -1 / LDAP_CONNECT_ERROR, the
// codes tls_g.c hands it after a failed verification or name check).
const GNUTLS_E_UNKNOWN: i32 = -1;

// gnutls_strerror(rc): tlsg_session_errmsg's whole body (tls_g.c:470-473).
fn gnutls_strerror(rc: i32) -> String {
    match rc {
        GNUTLS_E_FATAL_ALERT_RECEIVED => "A TLS fatal alert has been received.",
        GNUTLS_E_INVALID_REQUEST => "The request is invalid.",
        GNUTLS_E_PREMATURE_TERMINATION => "The TLS connection was non-properly terminated.",
        _ => "(unknown error code)",
    }
    .to_string()
}

// The gnutls_handshake code an OpenSSL handshake failure corresponds to:
// the peer going away is GNUTLS_E_PREMATURE_TERMINATION, an alert from the
// peer is GNUTLS_E_FATAL_ALERT_RECEIVED (SSL_AD_REASON_OFFSET-based reason
// codes), anything else renders as the unknown code.
fn gnutls_handshake_code(err: &openssl::ssl::Error) -> i32 {
    const SSL_R_UNEXPECTED_EOF_WHILE_READING: libc::c_int = 294;
    const SSL_AD_REASON_OFFSET: libc::c_int = 1000;
    if err.io_error().is_some() {
        return GNUTLS_E_PREMATURE_TERMINATION;
    }
    if let Some(first) = err.ssl_error().and_then(|es| es.errors().first()) {
        let reason = first.reason_code();
        if reason == SSL_R_UNEXPECTED_EOF_WHILE_READING {
            return GNUTLS_E_PREMATURE_TERMINATION;
        }
        if reason >= SSL_AD_REASON_OFFSET {
            return GNUTLS_E_FATAL_ALERT_RECEIVED;
        }
    }
    GNUTLS_E_UNKNOWN
}

// The `-----BEGIN ...-----` .. `end_marker` blocks of a PEM file.
fn pem_blocks<'a>(pem: &'a [u8], end_marker: &[u8]) -> Vec<&'a [u8]> {
    let mut blocks = Vec::new();
    let mut at = 0;
    while let Some(rel) = pem[at..]
        .windows(end_marker.len())
        .position(|w| w == end_marker)
    {
        let end = at + rel + end_marker.len();
        blocks.push(&pem[at..end]);
        at = end;
    }
    blocks
}

// gnutls_priority_init (tlsg_parse_ciphers): the GnuTLS priority-string
// grammar — initial keywords or an @profile, then `+`/`-`/`!` algorithm
// modifiers and `%` flags. The version modifiers are applied to the
// OpenSSL context (`-VERS-TLS1.0` etc.); cipher / MAC / key-exchange
// modifiers are accepted without effect (the provider negotiates its own
// suite lists). A string GnuTLS rejects (an element that is none of these)
// is Err: C's ctx init fails.
fn apply_gnutls_priority(b: &mut SslContextBuilder, suites: &str) -> Result<(), ()> {
    const KEYWORDS: &[&str] = &[
        "NORMAL",
        "PERFORMANCE",
        "SECURE128",
        "SECURE192",
        "SECURE256",
        "SUITEB128",
        "SUITEB192",
        "LEGACY",
        "PFS",
        "NONE",
        "EXPORT",
    ];
    let mut disabled = SslOptions::empty();
    for element in suites.split(':') {
        let element = element.trim();
        if element.is_empty() {
            continue;
        }
        if element.starts_with('@') || element.starts_with('%') {
            continue;
        }
        if KEYWORDS.iter().any(|k| element.eq_ignore_ascii_case(k)) {
            continue;
        }
        let Some(op) = element.chars().next() else { continue };
        if op != '+' && op != '-' && op != '!' {
            return Err(());
        }
        let name = &element[1..];
        let Some(vers) = name
            .strip_prefix("VERS-")
            .or_else(|| name.strip_prefix("vers-"))
        else {
            continue;
        };
        let vers = vers.to_ascii_uppercase();
        let flags = match vers.as_str() {
            "SSL3.0" => SslOptions::NO_SSLV3,
            "TLS1.0" => SslOptions::NO_TLSV1,
            "TLS1.1" => SslOptions::NO_TLSV1_1,
            "TLS1.2" => SslOptions::NO_TLSV1_2,
            "TLS1.3" => SslOptions::NO_TLSV1_3,
            "ALL" | "TLS-ALL" => {
                SslOptions::NO_SSLV3
                    | SslOptions::NO_TLSV1
                    | SslOptions::NO_TLSV1_1
                    | SslOptions::NO_TLSV1_2
                    | SslOptions::NO_TLSV1_3
            }
            _ => continue, // DTLS versions and unknown names: no TLS effect
        };
        if op == '+' {
            disabled.remove(flags);
        } else {
            disabled.insert(flags);
        }
    }
    if !disabled.is_empty() {
        b.set_options(disabled);
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NameType {
    Dns,
    Ip4([u8; 4]),
    Ip6([u8; 16]),
}

// strncasecmp over two equal-length spans.
fn eq_nocase(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

// tls_g.c:558-765 tlsg_session_chkhost: subjectAltName (DNS exact /
// wildcard, IP) under TLS_REQSAN, then the last CN (exact / wildcard).
// Err = LDAP_CONNECT_ERROR. (C also writes a "TLS: hostname does not
// match ..." ld_error here, which ldap_int_tls_connect then replaces
// with gnutls_strerror(LDAP_CONNECT_ERROR); the texts never surface.)
fn session_chkhost(ssl: &SslRef, name_in: &str, chk_san: i32) -> Result<(), i32> {
    let fqdn;
    let name: &str = if name_in.eq_ignore_ascii_case("localhost") {
        fqdn = ldap_int_hostname();
        &fqdn
    } else {
        name_in
    };
    let nb = name.as_bytes();
    let Some(cert) = ssl.peer_certificate() else {
        // "unable to get peer certificate": a fatal condition would have
        // aborted long before now.
        return Ok(());
    };
    let ntype = if let Ok(a6) = name.parse::<Ipv6Addr>() {
        NameType::Ip6(a6.octets())
    } else if name
        .rfind('.')
        .and_then(|i| nb.get(i + 1))
        .is_some_and(|c| c.is_ascii_digit())
    {
        match name.parse::<Ipv4Addr>() {
            Ok(a4) => NameType::Ip4(a4.octets()),
            Err(_) => NameType::Dns,
        }
    } else {
        NameType::Dns
    };
    let domain: Option<&[u8]> = if ntype == NameType::Dns {
        name.find('.').map(|i| &nb[i..])
    } else {
        None
    };

    let mut matched = false;
    let mut got_san = false;
    if chk_san != 0 {
        if let Some(alt) = cert.subject_alt_names() {
            for gn in alt.iter() {
                got_san = true;
                if let Some(dns) = gn.dnsname() {
                    if ntype != NameType::Dns {
                        continue;
                    }
                    let sn = dns.as_bytes();
                    if sn.is_empty() {
                        continue;
                    }
                    if eq_nocase(nb, sn) {
                        matched = true;
                        break;
                    }
                    if let Some(d) = domain {
                        if sn.len() >= 2
                            && sn[0] == b'*'
                            && sn[1] == b'.'
                            && d.len() == sn.len() - 1
                            && eq_nocase(d, &sn[1..])
                        {
                            matched = true;
                            break;
                        }
                    }
                } else if let Some(ip) = gn.ipaddress() {
                    let hit = match ntype {
                        NameType::Dns => false,
                        NameType::Ip4(a) => ip == a,
                        NameType::Ip6(a) => ip == a,
                    };
                    if hit {
                        matched = true;
                        break;
                    }
                }
            }
        }
    }
    if matched {
        return Ok(());
    }
    if chk_san != 0 {
        match chk_san {
            LDAP_OPT_X_TLS_DEMAND | LDAP_OPT_X_TLS_HARD => {
                // no SAN at all: "unable to get subjectAltName"; a SAN that
                // does not match: "hostname does not match subjectAltName"
                return Err(LDAP_CONNECT_ERROR);
            }
            LDAP_OPT_X_TLS_TRY => {
                if got_san {
                    return Err(LDAP_CONNECT_ERROR);
                }
            }
            _ => {} // LDAP_OPT_X_TLS_ALLOW
        }
    }
    // find the last CN
    let cn = cert
        .subject_name()
        .entries_by_nid(Nid::COMMONNAME)
        .last()
        .map(|e| e.data().as_slice().to_vec());
    let Some(cn) = cn else {
        return Err(LDAP_CONNECT_ERROR); // "unable to get CN from peer certificate"
    };
    if eq_nocase(nb, &cn) {
        return Ok(());
    }
    if cn.len() >= 2 && cn[0] == b'*' && cn[1] == b'.' {
        if let Some(d) = domain {
            if d.len() == cn.len() - 1 && eq_nocase(d, &cn[1..]) {
                return Ok(());
            }
        }
    }
    Err(LDAP_CONNECT_ERROR) // "hostname does not match name in peer certificate"
}

// tls_g.c:886-987 tlsg_session_pinning: the peer's SubjectPublicKeyInfo
// (hashed with `alg` when one is named) must equal the configured pin.
// Err(-1) = an unknown digest / no certificate / export failure,
// Err(LDAP_CONNECT_ERROR) = the pin does not match.
fn session_pinning(ssl: &SslRef, alg: Option<&str>, pin: &[u8]) -> Result<(), i32> {
    let md = match alg {
        Some(name) => Some(openssl::hash::MessageDigest::from_name(name).ok_or(-1)?),
        None => None,
    };
    let cert = ssl.peer_certificate().ok_or(-1)?;
    let key = cert
        .public_key()
        .and_then(|k| k.public_key_to_der())
        .map_err(|_| -1)?;
    let keyhash: Vec<u8> = match md {
        Some(md) => openssl::hash::hash(md, &key).map_err(|_| -1)?.to_vec(),
        None => key,
    };
    if keyhash != pin {
        return Err(LDAP_CONNECT_ERROR); // "public key hash does not match provided pin"
    }
    Ok(())
}

#[cfg(test)]
mod tls_glue_tests {
    use super::*;

    #[test]
    fn gnutls_priority_strings_are_recognized_like_gnutls_priority_init() {
        let mut b = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
        assert!(apply_gnutls_priority(&mut b, "NORMAL").is_ok());
        assert!(apply_gnutls_priority(&mut b, "normal:-VERS-TLS1.0:-VERS-TLS1.1:%SERVER_PRECEDENCE").is_ok());
        assert!(apply_gnutls_priority(&mut b, "@SYSTEM").is_ok());
        assert!(apply_gnutls_priority(&mut b, "SECURE256:+VERS-TLS1.3:!AES-128-CBC").is_ok());
        // OpenSSL cipher-list grammar is not a GnuTLS priority string
        assert!(apply_gnutls_priority(&mut b, "HIGH:!aNULL").is_err());
        assert!(apply_gnutls_priority(&mut b, "DEFAULT@SECLEVEL=2").is_err());
        let opts = b.options();
        assert!(opts.contains(SslOptions::NO_TLSV1) && opts.contains(SslOptions::NO_TLSV1_1));
        assert!(!opts.contains(SslOptions::NO_TLSV1_3));
    }

    #[test]
    fn gnutls_strerror_texts() {
        assert_eq!(gnutls_strerror(-1), "(unknown error code)");
        assert_eq!(gnutls_strerror(LDAP_CONNECT_ERROR), "(unknown error code)");
        assert_eq!(gnutls_strerror(GNUTLS_E_PREMATURE_TERMINATION), "The TLS connection was non-properly terminated.");
        assert_eq!(pem_blocks(b"junk-----BEGIN X509 CRL-----\nAA\n-----END X509 CRL-----\n-----BEGIN X509 CRL-----\nBB\n-----END X509 CRL-----\n", b"-----END X509 CRL-----").len(), 2);
    }
}

#[cfg(test)]
mod ber_tests {
    use super::*;

    #[test]
    fn tlv_short_and_long_lengths() {
        assert_eq!(tlv(0x04, b"ab"), vec![0x04, 0x02, b'a', b'b']);
        let long = vec![0u8; 200];
        let enc = tlv(0x04, &long);
        assert_eq!(&enc[..3], &[0x04, 0x81, 200]);
        assert_eq!(enc.len(), 203);
    }

    #[test]
    fn int_minimal_encoding() {
        assert_eq!(ber_int(0x02, 0), vec![0x02, 0x01, 0x00]);
        assert_eq!(ber_int(0x02, 3), vec![0x02, 0x01, 0x03]);
        assert_eq!(ber_int(0x02, 128), vec![0x02, 0x02, 0x00, 0x80]);
        assert_eq!(ber_int(0x02, -1), vec![0x02, 0x01, 0xff]);
        assert_eq!(decode_int(&[0x00, 0x80]).unwrap(), 128);
        assert_eq!(decode_int(&[0xff]).unwrap(), -1);
    }

    #[test]
    fn filter_parse_and_encode() {
        assert_eq!(
            parse_search_filter("(uid=alice)").unwrap(),
            Filter::Eq("uid".into(), b"alice".to_vec())
        );
        assert_eq!(
            parse_search_filter("uid=alice").unwrap(),
            Filter::Eq("uid".into(), b"alice".to_vec())
        );
        assert_eq!(
            parse_search_filter("(|(uid=a)(mail=a))").unwrap(),
            Filter::Or(vec![
                Filter::Eq("uid".into(), b"a".to_vec()),
                Filter::Eq("mail".into(), b"a".to_vec()),
            ])
        );
        assert_eq!(
            parse_search_filter("(objectClass=*)").unwrap(),
            Filter::Present("objectClass".into())
        );
        assert_eq!(
            parse_search_filter("(cn=ab*cd*ef)").unwrap(),
            Filter::Substrings(
                "cn".into(),
                Some(b"ab".to_vec()),
                vec![b"cd".to_vec()],
                Some(b"ef".to_vec())
            )
        );
        assert_eq!(
            parse_search_filter("(uid=a\\2ab)").unwrap(),
            Filter::Eq("uid".into(), b"a*b".to_vec())
        );
        assert!(parse_search_filter("(uid=alice").is_err());
        assert!(parse_search_filter("(&)").is_err());
        assert!(parse_search_filter("").is_err());

        // Encoded equality AVA: [3] { OCTET STRING attr, OCTET STRING val }.
        let enc = encode_filter(&Filter::Eq("uid".into(), b"a".to_vec()));
        assert_eq!(
            enc,
            vec![0xa3, 0x08, 0x04, 0x03, b'u', b'i', b'd', 0x04, 0x01, b'a']
        );
    }

    // RFC 4515 extensible matches, as libldap's str2filter hands them to the
    // server: [9] { [1] rule, [2] attr, [3] value, [4] dnAttributes }.
    #[test]
    fn extensible_match_filters() {
        let f = parse_search_filter("(cn:caseIgnoreMatch:=alice)").unwrap();
        assert_eq!(
            f,
            Filter::Extensible {
                attr: Some("cn".into()),
                rule: Some("caseIgnoreMatch".into()),
                value: b"alice".to_vec(),
                dn_attrs: false,
            }
        );
        let mut want = vec![0xa9, 0x1c, 0x81, 0x0f];
        want.extend_from_slice(b"caseIgnoreMatch");
        want.extend_from_slice(&[0x82, 0x02, b'c', b'n', 0x83, 0x05]);
        want.extend_from_slice(b"alice");
        assert_eq!(encode_filter(&f), want);

        assert_eq!(
            parse_search_filter("(cn:dn:=alice)").unwrap(),
            Filter::Extensible {
                attr: Some("cn".into()),
                rule: None,
                value: b"alice".to_vec(),
                dn_attrs: true,
            }
        );
        assert_eq!(
            encode_filter(&parse_search_filter("(:dn:2.4.6.8.10:=x)").unwrap()),
            vec![
                0xa9, 0x12, 0x81, 0x0a, b'2', b'.', b'4', b'.', b'6', b'.', b'8', b'.', b'1', b'0',
                0x83, 0x01, b'x', 0x84, 0x01, 0xff
            ]
        );
        assert_eq!(
            parse_search_filter("(&(objectClass=person)(uid:=a\\2ab))").unwrap(),
            Filter::And(vec![
                Filter::Eq("objectClass".into(), b"person".to_vec()),
                Filter::Extensible {
                    attr: Some("uid".into()),
                    rule: None,
                    value: b"a*b".to_vec(),
                    dn_attrs: false,
                },
            ])
        );
        assert!(parse_search_filter("(:=x)").is_err());
        assert!(parse_search_filter("(cn::=x)").is_err());
        assert!(parse_search_filter("(cn:a:b:=x)").is_err());
    }

    #[test]
    fn ber_reader_rejects_truncation() {
        let mut r = BerReader::new(&[0x30, 0x05, 0x02, 0x01]);
        assert!(r.read_tlv().is_err());
        let mut r = BerReader::new(&[0x30, 0x84]);
        assert!(r.read_tlv().is_err());
    }
}
