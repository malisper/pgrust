//! Minimal LDAPv3 client (RFC 4511/4515 subset) speaking BER over TCP —
//! the in-tree replacement for the libldap calls auth.c makes: simple bind,
//! search returning entry DNs, unbind. No TLS, SASL, referral chasing,
//! aliases, or controls. Result codes use libldap's convention: server
//! result codes >= 0, client-side codes < 0 (ldap_err2string in hba).

#![cfg(not(target_family = "wasm"))]

use std::io::{Read, Write};
use std::net::TcpStream;

pub const LDAP_SUCCESS: i32 = 0;
pub const LDAP_SERVER_DOWN: i32 = -1;
pub const LDAP_DECODING_ERROR: i32 = -4;
pub const LDAP_FILTER_ERROR: i32 = -7;

const LDAP_VERSION3: i64 = 3;

// Protocol op tags.
const TAG_BIND_REQUEST: u8 = 0x60;
const TAG_BIND_RESPONSE: u8 = 0x61;
const TAG_UNBIND_REQUEST: u8 = 0x42;
const TAG_SEARCH_REQUEST: u8 = 0x63;
const TAG_SEARCH_ENTRY: u8 = 0x64;
const TAG_SEARCH_DONE: u8 = 0x65;
const TAG_SEARCH_REFERENCE: u8 = 0x73;

const TAG_SEQUENCE: u8 = 0x30;
const TAG_INTEGER: u8 = 0x02;
const TAG_ENUMERATED: u8 = 0x0a;
const TAG_OCTET_STRING: u8 = 0x04;
const TAG_BOOLEAN: u8 = 0x01;

// ---------- BER encoding ----------

fn put_len(out: &mut Vec<u8>, len: usize) {
    if len < 0x80 {
        out.push(len as u8);
    } else {
        let be = (len as u64).to_be_bytes();
        let first = be.iter().position(|&b| b != 0).unwrap_or(7);
        out.push(0x80 | (8 - first) as u8);
        out.extend_from_slice(&be[first..]);
    }
}

fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(content.len() + 6);
    out.push(tag);
    put_len(&mut out, content.len());
    out.extend_from_slice(content);
    out
}

fn ber_int(tag: u8, v: i64) -> Vec<u8> {
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

struct BerReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BerReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn read_tlv(&mut self) -> Result<(u8, &'a [u8]), ()> {
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

fn decode_int(content: &[u8]) -> Result<i64, ()> {
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

pub struct LdapConn {
    hosts: Vec<(String, i32)>,
    stream: Option<TcpStream>,
    msgid: i64,
    diag: Option<String>,
}

impl LdapConn {
    pub fn new(hosts: Vec<(String, i32)>) -> Self {
        Self {
            hosts,
            stream: None,
            msgid: 0,
            diag: None,
        }
    }

    /// ldap_get_option(LDAP_OPT_DIAGNOSTIC_MESSAGE).
    pub fn diagnostic_message(&self) -> Option<&str> {
        self.diag.as_deref()
    }

    fn ensure_connected(&mut self) -> Result<(), i32> {
        if self.stream.is_some() {
            return Ok(());
        }
        for (host, port) in &self.hosts {
            let port = u16::try_from(*port).unwrap_or(0);
            if let Ok(s) = TcpStream::connect((host.as_str(), port)) {
                let _ = s.set_nodelay(true);
                self.stream = Some(s);
                return Ok(());
            }
        }
        self.diag = None;
        Err(LDAP_SERVER_DOWN)
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
        if stream.read_exact(&mut header).is_err() {
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
            if stream.read_exact(&mut lenbuf).is_err() {
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
        if stream.read_exact(&mut msg[at..]).is_err() {
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

    /// ldap_search_s with attrsonly=0; returns the entry DNs.
    pub fn search(
        &mut self,
        base: &str,
        scope: i32,
        filter: &Filter,
        attrs: &[&str],
    ) -> Result<Vec<String>, i32> {
        let mut body = tlv(TAG_OCTET_STRING, base.as_bytes());
        body.extend_from_slice(&ber_int(TAG_ENUMERATED, scope as i64));
        body.extend_from_slice(&ber_int(TAG_ENUMERATED, 0)); // neverDerefAliases
        body.extend_from_slice(&ber_int(TAG_INTEGER, 0)); // sizeLimit
        body.extend_from_slice(&ber_int(TAG_INTEGER, 0)); // timeLimit
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

    #[test]
    fn ber_reader_rejects_truncation() {
        let mut r = BerReader::new(&[0x30, 0x05, 0x02, 0x01]);
        assert!(r.read_tlv().is_err());
        let mut r = BerReader::new(&[0x30, 0x84]);
        assert!(r.read_tlv().is_err());
    }
}
