//! OpenLDAP libldap dnssrv.c (2.5.13, the libldap Debian's PGDG 18.6 links):
//! `ldap_dn2domain` and `ldap_domain2hostlist` — the DNS SRV discovery
//! auth.c's InitializeLDAPConnection (auth.c:2255-2290) runs when
//! pg_hba.conf names no ldapserver — over an in-tree `res_query`: the
//! libresolv subset glibc runs for one T_SRV query (the /etc/resolv.conf
//! nameservers, RES_OPTIONS, the retrans/retry schedule, a TC reply
//! retried over TCP; res_query itself never applies the search list).
//!
//! Result codes use libldap's convention (ldap_err2string in hba):
//! LDAP_PARAM_ERROR for an empty domain, LDAP_UNAVAILABLE when no server
//! is found (dnssrv.c:394-398). auth.c only tests for nonzero.

#![cfg(not(target_family = "wasm"))]

use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, UdpSocket};
use std::time::Duration;

pub const LDAP_PARAM_ERROR: i32 = -9;
pub const LDAP_UNAVAILABLE: i32 = 52;

// ---------- ldap_str2dn (LDAP_DN_FORMAT_LDAP subset) ----------

// One attribute-value assertion of an RDN. `value` is None for a non-string
// value (the `#hex` BER form): C's LDAPAVA without LDAP_AVA_STRING.
struct Ava {
    attr: String,
    value: Option<String>,
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn is_attr_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_'
}

// RFC 4514 string DN -> RDN list, the ldap_str2dn(LDAP_DN_FORMAT_LDAP)
// tolerances auth.c's callers can meet: ',' or ';' between RDNs, '+'
// between AVAs, spaces around separators, `\xx` / `\c` escapes, LDAPv2
// quoted values, `#hex` non-string values. Err = a parse failure
// (ldap_dn2domain returns -2: auth.c's "could not extract domain name").
fn str2dn(s: &str) -> Result<Vec<Vec<Ava>>, ()> {
    let b = s.as_bytes();
    let n = b.len();
    let mut pos = 0;
    let mut dn: Vec<Vec<Ava>> = Vec::new();
    while pos < n && b[pos] == b' ' {
        pos += 1;
    }
    if pos == n {
        return Ok(dn); // empty DN: C's dn == NULL
    }
    loop {
        let mut rdn: Vec<Ava> = Vec::new();
        loop {
            while pos < n && b[pos] == b' ' {
                pos += 1;
            }
            let start = pos;
            while pos < n && is_attr_char(b[pos]) {
                pos += 1;
            }
            if start == pos || !(b[start].is_ascii_alphanumeric()) {
                return Err(());
            }
            let attr = String::from_utf8_lossy(&b[start..pos]).into_owned();
            while pos < n && b[pos] == b' ' {
                pos += 1;
            }
            if pos >= n || b[pos] != b'=' {
                return Err(());
            }
            pos += 1;
            while pos < n && b[pos] == b' ' {
                pos += 1;
            }
            let value: Option<String>;
            if pos < n && b[pos] == b'#' {
                pos += 1;
                let vs = pos;
                while pos < n && hex_val(b[pos]).is_some() {
                    pos += 1;
                }
                if pos == vs || (pos - vs) % 2 != 0 {
                    return Err(());
                }
                value = None;
            } else if pos < n && b[pos] == b'"' {
                pos += 1;
                let mut out: Vec<u8> = Vec::new();
                loop {
                    if pos >= n {
                        return Err(()); // unterminated quote
                    }
                    match b[pos] {
                        b'"' => {
                            pos += 1;
                            break;
                        }
                        b'\\' => {
                            pos += 1;
                            if pos >= n {
                                return Err(());
                            }
                            if let (Some(h), Some(l)) = (
                                hex_val(b[pos]),
                                b.get(pos + 1).copied().and_then(hex_val),
                            ) {
                                out.push((h << 4) | l);
                                pos += 2;
                            } else {
                                out.push(b[pos]);
                                pos += 1;
                            }
                        }
                        c => {
                            out.push(c);
                            pos += 1;
                        }
                    }
                }
                value = Some(String::from_utf8_lossy(&out).into_owned());
            } else {
                let mut out: Vec<u8> = Vec::new();
                let mut trailing_spaces = 0usize;
                while pos < n {
                    match b[pos] {
                        b',' | b';' | b'+' => break,
                        b'\\' => {
                            pos += 1;
                            if pos >= n {
                                return Err(());
                            }
                            if let (Some(h), Some(l)) = (
                                hex_val(b[pos]),
                                b.get(pos + 1).copied().and_then(hex_val),
                            ) {
                                out.push((h << 4) | l);
                                pos += 2;
                            } else {
                                out.push(b[pos]);
                                pos += 1;
                            }
                            trailing_spaces = 0;
                        }
                        b' ' => {
                            out.push(b' ');
                            trailing_spaces += 1;
                            pos += 1;
                        }
                        c => {
                            out.push(c);
                            trailing_spaces = 0;
                            pos += 1;
                        }
                    }
                }
                out.truncate(out.len() - trailing_spaces);
                value = Some(String::from_utf8_lossy(&out).into_owned());
            }
            while pos < n && b[pos] == b' ' {
                pos += 1;
            }
            rdn.push(Ava { attr, value });
            if pos < n && b[pos] == b'+' {
                pos += 1;
                continue;
            }
            break;
        }
        dn.push(rdn);
        if pos >= n {
            break;
        }
        if b[pos] == b',' || b[pos] == b';' {
            pos += 1;
            let mut look = pos;
            while look < n && b[look] == b' ' {
                look += 1;
            }
            if look >= n {
                return Err(()); // trailing separator
            }
            continue;
        }
        return Err(());
    }
    Ok(dn)
}

/// ldap_dn2domain (dnssrv.c:40-118): the domain named by the base DN's
/// trailing run of single-valued DC components — "ou=blah,dc=foo,dc=bar"
/// -> "foo.bar"; any other RDN (or a multi-valued / non-string / empty DC)
/// resets what came before it. Ok(None) = C's NULL domain (no DC run at
/// the end); Err = ldap_str2dn failed (C returns -2).
pub fn ldap_dn2domain(dn_in: &str) -> Result<Option<String>, ()> {
    const DCOID: &str = "0.9.2342.19200300.100.1.25";
    let dn = str2dn(dn_in)?;
    let mut domain = String::new();
    for rdn in &dn {
        for (j, ava) in rdn.iter().enumerate() {
            let last = j + 1 == rdn.len();
            let is_dc = ava.attr.eq_ignore_ascii_case("DC") || ava.attr == DCOID;
            match &ava.value {
                Some(v) if last && !v.is_empty() && is_dc => {
                    if domain.is_empty() {
                        domain.push_str(v);
                    } else {
                        domain.push('.');
                        domain.push_str(v);
                    }
                }
                _ => domain.clear(),
            }
        }
    }
    Ok(if domain.is_empty() { None } else { Some(domain) })
}

// ---------- res_query: the glibc resolver subset for one query ----------

const RES_TIMEOUT: u32 = 5; // resolv.h RES_TIMEOUT (seconds, first try)
const RES_DFLRETRY: u32 = 2; // resolv.h RES_DFLRETRY
const RES_MAXRETRANS: u32 = 30;
const RES_MAXRETRY: u32 = 5;
const MAXNS: usize = 3;
const NS_DEFAULTPORT: u16 = 53;
const NS_MAXDNAME: usize = 1025;
const T_SRV: u16 = 33;
const C_IN: u16 = 1;

/// The resolver configuration `res_query` runs with (res_init's reading of
/// /etc/resolv.conf + RES_OPTIONS).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvConf {
    pub nameservers: Vec<SocketAddr>,
    pub retrans: u32,
    pub retry: u32,
    pub rotate: bool,
}

impl Default for ResolvConf {
    fn default() -> Self {
        ResolvConf {
            nameservers: Vec::new(),
            retrans: RES_TIMEOUT,
            retry: RES_DFLRETRY,
            rotate: false,
        }
    }
}

impl ResolvConf {
    // res_setoptions: "timeout:n attempts:n rotate ..." (RES_OPTIONS and the
    // `options` line share this parser; unknown words are ignored).
    fn set_options(&mut self, opts: &str) {
        for word in opts.split_whitespace() {
            if let Some(v) = word.strip_prefix("timeout:") {
                if let Ok(t) = v.parse::<u32>() {
                    self.retrans = t.min(RES_MAXRETRANS);
                }
            } else if let Some(v) = word.strip_prefix("attempts:") {
                if let Ok(a) = v.parse::<u32>() {
                    self.retry = a.min(RES_MAXRETRY);
                }
            } else if word == "rotate" {
                self.rotate = true;
            }
        }
    }

    /// res_init over resolv.conf text: `nameserver` (up to MAXNS, port 53,
    /// an IPv6 `%scope` suffix ignored), `options`; everything else is not
    /// consulted by res_query.
    pub fn parse(text: &str) -> ResolvConf {
        let mut conf = ResolvConf::default();
        for line in text.lines() {
            if line.starts_with(';') || line.starts_with('#') {
                continue;
            }
            let mut words = line.split_whitespace();
            match words.next() {
                Some("nameserver") => {
                    if conf.nameservers.len() >= MAXNS {
                        continue;
                    }
                    if let Some(addr) = words.next() {
                        let bare = addr.split('%').next().unwrap_or("");
                        if let Ok(ip) = bare.parse::<IpAddr>() {
                            conf.nameservers.push(SocketAddr::new(ip, NS_DEFAULTPORT));
                        }
                    }
                }
                Some("options") => {
                    let rest = line.trim_start().trim_start_matches("options");
                    conf.set_options(rest);
                }
                _ => {}
            }
        }
        conf
    }

    /// The process resolver configuration, as res_query reads it:
    /// /etc/resolv.conf (an unreadable file = no nameserver lines), the
    /// loopback default when none is listed, then RES_OPTIONS.
    pub fn system() -> ResolvConf {
        if let Some(ns) = test_nameservers() {
            let mut conf = ResolvConf::default();
            conf.nameservers = ns;
            return conf;
        }
        let text = std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default();
        let mut conf = ResolvConf::parse(&text);
        if conf.nameservers.is_empty() {
            conf.nameservers
                .push(SocketAddr::new(IpAddr::from([127, 0, 0, 1]), NS_DEFAULTPORT));
        }
        if let Some(opts) = crate::ldapconf::getenv("RES_OPTIONS") {
            conf.set_options(&opts);
        }
        conf
    }
}

#[cfg(test)]
fn test_nameservers() -> Option<Vec<SocketAddr>> {
    pgsync::lock(&crate::ldap::SRV_NAMESERVERS).clone()
}
#[cfg(not(test))]
fn test_nameservers() -> Option<Vec<SocketAddr>> {
    None
}

fn random_u16() -> u16 {
    let mut b = [0u8; 2];
    if !pg_strong_random::pg_strong_random(&mut b) {
        b = [0x4c, 0x44]; // deterministic fallback; the reply is matched by question too
    }
    u16::from_be_bytes(b)
}

// res_nmkquery(QUERY, name, C_IN, type): the wire query, or None when the
// name cannot be encoded (empty label, label > 63, name > 255 octets).
fn mkquery(name: &str, qtype: u16) -> Option<Vec<u8>> {
    let mut q = Vec::with_capacity(12 + name.len() + 6);
    q.extend_from_slice(&random_u16().to_be_bytes());
    q.extend_from_slice(&0x0100u16.to_be_bytes()); // RD
    q.extend_from_slice(&1u16.to_be_bytes());
    q.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    let name = name.strip_suffix('.').unwrap_or(name);
    let mut encoded = Vec::with_capacity(name.len() + 2);
    if !name.is_empty() {
        for label in name.split('.') {
            if label.is_empty() || label.len() > 63 {
                return None;
            }
            encoded.push(label.len() as u8);
            encoded.extend_from_slice(label.as_bytes());
        }
    }
    encoded.push(0);
    if encoded.len() > 255 {
        return None;
    }
    q.extend_from_slice(&encoded);
    q.extend_from_slice(&qtype.to_be_bytes());
    q.extend_from_slice(&C_IN.to_be_bytes());
    Some(q)
}

fn be16(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_be_bytes([*b.get(at)?, *b.get(at + 1)?]))
}

/// dn_expand: decompress the name at `pos`; returns (name, octets consumed
/// at `pos`). Root is "." (ns_name_ntop); other names carry no trailing dot.
fn dn_expand(msg: &[u8], pos: usize) -> Result<(String, usize), ()> {
    let mut labels: Vec<String> = Vec::new();
    let mut p = pos;
    let mut consumed: Option<usize> = None;
    let mut hops = 0;
    let mut total = 0usize;
    loop {
        let len = *msg.get(p).ok_or(())? as usize;
        if len == 0 {
            p += 1;
            break;
        }
        if len & 0xc0 == 0xc0 {
            let target = (((len & 0x3f) << 8) | *msg.get(p + 1).ok_or(())? as usize) as usize;
            if consumed.is_none() {
                consumed = Some(p + 2 - pos);
            }
            hops += 1;
            if hops > 128 || target >= msg.len() {
                return Err(());
            }
            p = target;
            continue;
        }
        if len > 63 {
            return Err(());
        }
        let label = msg.get(p + 1..p + 1 + len).ok_or(())?;
        total += len + 1;
        if total > NS_MAXDNAME {
            return Err(());
        }
        labels.push(String::from_utf8_lossy(label).into_owned());
        p += 1 + len;
    }
    let consumed = consumed.unwrap_or_else(|| p - pos);
    let name = if labels.is_empty() {
        ".".to_string()
    } else {
        labels.join(".")
    };
    Ok((name, consumed))
}

// Does `reply` answer `query` (res_queriesmatch: same id, same question)?
fn reply_matches(query: &[u8], reply: &[u8]) -> bool {
    if reply.len() < 12 || query.len() < 12 || reply[0..2] != query[0..2] {
        return false;
    }
    if reply[2] & 0x80 == 0 {
        return false; // not a response
    }
    let (Ok((qn, qc)), Ok((rn, rc))) = (dn_expand(query, 12), dn_expand(reply, 12)) else {
        return false;
    };
    if !qn.eq_ignore_ascii_case(&rn) {
        return false;
    }
    query.get(12 + qc..12 + qc + 4) == reply.get(12 + rc..12 + rc + 4)
}

enum Dg {
    Answer(Vec<u8>),
    Timeout,
    Error,
}

// send_dg: one UDP exchange with `ns`, waiting up to `seconds` for a reply
// that answers our query (foreign replies are ignored while waiting).
fn send_dg(ns: SocketAddr, query: &[u8], seconds: u32) -> Dg {
    let bind: SocketAddr = if ns.is_ipv4() {
        "0.0.0.0:0".parse().expect("literal")
    } else {
        "[::]:0".parse().expect("literal")
    };
    let Ok(sock) = UdpSocket::bind(bind) else { return Dg::Error };
    if sock.connect(ns).is_err() || sock.send(query).is_err() {
        return Dg::Error;
    }
    if sock
        .set_read_timeout(Some(Duration::from_secs(seconds as u64)))
        .is_err()
    {
        return Dg::Error;
    }
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match sock.recv(&mut buf) {
            Ok(n) => {
                if reply_matches(query, &buf[..n]) {
                    return Dg::Answer(buf[..n].to_vec());
                }
                // an old or foreign answer: keep waiting
            }
            Err(e) => {
                return match e.kind() {
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => Dg::Timeout,
                    _ => Dg::Error,
                };
            }
        }
    }
}

// send_vc: the same query over TCP (the TC retry).
fn send_vc(ns: SocketAddr, query: &[u8], seconds: u32) -> Option<Vec<u8>> {
    let tmo = Duration::from_secs(seconds as u64);
    let mut s = TcpStream::connect_timeout(&ns, tmo).ok()?;
    s.set_read_timeout(Some(tmo)).ok()?;
    s.set_write_timeout(Some(tmo)).ok()?;
    s.write_all(&(query.len() as u16).to_be_bytes()).ok()?;
    s.write_all(query).ok()?;
    let mut lb = [0u8; 2];
    s.read_exact(&mut lb).ok()?;
    let mut reply = vec![0u8; u16::from_be_bytes(lb) as usize];
    s.read_exact(&mut reply).ok()?;
    if reply_matches(query, &reply) {
        Some(reply)
    } else {
        None
    }
}

// res_send: the retry/nameserver schedule of glibc's __res_context_send.
fn res_send(conf: &ResolvConf, query: &[u8]) -> Option<Vec<u8>> {
    let nscount = conf.nameservers.len();
    if nscount == 0 {
        return None;
    }
    let start = if conf.rotate {
        random_u16() as usize % nscount
    } else {
        0
    };
    for attempt in 0..conf.retry {
        for k in 0..nscount {
            let ns = conf.nameservers[(start + k) % nscount];
            let mut seconds = conf.retrans << attempt;
            if attempt > 0 {
                seconds /= nscount as u32;
            }
            if seconds == 0 {
                seconds = 1;
            }
            let reply = match send_dg(ns, query, seconds) {
                Dg::Answer(r) => r,
                Dg::Timeout | Dg::Error => continue,
            };
            let reply = if reply[2] & 0x02 != 0 {
                // TC: retry the same server over TCP
                match send_vc(ns, query, seconds) {
                    Some(r) => r,
                    None => continue,
                }
            } else {
                reply
            };
            match reply[3] & 0x0f {
                2 | 4 | 5 => continue, // SERVFAIL, NOTIMP, REFUSED: next server
                _ => return Some(reply),
            }
        }
    }
    None
}

/// res_query(name, C_IN, qtype): the full reply, or None (C's -1) when the
/// query could not be sent, no server answered, the rcode is not NOERROR,
/// or the answer section is empty.
pub fn res_query(conf: &ResolvConf, name: &str, qtype: u16) -> Option<Vec<u8>> {
    let query = mkquery(name, qtype)?;
    let reply = res_send(conf, &query)?;
    if reply[3] & 0x0f != 0 || be16(&reply, 6)? == 0 {
        return None;
    }
    Some(reply)
}

// ---------- ldap_domain2hostlist ----------

#[derive(Clone, Debug, PartialEq, Eq)]
struct SrvRecord {
    priority: u16,
    weight: u16,
    port: u16,
    hostname: String,
}

// dnssrv.c:216-233 — the LCG libldap keeps for the RFC 2782 shuffle
// ("we don't want to interfere with anyone else's use of srand()"), in
// C's float arithmetic. Seeded once per process; C seeds from time(0),
// this port from the sanctioned entropy source (the seed is opaque either
// way).
static SRV_SEED: pgsync::Mutex<f32> = pgsync::Mutex::new(0.0);
const C_RAND_MAX: i32 = 2147483647;

fn srv_srand(seed: &mut f32, value: i32) {
    *seed = (value as f32) / (C_RAND_MAX as f32);
}

fn srv_rand(seed: &mut f32) -> f32 {
    let val: f32 = (9821.0f64 * (*seed as f64) + 0.211327f64) as f32;
    *seed = val - ((val as i32) as f32);
    *seed
}

// dnssrv.c:243-270 — RFC 2782 page 4: repeatedly pick the next record by
// weight (uniformly once the remaining weights are all zero).
fn srv_shuffle(seed: &mut f32, a: &mut [SrvRecord]) {
    let n = a.len();
    let mut total: i32 = a.iter().map(|r| r.weight as i32).sum();
    let mut base = 0usize;
    let mut p = n;
    while p > 1 {
        let mut j: usize;
        if total == 0 {
            j = (srv_rand(seed) * p as f32) as i32 as usize;
        } else {
            let mut r = (srv_rand(seed) * total as f32) as i32;
            j = 0;
            while j < p {
                r -= a[base + j].weight as i32;
                if r < 0 {
                    total -= a[base + j].weight as i32;
                    break;
                }
                j += 1;
            }
        }
        if j != 0 && j < p {
            a.swap(base, base + j);
        }
        base += 1;
        p -= 1;
    }
}

fn parse_srv_reply(reply: &[u8]) -> Result<Vec<SrvRecord>, ()> {
    let mut recs = Vec::new();
    let (_, n) = dn_expand(reply, 12)?;
    let mut p = 12 + n + 4;
    while p < reply.len() {
        let (_, n) = dn_expand(reply, p)?;
        p += n;
        let rtype = be16(reply, p).ok_or(())?;
        let size = be16(reply, p + 8).ok_or(())? as usize;
        p += 10;
        if rtype == T_SRV {
            let (host, _) = dn_expand(reply, p + 6)?;
            let priority = be16(reply, p).ok_or(())?;
            let weight = be16(reply, p + 2).ok_or(())?;
            let port = be16(reply, p + 4).ok_or(())?;
            if port != 0 && !host.is_empty() {
                let mut hostname = host;
                hostname.truncate(253); // MAXHOST - 1
                recs.push(SrvRecord {
                    priority,
                    weight,
                    port,
                    hostname,
                });
            }
        }
        p += size;
    }
    Ok(recs)
}

// The sort + per-priority shuffle of dnssrv.c:371-383 on parsed records.
fn order_srv_records(seed: &mut f32, recs: &mut [SrvRecord]) {
    recs.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| b.weight.cmp(&a.weight))
    });
    if *seed == 0.0 {
        let mut b = [0u8; 4];
        let value = if pg_strong_random::pg_strong_random(&mut b) {
            (i32::from_be_bytes(b) & C_RAND_MAX).max(1)
        } else {
            1
        };
        srv_srand(seed, value);
    }
    let mut j = 0usize;
    let mut priority = recs[0].priority;
    let mut i = 1usize;
    while i < recs.len() {
        if recs[i].priority != priority {
            priority = recs[i].priority;
            if i - j > 1 {
                srv_shuffle(seed, &mut recs[j..i]);
            }
            j = i;
        }
        i += 1;
    }
    if i - j > 1 {
        srv_shuffle(seed, &mut recs[j..i]);
    }
}

/// ldap_domain2hostlist (dnssrv.c:276-421): the `_ldap._tcp.<domain>` SRV
/// targets as (host, port) pairs in libldap's order — ascending priority,
/// the RFC 2782 weighted shuffle within a priority. Records with port 0 or
/// an empty target are dropped. Err(LDAP_PARAM_ERROR) for an empty domain,
/// Err(LDAP_UNAVAILABLE) when the lookup fails or names no server.
pub fn ldap_domain2hostlist(domain: &str, conf: &ResolvConf) -> Result<Vec<(String, u16)>, i32> {
    if domain.is_empty() {
        return Err(LDAP_PARAM_ERROR);
    }
    let request = format!("_ldap._tcp.{domain}");
    let reply = res_query(conf, &request, T_SRV).ok_or(LDAP_UNAVAILABLE)?;
    let mut recs = parse_srv_reply(&reply).map_err(|()| LDAP_UNAVAILABLE)?;
    if recs.is_empty() {
        return Err(LDAP_UNAVAILABLE);
    }
    {
        let mut seed = pgsync::lock(&SRV_SEED);
        order_srv_records(&mut seed, &mut recs);
    }
    Ok(recs.into_iter().map(|r| (r.hostname, r.port)).collect())
}

#[cfg(test)]
mod dnssrv_tests {
    use super::*;
    use crate::tests::ldap_fakes::{FakeDns, SrvRec};

    #[test]
    fn dn2domain_keeps_only_the_trailing_dc_run() {
        assert_eq!(ldap_dn2domain("ou=blah,dc=foo,dc=bar").unwrap().as_deref(), Some("foo.bar"));
        // an RDN after the DC run resets it (dnssrv.c:100-102)
        assert_eq!(ldap_dn2domain("dc=a,ou=x,dc=b,dc=c").unwrap().as_deref(), Some("b.c"));
        assert_eq!(ldap_dn2domain("dc=a,dc=b,ou=x").unwrap(), None);
        assert_eq!(ldap_dn2domain("ou=people,ou=x").unwrap(), None);
        assert_eq!(ldap_dn2domain("").unwrap(), None);
        // case-insensitive attribute, the DC OID, ';' and spaces
        assert_eq!(ldap_dn2domain("DC=Foo; 0.9.2342.19200300.100.1.25 = bar").unwrap().as_deref(), Some("Foo.bar"));
        // multi-valued RDN: only its last AVA counts, the others reset
        assert_eq!(ldap_dn2domain("dc=a,ou=x+dc=b").unwrap().as_deref(), Some("b"));
        assert_eq!(ldap_dn2domain("dc=a,dc=b+ou=x").unwrap(), None);
        // non-string (#hex) and empty values are not domain components
        assert_eq!(ldap_dn2domain("dc=#0403666f6f").unwrap(), None);
        assert_eq!(ldap_dn2domain("dc=").unwrap(), None);
        // escapes and quoting
        assert_eq!(ldap_dn2domain("dc=ex\\2dample,dc=\"com\"").unwrap().as_deref(), Some("ex-ample.com"));
        // parse failures: C's "could not extract domain name from ldapbasedn"
        assert!(ldap_dn2domain("garbage").is_err());
        assert!(ldap_dn2domain("dc=a,").is_err());
        assert!(ldap_dn2domain("=a").is_err());
        assert!(ldap_dn2domain("dc=\"unterminated").is_err());
    }

    #[test]
    fn resolv_conf_parse_matches_res_init() {
        let c = ResolvConf::parse(
            "# comment\nnameserver 10.0.0.1\n; also a comment\nsearch example.org\nnameserver fe80::1%eth0\noptions ndots:2 timeout:99 attempts:9 rotate\nnameserver 10.0.0.3\nnameserver 10.0.0.4\n",
        );
        assert_eq!(
            c.nameservers,
            vec![
                "10.0.0.1:53".parse::<SocketAddr>().unwrap(),
                "[fe80::1]:53".parse::<SocketAddr>().unwrap(),
                "10.0.0.3:53".parse::<SocketAddr>().unwrap(),
            ]
        );
        assert_eq!((c.retrans, c.retry, c.rotate), (RES_MAXRETRANS, RES_MAXRETRY, true));
        let d = ResolvConf::parse("");
        assert_eq!((d.retrans, d.retry, d.rotate), (RES_TIMEOUT, RES_DFLRETRY, false));
        assert!(d.nameservers.is_empty());
    }

    #[test]
    fn srv_records_order_by_priority_then_weighted_shuffle() {
        let mut seed = 0.0f32;
        srv_srand(&mut seed, 12345);
        let mut recs = vec![
            SrvRecord { priority: 20, weight: 0, port: 1, hostname: "d".into() },
            SrvRecord { priority: 10, weight: 5, port: 1, hostname: "b".into() },
            SrvRecord { priority: 0, weight: 0, port: 1, hostname: "a".into() },
            SrvRecord { priority: 10, weight: 100, port: 1, hostname: "c".into() },
        ];
        order_srv_records(&mut seed, &mut recs);
        let names: Vec<&str> = recs.iter().map(|r| r.hostname.as_str()).collect();
        assert_eq!(names[0], "a");
        assert_eq!(names[3], "d");
        assert!(names[1..3].contains(&"b") && names[1..3].contains(&"c"));
        // the LCG itself (dnssrv.c:226-229) is deterministic for a seed
        let mut s1 = 0.0f32;
        srv_srand(&mut s1, 7);
        let mut s2 = 0.0f32;
        srv_srand(&mut s2, 7);
        assert_eq!(srv_rand(&mut s1), srv_rand(&mut s2));
        assert!((0.0..1.0).contains(&srv_rand(&mut s1)));
    }

    // Live lookup against the in-process fake: a truncated UDP answer is
    // retried over TCP; records are returned by priority; port-0 records
    // are dropped; NXDOMAIN / no records is LDAP_UNAVAILABLE.
    #[test]
    fn domain2hostlist_over_udp_tc_then_tcp() {
        let dns = FakeDns::start(
            vec![
                SrvRec { priority: 5, weight: 0, port: 3890, target: "second.example.test" },
                SrvRec { priority: 0, weight: 0, port: 389, target: "first.example.test" },
                SrvRec { priority: 1, weight: 0, port: 0, target: "dropped.example.test" },
            ],
            true,
        );
        let conf = ResolvConf { nameservers: vec![dns.addr], retrans: 2, retry: 1, rotate: false };
        assert_eq!(
            ldap_domain2hostlist("example.test", &conf).unwrap(),
            vec![("first.example.test".to_string(), 389), ("second.example.test".to_string(), 3890)]
        );
        let empty = FakeDns::start(vec![], false);
        let conf = ResolvConf { nameservers: vec![empty.addr], retrans: 2, retry: 1, rotate: false };
        assert_eq!(ldap_domain2hostlist("example.test", &conf), Err(LDAP_UNAVAILABLE));
        assert_eq!(ldap_domain2hostlist("", &conf), Err(LDAP_PARAM_ERROR));
    }
}
