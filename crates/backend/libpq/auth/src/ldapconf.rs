//! OpenLDAP libldap global options (2.5.13 init.c `ldap_int_initialize`):
//! the ldap.conf / ldaprc / `LDAP<ATTR>` environment surface, read the way
//! every C backend reads it at its first libldap call — the system file
//! (Debian's `/etc/ldap/ldap.conf`, the SYSCONFDIR of the libldap PGDG's
//! 18.6 links), the user files `~/ldaprc`, `~/.ldaprc`, `./ldaprc`, then
//! `$LDAPCONF` (system semantics), `$LDAPRC` (user semantics), then the
//! environment; `LDAPNOINIT` skips all of it (init.c:717-778).
//!
//! The rows kept are the ones that shape the operations auth.c performs:
//! the TLS_* table of a GnuTLS build (Debian's libldap: init.c:118-141
//! under HAVE_GNUTLS — TLS_CRLFILE, no TLS_CRLCHECK) as tls2.c
//! `ldap_pvt_tls_config` parses it, the NETWORK_TIMEOUT / TIMEOUT waits,
//! DEREF / SIZELIMIT / TIMELIMIT of the search request. URI/HOST/BASE/
//! BINDDN/REFERRALS/SASL_*/KEEPALIVE_* and the TLS rows tls_g.c never
//! consumes (TLS_RANDFILE, TLS_PROTOCOL_MIN/MAX, TLS_ECNAME) are read past
//! like C reads them.

#![cfg(not(target_family = "wasm"))]

use std::time::Duration;

pub const LDAP_OPT_X_TLS_NEVER: i32 = 0;
pub const LDAP_OPT_X_TLS_HARD: i32 = 1;
pub const LDAP_OPT_X_TLS_DEMAND: i32 = 2;
pub const LDAP_OPT_X_TLS_ALLOW: i32 = 3;
pub const LDAP_OPT_X_TLS_TRY: i32 = 4;

pub const LDAP_DEREF_NEVER: i32 = 0;
pub const LDAP_DEREF_SEARCHING: i32 = 1;
pub const LDAP_DEREF_FINDING: i32 = 2;
pub const LDAP_DEREF_ALWAYS: i32 = 3;

const LDAP_CONF_FILE: &str = "/etc/ldap/ldap.conf";
const LDAP_USERRC_FILE: &str = "ldaprc";
const LDAP_ENV_PREFIX: &str = "LDAP";

/// `struct ldapoptions`, the subset this client consumes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LdapOptions {
    pub tls_cacertfile: Option<String>,
    pub tls_cacertdir: Option<String>,
    pub tls_certfile: Option<String>,
    pub tls_keyfile: Option<String>,
    pub tls_require_cert: i32,
    pub tls_require_san: i32,
    /// TLS_CIPHER_SUITE: a GnuTLS priority string (tlsg_parse_ciphers).
    pub tls_ciphersuite: Option<String>,
    /// TLS_CRLFILE (GnuTLS only): PEM CRLs for the peer chain.
    pub tls_crlfile: Option<String>,
    /// TLS_PEERKEY_HASH: (hash algorithm name, decoded pin); None = unset.
    pub tls_pin: Option<(Option<String>, Vec<u8>)>,
    /// NETWORK_TIMEOUT (ldo_tm_net): connect + TLS handshake bound.
    pub tm_net: Option<Duration>,
    /// TIMEOUT (ldo_tm_api): the *_s result wait bound.
    pub tm_api: Option<Duration>,
    pub deref: i32,
    pub sizelimit: i32,
    pub timelimit: i32,
}

impl Default for LdapOptions {
    // ldap_int_initialize_global_options (init.c:560-640).
    fn default() -> Self {
        LdapOptions {
            tls_cacertfile: None,
            tls_cacertdir: None,
            tls_certfile: None,
            tls_keyfile: None,
            tls_require_cert: LDAP_OPT_X_TLS_DEMAND,
            tls_require_san: LDAP_OPT_X_TLS_ALLOW,
            tls_ciphersuite: None,
            tls_crlfile: None,
            tls_pin: None,
            tm_net: None,
            tm_api: None,
            deref: LDAP_DEREF_NEVER,
            sizelimit: 0,
            timelimit: 0,
        }
    }
}

/// getenv(3) for the LDAP* / RES_OPTIONS reads of this client (one site).
pub(crate) fn getenv(name: &str) -> Option<String> {
    std::env::var_os(name).map(|v| v.to_string_lossy().into_owned())
}

// strtol(s, &next, 10): (value, bytes consumed). consumed == 0 is C's
// `next == opt` (no conversion).
fn c_strtol(s: &str) -> (i64, usize) {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && (b[i] == b' ' || (b'\t'..=b'\r').contains(&b[i])) {
        i += 1;
    }
    let mut neg = false;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        neg = b[i] == b'-';
        i += 1;
    }
    let digits = i;
    let mut v: i64 = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        v = v.saturating_mul(10).saturating_add((b[i] - b'0') as i64);
        i += 1;
    }
    if i == digits {
        return (0, 0);
    }
    (if neg { -v } else { v }, i)
}

// The whole string is one number (init.c ATTR_INT / ATTR_OPT_*:
// `next != opt && next[0] == '\0'`).
fn whole_number(s: &str) -> Option<i64> {
    let (v, n) = c_strtol(s);
    if n != 0 && n == s.len() {
        Some(v)
    } else {
        None
    }
}

// C atoi (env ATTR_INT path, init.c:497).
fn c_atoi(s: &str) -> i32 {
    c_strtol(s).0.clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

fn b64_decode(s: &[u8]) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    let mut pad = 0usize;
    for &c in s {
        if c == b'=' {
            pad += 1;
            continue;
        }
        if pad > 0 {
            return None;
        }
        acc = (acc << 6) | val(c)? as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    if pad > 2 || (s.len() % 4 != 0) {
        return None;
    }
    Some(out)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TlsOpt {
    CaCertFile,
    CaCertDir,
    CertFile,
    KeyFile,
    RequireCert,
    RequireSan,
    CipherSuite,
    PeerKeyHash,
    CrlFile,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Attr {
    /// ATTR_OPT_TV: TIMEOUT / NETWORK_TIMEOUT
    OptTv(bool),
    /// ATTR_INT: SIZELIMIT / TIMELIMIT (true = sizelimit)
    Int(bool),
    /// ATTR_KV: DEREF
    Deref,
    Tls(TlsOpt),
    /// Rows this client reads past (no consumer).
    Ignored,
}

// init.c:73-136 `attrs[]`: (name, useronly, kind). Order is the env scan
// order; every row is looked up by strcasecmp.
const ATTRS: &[(&str, bool, Attr)] = &[
    ("TIMEOUT", false, Attr::OptTv(false)),
    ("NETWORK_TIMEOUT", false, Attr::OptTv(true)),
    ("VERSION", false, Attr::Ignored),
    ("DEREF", false, Attr::Deref),
    ("SIZELIMIT", false, Attr::Int(true)),
    ("TIMELIMIT", false, Attr::Int(false)),
    ("BINDDN", true, Attr::Ignored),
    ("BASE", false, Attr::Ignored),
    ("PORT", false, Attr::Ignored),
    ("HOST", false, Attr::Ignored),
    ("URI", false, Attr::Ignored),
    ("SOCKET_BIND_ADDRESSES", false, Attr::Ignored),
    ("REFERRALS", false, Attr::Ignored),
    ("KEEPALIVE_IDLE", false, Attr::Ignored),
    ("KEEPALIVE_PROBES", false, Attr::Ignored),
    ("KEEPALIVE_INTERVAL", false, Attr::Ignored),
    ("SASL_MECH", false, Attr::Ignored),
    ("SASL_REALM", false, Attr::Ignored),
    ("SASL_AUTHCID", true, Attr::Ignored),
    ("SASL_AUTHZID", true, Attr::Ignored),
    ("SASL_SECPROPS", false, Attr::Ignored),
    ("SASL_NOCANON", false, Attr::Ignored),
    ("SASL_CBINDING", false, Attr::Ignored),
    ("TLS_CERT", true, Attr::Tls(TlsOpt::CertFile)),
    ("TLS_KEY", true, Attr::Tls(TlsOpt::KeyFile)),
    ("TLS_CACERT", false, Attr::Tls(TlsOpt::CaCertFile)),
    ("TLS_CACERTDIR", false, Attr::Tls(TlsOpt::CaCertDir)),
    ("TLS_REQCERT", false, Attr::Tls(TlsOpt::RequireCert)),
    ("TLS_REQSAN", false, Attr::Tls(TlsOpt::RequireSan)),
    // TLS_RANDFILE / TLS_PROTOCOL_MIN / TLS_PROTOCOL_MAX / TLS_ECNAME are
    // parsed and stored by tls2.c but tls_g.c reads none of them.
    ("TLS_RANDFILE", false, Attr::Ignored),
    ("TLS_CIPHER_SUITE", false, Attr::Tls(TlsOpt::CipherSuite)),
    ("TLS_PROTOCOL_MIN", false, Attr::Ignored),
    ("TLS_PROTOCOL_MAX", false, Attr::Ignored),
    ("TLS_PEERKEY_HASH", false, Attr::Tls(TlsOpt::PeerKeyHash)),
    ("TLS_ECNAME", false, Attr::Ignored),
    // GnuTLS builds carry TLS_CRLFILE; TLS_CRLCHECK (OpenSSL only) is an
    // unknown option to them and is skipped.
    ("TLS_CRLFILE", false, Attr::Tls(TlsOpt::CrlFile)),
];

fn nonempty(arg: &str) -> Option<String> {
    if arg.is_empty() {
        None
    } else {
        Some(arg.to_string())
    }
}

impl LdapOptions {
    // tls2.c:572-654 ldap_pvt_tls_config + the ldap_pvt_tls_set_option
    // arms it reaches. A value C rejects (-1) leaves the option unchanged.
    fn tls_config(&mut self, opt: TlsOpt, arg: &str) {
        match opt {
            TlsOpt::CaCertFile => self.tls_cacertfile = nonempty(arg),
            TlsOpt::CaCertDir => self.tls_cacertdir = nonempty(arg),
            TlsOpt::CertFile => self.tls_certfile = nonempty(arg),
            TlsOpt::KeyFile => self.tls_keyfile = nonempty(arg),
            TlsOpt::CrlFile => self.tls_crlfile = nonempty(arg),
            TlsOpt::CipherSuite => self.tls_ciphersuite = Some(arg.to_string()),
            TlsOpt::PeerKeyHash => {
                if arg.is_empty() {
                    self.tls_pin = None;
                    return;
                }
                let (alg, b64) = match arg.split_once(':') {
                    Some((a, h)) => (Some(a.to_string()), h),
                    None => (None, arg),
                };
                if let Some(pin) = b64_decode(b64.as_bytes()) {
                    self.tls_pin = Some((alg, pin));
                }
            }
            TlsOpt::RequireCert | TlsOpt::RequireSan => {
                let i = if arg.eq_ignore_ascii_case("never") {
                    LDAP_OPT_X_TLS_NEVER
                } else if arg.eq_ignore_ascii_case("demand") {
                    LDAP_OPT_X_TLS_DEMAND
                } else if arg.eq_ignore_ascii_case("allow") {
                    LDAP_OPT_X_TLS_ALLOW
                } else if arg.eq_ignore_ascii_case("try") {
                    LDAP_OPT_X_TLS_TRY
                } else if arg.eq_ignore_ascii_case("hard")
                    || arg.eq_ignore_ascii_case("on")
                    || arg.eq_ignore_ascii_case("yes")
                    || arg.eq_ignore_ascii_case("true")
                {
                    LDAP_OPT_X_TLS_HARD
                } else {
                    return;
                };
                if opt == TlsOpt::RequireCert {
                    self.tls_require_cert = i;
                } else {
                    self.tls_require_san = i;
                }
            }
        }
    }

    // init.c:144-296 ldap_int_conf_option (from_env = the
    // openldap_ldap_init_w_env arms, which skip the useronly check and use
    // atoi for ATTR_INT).
    fn conf_option(&mut self, cmd: &str, opt: &str, userconf: bool, from_env: bool) {
        for (name, useronly, kind) in ATTRS {
            if !from_env && !userconf && *useronly {
                continue;
            }
            if !cmd.eq_ignore_ascii_case(name) {
                continue;
            }
            match kind {
                Attr::OptTv(net) => {
                    if let Some(sec) = whole_number(opt) {
                        if sec > 0 {
                            let d = Some(Duration::from_secs(sec as u64));
                            if *net {
                                self.tm_net = d;
                            } else {
                                self.tm_api = d;
                            }
                        }
                    }
                }
                Attr::Int(size) => {
                    let v = if from_env {
                        Some(c_atoi(opt))
                    } else {
                        whole_number(opt).map(|l| l as i32)
                    };
                    if let Some(v) = v {
                        if *size {
                            self.sizelimit = v;
                        } else {
                            self.timelimit = v;
                        }
                    }
                }
                Attr::Deref => {
                    for (key, value) in [
                        ("never", LDAP_DEREF_NEVER),
                        ("searching", LDAP_DEREF_SEARCHING),
                        ("finding", LDAP_DEREF_FINDING),
                        ("always", LDAP_DEREF_ALWAYS),
                    ] {
                        if opt.eq_ignore_ascii_case(key) {
                            self.deref = value;
                            break;
                        }
                    }
                }
                Attr::Tls(t) => self.tls_config(*t, opt),
                Attr::Ignored => {}
            }
            break;
        }
    }

    // init.c:319-385 openldap_ldap_init_w_conf: one file, C's line grammar
    // (a '#' in column 0 is a comment; the command ends at whitespace; a
    // command without an argument is skipped; the argument is the rest of
    // the line with trailing whitespace trimmed).
    pub(crate) fn init_w_conf_text(&mut self, text: &[u8], userconf: bool) {
        let is_space = |b: u8| b == b' ' || (b'\t'..=b'\r').contains(&b);
        for line in text.split(|&b| b == b'\n') {
            if line.first() == Some(&b'#') {
                continue;
            }
            let mut start = 0;
            while start < line.len() && is_space(line[start]) {
                start += 1;
            }
            let mut end = line.len();
            while end > start && is_space(line[end - 1]) {
                end -= 1;
            }
            if start >= end {
                continue;
            }
            let line = &line[start..end];
            let Some(cmd_end) = line.iter().position(|&b| is_space(b)) else {
                continue; // command has no argument
            };
            let cmd = String::from_utf8_lossy(&line[..cmd_end]).into_owned();
            let mut o = cmd_end;
            while o < line.len() && is_space(line[o]) {
                o += 1;
            }
            let opt = String::from_utf8_lossy(&line[o..]).into_owned();
            self.conf_option(&cmd, &opt, userconf, false);
        }
    }

    fn init_w_conf(&mut self, file: &str, userconf: bool) {
        let Ok(text) = std::fs::read(file) else { return };
        self.init_w_conf_text(&text, userconf);
    }

    // init.c:397-434 openldap_ldap_init_w_userconf: ~/file, ~/.file, ./file.
    fn init_w_userconf(&mut self, file: &str) {
        if let Some(home) = getenv("HOME") {
            self.init_w_conf(&format!("{home}/{file}"), true);
            self.init_w_conf(&format!("{home}/.{file}"), true);
        }
        self.init_w_conf(file, true);
    }

    // init.c:436-527 openldap_ldap_init_w_env: LDAP<ATTR> for every row.
    fn init_w_env(&mut self) {
        for (name, _, _) in ATTRS {
            if let Some(value) = getenv(&format!("{LDAP_ENV_PREFIX}{name}")) {
                self.conf_option(name, &value, true, true);
            }
        }
    }

    /// ldap_int_initialize (init.c:685-782): the global options a fresh
    /// backend's libldap would carry into ldap_initialize.
    pub fn initialize() -> LdapOptions {
        let mut o = LdapOptions::default();
        if getenv("LDAPNOINIT").is_some() {
            return o;
        }
        o.init_w_conf(LDAP_CONF_FILE, false);
        // SAFETY: plain libc uid queries, no memory involved.
        if unsafe { libc::geteuid() != libc::getuid() } {
            return o;
        }
        o.init_w_userconf(LDAP_USERRC_FILE);
        if let Some(alt) = getenv(&format!("{LDAP_ENV_PREFIX}CONF")) {
            o.init_w_conf(&alt, false);
        }
        if let Some(alt) = getenv(&format!("{LDAP_ENV_PREFIX}RC")) {
            o.init_w_userconf(&alt);
        }
        o.init_w_env();
        o
    }
}

/// ldap_int_hostname (init.c:695-706 via util-int.c ldap_pvt_get_fqdn):
/// gethostname() canonicalized through getaddrinfo(AI_CANONNAME), falling
/// back to the bare name, then to "localhost". The name libldap checks a
/// server certificate against when the URI host is "localhost".
pub fn ldap_int_hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: buf is a valid writable buffer of the stated length.
    let name = if unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len() - 1) } == 0 {
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len() - 1);
        String::from_utf8_lossy(&buf[..end]).into_owned()
    } else {
        "localhost".to_string()
    };
    let Ok(cname) = std::ffi::CString::new(name.clone()) else { return name };
    // SAFETY: zeroed hints, a NUL-terminated node, and the result pointer
    // is freed with freeaddrinfo before returning.
    unsafe {
        let mut hints: libc::addrinfo = std::mem::zeroed();
        hints.ai_family = libc::AF_UNSPEC;
        hints.ai_flags = libc::AI_CANONNAME;
        let mut res: *mut libc::addrinfo = std::ptr::null_mut();
        let rc = libc::getaddrinfo(cname.as_ptr(), std::ptr::null(), &hints, &mut res);
        let mut fqdn = name.clone();
        if rc == 0 {
            if !res.is_null() && !(*res).ai_canonname.is_null() {
                fqdn = std::ffi::CStr::from_ptr((*res).ai_canonname)
                    .to_string_lossy()
                    .into_owned();
            }
            if !res.is_null() {
                libc::freeaddrinfo(res);
            }
        }
        fqdn
    }
}

#[cfg(test)]
mod ldapconf_tests {
    use super::*;

    #[test]
    fn conf_text_follows_init_c_grammar_and_tls_config() {
        let mut o = LdapOptions::default();
        o.init_w_conf_text(
            b"# TLS_CACERT /commented\n   \t\nTLS_CACERT   /etc/ca.pem  \r\ntls_reqcert TRY\nTLS_REQSAN demand\nTLS_PROTOCOL_MIN 3.3\nTLS_CRLCHECK peer\nTLS_CRLFILE /etc/crl.pem\nTLS_CIPHER_SUITE NORMAL:-VERS-TLS1.0\nTLS_PEERKEY_HASH sha256:AQID\nTLS_ECNAME prime256v1\nNETWORK_TIMEOUT 7\nTIMEOUT 0\nDEREF Always\nSIZELIMIT 12\nTIMELIMIT 3x\nTLS_CERT /user-only.pem\nBOGUS x\nNOARG\n",
            false,
        );
        assert_eq!(o.tls_cacertfile.as_deref(), Some("/etc/ca.pem"));
        assert_eq!(o.tls_require_cert, LDAP_OPT_X_TLS_TRY);
        assert_eq!(o.tls_require_san, LDAP_OPT_X_TLS_DEMAND);
        assert_eq!(o.tls_crlfile.as_deref(), Some("/etc/crl.pem"));
        assert_eq!(o.tls_ciphersuite.as_deref(), Some("NORMAL:-VERS-TLS1.0"));
        assert_eq!(o.tls_pin, Some((Some("sha256".to_string()), vec![1, 2, 3])));
        assert_eq!(o.tm_net, Some(Duration::from_secs(7)));
        assert_eq!(o.tm_api, None); // TIMEOUT 0 is not > 0
        assert_eq!(o.deref, LDAP_DEREF_ALWAYS);
        assert_eq!((o.sizelimit, o.timelimit), (12, 0)); // "3x" is not a whole number
        assert_eq!(o.tls_certfile, None); // useronly row skipped in a system file
        // the same TLS_CERT line in a user file is honored; a bad keyword
        // leaves the previous value; an empty file value unsets
        o.init_w_conf_text(b"TLS_CERT /user.pem\nTLS_REQCERT maybe\nTLS_PEERKEY_HASH not*base64\n", true);
        assert_eq!(o.tls_certfile.as_deref(), Some("/user.pem"));
        assert_eq!(o.tls_require_cert, LDAP_OPT_X_TLS_TRY);
        assert_eq!(o.tls_pin, Some((Some("sha256".to_string()), vec![1, 2, 3])));
        o.conf_option("TLS_CACERT", "", false, false);
        assert_eq!(o.tls_cacertfile, None);
        // the env arms: useronly ignored, ATTR_INT via atoi
        o.conf_option("TLS_KEY", "/k.pem", false, true);
        o.conf_option("SIZELIMIT", "42abc", false, true);
        assert_eq!(o.tls_keyfile.as_deref(), Some("/k.pem"));
        assert_eq!(o.sizelimit, 42);
    }

    #[test]
    fn defaults_match_ldap_int_initialize_global_options() {
        let o = LdapOptions::default();
        assert_eq!(o.tls_require_cert, LDAP_OPT_X_TLS_DEMAND);
        assert_eq!(o.tls_require_san, LDAP_OPT_X_TLS_ALLOW);
        assert_eq!(o.deref, LDAP_DEREF_NEVER);
        assert_eq!((o.sizelimit, o.timelimit), (0, 0));
        assert_eq!((o.tm_net, o.tm_api), (None, None));
        assert_eq!(b64_decode(b"AQID"), Some(vec![1, 2, 3]));
        assert_eq!(b64_decode(b"AQ=="), Some(vec![1]));
        assert_eq!(b64_decode(b"A"), None);
        assert_eq!(c_strtol("  -12x"), (-12, 5));
        assert_eq!(c_strtol("x"), (0, 0));
    }

    #[test]
    fn fqdn_is_nonempty() {
        assert!(!ldap_int_hostname().is_empty());
    }
}
