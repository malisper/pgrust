// PQconninfoParse's scanner plus the defaults ladder (explicit > service
// file > environment > compiled default). Error strings are user-visible
// through dblink and must match libpq byte-for-byte.

use pg_string::isspace_c_locale;

pub struct ConnOption {
    pub keyword: &'static str,
    pub envvar: Option<&'static str>,
    pub compiled: Option<&'static str>,
    // libpq dispchar: "*" = secure (user-mapping-only for FDW validators),
    // "D" = debug (never valid as an FDW option).
    pub dispchar: &'static str,
}

macro_rules! conn_options {
    ($(($kw:literal, $env:expr, $def:expr, $disp:literal),)*) => {
        pub const CONNINFO_OPTIONS: &[ConnOption] = &[
            $(ConnOption { keyword: $kw, envvar: $env, compiled: $def, dispchar: $disp },)*
        ];
    };
}

conn_options! {
    ("service", Some("PGSERVICE"), None, ""),
    ("user", Some("PGUSER"), None, ""),
    ("password", Some("PGPASSWORD"), None, "*"),
    ("passfile", Some("PGPASSFILE"), None, ""),
    ("channel_binding", Some("PGCHANNELBINDING"), Some("prefer"), ""),
    ("connect_timeout", Some("PGCONNECT_TIMEOUT"), None, ""),
    ("dbname", Some("PGDATABASE"), None, ""),
    ("host", Some("PGHOST"), None, ""),
    ("hostaddr", Some("PGHOSTADDR"), None, ""),
    ("port", Some("PGPORT"), Some("5432"), ""),
    ("client_encoding", Some("PGCLIENTENCODING"), None, ""),
    ("options", Some("PGOPTIONS"), Some(""), ""),
    ("application_name", Some("PGAPPNAME"), None, ""),
    ("fallback_application_name", None, None, ""),
    ("keepalives", None, None, ""),
    ("keepalives_idle", None, None, ""),
    ("keepalives_interval", None, None, ""),
    ("keepalives_count", None, None, ""),
    ("tcp_user_timeout", None, None, ""),
    ("sslmode", Some("PGSSLMODE"), Some("prefer"), ""),
    ("sslnegotiation", Some("PGSSLNEGOTIATION"), Some("postgres"), ""),
    ("sslcompression", Some("PGSSLCOMPRESSION"), Some("0"), ""),
    ("sslcert", Some("PGSSLCERT"), None, ""),
    ("sslkey", Some("PGSSLKEY"), None, ""),
    ("sslcertmode", Some("PGSSLCERTMODE"), None, ""),
    ("sslpassword", None, None, "*"),
    ("sslrootcert", Some("PGSSLROOTCERT"), None, ""),
    ("sslcrl", Some("PGSSLCRL"), None, ""),
    ("sslcrldir", Some("PGSSLCRLDIR"), None, ""),
    ("sslsni", Some("PGSSLSNI"), Some("1"), ""),
    ("requirepeer", Some("PGREQUIREPEER"), None, ""),
    ("require_auth", Some("PGREQUIREAUTH"), None, ""),
    ("min_protocol_version", Some("PGMINPROTOCOLVERSION"), None, ""),
    ("max_protocol_version", Some("PGMAXPROTOCOLVERSION"), None, ""),
    ("ssl_min_protocol_version", Some("PGSSLMINPROTOCOLVERSION"), Some("TLSv1.2"), ""),
    ("ssl_max_protocol_version", Some("PGSSLMAXPROTOCOLVERSION"), None, ""),
    ("gssencmode", Some("PGGSSENCMODE"), Some("prefer"), ""),
    ("krbsrvname", Some("PGKRBSRVNAME"), Some("postgres"), ""),
    ("gsslib", Some("PGGSSLIB"), None, ""),
    ("gssdelegation", Some("PGGSSDELEGATION"), Some("0"), ""),
    ("replication", None, None, "D"),
    ("target_session_attrs", Some("PGTARGETSESSIONATTRS"), Some("any"), ""),
    ("load_balance_hosts", Some("PGLOADBALANCEHOSTS"), Some("disable"), ""),
    ("scram_client_key", None, None, "D"),
    ("scram_server_key", None, None, "D"),
    ("oauth_issuer", None, None, ""),
    ("oauth_client_id", None, None, ""),
    ("oauth_client_secret", None, None, "*"),
    ("oauth_scope", None, None, ""),
    ("sslkeylogfile", None, None, "D"),
}

pub fn lookup_option(keyword: &str) -> Option<&'static ConnOption> {
    CONNINFO_OPTIONS.iter().find(|o| o.keyword == keyword)
}

// fe-connect.c uri_prefix_length: the two URI designators PQconninfoParse /
// parse_connection_string route to conninfo_uri_parse.
fn uri_prefix_length(s: &str) -> usize {
    const URI_DESIGNATOR: &str = "postgresql://";
    const SHORT_URI_DESIGNATOR: &str = "postgres://";
    if s.starts_with(URI_DESIGNATOR) {
        URI_DESIGNATOR.len()
    } else if s.starts_with(SHORT_URI_DESIGNATOR) {
        SHORT_URI_DESIGNATOR.len()
    } else {
        0
    }
}

// conninfo_storeval's "replace the option's value" effect on the option
// array: one entry per keyword, the last store wins.
fn store(opts: &mut Vec<(String, String)>, key: &str, val: String) {
    opts.retain(|(k, _)| k != key);
    opts.push((key.to_string(), val));
}

// fe-connect.c get_hexdigit: A-F and a-f are treated identically.
fn get_hexdigit(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}

// fe-connect.c conninfo_uri_decode: replace %xy triplets, skipping leading
// and trailing spaces; a space anywhere else ends the value and is an error
// ("use percent-encoded spaces (%20) instead"). C stores the raw decoded
// bytes; a percent-encoded non-UTF-8 byte degrades lossily here, as the
// keyword=value scanner already does.
fn conninfo_uri_decode(s: &str) -> Result<String, String> {
    let b = s.as_bytes();
    let mut q = 0usize;
    while q < b.len() && b[q] == b' ' {
        q += 1;
    }
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    while q < b.len() && b[q] != b' ' {
        if b[q] != b'%' {
            out.push(b[q]);
            q += 1;
        } else {
            q += 1; // skip the percent sign itself
            // C reads *q++ twice; end-of-string is caught by the first
            // failing get_hexdigit (NUL is not a hex digit).
            let d1 = b.get(q).copied().unwrap_or(0);
            q += 1;
            let Some(hi) = get_hexdigit(d1) else {
                return Err(format!("invalid percent-encoded token: \"{s}\""));
            };
            let d2 = b.get(q).copied().unwrap_or(0);
            q += 1;
            let Some(lo) = get_hexdigit(d2) else {
                return Err(format!("invalid percent-encoded token: \"{s}\""));
            };
            let c = (hi << 4) | lo;
            if c == 0 {
                return Err(format!("forbidden value %00 in percent-encoded value: \"{s}\""));
            }
            out.push(c);
        }
    }
    while q < b.len() && b[q] == b' ' {
        q += 1;
    }
    if q < b.len() {
        return Err(format!(
            "unexpected spaces found in \"{s}\", use percent-encoded spaces (%20) instead"
        ));
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

// conninfo_storeval (fe-connect.c) as conninfo_uri_parse_options calls it:
// uri_decode=true, ignoreMissing=false. requiressl is translated BEFORE the
// decode (C reads value[0] of the still-encoded string); the keyword is one
// of user/password/host/port/dbname here, always a known option.
fn store_uri_component(
    opts: &mut Vec<(String, String)>,
    keyword: &str,
    value_encoded: &str,
) -> Result<(), String> {
    let (keyword, value) = if keyword == "requiressl" {
        (
            "sslmode",
            if value_encoded.starts_with('1') { "require" } else { "prefer" }.to_string(),
        )
    } else {
        (keyword, conninfo_uri_decode(value_encoded)?)
    };
    if lookup_option(keyword).is_none() {
        return Err(format!("invalid connection option \"{keyword}\""));
    }
    store(opts, keyword, value);
    Ok(())
}

// fe-connect.c conninfo_uri_parse_params: ?param1=value1&param2=value2...
// Keyword and value are percent-decoded; ssl=true is rewritten to
// sslmode=require (JDBC compatibility); an unknown keyword is "invalid URI
// query parameter" (conninfo_storeval with ignoreMissing=true adds no
// message of its own).
fn conninfo_uri_parse_params(params: &str, opts: &mut Vec<(String, String)>) -> Result<(), String> {
    let b = params.as_bytes();
    let mut p = 0usize;
    while p < b.len() {
        let kw_start = p;
        let mut eq: Option<usize> = None;
        let seg_end;
        loop {
            if p >= b.len() || b[p] == b'&' {
                seg_end = p;
                break;
            }
            if b[p] == b'=' {
                match eq {
                    None => eq = Some(p),
                    Some(first) => {
                        return Err(format!(
                            "extra key/value separator \"=\" in URI query parameter: \"{}\"",
                            &params[kw_start..first]
                        ))
                    }
                }
            }
            p += 1;
        }
        let Some(eqpos) = eq else {
            return Err(format!(
                "missing key/value separator \"=\" in URI query parameter: \"{}\"",
                &params[kw_start..seg_end]
            ));
        };
        if p < b.len() {
            p += 1; // advance past '&'
        }
        let keyword = conninfo_uri_decode(&params[kw_start..eqpos])?;
        let value = conninfo_uri_decode(&params[eqpos + 1..seg_end])?;
        let (keyword, value) = if keyword == "ssl" && value == "true" {
            ("sslmode".to_string(), "require".to_string())
        } else {
            (keyword, value)
        };
        // conninfo_storeval, uri_decode=false arm: requiressl reads the
        // decoded first byte.
        let (keyword, value) = if keyword == "requiressl" {
            (
                "sslmode".to_string(),
                if value.starts_with('1') { "require" } else { "prefer" }.to_string(),
            )
        } else {
            (keyword, value)
        };
        if lookup_option(&keyword).is_none() {
            return Err(format!("invalid URI query parameter: \"{keyword}\""));
        }
        store(opts, &keyword, value);
    }
    Ok(())
}

// fe-connect.c conninfo_uri_parse_options (RFC 3986 form):
//   postgresql://[user[:password]@][netloc][:port][,...][/dbname][?params]
// netloc = host name, IPv4 address, or a bracketed IPv6 address; several
// netloc[:port] pairs may be comma-separated (host/port become comma lists).
// Every component may be percent-encoded. Component boundaries are ASCII
// delimiters, so the byte-indexed slices below are always char boundaries.
fn conninfo_uri_parse(uri: &str) -> Result<Vec<(String, String)>, String> {
    let mut opts: Vec<(String, String)> = Vec::new();
    let b = uri.as_bytes();
    let prefix_len = uri_prefix_length(uri);
    if prefix_len == 0 {
        // Should never happen.
        return Err(format!("invalid URI propagated to internal parser routine: \"{uri}\""));
    }
    let start = prefix_len;
    let mut p = start;

    // Look ahead for possible user credentials designator.
    while p < b.len() && b[p] != b'@' && b[p] != b'/' {
        p += 1;
    }
    if p < b.len() && b[p] == b'@' {
        // scheme://user[:password]@[netloc]  ('@' is at p, so both scans
        // below terminate before the end of the string).
        let at = p;
        let mut e = start;
        while b[e] != b':' && b[e] != b'@' {
            e += 1;
        }
        let user = &uri[start..e];
        if !user.is_empty() {
            store_uri_component(&mut opts, "user", user)?;
        }
        if b[e] == b':' {
            let pw_start = e + 1;
            let password = &uri[pw_start..at];
            if !password.is_empty() {
                store_uri_component(&mut opts, "password", password)?;
            }
        }
        p = at + 1; // advance past end of parsed user name or password token
    } else {
        // No username/password designator found. Reset to start of URI.
        p = start;
    }

    // There may be multiple netloc[:port] pairs, each separated from the
    // next by a comma.
    let mut hostbuf = String::new();
    let mut portbuf = String::new();
    let mut prevchar: u8;
    loop {
        let host_start;
        let host_end;
        if p < b.len() && b[p] == b'[' {
            // IPv6 address.
            p += 1;
            host_start = p;
            while p < b.len() && b[p] != b']' {
                p += 1;
            }
            if p >= b.len() {
                return Err(format!(
                    "end of string reached when looking for matching \"]\" in IPv6 host address in URI: \"{uri}\""
                ));
            }
            if p == host_start {
                return Err(format!("IPv6 host address may not be empty in URI: \"{uri}\""));
            }
            host_end = p;
            p += 1; // cut off the bracket and advance
            // The address may be followed by a port specifier or a slash or
            // a query or a separator comma.
            if p < b.len() && b[p] != b':' && b[p] != b'/' && b[p] != b'?' && b[p] != b',' {
                return Err(format!(
                    "unexpected character \"{}\" at position {} in URI (expected \":\" or \"/\"): \"{uri}\"",
                    b[p] as char,
                    p + 1
                ));
            }
        } else {
            // Not an IPv6 address: DNS-named or IPv4 netloc.
            host_start = p;
            while p < b.len() && b[p] != b':' && b[p] != b'/' && b[p] != b'?' && b[p] != b',' {
                p += 1;
            }
            host_end = p;
        }
        prevchar = b.get(p).copied().unwrap_or(0);
        hostbuf.push_str(&uri[host_start..host_end]);

        if prevchar == b':' {
            p += 1; // advance past host terminator
            let port_start = p;
            while p < b.len() && b[p] != b'/' && b[p] != b'?' && b[p] != b',' {
                p += 1;
            }
            prevchar = b.get(p).copied().unwrap_or(0);
            portbuf.push_str(&uri[port_start..p]);
        }

        if prevchar != b',' {
            break;
        }
        p += 1; // advance past comma separator
        hostbuf.push(',');
        portbuf.push(',');
    }

    if !hostbuf.is_empty() {
        store_uri_component(&mut opts, "host", &hostbuf)?;
    }
    if !portbuf.is_empty() {
        store_uri_component(&mut opts, "port", &portbuf)?;
    }

    if prevchar != 0 && prevchar != b'?' {
        p += 1; // advance past host terminator
        let db_start = p;
        while p < b.len() && b[p] != b'?' {
            p += 1;
        }
        prevchar = b.get(p).copied().unwrap_or(0);
        // An empty dbname is not set at all: setting it to "" would force
        // the default (user name) and ignore $PGDATABASE.
        let dbname = &uri[db_start..p];
        if !dbname.is_empty() {
            store_uri_component(&mut opts, "dbname", dbname)?;
        }
    }

    if prevchar != 0 {
        p += 1; // advance past terminator
        conninfo_uri_parse_params(&uri[p..], &mut opts)?;
    }
    Ok(opts)
}

// fe-connect.c parse_connection_string: a string carrying a URI designator
// goes to conninfo_uri_parse, anything else to the keyword=value scanner.
pub fn parse_conninfo(s: &str) -> Result<Vec<(String, String)>, String> {
    if uri_prefix_length(s) != 0 {
        return conninfo_uri_parse(s);
    }
    let b = s.as_bytes();
    let mut i = 0;
    let mut opts: Vec<(String, String)> = Vec::new();
    // fe-connect.c conninfo_parse skips blanks with C-locale isspace(),
    // which includes VT (0x0b) -- not the is_ascii_whitespace set.
    loop {
        while i < b.len() && isspace_c_locale(b[i]) {
            i += 1;
        }
        if i >= b.len() {
            return Ok(opts);
        }
        let kstart = i;
        while i < b.len() && b[i] != b'=' && !isspace_c_locale(b[i]) {
            i += 1;
        }
        let key = s[kstart..i].to_string();
        while i < b.len() && isspace_c_locale(b[i]) {
            i += 1;
        }
        if i >= b.len() || b[i] != b'=' {
            return Err(format!(
                "missing \"=\" after \"{key}\" in connection info string"
            ));
        }
        i += 1;
        while i < b.len() && isspace_c_locale(b[i]) {
            i += 1;
        }
        let mut val = Vec::new();
        if i < b.len() && b[i] == b'\'' {
            i += 1;
            loop {
                if i >= b.len() {
                    return Err("unterminated quoted string in connection info string".into());
                }
                match b[i] {
                    b'\'' => {
                        i += 1;
                        break;
                    }
                    b'\\' if i + 1 < b.len() => {
                        val.push(b[i + 1]);
                        i += 2;
                    }
                    c => {
                        val.push(c);
                        i += 1;
                    }
                }
            }
        } else {
            while i < b.len() && !isspace_c_locale(b[i]) {
                if b[i] == b'\\' && i + 1 < b.len() {
                    val.push(b[i + 1]);
                    i += 2;
                } else {
                    val.push(b[i]);
                    i += 1;
                }
            }
        }
        let val = String::from_utf8_lossy(&val).into_owned();
        opts.retain(|(k, _)| *k != key);
        opts.push((key, val));
    }
}

pub fn opt<'a>(opts: &'a [(String, String)], key: &str) -> Option<&'a str> {
    opts.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

fn set_default(opts: &mut Vec<(String, String)>, key: &str, val: &str) {
    if opt(opts, key).is_none() {
        opts.push((key.to_string(), val.to_string()));
    }
}

// conninfo_array_parse's defaults ladder: service file first, then
// environment, then compiled defaults. Unknown keywords are rejected with
// libpq's wording.
pub fn resolve_conninfo(conninfo: &str) -> Result<Vec<(String, String)>, String> {
    let mut opts = parse_conninfo(conninfo)?;
    for (k, _) in &opts {
        if lookup_option(k).is_none() {
            return Err(format!("invalid connection option \"{k}\""));
        }
    }
    parse_service_info(&mut opts)?;
    for o in CONNINFO_OPTIONS {
        if opt(&opts, o.keyword).is_some() {
            continue;
        }
        if let Some(env) = o.envvar {
            if let Ok(v) = std::env::var(env) {
                opts.push((o.keyword.to_string(), v));
                continue;
            }
        }
        if let Some(def) = o.compiled {
            opts.push((o.keyword.to_string(), def.to_string()));
        }
    }
    // connectOptions2: dbname defaults to the user name.
    if opt(&opts, "dbname").is_none() {
        let user = opt(&opts, "user").map(|s| s.to_string()).unwrap_or_else(super::os_user_name);
        opts.push(("dbname".to_string(), user));
    }
    Ok(opts)
}

fn parse_service_info(opts: &mut Vec<(String, String)>) -> Result<(), String> {
    let service = match opt(opts, "service") {
        Some(s) => s.to_string(),
        None => match std::env::var("PGSERVICE") {
            Ok(s) => s,
            _ => return Ok(()),
        },
    };
    let mut group_found = false;
    if let Ok(f) = std::env::var("PGSERVICEFILE") {
        parse_service_file(&f, &service, opts, &mut group_found)?;
        if group_found {
            return Ok(());
        }
    } else if let Some(home) = std::env::var_os("HOME") {
        let f = format!("{}/.pg_service.conf", home.to_string_lossy());
        if std::fs::metadata(&f).is_ok() {
            parse_service_file(&f, &service, opts, &mut group_found)?;
            if group_found {
                return Ok(());
            }
        }
    }
    let sysconf = std::env::var("PGSYSCONFDIR").unwrap_or_else(|_| "/etc/postgresql-common".into());
    let f = format!("{sysconf}/pg_service.conf");
    if std::fs::metadata(&f).is_ok() {
        parse_service_file(&f, &service, opts, &mut group_found)?;
    }
    if !group_found {
        return Err(format!("definition of service \"{service}\" not found"));
    }
    Ok(())
}

pub(crate) fn parse_service_file(
    service_file: &str,
    service: &str,
    opts: &mut Vec<(String, String)>,
    group_found: &mut bool,
) -> Result<(), String> {
    *group_found = false;
    let content = match std::fs::read(service_file) {
        Ok(c) => c,
        Err(_) => return Err(format!("service file \"{service_file}\" not found")),
    };
    for (idx, raw) in content.split(|&c| c == b'\n').enumerate() {
        let linenr = idx + 1;
        // fgets(buf[1024]) overflow check: fires once content-sans-newline
        // reaches 1022 bytes.
        if raw.len() >= 1022 {
            return Err(format!(
                "line {linenr} too long in service file \"{service_file}\""
            ));
        }
        let line = String::from_utf8_lossy(raw);
        // parseServiceFile trims leading/trailing C-locale isspace() (VT
        // included), not the narrower is_ascii_whitespace set.
        let line =
            line.trim_matches(|c: char| c.is_ascii() && isspace_c_locale(c as u8));
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix('[') {
            if *group_found {
                return Ok(());
            }
            *group_found = rest.strip_prefix(service).map(|t| t.starts_with(']')) == Some(true);
        } else if *group_found {
            // Non-LDAP build: an ldap:// line falls through to the key=value
            // check and reads as a syntax error, which the dblink corpus's
            // LDAP guard depends on.
            let Some((key, val)) = line.split_once('=') else {
                return Err(format!(
                    "syntax error in service file \"{service_file}\", line {linenr}"
                ));
            };
            if key == "service" {
                return Err(format!(
                    "nested service specifications not supported in service file \"{service_file}\", line {linenr}"
                ));
            }
            if lookup_option(key).is_none() {
                return Err(format!(
                    "syntax error in service file \"{service_file}\", line {linenr}"
                ));
            }
            set_default(opts, key, val);
        }
    }
    Ok(())
}

#[cfg(test)]
mod ws_tests {
    use super::parse_conninfo;

    /// fe-connect.c conninfo_parse skips blanks with C-locale isspace()
    /// (VT 0x0b included).  Ground truth (PostgreSQL 18.3 via dblink):
    ///   dblink_connect(E'\x0bdbname=postgres user=postgres')     -> OK
    ///   dblink_connect(E'dbname=postgres\x0buser=postgres')      -> OK
    #[test]
    fn vt_is_conninfo_whitespace() {
        let o = parse_conninfo("\x0bdbname=x").unwrap();
        assert_eq!(o, vec![("dbname".to_string(), "x".to_string())]);
        // VT terminates a keyword and an unquoted value.
        let o = parse_conninfo("host\x0b= y").unwrap();
        assert_eq!(o, vec![("host".to_string(), "y".to_string())]);
        let o = parse_conninfo("dbname=a\x0bhost=b").unwrap();
        assert_eq!(
            o,
            vec![
                ("dbname".to_string(), "a".to_string()),
                ("host".to_string(), "b".to_string())
            ]
        );
        // VT after '=' is skipped before the value.
        let o = parse_conninfo("port=\x0b5432").unwrap();
        assert_eq!(o, vec![("port".to_string(), "5432".to_string())]);
        // Non-ASCII Unicode space is NOT conninfo whitespace: it becomes
        // part of the keyword.
        let o = parse_conninfo("\u{a0}dbname=x").unwrap();
        assert_eq!(o[0].0, "\u{a0}dbname");
    }
}
