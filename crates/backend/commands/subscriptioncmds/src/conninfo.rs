// libpq conninfo syntax (fe-connect.c conninfo_parse) — DDL validation only;
// error strings keep libpq's trailing '\n' so psql output stays byte-exact.
// Blank-skipping uses C-locale isspace() (VT 0x0b included), as in
// fe-connect.c, NOT Rust's is_ascii_whitespace.

use pg_string::isspace_c_locale;

use mcx::{Mcx, PgString, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED, ERRCODE_SYNTAX_ERROR,
};

const KNOWN_OPTIONS: &[&str] = &[
    "service",
    "user",
    "password",
    "passfile",
    "channel_binding",
    "connect_timeout",
    "dbname",
    "host",
    "hostaddr",
    "port",
    "client_encoding",
    "options",
    "application_name",
    "fallback_application_name",
    "keepalives",
    "keepalives_idle",
    "keepalives_interval",
    "keepalives_count",
    "tcp_user_timeout",
    "sslmode",
    "sslnegotiation",
    "sslcompression",
    "sslcert",
    "sslkey",
    "sslcertmode",
    "sslpassword",
    "sslrootcert",
    "sslcrl",
    "sslcrldir",
    "sslsni",
    "requirepeer",
    "require_auth",
    "min_protocol_version",
    "max_protocol_version",
    "ssl_min_protocol_version",
    "ssl_max_protocol_version",
    "gssencmode",
    "krbsrvname",
    "gsslib",
    "gssdelegation",
    "replication",
    "target_session_attrs",
    "load_balance_hosts",
    "scram_client_key",
    "scram_server_key",
    "oauth_issuer",
    "oauth_client_id",
    "oauth_client_secret",
    "oauth_scope",
    "sslkeylogfile",
];

fn recognized_connection_string(s: &str) -> bool {
    s.starts_with("postgresql://") || s.starts_with("postgres://")
}

fn store_opt<'mcx>(
    mcx: Mcx<'mcx>,
    opts: &mut PgVec<'mcx, (usize, PgString<'mcx>)>,
    idx: usize,
    value: &str,
) -> Result<(), String> {
    let pv = PgString::from_str_in(value, mcx).map_err(|_| "out of memory\n".to_string())?;
    if let Some(slot) = opts.iter_mut().find(|(i, _)| *i == idx) {
        slot.1 = pv;
    } else {
        opts.push((idx, pv));
    }
    Ok(())
}

fn get_hexdigit(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}

// conninfo_uri_decode (fe-connect.c): replace %xy triplets, with libpq's
// leading/trailing-space skipping and the mid-string space rejection.
fn conninfo_uri_decode(s: &str) -> Result<String, String> {
    let b = s.as_bytes();
    let mut q = 0usize;
    // Skip leading whitespaces.
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
            let d1 = if q < b.len() { b[q] } else { 0 };
            q += 1;
            let Some(hi) = get_hexdigit(d1) else {
                return Err(format!("invalid percent-encoded token: \"{s}\"\n"));
            };
            let d2 = if q < b.len() { b[q] } else { 0 };
            q += 1;
            let Some(lo) = get_hexdigit(d2) else {
                return Err(format!("invalid percent-encoded token: \"{s}\"\n"));
            };
            let c = (hi << 4) | lo;
            if c == 0 {
                return Err(format!(
                    "forbidden value %00 in percent-encoded value: \"{s}\"\n"
                ));
            }
            out.push(c);
        }
    }
    // Skip trailing whitespaces; anything further is an error.
    while q < b.len() && b[q] == b' ' {
        q += 1;
    }
    if q < b.len() {
        return Err(format!(
            "unexpected spaces found in \"{s}\", use percent-encoded spaces (%20) instead\n"
        ));
    }
    // C stores raw bytes; this DDL-validation lane only tests option
    // emptiness, so a (percent-encoded) non-UTF-8 byte degrades lossily.
    Ok(String::from_utf8_lossy(&out).into_owned())
}

// conninfo_storeval (fe-connect.c), URI-component arm (value still
// percent-encoded; requiressl's translation reads the raw first byte).
fn store_uri_val<'mcx>(
    mcx: Mcx<'mcx>,
    opts: &mut PgVec<'mcx, (usize, PgString<'mcx>)>,
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
    let Some(idx) = KNOWN_OPTIONS.iter().position(|k| *k == keyword) else {
        return Err(format!("invalid connection option \"{keyword}\"\n"));
    };
    store_opt(mcx, opts, idx, &value)
}

// conninfo_uri_parse_params (fe-connect.c): ?param1=value1&... with
// percent-decoding and the ssl=true JDBC-compatibility rewrite.
fn conninfo_uri_parse_params<'mcx>(
    mcx: Mcx<'mcx>,
    params: &str,
    opts: &mut PgVec<'mcx, (usize, PgString<'mcx>)>,
) -> Result<(), String> {
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
                            "extra key/value separator \"=\" in URI query parameter: \"{}\"\n",
                            &params[kw_start..first]
                        ))
                    }
                }
            }
            p += 1;
        }
        let Some(eqpos) = eq else {
            return Err(format!(
                "missing key/value separator \"=\" in URI query parameter: \"{}\"\n",
                &params[kw_start..seg_end]
            ));
        };
        if p < b.len() {
            p += 1; // advance past '&'
        }
        let keyword = conninfo_uri_decode(&params[kw_start..eqpos])?;
        let value = conninfo_uri_decode(&params[eqpos + 1..seg_end])?;
        // Special keyword handling for improved JDBC compatibility.
        let (keyword, value) = if keyword == "ssl" && value == "true" {
            ("sslmode".to_string(), "require".to_string())
        } else {
            (keyword, value)
        };
        // conninfo_storeval, already-decoded arm (requiressl reads the
        // decoded first byte here).
        let (keyword, value) = if keyword == "requiressl" {
            (
                "sslmode".to_string(),
                if value.starts_with('1') { "require" } else { "prefer" }.to_string(),
            )
        } else {
            (keyword, value)
        };
        let Some(idx) = KNOWN_OPTIONS.iter().position(|k| *k == keyword.as_str()) else {
            return Err(format!("invalid URI query parameter: \"{keyword}\"\n"));
        };
        store_opt(mcx, opts, idx, &value)?;
    }
    Ok(())
}

// conninfo_uri_parse_options (fe-connect.c): RFC 3986 URI syntax
// postgresql://[user[:password]@][netloc][:port][,...][/dbname][?params],
// netloc = hostname, IPv4 address, or bracketed IPv6 address.
fn conninfo_uri_parse<'mcx>(
    mcx: Mcx<'mcx>,
    uri: &str,
    opts: &mut PgVec<'mcx, (usize, PgString<'mcx>)>,
) -> Result<(), String> {
    let prefix_len = if uri.starts_with("postgresql://") {
        "postgresql://".len()
    } else if uri.starts_with("postgres://") {
        "postgres://".len()
    } else {
        // Should never happen.
        return Err(format!(
            "invalid URI propagated to internal parser routine: \"{uri}\"\n"
        ));
    };
    let b = uri.as_bytes();
    let start = prefix_len;
    let mut p = start;

    // Look ahead for possible user credentials designator.
    let mut q = p;
    while q < b.len() && b[q] != b'@' && b[q] != b'/' {
        q += 1;
    }
    if q < b.len() && b[q] == b'@' {
        // scheme://user[:password]@[netloc]
        let mut e = start;
        while b[e] != b':' && b[e] != b'@' {
            e += 1;
        }
        let user = &uri[start..e];
        if !user.is_empty() {
            store_uri_val(mcx, opts, "user", user)?;
        }
        if b[e] == b':' {
            let pw_start = e + 1;
            while b[e] != b'@' {
                e += 1;
            }
            let password = &uri[pw_start..e];
            if !password.is_empty() {
                store_uri_val(mcx, opts, "password", password)?;
            }
        }
        p = q + 1; // advance past '@'
    }

    // Multiple netloc[:port] pairs may follow, comma-separated.
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
                    "end of string reached when looking for matching \"]\" in IPv6 host \
                     address in URI: \"{uri}\"\n"
                ));
            }
            if p == host_start {
                return Err(format!("IPv6 host address may not be empty in URI: \"{uri}\"\n"));
            }
            host_end = p;
            p += 1; // cut off the bracket and advance
            if p < b.len() && b[p] != b':' && b[p] != b'/' && b[p] != b'?' && b[p] != b',' {
                return Err(format!(
                    "unexpected character \"{}\" at position {} in URI (expected \":\" or \
                     \"/\"): \"{uri}\"\n",
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
        prevchar = if p < b.len() { b[p] } else { 0 };
        hostbuf.push_str(&uri[host_start..host_end]);

        if prevchar == b':' {
            p += 1; // advance past host terminator
            let port_start = p;
            while p < b.len() && b[p] != b'/' && b[p] != b'?' && b[p] != b',' {
                p += 1;
            }
            prevchar = if p < b.len() { b[p] } else { 0 };
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
        store_uri_val(mcx, opts, "host", &hostbuf)?;
    }
    if !portbuf.is_empty() {
        store_uri_val(mcx, opts, "port", &portbuf)?;
    }

    if prevchar != 0 && prevchar != b'?' {
        p += 1; // advance past host terminator
        let db_start = p;
        while p < b.len() && b[p] != b'?' {
            p += 1;
        }
        prevchar = if p < b.len() { b[p] } else { 0 };
        // An empty dbname is not set at all (it would force the default).
        let dbname = &uri[db_start..p];
        if !dbname.is_empty() {
            store_uri_val(mcx, opts, "dbname", dbname)?;
        }
    }

    if prevchar != 0 {
        p += 1; // advance past terminator
        conninfo_uri_parse_params(mcx, &uri[p..], opts)?;
    }
    Ok(())
}

pub(crate) fn conninfo_parse<'mcx>(
    mcx: Mcx<'mcx>,
    conninfo: &str,
) -> Result<PgVec<'mcx, (usize, PgString<'mcx>)>, String> {
    if recognized_connection_string(conninfo) {
        let mut opts: PgVec<'mcx, (usize, PgString<'mcx>)> = PgVec::new_in(mcx);
        conninfo_uri_parse(mcx, conninfo, &mut opts)?;
        return Ok(opts);
    }

    let mut opts: PgVec<'mcx, (usize, PgString<'mcx>)> = PgVec::new_in(mcx);
    let bytes = conninfo.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if isspace_c_locale(bytes[i]) {
            i += 1;
            continue;
        }
        let name_start = i;
        let mut name_end = None;
        while i < bytes.len() {
            if bytes[i] == b'=' {
                name_end = Some(i);
                break;
            }
            if isspace_c_locale(bytes[i]) {
                name_end = Some(i);
                i += 1;
                while i < bytes.len() && isspace_c_locale(bytes[i]) {
                    i += 1;
                }
                break;
            }
            i += 1;
        }
        let pname = &conninfo[name_start..name_end.unwrap_or(i)];
        if i >= bytes.len() || bytes[i] != b'=' {
            return Err(format!(
                "missing \"=\" after \"{pname}\" in connection info string\n"
            ));
        }
        i += 1;
        while i < bytes.len() && isspace_c_locale(bytes[i]) {
            i += 1;
        }

        let mut val: Vec<u8> = Vec::new();
        if i < bytes.len() && bytes[i] == b'\'' {
            i += 1;
            loop {
                if i >= bytes.len() {
                    return Err("unterminated quoted string in connection info string\n".into());
                }
                match bytes[i] {
                    b'\\' => {
                        i += 1;
                        if i < bytes.len() {
                            val.push(bytes[i]);
                            i += 1;
                        }
                    }
                    b'\'' => {
                        i += 1;
                        break;
                    }
                    c => {
                        val.push(c);
                        i += 1;
                    }
                }
            }
        } else {
            while i < bytes.len() {
                match bytes[i] {
                    c if isspace_c_locale(c) => {
                        i += 1;
                        break;
                    }
                    b'\\' => {
                        i += 1;
                        if i < bytes.len() {
                            val.push(bytes[i]);
                            i += 1;
                        }
                    }
                    c => {
                        val.push(c);
                        i += 1;
                    }
                }
            }
        }
        let val = String::from_utf8(val).expect("conninfo input is UTF-8");

        let Some(idx) = KNOWN_OPTIONS.iter().position(|k| *k == pname) else {
            return Err(format!("invalid connection option \"{pname}\"\n"));
        };
        let pv = PgString::from_str_in(&val, mcx).map_err(|_| "out of memory\n".to_string())?;
        if let Some(slot) = opts.iter_mut().find(|(i, _)| *i == idx) {
            slot.1 = pv;
        } else {
            opts.push((idx, pv));
        }
    }
    Ok(opts)
}

pub(crate) fn walrcv_check_conninfo(
    mcx: Mcx<'_>,
    conninfo: &str,
    must_use_password: bool,
) -> PgResult<()> {
    let opts = match conninfo_parse(mcx, conninfo) {
        Ok(opts) => opts,
        Err(msg) => {
            return Err(Box::new(
                PgError::error(format!("invalid connection string syntax: {msg}"))
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR),
            ));
        }
    };

    if must_use_password {
        let password_idx = KNOWN_OPTIONS.iter().position(|k| *k == "password").unwrap();
        let uses_password =
            opts.iter().any(|(i, v)| *i == password_idx && !v.as_str().is_empty());
        if !uses_password {
            return Err(Box::new(
                PgError::error("password is required")
                    .with_sqlstate(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED)
                    .with_detail("Non-superusers must provide a password in the connection string."),
            ));
        }
    }
    Ok(())
}

// The pre-connect validation arm that used to live here (walrcv_connect
// stub: libpq port-range checks without networking) moved to its C-parity
// location — walreceiver::client::connect_extended validates the port option
// (PQconnectPoll's try-next-host arm) before any socket is opened, so
// 'port=-1' fails "invalid port number" without a connection attempt on the
// real connect path all subscription commands now use.

#[cfg(test)]
mod tests {
    use super::*;

    fn parse<'m>(mcx: Mcx<'m>, s: &str) -> Result<Vec<(&'static str, String)>, String> {
        let opts = conninfo_parse(mcx, s)?;
        Ok(opts
            .iter()
            .map(|(i, v)| (KNOWN_OPTIONS[*i], v.as_str().to_string()))
            .collect())
    }

    fn get(opts: &[(&str, String)], key: &str) -> Option<String> {
        opts.iter().find(|(k, _)| *k == key).map(|(_, v)| v.clone())
    }

    // conninfo_uri_parse_options (fe-connect.c): the documented URI form
    // with credentials, port, dbname and query parameters.
    #[test]
    fn uri_full_form_parses_like_c() {
        let cx = mcx::MemoryContext::new("conninfo-test");
        let mcx = cx.mcx();
        let opts = parse(
            mcx,
            "postgresql://uri-user:secret@host:12345/mydb?connect_timeout=10&application_name=myapp",
        )
        .unwrap();
        assert_eq!(get(&opts, "user").as_deref(), Some("uri-user"));
        assert_eq!(get(&opts, "password").as_deref(), Some("secret"));
        assert_eq!(get(&opts, "host").as_deref(), Some("host"));
        assert_eq!(get(&opts, "port").as_deref(), Some("12345"));
        assert_eq!(get(&opts, "dbname").as_deref(), Some("mydb"));
        assert_eq!(get(&opts, "connect_timeout").as_deref(), Some("10"));
        assert_eq!(get(&opts, "application_name").as_deref(), Some("myapp"));

        // Short designator, empty components not stored.
        let opts = parse(mcx, "postgres:///mydb").unwrap();
        assert_eq!(get(&opts, "dbname").as_deref(), Some("mydb"));
        assert!(get(&opts, "host").is_none());
        assert!(get(&opts, "user").is_none());
    }

    // Percent-encoding decodes in every component; %00 and bad tokens are
    // catchable errors with libpq's messages.
    #[test]
    fn uri_percent_encoding_matches_c() {
        let cx = mcx::MemoryContext::new("conninfo-test");
        let mcx = cx.mcx();
        let opts = parse(mcx, "postgresql://uri%2Duser@host/db%2Fname").unwrap();
        assert_eq!(get(&opts, "user").as_deref(), Some("uri-user"));
        assert_eq!(get(&opts, "dbname").as_deref(), Some("db/name"));

        let e = parse(mcx, "postgresql://host/db?application_name=a%zzb").unwrap_err();
        assert_eq!(e, "invalid percent-encoded token: \"a%zzb\"\n");
        let e = parse(mcx, "postgresql://host/db?application_name=a%00b").unwrap_err();
        assert_eq!(e, "forbidden value %00 in percent-encoded value: \"a%00b\"\n");
    }

    // Bracketed IPv6 netlocs and comma-separated multi-host lists build the
    // comma-joined host/port values C produces.
    #[test]
    fn uri_ipv6_and_multihost_match_c() {
        let cx = mcx::MemoryContext::new("conninfo-test");
        let mcx = cx.mcx();
        let opts = parse(mcx, "postgresql://[::1]:5433/db").unwrap();
        assert_eq!(get(&opts, "host").as_deref(), Some("::1"));
        assert_eq!(get(&opts, "port").as_deref(), Some("5433"));

        let opts = parse(mcx, "postgresql://h1:5432,h2:5433/db").unwrap();
        assert_eq!(get(&opts, "host").as_deref(), Some("h1,h2"));
        assert_eq!(get(&opts, "port").as_deref(), Some("5432,5433"));

        let e = parse(mcx, "postgresql://[::1/db").unwrap_err();
        assert_eq!(
            e,
            "end of string reached when looking for matching \"]\" in IPv6 host address \
             in URI: \"postgresql://[::1/db\"\n"
        );
        let e = parse(mcx, "postgresql://[]/db").unwrap_err();
        assert_eq!(
            e,
            "IPv6 host address may not be empty in URI: \"postgresql://[]/db\"\n"
        );
        let e = parse(mcx, "postgresql://[::1]x/db").unwrap_err();
        assert_eq!(
            e,
            "unexpected character \"x\" at position 19 in URI (expected \":\" or \"/\"): \
             \"postgresql://[::1]x/db\"\n"
        );
    }

    // Query-parameter quirks: ssl=true JDBC rewrite, unknown parameters,
    // and the =-separator diagnostics.
    #[test]
    fn uri_query_parameter_quirks_match_c() {
        let cx = mcx::MemoryContext::new("conninfo-test");
        let mcx = cx.mcx();
        let opts = parse(mcx, "postgresql://host/db?ssl=true").unwrap();
        assert_eq!(get(&opts, "sslmode").as_deref(), Some("require"));

        let e = parse(mcx, "postgresql://host/db?bogus=x").unwrap_err();
        assert_eq!(e, "invalid URI query parameter: \"bogus\"\n");
        let e = parse(mcx, "postgresql://host/db?foo").unwrap_err();
        assert_eq!(e, "missing key/value separator \"=\" in URI query parameter: \"foo\"\n");
        let e = parse(mcx, "postgresql://host/db?foo=a=b").unwrap_err();
        assert_eq!(e, "extra key/value separator \"=\" in URI query parameter: \"foo\"\n");
    }

    // The keyword=value lane is untouched: still parses and still rejects
    // unknown options.
    #[test]
    fn keyword_value_lane_still_parses() {
        let cx = mcx::MemoryContext::new("conninfo-test");
        let mcx = cx.mcx();
        let opts = parse(mcx, "host=h port=5432 dbname=db").unwrap();
        assert_eq!(get(&opts, "host").as_deref(), Some("h"));
        assert_eq!(get(&opts, "port").as_deref(), Some("5432"));
        assert_eq!(get(&opts, "dbname").as_deref(), Some("db"));
        let e = parse(mcx, "bogus=x").unwrap_err();
        assert_eq!(e, "invalid connection option \"bogus\"\n");
    }
}
