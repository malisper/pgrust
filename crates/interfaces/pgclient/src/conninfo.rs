// PQconninfoParse's scanner plus the defaults ladder (explicit > service
// file > environment > compiled default). Error strings are user-visible
// through dblink and must match libpq byte-for-byte.

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

pub fn parse_conninfo(s: &str) -> Result<Vec<(String, String)>, String> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut opts: Vec<(String, String)> = Vec::new();
    loop {
        while i < b.len() && (b[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() {
            return Ok(opts);
        }
        let kstart = i;
        while i < b.len() && b[i] != b'=' && !(b[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        let key = s[kstart..i].to_string();
        while i < b.len() && (b[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() || b[i] != b'=' {
            return Err(format!(
                "missing \"=\" after \"{key}\" in connection info string"
            ));
        }
        i += 1;
        while i < b.len() && (b[i] as char).is_ascii_whitespace() {
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
            while i < b.len() && !(b[i] as char).is_ascii_whitespace() {
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
        let line = line.trim_matches(|c: char| c.is_ascii_whitespace());
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
