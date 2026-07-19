// libpq conninfo syntax (fe-connect.c conninfo_parse) — DDL validation only;
// error strings keep libpq's trailing '\n' so psql output stays byte-exact.

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

pub(crate) fn conninfo_parse<'mcx>(
    mcx: Mcx<'mcx>,
    conninfo: &str,
) -> Result<PgVec<'mcx, (usize, PgString<'mcx>)>, String> {
    if recognized_connection_string(conninfo) {
        // unported: conninfo URI parse (postgresql:// connection strings)
        return Err("URI-format connection strings are not supported yet".to_string());
    }

    let mut opts: PgVec<'mcx, (usize, PgString<'mcx>)> = PgVec::new_in(mcx);
    let bytes = conninfo.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
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
            if bytes[i].is_ascii_whitespace() {
                name_end = Some(i);
                i += 1;
                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
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
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
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
                    c if c.is_ascii_whitespace() => {
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
