//! psql, ported to Rust for pgrust. Increment 1 scope (see README.md):
//! v3-protocol client (startup/auth/simple/extended/COPY), the interactive
//! REPL with psql's prompt rules, statement extraction, the core
//! meta-commands, and print.c-fidelity result rendering.

#![allow(clippy::too_many_lines)]

mod auth;
mod commands;
mod describe;
mod errmsg;
mod help;
mod input;
mod lexer;
mod print;
mod proto;

use std::collections::HashMap;
use std::io::{IsTerminal, Read, Write};

use lexer::{ScanItem, ScanState};
use print::PrintOptions;
use proto::{Conn, ExecStatus, QueryResult};

/// The psql version whose behavior this port tracks.
pub const PSQL_VERSION: &str = "18.3";

pub struct ConnParams {
    pub host: String,
    pub port: String,
    pub user: String,
    pub dbname: String,
    pub password: Option<String>,
}

pub struct PsqlState {
    pub conn: Option<Conn>,
    pub cparams: ConnParams,
    pub vars: HashMap<String, String>,
    pub popt: PrintOptions,
    pub timing: bool,
    pub quiet: bool,
    pub interactive: bool,
    pub last_error: bool,
    /// wasm raw-fd transport override (fd 3 read / fd 4 write).
    pub fd_transport: Option<(i32, i32)>,
    /// Lines pending from \i etc. are handled by the caller's input stack.
    pub quit: bool,
    pub exit_code: i32,
}

impl PsqlState {
    fn var_bool(&self, name: &str) -> bool {
        matches!(
            self.vars.get(name).map(|s| s.as_str()),
            Some("on") | Some("true") | Some("1") | Some("yes")
        )
    }
}

fn env_or(name: &str, def: &str) -> String {
    std::env::var(name).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| def.to_string())
}

fn os_user() -> String {
    #[cfg(not(target_family = "wasm"))]
    unsafe {
        let pw = libc::getpwuid(libc::geteuid());
        if !pw.is_null() && !(*pw).pw_name.is_null() {
            return std::ffi::CStr::from_ptr((*pw).pw_name).to_string_lossy().into_owned();
        }
    }
    env_or("USER", "postgres")
}

/// Open a transport + run startup for the given params. Returns the live
/// conn or a libpq-shaped error message.
pub fn do_connect(st: &PsqlState, p: &ConnParams) -> Result<Conn, String> {
    let transport: Box<dyn proto::Transport> = if let Some((rfd, wfd)) = st.fd_transport {
        Box::new(proto::FdTransport::new(rfd, wfd))
    } else if p.host.starts_with('/') {
        #[cfg(unix)]
        {
            let path = format!("{}/.s.PGSQL.{}", p.host, p.port);
            match std::os::unix::net::UnixStream::connect(&path) {
                Ok(s) => Box::new(proto::UnixTransport(s)),
                Err(e) => {
                    return Err(format!(
                        "connection to server on socket \"{path}\" failed: {e}"
                    ))
                }
            }
        }
        #[cfg(not(unix))]
        {
            return Err("unix-domain sockets are not supported on this platform".into());
        }
    } else {
        match std::net::TcpStream::connect((p.host.as_str(), p.port.parse().unwrap_or(5432u16))) {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                Box::new(proto::TcpTransport(s))
            }
            Err(e) => {
                return Err(format!(
                    "connection to server at \"{}\", port {} failed: {e}",
                    p.host, p.port
                ))
            }
        }
    };
    let mut conn = Conn::new(transport);
    conn.notice_hook = Box::new(|f| {
        let msg = errmsg::build_message(f, None, false);
        eprint!("{msg}");
        let _ = std::io::stderr().flush();
    });
    let mut params: Vec<(&str, &str)> =
        vec![("user", &p.user), ("database", &p.dbname), ("application_name", "psql")];
    let enc = std::env::var("PGCLIENTENCODING").ok().filter(|v| !v.is_empty());
    if let Some(e) = enc.as_deref() {
        params.push(("client_encoding", e));
    }
    conn.startup(&params, &p.user, p.password.as_deref()).map_err(|e| {
        let prefix = if p.host.starts_with('/') {
            format!(
                "connection to server on socket \"{}/.s.PGSQL.{}\" failed: ",
                p.host, p.port
            )
        } else {
            format!("connection to server at \"{}\", port {} failed: ", p.host, p.port)
        };
        // Server-sent errors already carry "FATAL:" etc.; put them on the
        // libpq shape.
        format!("{prefix}{e}")
    })?;
    Ok(conn)
}

// ------------------------------------------------------------ query results

fn field_align(type_oid: u32) -> char {
    // printquery.c column alignment: numerics are right-aligned.
    match type_oid {
        20 | 21 | 23 | 26 | 28 | 29 | 700 | 701 | 790 | 1700 | 5069 => 'r',
        _ => 'l',
    }
}

pub fn result_to_table(r: &QueryResult) -> print::Table {
    print::Table {
        title: None,
        headers: r.fields.iter().map(|f| f.name.clone()).collect(),
        aligns: r.fields.iter().map(|f| field_align(f.type_oid)).collect(),
        cells: r
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|c| {
                        c.as_ref().map(|b| String::from_utf8_lossy(b).into_owned())
                    })
                    .collect()
            })
            .collect(),
        footers: None,
    }
}

/// PrintTiming (common.c).
fn print_timing(elapsed_ms: f64) {
    if elapsed_ms < 1000.0 {
        println!("Time: {elapsed_ms:.3} ms");
        return;
    }
    let seconds = elapsed_ms / 1000.0;
    if seconds < 60.0 {
        let minutes = (seconds / 60.0) as i64;
        println!("Time: {elapsed_ms:.3} ms ({:02}:{:06.3})", minutes, seconds - (minutes as f64) * 60.0);
        return;
    }
    let minutes = (seconds / 60.0) as i64;
    let seconds = seconds - (minutes as f64) * 60.0;
    if minutes < 60 {
        println!("Time: {elapsed_ms:.3} ms ({minutes:02}:{seconds:06.3})");
        return;
    }
    let hours = minutes / 60;
    let minutes = minutes % 60;
    if hours < 24 {
        println!("Time: {elapsed_ms:.3} ms ({hours:02}:{minutes:02}:{seconds:06.3})");
    } else {
        let days = hours / 24;
        let hours = hours % 24;
        println!(
            "Time: {elapsed_ms:.3} ms ({days:.0} d {hours:02}:{minutes:02}:{seconds:06.3})"
        );
    }
}

fn print_notifications(conn: &mut Conn) {
    while let Some(n) = conn.notifies.pop_front() {
        if n.extra.is_empty() {
            println!(
                "Asynchronous notification \"{}\" received from server process with PID {}.",
                n.channel, n.be_pid
            );
        } else {
            println!(
                "Asynchronous notification \"{}\" with payload \"{}\" received from server process with PID {}.",
                n.channel, n.extra, n.be_pid
            );
        }
    }
    let _ = std::io::stdout().flush();
}

/// Send one SQL statement (simple protocol) and render every result, as
/// psql's SendQuery/SendQueryAndProcessResults does. Returns success.
pub fn send_query(st: &mut PsqlState, query: &str, input: &mut input::InputStack) -> bool {
    let Some(conn) = st.conn.as_mut() else {
        eprintln!("You are currently not connected to a database.");
        return false;
    };
    let start = std::time::Instant::now();
    if let Err(e) = conn.send_query(query) {
        eprintln!("{e}");
        st.conn = None;
        return false;
    }
    let mut ok = true;
    let mut suppress_next_tag = false;
    loop {
        match conn.get_result() {
            Err(e) => {
                // Connection-level failure.
                eprintln!("{e}");
                let dead = conn.is_dead();
                if dead {
                    st.conn = None;
                    if !st.interactive {
                        st.quit = true;
                        st.exit_code = 2;
                    } else {
                        eprintln!("The connection to the server was lost. Attempting reset: Failed.");
                    }
                }
                ok = false;
                break;
            }
            Ok(None) => break,
            Ok(Some(r)) => match r.status {
                ExecStatus::TuplesOk => {
                    let t = result_to_table(&r);
                    let mut out = std::io::stdout();
                    let _ = print::print_table(&t, &st.popt, &mut out);
                    let _ = out.flush();
                    // psql also prints the command tag after RETURNING
                    // result sets (PrintQueryResult, common.c).
                    if !st.quiet
                        && ["INSERT ", "UPDATE ", "DELETE ", "MERGE "]
                            .iter()
                            .any(|p| r.cmd_tag.starts_with(p))
                    {
                        println!("{}", r.cmd_tag);
                        let _ = std::io::stdout().flush();
                    }
                }
                ExecStatus::CommandOk => {
                    if !st.quiet && !st.popt.tuples_only && !r.cmd_tag.is_empty() && !suppress_next_tag {
                        println!("{}", r.cmd_tag);
                        let _ = std::io::stdout().flush();
                    }
                    suppress_next_tag = false;
                }
                ExecStatus::Empty => {}
                ExecStatus::Error => {
                    let diag = r.diag.as_ref();
                    let msg = match diag {
                        Some(f) => errmsg::build_message(f, Some(query), true),
                        None => format!("{}\n", r.conn_err),
                    };
                    eprint!("{msg}");
                    let _ = std::io::stderr().flush();
                    ok = false;
                }
                ExecStatus::CopyIn => {
                    handle_copy_in(conn, input);
                }
                ExecStatus::CopyOut => {
                    handle_copy_out(conn);
                    suppress_next_tag = true;
                }
            },
        }
    }
    if st.timing {
        print_timing(start.elapsed().as_secs_f64() * 1000.0);
    }
    if let Some(conn) = st.conn.as_mut() {
        print_notifications(conn);
    }
    st.last_error = !ok;
    ok
}

fn handle_copy_in(conn: &mut Conn, input: &mut input::InputStack) {
    // Read data lines from the current input source until "\." or EOF.
    loop {
        match input.read_line_raw() {
            None => {
                let _ = conn.copy_put_done();
                break;
            }
            Some(line) => {
                if line == "\\." {
                    let _ = conn.copy_put_done();
                    break;
                }
                let mut data = line.into_bytes();
                data.push(b'\n');
                let _ = conn.copy_put_data(&data);
            }
        }
    }
}

fn handle_copy_out(conn: &mut Conn) {
    let mut out = std::io::stdout();
    loop {
        match conn.copy_get_data() {
            Ok(proto::CopyMsg::Data(d)) => {
                let _ = out.write_all(&d);
            }
            Ok(proto::CopyMsg::Done) => break,
            Ok(proto::CopyMsg::Error(f)) => {
                let msg = errmsg::build_message(&f, None, true);
                eprint!("{msg}");
                break;
            }
            Err(e) => {
                eprintln!("{e}");
                break;
            }
        }
    }
    let _ = out.flush();
}

// ------------------------------------------------------------------ prompts

fn prompt(st: &PsqlState, scan: &ScanState, first: bool) -> String {
    let db = st.cparams.dbname.clone();
    let txn = match st.conn.as_ref().map(|c| c.txn_status).unwrap_or(b'I') {
        b'T' => "*",
        b'E' => "!",
        _ => "",
    };
    let superuser = st
        .conn
        .as_ref()
        .and_then(|c| c.parameter_status("is_superuser"))
        .map(|v| v == "on")
        .unwrap_or(false);
    let mark = if superuser { "#" } else { ">" };
    if first {
        format!("{db}={txn}{mark} ")
    } else {
        format!("{db}{}{txn}{mark} ", scan.prompt2_char())
    }
}

// --------------------------------------------------------------------- main

fn main() {
    let mut args = std::env::args().skip(1);
    let mut host = env_or("PGHOST", "/tmp");
    let mut port = env_or("PGPORT", "5432");
    let mut user = env_or("PGUSER", &os_user());
    let mut dbname = std::env::var("PGDATABASE").ok().filter(|v| !v.is_empty());
    let mut commands: Vec<String> = Vec::new();
    let mut files: Vec<String> = Vec::new();
    let mut quiet = false;
    let mut set_vars: Vec<(String, String)> = Vec::new();
    let mut popt = PrintOptions::default();
    let mut fd_transport: Option<(i32, i32)> = None;
    let mut positional: Vec<String> = Vec::new();

    while let Some(a) = args.next() {
        let need = |args: &mut dyn Iterator<Item = String>| -> String {
            args.next().unwrap_or_else(|| {
                eprintln!("psql: error: missing argument");
                std::process::exit(1);
            })
        };
        match a.as_str() {
            "-h" | "--host" => host = need(&mut args),
            "-p" | "--port" => port = need(&mut args),
            "-U" | "--username" => user = need(&mut args),
            "-d" | "--dbname" => dbname = Some(need(&mut args)),
            "-c" | "--command" => commands.push(need(&mut args)),
            "-f" | "--file" => files.push(need(&mut args)),
            "-X" | "--no-psqlrc" => {}
            "-q" | "--quiet" => quiet = true,
            "-t" | "--tuples-only" => popt.tuples_only = true,
            "-A" | "--no-align" => popt.format = print::FORMAT_UNALIGNED,
            "-x" | "--expanded" => popt.expanded = true,
            "-v" | "--set" | "--variable" => {
                let kv = need(&mut args);
                match kv.split_once('=') {
                    Some((k, v)) => set_vars.push((k.to_string(), v.to_string())),
                    None => set_vars.push((kv, String::new())),
                }
            }
            "--fd-wire" => {
                // Raw-fd/pipe transport: "R,W" fd numbers. Default 4,5 —
                // NOT 3: in the wasm host (tools/wasm-web/pgrust-wasi.js)
                // fd 3 is the WASI "/" preopen directory.
                let v = need(&mut args);
                let (r, w) = v.split_once(',').unwrap_or(("4", "5"));
                fd_transport = Some((r.parse().unwrap_or(4), w.parse().unwrap_or(5)));
            }
            "--version" | "-V" => {
                println!("psql (PostgreSQL) {PSQL_VERSION} (pgrust)");
                return;
            }
            "--help" => {
                println!("psql is the PostgreSQL interactive terminal (pgrust port).\n\nUsage:\n  psql [OPTION]... [DBNAME [USERNAME]]\n\nRun \\? inside psql for meta-command help.");
                return;
            }
            _ if a.starts_with('-') && a.len() > 1 => {
                eprintln!("psql: error: unrecognized option \"{a}\"");
                std::process::exit(1);
            }
            _ => positional.push(a),
        }
    }
    if let Some(p) = positional.first() {
        if dbname.is_none() {
            dbname = Some(p.clone());
        }
    }
    if let Some(p) = positional.get(1) {
        user = p.clone();
    }
    let dbname = dbname.unwrap_or_else(|| user.clone());

    // wasm32-wasip1 has no sockets: default to the raw-fd transport on
    // fd 4 (read from server) / fd 5 (write to server); the host
    // cross-connects these to the server instance's stdout/stdin.
    #[cfg(target_family = "wasm")]
    let fd_transport = Some(fd_transport.unwrap_or((4, 5)));

    let cparams = ConnParams {
        host,
        port,
        user,
        dbname,
        password: std::env::var("PGPASSWORD").ok().filter(|v| !v.is_empty()),
    };

    // PSQL_INTERACTIVE overrides isatty where the host's fd report is absent
    // or wrong (wasm hosts report stdin as a character device even when the
    // input is a piped script): "1" forces interactive (prompts, banner),
    // "0" forces script mode; unset defers to isatty.
    let tty = match std::env::var("PSQL_INTERACTIVE").as_deref() {
        Ok("1") => true,
        Ok("0") => false,
        _ => std::io::stdin().is_terminal(),
    };
    let interactive = commands.is_empty() && files.is_empty() && tty;

    let mut st = PsqlState {
        conn: None,
        cparams,
        vars: HashMap::new(),
        popt,
        timing: false,
        quiet,
        interactive,
        last_error: false,
        fd_transport,
        quit: false,
        exit_code: 0,
    };
    for (k, v) in set_vars {
        st.vars.insert(k, v);
    }

    match do_connect(&st, &st.cparams) {
        Ok(c) => st.conn = Some(c),
        Err(e) => {
            eprintln!("psql: error: {e}");
            std::process::exit(2);
        }
    }

    if st.interactive && !st.quiet {
        let sv = st
            .conn
            .as_ref()
            .and_then(|c| c.parameter_status("server_version"))
            .unwrap_or("");
        if server_major(sv) == client_major() {
            println!("psql ({PSQL_VERSION})");
        } else {
            println!("psql ({PSQL_VERSION}, server {sv})");
        }
        println!("Type \"help\" for help.");
        println!();
    }

    // -c commands: each is sent as ONE simple query (psql -c semantics).
    if !commands.is_empty() {
        let mut input = input::InputStack::empty();
        let mut rc = 0;
        for c in &commands {
            if let Some(rest) = c.trim_start().strip_prefix('\\') {
                let mut scan = ScanState::new();
                commands::exec_meta(&mut st, rest, &mut input, &mut scan);
            } else if !send_query(&mut st, c, &mut input) {
                rc = 1;
            }
            if st.quit {
                break;
            }
        }
        if let Some(mut c) = st.conn.take() {
            c.terminate();
        }
        std::process::exit(if st.exit_code != 0 { st.exit_code } else { rc });
    }

    // -f files then stdin.
    let mut input = if files.is_empty() {
        input::InputStack::stdin()
    } else {
        match input::InputStack::files(&files) {
            Ok(i) => i,
            Err(e) => {
                eprintln!("psql: error: {e}");
                std::process::exit(1);
            }
        }
    };

    main_loop(&mut st, &mut input);

    if let Some(mut c) = st.conn.take() {
        c.terminate();
    }
    std::process::exit(st.exit_code);
}

fn server_major(v: &str) -> i32 {
    v.split(|c: char| !c.is_ascii_digit()).next().and_then(|s| s.parse().ok()).unwrap_or(0)
}

fn client_major() -> i32 {
    PSQL_VERSION.split('.').next().and_then(|s| s.parse().ok()).unwrap_or(0)
}

fn main_loop(st: &mut PsqlState, input: &mut input::InputStack) {
    let mut scan = ScanState::new();
    loop {
        if st.quit {
            break;
        }
        // Track standard_conforming_strings from the server.
        if let Some(c) = st.conn.as_ref() {
            scan.standard_strings =
                c.parameter_status("standard_conforming_strings").map(|v| v == "on").unwrap_or(true);
        }
        let line = if st.interactive {
            let p = prompt(st, &scan, scan.buffer_empty() && scan.buf.is_empty());
            input.read_line_interactive(&p)
        } else {
            input.read_line_raw()
        };
        let Some(line) = line else {
            // EOF: a non-empty query buffer is executed even without its
            // terminating semicolon (psql MainLoop's EOF arm).
            if !scan.buffer_empty() {
                let q = std::mem::take(&mut scan.buf);
                send_query(st, &q, input);
            }
            if input.pop() {
                continue;
            }
            break;
        };

        // Interactive convenience words (psql: only when buffer is empty).
        if st.interactive && scan.buf.is_empty() {
            let t = line.trim();
            if t == "quit" || t == "exit" {
                break;
            }
            if t == "help" {
                println!("You are using psql, the command-line interface to PostgreSQL.");
                println!("Type:  \\copyright for distribution terms");
                println!("       \\h for help with SQL commands");
                println!("       \\? for help with psql commands");
                println!("       \\g or terminate with semicolon to execute query");
                println!("       \\q to quit");
                continue;
            }
        }

        let vars = st.vars.clone();
        for item in scan.scan_line(&line, &vars) {
            match item {
                ScanItem::Statement(s) => {
                    send_query(st, &s, input);
                    if st.var_bool("ON_ERROR_STOP") && st.last_error {
                        st.quit = true;
                        st.exit_code = 3;
                    }
                }
                ScanItem::Backslash(rest) => {
                    commands::exec_meta(st, &rest, input, &mut scan);
                }
            }
            if st.quit {
                break;
            }
        }
    }
}
