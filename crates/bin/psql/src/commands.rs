//! Meta-command (backslash command) parsing and execution, mirroring psql's
//! command.c semantics for the increment-1 command set:
//!   \q \c[onnect] \conninfo \l \d \dt \di \dv \ds \df \dn \? \timing \x \a
//!   \echo \i \set \unset
//! Everything else gets psql's "invalid command" error; the deliberately
//! unimplemented psql features (\copy, \gexec, crosstabview, \h, large
//! objects) get a clean "not supported" message instead of a half port.

use crate::input::InputStack;
use crate::lexer::ScanState;
use crate::{describe, do_connect, help, print, ConnParams, PsqlState};

/// One parsed option token, with psql's OT_NORMAL quoting: single quotes
/// stripped (with escape processing), double quotes KEPT (patterns need
/// them), :var expanded outside quotes.
fn scan_option(rest: &mut &str, vars: &std::collections::HashMap<String, String>) -> Option<String> {
    let s = rest.trim_start();
    if s.is_empty() || s.starts_with('\\') {
        *rest = s;
        return None;
    }
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0usize;
    let n = chars.len();
    let mut out = String::new();
    while i < n {
        let c = chars[i];
        match c {
            ' ' | '\t' => break,
            '\\' => break, // next backslash command starts here
            '\'' => {
                i += 1;
                while i < n {
                    let q = chars[i];
                    if q == '\'' {
                        if i + 1 < n && chars[i + 1] == '\'' {
                            out.push('\'');
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    } else if q == '\\' && i + 1 < n {
                        // C-style escapes inside single quotes.
                        let e = chars[i + 1];
                        out.push(match e {
                            'n' => '\n',
                            't' => '\t',
                            'r' => '\r',
                            'b' => '\u{8}',
                            'f' => '\u{c}',
                            _ => e,
                        });
                        i += 2;
                    } else {
                        out.push(q);
                        i += 1;
                    }
                }
            }
            '"' => {
                // Copied verbatim INCLUDING the quotes.
                out.push('"');
                i += 1;
                while i < n {
                    let q = chars[i];
                    out.push(q);
                    i += 1;
                    if q == '"' {
                        break;
                    }
                }
            }
            ':' if i + 1 < n && (chars[i + 1].is_alphanumeric() || chars[i + 1] == '_') => {
                let mut j = i + 1;
                while j < n && (chars[j].is_alphanumeric() || chars[j] == '_') {
                    j += 1;
                }
                let name: String = chars[i + 1..j].iter().collect();
                if let Some(v) = vars.get(&name) {
                    out.push_str(v);
                    i = j;
                } else {
                    out.push(':');
                    i += 1;
                }
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    *rest = &s[chars[..i].iter().map(|c| c.len_utf8()).sum::<usize>()..];
    Some(out)
}

/// Execute one backslash line (without the leading '\'). May contain a
/// chained further command after this one's arguments.
pub fn exec_meta(st: &mut PsqlState, line: &str, input: &mut InputStack, scan: &mut ScanState) {
    let mut rest = line;
    // Command word: everything up to whitespace or backslash.
    let cmd_end = rest
        .char_indices()
        .find(|(_, c)| c.is_whitespace() || *c == '\\')
        .map(|(i, _)| i)
        .unwrap_or(rest.len());
    let cmd = &rest[..cmd_end];
    rest = &rest[cmd_end..];
    if cmd.is_empty() {
        // "\\" alone: psql's separator; nothing to do.
        return;
    }
    let vars = st.vars.clone();
    let mut opts: Vec<String> = Vec::new();
    let mut opt_rest = rest;
    // Pre-scan all options (commands consume what they need).
    loop {
        let before = opt_rest;
        match scan_option(&mut opt_rest, &vars) {
            Some(o) => opts.push(o),
            None => {
                opt_rest = before.trim_start();
                break;
            }
        }
    }
    let chained = opt_rest.strip_prefix('\\').map(|s| s.to_string());

    dispatch(st, cmd, &opts, rest.trim_start(), input, scan);

    if let Some(next) = chained {
        if !next.is_empty() {
            exec_meta(st, &next, input, scan);
        }
    }
}

fn on_off(b: bool) -> &'static str {
    if b {
        "on"
    } else {
        "off"
    }
}

fn parse_bool(v: &str, name: &str, cur: bool) -> Option<bool> {
    match v.to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Some(true),
        "off" | "false" | "no" | "0" => Some(false),
        _ => {
            eprintln!("unrecognized value \"{v}\" for \"{name}\": Boolean expected");
            let _ = cur;
            None
        }
    }
}

fn dispatch(
    st: &mut PsqlState,
    cmd: &str,
    opts: &[String],
    raw_rest: &str,
    input: &mut InputStack,
    scan: &mut ScanState,
) {
    match cmd {
        "q" | "quit" => st.quit = true,
        "x" => {
            match opts.first() {
                None => st.popt.expanded = !st.popt.expanded,
                Some(v) => {
                    if let Some(b) = parse_bool(v, "\\x", st.popt.expanded) {
                        st.popt.expanded = b;
                    } else {
                        st.last_error = true;
                        return;
                    }
                }
            }
            if !st.quiet {
                println!("Expanded display is {}.", on_off(st.popt.expanded));
            }
        }
        "a" => {
            if st.popt.format == print::FORMAT_ALIGNED {
                st.popt.format = print::FORMAT_UNALIGNED;
            } else {
                st.popt.format = print::FORMAT_ALIGNED;
            }
            if !st.quiet {
                if st.popt.format == print::FORMAT_ALIGNED {
                    println!("Output format is aligned.");
                } else {
                    println!("Output format is unaligned.");
                }
            }
        }
        "t" => {
            match opts.first() {
                None => st.popt.tuples_only = !st.popt.tuples_only,
                Some(v) => match parse_bool(v, "\\t", st.popt.tuples_only) {
                    Some(b) => st.popt.tuples_only = b,
                    None => {
                        st.last_error = true;
                        return;
                    }
                },
            }
            if !st.quiet {
                println!("Tuples only is {}.", on_off(st.popt.tuples_only));
            }
        }
        "timing" => {
            match opts.first() {
                None => st.timing = !st.timing,
                Some(v) => match parse_bool(v, "\\timing", st.timing) {
                    Some(b) => st.timing = b,
                    None => {
                        st.last_error = true;
                        return;
                    }
                },
            }
            if !st.quiet {
                println!("Timing is {}.", on_off(st.timing));
            }
        }
        "echo" => {
            // \echo [-n] args...  Note: uses the RAW remainder split into
            // the already-scanned options.
            let mut items: Vec<&str> = opts.iter().map(|s| s.as_str()).collect();
            let mut newline = true;
            if items.first() == Some(&"-n") {
                newline = false;
                items.remove(0);
            }
            let text = items.join(" ");
            if newline {
                println!("{text}");
            } else {
                print!("{text}");
            }
            let _ = std::io::Write::flush(&mut std::io::stdout());
            let _ = raw_rest;
        }
        "set" => match opts.len() {
            0 => {
                let mut names: Vec<&String> = st.vars.keys().collect();
                names.sort();
                for n in names {
                    println!("{n} = '{}'", st.vars[n]);
                }
            }
            _ => {
                let name = opts[0].clone();
                let value = opts[1..].concat();
                st.vars.insert(name, value);
            }
        },
        "unset" => match opts.first() {
            None => {
                eprintln!("\\unset: missing required argument");
                st.last_error = true;
            }
            Some(n) => {
                st.vars.remove(n.as_str());
            }
        },
        "i" | "include" => match opts.first() {
            None => {
                eprintln!("\\i: missing required argument");
                st.last_error = true;
            }
            Some(f) => {
                if let Err(e) = input.push_file(f) {
                    eprintln!("{e}");
                    st.last_error = true;
                }
            }
        },
        "?" => {
            let db = if st.conn.is_some() { Some(st.cparams.dbname.as_str()) } else { None };
            print!("{}", help::slash_usage(db));
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        "conninfo" => {
            describe::conninfo(st);
        }
        "c" | "connect" => {
            do_connect_meta(st, opts);
        }
        "l" | "list" | "l+" | "list+" => {
            let plus = cmd.ends_with('+');
            if let Err(e) = describe::list_databases(st, opts.first().map(|s| s.as_str()), plus) {
                eprintln!("{e}");
                st.last_error = true;
            }
        }
        "d" | "d+" | "dt" | "dt+" | "di" | "di+" | "dv" | "dv+" | "ds" | "ds+" | "dn"
        | "dn+" | "df" | "df+" => {
            let plus = cmd.ends_with('+');
            let base = cmd.trim_end_matches('+');
            let pattern = opts.first().map(|s| s.as_str());
            let r = match base {
                "d" => describe::d_command(st, pattern, plus),
                "dn" => describe::list_schemas(st, pattern, plus),
                "df" => describe::list_functions(st, pattern, plus),
                _ => {
                    let kinds = &base[1..]; // t / i / v / s
                    describe::list_relations(st, kinds, pattern, plus)
                }
            };
            if let Err(e) = r {
                eprintln!("{e}");
                st.last_error = true;
            }
        }
        // Deliberately-unsupported psql features (documented in README):
        // clean message, no half implementation.
        "copy" | "crosstabview" | "gexec" | "gx" | "h" | "help" | "lo_import"
        | "lo_export" | "lo_list" | "lo_unlink" => {
            eprintln!("\\{cmd}: not supported by this psql port yet");
            st.last_error = true;
        }
        _ => {
            eprintln!("invalid command \\{cmd}");
            if st.interactive {
                eprintln!("Try \\? for help.");
            }
            st.last_error = true;
        }
    }
    let _ = scan;
}

fn do_connect_meta(st: &mut PsqlState, opts: &[String]) {
    // \c [dbname [user [host [port]]]]; '-' keeps the current value.
    let cur = &st.cparams;
    let pick = |i: usize, cur: &str| -> String {
        match opts.get(i).map(|s| s.as_str()) {
            None | Some("-") => cur.to_string(),
            Some(v) => v.trim_matches('"').to_string(),
        }
    };
    let newp = ConnParams {
        dbname: pick(0, &cur.dbname),
        user: pick(1, &cur.user),
        host: pick(2, &cur.host),
        port: pick(3, &cur.port),
        password: cur.password.clone(),
    };
    let host_changed = newp.host != cur.host || newp.port != cur.port;
    // Raw-fd (wasm) transport: there is only ONE pipe pair to the server, so
    // the old session must Terminate BEFORE the new startup goes out (the
    // host respawns a server instance on the same VFS). psql's native order
    // (connect first, keep old on failure) is impossible there; on failure
    // the connection is simply gone — documented divergence.
    if st.fd_transport.is_some() {
        if let Some(mut old) = st.conn.take() {
            old.terminate();
        }
    }
    match do_connect(st, &newp) {
        Ok(conn) => {
            if let Some(mut old) = st.conn.take() {
                old.terminate();
            }
            st.conn = Some(conn);
            st.cparams = newp;
            if !st.quiet {
                if host_changed {
                    if st.cparams.host.starts_with('/') {
                        println!(
                            "You are now connected to database \"{}\" as user \"{}\" via socket in \"{}\" at port \"{}\".",
                            st.cparams.dbname, st.cparams.user, st.cparams.host, st.cparams.port
                        );
                    } else {
                        println!(
                            "You are now connected to database \"{}\" as user \"{}\" on host \"{}\" at port \"{}\".",
                            st.cparams.dbname, st.cparams.user, st.cparams.host, st.cparams.port
                        );
                    }
                } else {
                    println!(
                        "You are now connected to database \"{}\" as user \"{}\".",
                        st.cparams.dbname, st.cparams.user
                    );
                }
            }
        }
        Err(e) => {
            eprintln!("\\connect: {e}");
            st.last_error = true;
            if !st.interactive {
                // Non-interactive \connect failure is fatal, exit status 2
                // (psql do_connect).
                st.quit = true;
                st.exit_code = 2;
                if let Some(mut old) = st.conn.take() {
                    old.terminate();
                }
            } else if st.fd_transport.is_some() {
                // The single pipe pair means the old session is already
                // gone; emulate "previous connection kept" by reconnecting
                // to the previous parameters (fresh session, same server
                // state on the shared VFS).
                let orig = ConnParams {
                    host: st.cparams.host.clone(),
                    port: st.cparams.port.clone(),
                    user: st.cparams.user.clone(),
                    dbname: st.cparams.dbname.clone(),
                    password: st.cparams.password.clone(),
                };
                match do_connect(st, &orig) {
                    Ok(conn) => {
                        st.conn = Some(conn);
                        eprintln!("Previous connection kept");
                    }
                    Err(e2) => eprintln!("\\connect: {e2}"),
                }
            } else {
                eprintln!("Previous connection kept");
            }
        }
    }
}
