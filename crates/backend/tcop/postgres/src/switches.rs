// process_postgres_switches (postgres.c:3790). The client-argv arm is the hot
// caller (postinit startup options); the secure arm (ctx == PGC_POSTMASTER)
// is the single-user command line, with the -E/-j/-r/-D/-b/-v specials and
// the trailing DBNAME pickup.

use std::cell::RefCell;

use ::types_error::{PgResult, ERRCODE_SYNTAX_ERROR, ERROR, FATAL};
use elog::ereport;
use types_guc::{GucContext, GucSource};

use crate::{
    get_stats_option_name, guc_context_from_u8, loc, set_debug_options,
    set_plan_disabling_options,
};

const ARG_TAKING_FLAGS: &[u8] = b"BCcDdfhkNprStvW-";

thread_local! {
    // userDoption (postgres.c:106): -D, consumed by SelectConfigFiles in
    // PostgresSingleUserMain.
    static USER_D_OPTION: RefCell<Option<String>> = const { RefCell::new(None) };
}

pub(crate) fn user_d_option() -> Option<String> {
    USER_D_OPTION.with_borrow(|v| v.clone())
}

fn c_atoi(s: &str) -> i32 {
    let t = s.trim_start();
    let (sign, digits) = match t.as_bytes().first() {
        Some(b'-') => (-1i64, &t[1..]),
        Some(b'+') => (1, &t[1..]),
        _ => (1, t),
    };
    let mut v: i64 = 0;
    for b in digits.bytes().take_while(|b| b.is_ascii_digit()) {
        v = (v * 10 + (b - b'0') as i64).min(i32::MAX as i64 + 1);
    }
    (sign * v).clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

fn is_dispatch_option(name: &str) -> bool {
    // DispatchOptionNames (main.c) minus forkchild (EXEC_BACKEND only).
    let bare = name.split('=').next().unwrap_or(name);
    // + the pgrust-extension stdio-wire / sim-net modes (main_main dispatch).
    matches!(bare, "check" | "boot" | "describe-config" | "single" | "stdio-wire" | "sim-net")
}

pub fn process_postgres_switches(argv: &[String], gucctx: u8) -> PgResult<()> {
    process_postgres_switches_inner(argv, gucctx, None)
}

/// The &dbname form (single-user): first bare argument becomes the database
/// name instead of an error.
pub fn process_postgres_switches_dbname(
    argv: &[String],
    gucctx: u8,
    dbname: &mut Option<String>,
) -> PgResult<()> {
    process_postgres_switches_inner(argv, gucctx, Some(dbname))
}

fn process_postgres_switches_inner(
    argv: &[String],
    gucctx: u8,
    mut dbname: Option<&mut Option<String>>,
) -> PgResult<()> {
    let ctx = guc_context_from_u8(gucctx);
    let secure = ctx == GucContext::PGC_POSTMASTER;
    let source = if secure { GucSource::PGC_S_ARGV } else { GucSource::PGC_S_CLIENT };
    let fname = "process_postgres_switches";

    let set = |name: &str, value: &str| guc::SetConfigOption(name, Some(value), ctx, source);

    let mut errs = 0usize;
    let mut i = 1usize;
    // Ignore the initial --single (or pgrust --stdio-wire / --sim-net)
    // argument, if present.
    if secure
        && argv
            .get(1)
            .is_some_and(|a| a == "--single" || a == "--stdio-wire" || a == "--sim-net")
    {
        i = 2;
    }
    let mut bad: Option<&str> = None;
    'outer: while i < argv.len() && errs == 0 {
        let tok = argv[i].as_str();
        i += 1;
        let b = tok.as_bytes();
        if b.len() < 2 || b[0] != b'-' {
            // Optional database name: only when the caller passed the out
            // param (single-user) and it is still unset; getopt would have
            // stopped at the first non-option, so trailing args past it are
            // the argc != optind error below.
            if let Some(dbname) = dbname.as_deref_mut() {
                if dbname.is_none() {
                    *dbname = Some(tok.to_string());
                    if i < argv.len() {
                        bad = Some(argv[i].as_str());
                        break;
                    }
                    continue;
                }
            }
            bad = Some(tok);
            break;
        }
        if tok == "--" {
            if i < argv.len() {
                bad = Some(argv[i].as_str());
            }
            break;
        }

        let mut chars = &tok[1..];
        while !chars.is_empty() {
            let flag = chars.as_bytes()[0];
            chars = &chars[1..];
            let optarg: &str;
            if ARG_TAKING_FLAGS.contains(&flag) {
                if !chars.is_empty() {
                    optarg = chars;
                } else if i < argv.len() && flag != b'-' {
                    optarg = argv[i].as_str();
                    i += 1;
                } else {
                    errs += 1;
                    bad = Some(tok);
                    continue 'outer;
                }
                chars = "";
            } else {
                optarg = "";
            }

            match flag {
                b'B' => set("shared_buffers", optarg)?,
                // 'C'/'n'/'T' are always ignored (consistency with the
                // postmaster); the rest are secure-only.
                b'C' | b'n' | b'T' => {}
                b'b' => {
                    /* Undocumented flag used for binary upgrades */
                    if secure {
                        init_small::globals::SetIsBinaryUpgrade(true);
                    }
                }
                b'D' => {
                    if secure {
                        USER_D_OPTION.with_borrow_mut(|v| *v = Some(optarg.to_string()));
                    }
                }
                b'E' => {
                    if secure {
                        crate::set_echo_query(true);
                    }
                }
                b'j' => {
                    if secure {
                        crate::set_use_semi_newline_newline(true);
                    }
                }
                b'r' => {
                    /* send output (stdout and stderr) to the given file */
                    if secure {
                        let mut buf = [0u8; types_core::MAXPGPATH];
                        let n = optarg.len().min(types_core::MAXPGPATH - 1);
                        buf[..n].copy_from_slice(&optarg.as_bytes()[..n]);
                        init_small::globals::SetOutputFileName(buf);
                        // C has one OutputFileName global; this tree has two
                        // slots (elog can't depend on init_small). The only
                        // consumer, elog::DebugFileOpen (BaseInit), reads the
                        // elog mirror — keep it in sync, same truncation.
                        elog::config::set_output_file_name(Some(
                            String::from_utf8_lossy(&buf[..n]).into_owned(),
                        ));
                    }
                }
                // -v (FrontendProtocol override): kept by C only for a
                // hypothetical FE/BE-protocol standalone mode; storage exists
                // (init_small::globals::SetFrontendProtocol) but nothing
                // standalone reads it, so it is deliberately parsed and
                // dropped here.
                b'v' => {}
                b'-' | b'c' => {
                    if flag == b'-' && is_dispatch_option(optarg) {
                        return ereport(ERROR)
                            .errcode(ERRCODE_SYNTAX_ERROR)
                            .errmsg(format!("--{optarg} must be first argument"))
                            .finish(loc(3975, fname));
                    }
                    let (name, value) = guc::ParseLongOption(optarg);
                    let Some(value) = value else {
                        let msg = if flag == b'-' {
                            format!("--{optarg} requires a value")
                        } else {
                            format!("-c {optarg} requires a value")
                        };
                        return ereport(ERROR)
                            .errcode(ERRCODE_SYNTAX_ERROR)
                            .errmsg(msg)
                            .finish(loc(3994, fname));
                    };
                    set(&name, &value)?;
                }
                b'd' => set_debug_options(c_atoi(optarg), gucctx)?,
                b'e' => set("datestyle", "euro")?,
                b'F' => set("fsync", "false")?,
                b'f' => {
                    if !set_plan_disabling_options(optarg, gucctx)? {
                        errs += 1;
                        bad = Some(tok);
                    }
                }
                b'h' => set("listen_addresses", optarg)?,
                b'i' => set("listen_addresses", "*")?,
                b'k' => set("unix_socket_directories", optarg)?,
                b'l' => set("ssl", "true")?,
                b'N' => set("max_connections", optarg)?,
                b'O' => set("allow_system_table_mods", "true")?,
                b'P' => set("ignore_system_indexes", "true")?,
                b'p' => set("port", optarg)?,
                b'S' => set("work_mem", optarg)?,
                b's' => set("log_statement_stats", "true")?,
                b't' => match get_stats_option_name(optarg) {
                    Some(name) => set(name, "true")?,
                    None => {
                        errs += 1;
                        bad = Some(tok);
                    }
                },
                _ => {
                    errs += 1;
                    bad = Some(tok);
                }
            }
        }
    }

    if let Some(badarg) = bad {
        // Spelled differently depending on context, as in C (postgres.c:3999).
        let msg = if init_small::globals::IsUnderPostmaster() {
            format!("invalid command-line argument for server process: {badarg}")
        } else {
            format!("postgres: invalid command-line argument: {badarg}")
        };
        return ereport(FATAL)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(msg)
            .errhint("Try \"postgres --help\" for more information.")
            .finish(loc(4165, fname));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // FATAL emits and proc_exits, so FATAL arms assert on a panicking stub.
    fn setup() {
        crate::session_tests::install_shared_stubs();
    }

    fn av(args: &[&str]) -> Vec<String> {
        let mut v = vec!["postgres".to_string()];
        v.extend(args.iter().map(|s| s.to_string()));
        v
    }

    // SetConfigOption arms need the full guc harness; these pin the arms
    // that fail before any GUC write.
    #[test]
    fn empty_argv_is_ok() {
        assert!(process_postgres_switches(&av(&[]), GucContext::PGC_BACKEND as u8).is_ok());
    }

    #[test]
    #[should_panic(expected = "proc_exit(1)")]
    fn unknown_switch_is_fatal() {
        setup();
        let _ = process_postgres_switches(&av(&["-Z"]), GucContext::PGC_BACKEND as u8);
    }

    #[test]
    #[should_panic(expected = "proc_exit(1)")]
    fn trailing_dbname_rejected_when_no_out_param() {
        setup();
        let _ = process_postgres_switches(&av(&["mydb"]), GucContext::PGC_BACKEND as u8);
    }

    #[test]
    fn c_without_value_errors() {
        let err = process_postgres_switches(&av(&["-c", "work_mem"]), GucContext::PGC_BACKEND as u8)
            .unwrap_err();
        assert!(format!("{err:?}").contains("-c work_mem requires a value"));
    }

    #[test]
    fn misplaced_dispatch_option_errors() {
        let err = process_postgres_switches(&av(&["--single"]), GucContext::PGC_BACKEND as u8)
            .unwrap_err();
        assert!(format!("{err:?}").contains("--single must be first argument"));
    }
}
