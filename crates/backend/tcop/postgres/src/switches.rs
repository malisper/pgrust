// Client-argv arm only; the secure arm (single-user/postmaster argv) has no
// in-tree caller and stays a named panic.

use ::types_error::{PgResult, ERRCODE_SYNTAX_ERROR, ERROR, FATAL};
use elog::ereport;
use types_guc::{GucContext, GucSource};

use crate::{
    get_stats_option_name, guc_context_from_u8, loc, set_debug_options,
    set_plan_disabling_options,
};

const ARG_TAKING_FLAGS: &[u8] = b"BCcDdfhkNprStvW-";

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
    matches!(bare, "check" | "boot" | "describe-config" | "single")
}

pub fn process_postgres_switches(argv: &[String], gucctx: u8) -> PgResult<()> {
    let ctx = guc_context_from_u8(gucctx);
    if ctx == GucContext::PGC_POSTMASTER {
        panic!("process_postgres_switches: secure (single-user/postmaster) argv arm unported (postgres.c)");
    }
    let source = GucSource::PGC_S_CLIENT;
    let fname = "process_postgres_switches";

    let mut set = |name: &str, value: &str| guc::SetConfigOption(name, Some(value), ctx, source);

    let mut errs = 0usize;
    let mut i = 1usize;
    let mut bad: Option<&str> = None;
    'outer: while i < argv.len() && errs == 0 {
        let tok = argv[i].as_str();
        i += 1;
        let b = tok.as_bytes();
        if b.len() < 2 || b[0] != b'-' {
            // C consumes a bare word as dbname only via the out param postinit never passes.
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
                // secure-only or explicitly-ignored switches: C no-ops when !secure.
                b'b' | b'C' | b'D' | b'E' | b'j' | b'n' | b'r' | b'T' | b'v' => {}
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
        // Under-postmaster spelling only: standalone is the secure panic above.
        return ereport(FATAL)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "invalid command-line argument for server process: {badarg}"
            ))
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
