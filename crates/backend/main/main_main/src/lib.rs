#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::{ErrorLocation, PgResult, FATAL};

// wasm32: no LC_* names in the wasi libc crate; musl numbering (the
// pg_locale wasm arm's convention, matching the linked wasi-libc).
#[cfg(not(target_family = "wasm"))]
use libc::{LC_COLLATE, LC_CTYPE, LC_MESSAGES, LC_MONETARY, LC_NUMERIC, LC_TIME};
#[cfg(target_family = "wasm")]
mod wasm_lc {
    pub const LC_CTYPE: i32 = 0;
    pub const LC_NUMERIC: i32 = 1;
    pub const LC_TIME: i32 = 2;
    pub const LC_COLLATE: i32 = 3;
    pub const LC_MONETARY: i32 = 4;
    pub const LC_MESSAGES: i32 = 5;
}
#[cfg(target_family = "wasm")]
use wasm_lc::*;

pub const PG_BACKEND_VERSIONSTR: &str = "postgres (PostgreSQL) 18.3\n";

const SRC: &str = "src/backend/main/main.c";

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRC, line, func)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchOption {
    Check,
    Boot,
    Forkchild,
    DescribeConfig,
    Single,
    // pgrust extension (no C counterpart): one wire-protocol session over
    // the boot-installed stdio transport provider (§2.4 seam). The
    // wasm32-wasip1 client-server mode — WASI p1 has no socket(); native
    // --stdio-wire is the differential arm.
    StdioWire,
    // pgrust extension (P4 sim-net, `--cfg pgrust_sim` builds only): one
    // deterministic wire-protocol session over the in-memory sim-net
    // transport pair, driven by the in-process scripted client.
    #[cfg(pgrust_sim)]
    SimNet,
    Postmaster,
}

const DISPATCH_OPTION_NAMES: &[(DispatchOption, &str)] = &[
    (DispatchOption::Check, "check"),
    (DispatchOption::Boot, "boot"),
    (DispatchOption::Forkchild, "forkchild"),
    (DispatchOption::DescribeConfig, "describe-config"),
    (DispatchOption::Single, "single"),
    (DispatchOption::StdioWire, "stdio-wire"),
    #[cfg(pgrust_sim)]
    (DispatchOption::SimNet, "sim-net"),
];

pub fn parse_dispatch_option(name: &str) -> DispatchOption {
    for &(option, option_name) in DISPATCH_OPTION_NAMES {
        // "forkchild" is EXEC_BACKEND-only (prefix-matched there); never built here.
        if option == DispatchOption::Forkchild {
            continue;
        }
        if option_name == name {
            return option;
        }
    }
    DispatchOption::Postmaster
}

// get_progname (src/port/path.c): basename of argv[0]; the .exe strip is Windows-only.
pub fn get_progname(argv0: &str) -> &str {
    argv0.rsplit('/').next().unwrap_or(argv0)
}

fn init_locale(mcx: Mcx<'_>, categoryname: &str, category: i32, locale: &str) -> PgResult<()> {
    if pg_locale::pg_perm_setlocale(mcx, category, locale)?.is_some()
        || pg_locale::pg_perm_setlocale(mcx, category, "C")?.is_some()
    {
        return Ok(());
    }
    elog::ereport(FATAL)
        .errmsg(format!(
            "could not adopt \"{locale}\" locale nor C locale for {categoryname}"
        ))
        .finish(loc(407, "init_locale"))
}

// wasm32: WASI has no uids — root cannot exist and there is nothing to
// refuse (C's WIN32 arm skips the check the same way).
#[cfg(target_family = "wasm")]
fn check_root(_progname: &str) {}

#[cfg(not(target_family = "wasm"))]
fn check_root(progname: &str) {
    // SAFETY: geteuid/getuid have no preconditions and never fail.
    let (uid, euid) = unsafe { (libc::getuid(), libc::geteuid()) };
    if euid == 0 {
        elog::write_stderr(
            "\"root\" execution of the PostgreSQL server is not permitted.\n\
             The server must be started under an unprivileged user ID to prevent\n\
             possible system security compromise.  See the documentation for\n\
             more information on how to properly start the server.\n",
        );
        std::process::exit(1);
    }
    if uid != euid {
        elog::write_stderr(&format!("{progname}: real and effective user IDs must match\n"));
        std::process::exit(1);
    }
}

pub fn pg_main(argv: &[String]) -> PgResult<()> {
    let mut do_check_root = true;
    let mut dispatch_option = DispatchOption::Postmaster;

    let progname = get_progname(argv.first().map(|s| s.as_str()).unwrap_or("postgres")).to_string();

    startup_hacks(&progname);

    ps_status::save_ps_display_args();

    init_small::globals::SetMyProcPid(init_small::globals::process_id() as i32);
    // MemoryContextInit: top-level contexts are owner-created here; ErrorContext is PgResult.

    stack_depth::set_stack_base();

    // set_pglocale_pgservice: NLS/gettext unported; PGSYSCONFDIR default suffices.

    let main_context = mcx::MemoryContext::new("Main");
    let mcx = main_context.mcx();
    init_locale(mcx, "LC_COLLATE", LC_COLLATE, "")?;
    init_locale(mcx, "LC_CTYPE", LC_CTYPE, "")?;
    init_locale(mcx, "LC_MESSAGES", LC_MESSAGES, "")?;
    init_locale(mcx, "LC_MONETARY", LC_MONETARY, "C")?;
    init_locale(mcx, "LC_NUMERIC", LC_NUMERIC, "C")?;
    init_locale(mcx, "LC_TIME", LC_TIME, "C")?;
    // SAFETY: single-threaded process startup; no concurrent getenv.
    unsafe {
        libc::unsetenv(c"LC_ALL".as_ptr());
    }

    if argv.len() > 1 {
        let arg1 = argv[1].as_str();
        if arg1 == "--help" || arg1 == "-?" {
            print!("{}", help(&progname));
            std::process::exit(0);
        }
        if arg1 == "--version" || arg1 == "-V" {
            print!("{PG_BACKEND_VERSIONSTR}");
            std::process::exit(0);
        }
        if arg1 == "--describe-config" {
            do_check_root = false;
        } else if argv.len() > 2 && arg1 == "-C" {
            do_check_root = false;
        }
    }

    if do_check_root {
        check_root(&progname);
    }

    if let Some(rest) = argv.get(1).and_then(|a| a.strip_prefix("--")) {
        dispatch_option = parse_dispatch_option(rest);
    }

    match dispatch_option {
        DispatchOption::Check => {
            panic!("BootstrapModeMain(check_only) unported: unit backend-bootstrap (initdb runs against C postgres)")
        }
        DispatchOption::Boot => {
            panic!("BootstrapModeMain unported: unit backend-bootstrap (initdb runs against C postgres)")
        }
        DispatchOption::Forkchild => {
            panic!("DISPATCH_FORKCHILD reached without EXEC_BACKEND")
        }
        DispatchOption::DescribeConfig => {
            panic!("GucInfoMain unported: unit backend-utils-misc-help-config")
        }
        DispatchOption::Single => {
            // main.c:222: PostgresSingleUserMain(argc, argv,
            // strdup(get_user_name_or_exit(progname))). Exits the process.
            let username = get_user_name_or_exit(&progname);
            postgres_seams::postgres_single_user_main::call(argv, &username)
        }
        DispatchOption::StdioWire => {
            // pgrust extension: identity ultimately comes from the startup
            // packet; the OS/env user is the single-user-style fallback.
            let username = get_user_name_or_exit(&progname);
            postgres_seams::postgres_stdio_wire_main::call(argv, &username)
        }
        #[cfg(pgrust_sim)]
        DispatchOption::SimNet => {
            // P4 sim-net (sim builds only): same identity story as the
            // stdio wire mode.
            let username = get_user_name_or_exit(&progname);
            postgres_seams::postgres_sim_net_main::call(argv, &username)
        }
        DispatchOption::Postmaster => postmaster::PostmasterMain(argv),
    }
}

fn startup_hacks(_progname: &str) {}

// get_user_name_or_exit (src/common/username.c:74): effective user's
// pw_name, or print the lookup error and exit(1).
// wasm32: no uids and no passwd db on WASI; the operator supplies the
// identity through the environment (wasmtime --env USER=<name>, matching
// the role the datadir was initdb'd with). Absent that, C's exit(1) shape.
#[cfg(target_family = "wasm")]
fn get_user_name_or_exit(progname: &str) -> String {
    match std::env::var("USER") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            elog::write_stderr(&format!(
                "{progname}: could not determine the effective user name: \
                 set the USER environment variable\n"
            ));
            std::process::exit(1);
        }
    }
}

#[cfg(not(target_family = "wasm"))]
fn get_user_name_or_exit(progname: &str) -> String {
    // SAFETY: geteuid never fails; getpwuid returns a static-storage struct
    // (single-threaded startup, per C's use) or NULL with errno set.
    let (user_id, pw) = unsafe {
        let uid = libc::geteuid();
        set_errno_zero();
        (uid, libc::getpwuid(uid))
    };
    if pw.is_null() {
        let err = std::io::Error::last_os_error();
        let detail = if err.raw_os_error().unwrap_or(0) != 0 {
            err.to_string()
        } else {
            "user does not exist".to_string()
        };
        elog::write_stderr(&format!(
            "{progname}: could not look up effective user ID {user_id}: {detail}\n"
        ));
        std::process::exit(1);
    }
    // SAFETY: non-NULL passwd from getpwuid has a NUL-terminated pw_name.
    unsafe { std::ffi::CStr::from_ptr((*pw).pw_name) }
        .to_string_lossy()
        .into_owned()
}

#[cfg(not(target_family = "wasm"))] // wasm32: only the native getpwuid path clears errno
fn set_errno_zero() {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    // SAFETY: __error returns this thread's valid errno location.
    unsafe {
        *libc::__error() = 0;
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    // SAFETY: __errno_location returns this thread's valid errno location.
    unsafe {
        *libc::__errno_location() = 0;
    }
}

pub fn help(progname: &str) -> String {
    let mut s = String::with_capacity(2048);
    s.push_str(&format!("{progname} is the PostgreSQL server.\n\n"));
    s.push_str(&format!("Usage:\n  {progname} [OPTION]...\n\n"));
    s.push_str("Options:\n");
    s.push_str("  -B NBUFFERS        number of shared buffers\n");
    s.push_str("  -c NAME=VALUE      set run-time parameter\n");
    s.push_str("  -C NAME            print value of run-time parameter, then exit\n");
    s.push_str("  -d 1-5             debugging level\n");
    s.push_str("  -D DATADIR         database directory\n");
    s.push_str("  -e                 use European date input format (DMY)\n");
    s.push_str("  -F                 turn fsync off\n");
    s.push_str("  -h HOSTNAME        host name or IP address to listen on\n");
    s.push_str("  -i                 enable TCP/IP connections (deprecated)\n");
    s.push_str("  -k DIRECTORY       Unix-domain socket location\n");
    s.push_str("  -N MAX-CONNECT     maximum number of allowed connections\n");
    s.push_str("  -p PORT            port number to listen on\n");
    s.push_str("  -s                 show statistics after each query\n");
    s.push_str("  -S WORK-MEM        set amount of memory for sorts (in kB)\n");
    s.push_str("  -V, --version      output version information, then exit\n");
    s.push_str("  --NAME=VALUE       set run-time parameter\n");
    s.push_str("  --describe-config  describe configuration parameters, then exit\n");
    s.push_str("  -?, --help         show this help, then exit\n");
    s.push_str("\nDeveloper options:\n");
    s.push_str("  -f s|i|o|b|t|n|m|h forbid use of some plan types\n");
    s.push_str("  -O                 allow system table structure changes\n");
    s.push_str("  -P                 disable system indexes\n");
    s.push_str("  -t pa|pl|ex        show timings after each query\n");
    s.push_str("  -T                 send SIGABRT to all backend processes if one dies\n");
    s.push_str("  -W NUM             wait NUM seconds to allow attach from a debugger\n");
    s.push_str("\nOptions for single-user mode:\n");
    s.push_str("  --single           selects single-user mode (must be first argument)\n");
    s.push_str("  DBNAME             database name (defaults to user name)\n");
    s.push_str("  -d 0-5             override debugging level\n");
    s.push_str("  -E                 echo statement before execution\n");
    s.push_str("  -j                 do not use newline as interactive query delimiter\n");
    s.push_str("  -r FILENAME        send stdout and stderr to given file\n");
    s.push_str("\nOptions for bootstrapping mode:\n");
    s.push_str("  --boot             selects bootstrapping mode (must be first argument)\n");
    s.push_str("  --check            selects check mode (must be first argument)\n");
    s.push_str("  DBNAME             database name (mandatory argument in bootstrapping mode)\n");
    s.push_str("  -r FILENAME        send stdout and stderr to given file\n");
    s.push_str(
        "\nPlease read the documentation for the complete list of run-time\n\
         configuration settings and how to set them on the command line or in\n\
         the configuration file.\n\n\
         Report bugs to <pgsql-bugs@lists.postgresql.org>.\n",
    );
    s.push_str("PostgreSQL home page: <https://www.postgresql.org/>\n");
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_names_match_c() {
        assert_eq!(parse_dispatch_option("check"), DispatchOption::Check);
        assert_eq!(parse_dispatch_option("boot"), DispatchOption::Boot);
        assert_eq!(parse_dispatch_option("describe-config"), DispatchOption::DescribeConfig);
        assert_eq!(parse_dispatch_option("single"), DispatchOption::Single);
        assert_eq!(parse_dispatch_option("stdio-wire"), DispatchOption::StdioWire);
        assert_eq!(parse_dispatch_option("forkchild"), DispatchOption::Postmaster);
        assert_eq!(parse_dispatch_option("nonsense"), DispatchOption::Postmaster);
        assert_eq!(parse_dispatch_option(""), DispatchOption::Postmaster);
    }

    #[test]
    fn progname_is_basename() {
        assert_eq!(get_progname("/usr/local/bin/postgres"), "postgres");
        assert_eq!(get_progname("postgres"), "postgres");
    }
}
