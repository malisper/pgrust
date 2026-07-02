#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::{ErrorLocation, PgResult, FATAL};

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
    Postmaster,
}

const DISPATCH_OPTION_NAMES: &[(DispatchOption, &str)] = &[
    (DispatchOption::Check, "check"),
    (DispatchOption::Boot, "boot"),
    (DispatchOption::Forkchild, "forkchild"),
    (DispatchOption::DescribeConfig, "describe-config"),
    (DispatchOption::Single, "single"),
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

    init_small::globals::SetMyProcPid(std::process::id() as i32);
    // MemoryContextInit: top-level contexts are owner-created here; ErrorContext is PgResult.

    stack_depth::set_stack_base();

    // set_pglocale_pgservice: NLS/gettext unported; PGSYSCONFDIR default suffices.

    let main_context = mcx::MemoryContext::new("Main");
    let mcx = main_context.mcx();
    init_locale(mcx, "LC_COLLATE", libc::LC_COLLATE, "")?;
    init_locale(mcx, "LC_CTYPE", libc::LC_CTYPE, "")?;
    init_locale(mcx, "LC_MESSAGES", libc::LC_MESSAGES, "")?;
    init_locale(mcx, "LC_MONETARY", libc::LC_MONETARY, "C")?;
    init_locale(mcx, "LC_NUMERIC", libc::LC_NUMERIC, "C")?;
    init_locale(mcx, "LC_TIME", libc::LC_TIME, "C")?;
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
            panic!("PostgresSingleUserMain unported: unit backend-tcop-postgres (single-user arm)")
        }
        DispatchOption::Postmaster => postmaster::PostmasterMain(argv),
    }
}

fn startup_hacks(_progname: &str) {}

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
