#![allow(non_snake_case)]

//! Port of PostgreSQL's `main.c` (`src/backend/main/main.c`).
//!
//! The stub `main()` routine for the postgres executable: it performs the
//! startup tasks common to every incarnation of the server (postmaster,
//! standalone backend, standalone bootstrap, or a separately exec'd child of a
//! postmaster) and then dispatches to the proper `FooMain()` routine for the
//! incarnation.
//!
//! The C `main()` is the process entry point that returns an `int` to the OS;
//! it never actually returns from the dispatch switch (each subprogram either
//! runs forever or `proc_exit`s, and the trailing `abort()` is unreachable).
//! This port models the same flow as [`pg_main`], driving the startup sequence
//! and the dispatch switch. The actual C-ABI `extern "C" fn main` binary entry
//! is a thin shell over this and is not part of this library crate; it owns the
//! top-level [`mcx::MemoryContext`] passed in here (created right after the C
//! `MemoryContextInit`).
//!
//! `progname` (a process global in C) is threaded as a parameter to the
//! helpers exactly as the C helpers take it. `reached_main` is a per-process
//! flag consulted only by the `ubsan_default_options` sanitizer hook; it is a
//! thread-local here.

use std::cell::Cell;

use backend_common_exec_seams::set_pglocale_pgservice;
use common_path_seams::get_progname;
use backend_postmaster_postmaster_seams::postmaster_main;
use backend_tcop_postgres_seams::{postgres_single_user_main, set_stack_base};
use backend_utils_adt_pg_locale_seams::LcCategory;
use backend_utils_mmgr_mcxt_seams::memory_context_init;
use common_username_seams::get_user_name_or_exit;
use mcx::Mcx;
use types_error::PgResult;
use types_startup::DispatchOption;

mod help;
mod locale;

pub use help::help;
pub use locale::init_locale;

/// `PG_BACKEND_VERSIONSTR` (`pg_config.h`): the `--version` banner.
pub const PG_BACKEND_VERSIONSTR: &str = "postgres (PostgreSQL) 18.3\n";

thread_local! {
    /// `static bool reached_main` — set once `main()` is entered, so the
    /// `ubsan_default_options` weak symbol knows libc is safe to call.
    static REACHED_MAIN: Cell<bool> = const { Cell::new(false) };
}

/// `DispatchOptionNames[]` (main.c): names of the special must-be-first
/// options that dispatch to a subprogram. `DISPATCH_POSTMASTER` has no name
/// (it is the no-match result), and `DISPATCH_FORKCHILD` exists only under
/// `EXEC_BACKEND`.
const DISPATCH_OPTION_NAMES: &[(DispatchOption, &str)] = &[
    (DispatchOption::DISPATCH_CHECK, "check"),
    (DispatchOption::DISPATCH_BOOT, "boot"),
    (DispatchOption::DISPATCH_FORKCHILD, "forkchild"),
    (DispatchOption::DISPATCH_DESCRIBE_CONFIG, "describe-config"),
    (DispatchOption::DISPATCH_SINGLE, "single"),
    // DISPATCH_POSTMASTER has no name.
];

/// `parse_dispatch_option(name)` (main.c): map a must-be-first option name to
/// its [`DispatchOption`]. An unmatched name yields `DISPATCH_POSTMASTER`.
///
/// "forkchild" takes an argument, so it is matched by prefix; for non-
/// `EXEC_BACKEND` builds (the only configuration here) `DISPATCH_FORKCHILD` is
/// never returned, so it is skipped during the scan.
pub fn parse_dispatch_option(name: &str) -> DispatchOption {
    for &(option, option_name) in DISPATCH_OPTION_NAMES {
        // Unlike the other dispatch options, "forkchild" takes an argument, so
        // we just look for the prefix for that one. For non-EXEC_BACKEND
        // builds, we never want to return DISPATCH_FORKCHILD, so skip it.
        if option == DispatchOption::DISPATCH_FORKCHILD {
            continue;
        }

        if option_name == name {
            return option;
        }
    }

    // No match means this is a postmaster.
    DispatchOption::DISPATCH_POSTMASTER
}

/// The result of `pg_main`'s standard-option pre-scan: either dispatch ran
/// (unreachable in practice — every subprogram diverges), or the program
/// should write some text to stdout and `exit(0)` (`--help` / `--version` /
/// `--describe-config`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MainOutcome {
    /// Dispatch ran (in practice unreachable: every subprogram diverges).
    Dispatched,
    /// Print this text to stdout and `exit(0)`.
    PrintAndExit(String),
}

/// `main(argc, argv)` (main.c): the common server-process startup and the
/// dispatch switch. `argv[0]` is the executable name; `mcx` is the top-level
/// context the binary shell created right after `MemoryContextInit`.
///
/// Returns [`MainOutcome::PrintAndExit`] for the `--help`/`--version`/
/// `--describe-config` fast paths (where C writes stdout and `exit(0)`s);
/// otherwise it runs the dispatch switch, whose arms never return, so the
/// `Dispatched` outcome is effectively unreachable — mirroring the trailing
/// `abort()` in C.
pub fn pg_main(mcx: Mcx<'static>, argv: &[&str]) -> PgResult<MainOutcome> {
    let mut do_check_root = true;
    let mut dispatch_option = DispatchOption::DISPATCH_POSTMASTER;

    REACHED_MAIN.with(|r| r.set(true));

    let progname = get_progname::call(argv.first().copied().unwrap_or(""));

    // Platform-specific startup hacks (Windows only; a no-op here).
    startup_hacks(&progname);

    // Remember the physical location of the initial argv[] for the ps display.
    // save_ps_display_args may copy argv and move the environment strings, so
    // it must run as early as possible.
    backend_utils_misc_more::ps_status::save_ps_display_args(argv);

    // Fire up essential subsystems: error and memory management. Code after
    // this point may use elog/ereport.
    //
    // (MyProcPid = getpid() is owned by the per-backend init globals, not by
    // main(); the top-level MemoryContext was created by our caller.)
    memory_context_init::call()?;

    // Set the reference point for stack-depth checking.
    set_stack_base::call();

    // Set up locale information.
    // PG_TEXTDOMAIN("postgres") expands to "postgres" "-" PG_MAJORVERSION,
    // i.e. "postgres-18" — the gettext message domain.
    set_pglocale_pgservice::call(argv.first().copied().unwrap_or(""), "postgres-18");

    // In the postmaster, absorb the environment values for LC_COLLATE and
    // LC_CTYPE. Individual backends change these later from pg_database, but
    // the postmaster cannot, and leaving them "C" would hurt localization.
    init_locale(mcx, "LC_COLLATE", LcCategory::LcCollate, "")?;
    init_locale(mcx, "LC_CTYPE", LcCategory::LcCtype, "")?;

    // LC_MESSAGES gets set later during GUC processing, but set it here too so
    // startup error messages can be localized.
    init_locale(mcx, "LC_MESSAGES", LcCategory::LcMessages, "")?;

    // We keep these set to "C" always. See pg_locale.c for explanation.
    init_locale(mcx, "LC_MONETARY", LcCategory::LcMonetary, "C")?;
    init_locale(mcx, "LC_NUMERIC", LcCategory::LcNumeric, "C")?;
    init_locale(mcx, "LC_TIME", LcCategory::LcTime, "C")?;

    // Now that we have absorbed what we wish from the locale environment,
    // remove any LC_ALL setting so the pg_perm_setlocale values have force.
    // SAFETY: this runs single-threaded at process startup.
    unsafe {
        libc::unsetenv(c"LC_ALL".as_ptr());
    }

    // Catch standard options before doing much else, in particular before we
    // insist on not being root.
    if argv.len() > 1 {
        let arg1 = argv[1];
        if arg1 == "--help" || arg1 == "-?" {
            return Ok(MainOutcome::PrintAndExit(help(&progname)));
        }
        if arg1 == "--version" || arg1 == "-V" {
            return Ok(MainOutcome::PrintAndExit(PG_BACKEND_VERSIONSTR.to_string()));
        }

        // We also allow "--describe-config" and "-C var" to be called by root,
        // since these are read-only. The -C case matters because pg_ctl may
        // invoke it while still holding administrator privileges on Windows.
        // To bypass the root check, -C must be first (reducing the risk of
        // misinterpreting some other mode's -C as the postmaster one).
        if arg1 == "--describe-config" {
            do_check_root = false;
        } else if argv.len() > 2 && arg1 == "-C" {
            do_check_root = false;
        }
    }

    // Make sure we are not running as root, unless it's safe for the option.
    if do_check_root {
        check_root(&progname)?;
    }

    // Dispatch to one of various subprograms depending on the first argument.
    if argv.len() > 1 {
        let arg1 = argv[1].as_bytes();
        if arg1.len() >= 2 && arg1[0] == b'-' && arg1[1] == b'-' {
            dispatch_option = parse_dispatch_option(&argv[1][2..]);
        }
    }

    match dispatch_option {
        DispatchOption::DISPATCH_CHECK => {
            run_bootstrap(mcx, argv, true)?;
        }
        DispatchOption::DISPATCH_BOOT => {
            run_bootstrap(mcx, argv, false)?;
        }
        DispatchOption::DISPATCH_FORKCHILD => {
            // SubPostmasterMain (EXEC_BACKEND only). Not built on this
            // platform; the C code Asserts this is unreachable.
            unreachable_dispatch("DISPATCH_FORKCHILD reached without EXEC_BACKEND");
        }
        DispatchOption::DISPATCH_DESCRIBE_CONFIG => {
            // GucInfoMain: print every visible GUC and exit(0). The owner
            // returns the rendered text; main writes it and exits.
            let text = backend_utils_misc_help_config::GucInfoMain()?;
            return Ok(MainOutcome::PrintAndExit(text));
        }
        DispatchOption::DISPATCH_SINGLE => {
            let username = get_user_name_or_exit::call(&progname)?;
            postgres_single_user_main::call(argv, &username);
        }
        DispatchOption::DISPATCH_POSTMASTER => {
            postmaster_main::call(argv);
        }
    }

    // The functions above should not return.
    Ok(MainOutcome::Dispatched)
}

/// `BootstrapModeMain(argc, argv, check_only)` arm. The bootstrap owner takes
/// an owned `argv` and the top-level memory context.
fn run_bootstrap(mcx: Mcx<'static>, argv: &[&str], check_only: bool) -> PgResult<()> {
    let argv_owned: Vec<String> = argv.iter().map(|s| s.to_string()).collect();
    backend_bootstrap_bootstrap::BootstrapModeMain(mcx, argv_owned, check_only)
}

#[cold]
fn unreachable_dispatch(reason: &str) -> ! {
    panic!("main dispatch reached an impossible state: {reason}");
}

/// `startup_hacks(progname)` (main.c): platform-specific early startup. The
/// entire body is Windows-only (Winsock init, abort/error-mode tweaks); on
/// every other platform it is a no-op. Kept as a named function so the call
/// site mirrors C and a future Windows port has the hook.
fn startup_hacks(_progname: &str) {
    // Windows-only execution-environment hacking; nothing to do elsewhere.
}

/// `check_root(progname)` (main.c): refuse to run as root.
///
/// Postgres must not run as `root`: a server compromise would then be a system
/// compromise. We also require that the real and effective uids match, since a
/// setuid-from-root binary is itself a hole. Both failures `exit(1)` in C; here
/// they return `Err`, leaving the caller to print and exit.
fn check_root(progname: &str) -> PgResult<()> {
    // SAFETY: geteuid/getuid never fail and have no preconditions.
    let euid = unsafe { libc::geteuid() };
    if euid == 0 {
        return Err(types_error::PgError::new(
            types_error::FATAL,
            "\"root\" execution of the PostgreSQL server is not permitted.\n\
             The server must be started under an unprivileged user ID to prevent\n\
             possible system security compromise.  See the documentation for\n\
             more information on how to properly start the server."
                .to_string(),
        ));
    }

    // Also make sure real and effective uids match. Executing as a setuid
    // program from a root shell is a security hole, since on many platforms a
    // nefarious subroutine could setuid back to root if the real uid is root.
    // SAFETY: getuid never fails.
    let uid = unsafe { libc::getuid() };
    if uid != euid {
        return Err(types_error::PgError::new(
            types_error::FATAL,
            format!("{progname}: real and effective user IDs must match"),
        ));
    }

    Ok(())
}

/// `__ubsan_default_options()` (main.c): the weak symbol libsanitizer consults
/// for default options. Returns `UBSAN_OPTIONS` from the environment, but only
/// once `main()` has been reached, so we don't rely on a not-yet-working
/// `getenv()` during very early sanitizer initialization.
pub fn ubsan_default_options() -> String {
    // Don't call libc before it's guaranteed to be initialized.
    if !REACHED_MAIN.with(|r| r.get()) {
        return String::new();
    }

    std::env::var("UBSAN_OPTIONS").unwrap_or_default()
}

/// Install this unit's inward seams (`parse_dispatch_option`).
pub fn init_seams() {
    backend_main_main_seams::parse_dispatch_option::set(parse_dispatch_option);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_names_match() {
        assert_eq!(parse_dispatch_option("check"), DispatchOption::DISPATCH_CHECK);
        assert_eq!(parse_dispatch_option("boot"), DispatchOption::DISPATCH_BOOT);
        assert_eq!(
            parse_dispatch_option("describe-config"),
            DispatchOption::DISPATCH_DESCRIBE_CONFIG
        );
        assert_eq!(parse_dispatch_option("single"), DispatchOption::DISPATCH_SINGLE);
    }

    #[test]
    fn forkchild_never_matches_without_exec_backend() {
        // Non-EXEC_BACKEND: "forkchild" must fall through to postmaster.
        assert_eq!(
            parse_dispatch_option("forkchild"),
            DispatchOption::DISPATCH_POSTMASTER
        );
    }

    #[test]
    fn unknown_option_is_postmaster() {
        assert_eq!(parse_dispatch_option("nonsense"), DispatchOption::DISPATCH_POSTMASTER);
        assert_eq!(parse_dispatch_option(""), DispatchOption::DISPATCH_POSTMASTER);
    }

    #[test]
    fn help_text_mentions_program_name() {
        let text = help("postgres");
        assert!(text.starts_with("postgres is the PostgreSQL server."));
        assert!(text.contains("--single"));
        assert!(text.contains("--boot"));
        assert!(text.contains("--describe-config"));
    }
}
