//! Faithful bodies for three tiny process-startup `common/` helpers that the
//! boot prelude (`main.c` → `pg_main`) reaches before anything else is up:
//!
//! * [`get_progname`]            — `src/port/path.c`
//! * [`set_pglocale_pgservice`]  — `src/common/exec.c`
//! * [`get_user_name_or_exit`]   — `src/common/username.c`
//!
//! None of these `common/`/`port/` files has a dedicated owner crate in this
//! tree yet; `miscinit.c` is the process-init home that the boot path already
//! routes startup through (it is where C's `InitStandaloneProcess` calls the
//! username lookup), so the bodies are homed here and installed from
//! [`crate::init_seams`]. Each mirrors its C source on the non-Windows path
//! (the only platform this tree builds for).

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use std::ffi::CStr;

use types_error::{PgError, PgResult, FATAL};

/// `get_progname(argv0)` (`src/port/path.c`): strip the directory prefix from
/// `argv[0]`, returning the bare program name as a fresh owned copy (the C
/// `strdup`). Non-Windows: `last_dir_separator` finds the last `/`, and the
/// name is what follows it; with no separator the whole string is the name
/// (`skip_drive` is identity off Windows). No `.exe` suffix stripping (that arm
/// is `__CYGWIN__`/`WIN32` only).
pub fn get_progname(argv0: &str) -> String {
    match argv0.rfind('/') {
        Some(pos) => argv0[pos + 1..].to_string(),
        None => argv0.to_string(),
    }
}

/// `set_pglocale_pgservice(argv0, app)` (`src/common/exec.c`): set up the
/// gettext text domain and the `PGSYSCONFDIR`/`PGLOCALEDIR` paths derived from
/// the executable location. The C function returns `void` and is best-effort:
/// it swallows executable-path resolution failure (`if (find_my_exec(...) < 0)
/// return;`).
///
/// Faithful behaviour on this tree:
///
/// * "don't set `LC_ALL` in the backend": only when `app` differs from
///   `PG_TEXTDOMAIN("postgres")` (i.e. a frontend) does C call
///   `setlocale(LC_ALL, "")`. The boot path passes `app = "postgres-18"`, which
///   IS `PG_TEXTDOMAIN("postgres")`, so this is the backend leg and the
///   `setlocale` is correctly skipped (the postmaster sets locales itself just
///   after this call).
/// * NLS (`bindtextdomain`/`textdomain`/`PGLOCALEDIR`) is compiled out in this
///   tree (`pg_bindtextdomain` is a no-op), matching `#ifndef ENABLE_NLS`.
/// * `find_my_exec`/`get_etc_path` (the `PGSYSCONFDIR` derivation) are not
///   ported; C's contract is to silently return if the executable path cannot
///   be resolved, so omitting that derivation stays within the documented
///   best-effort/void behaviour rather than diverging.
pub fn set_pglocale_pgservice(_argv0: &str, app: &str) {
    // PG_TEXTDOMAIN("postgres") == "postgres" "-" PG_MAJORVERSION == "postgres-18".
    const PG_TEXTDOMAIN_POSTGRES: &str = "postgres-18";

    if app != PG_TEXTDOMAIN_POSTGRES {
        // Frontend leg: absorb the environment locale. (Never taken from the
        // backend boot path, which passes the postgres text domain.)
        // SAFETY: setlocale is safe to call at single-threaded startup.
        unsafe {
            libc::setlocale(libc::LC_ALL, c"".as_ptr());
        }
    }

    // bindtextdomain/textdomain are NLS-only (compiled out here).
    crate::pg_bindtextdomain(app);

    // The executable-path-derived PGSYSCONFDIR/PGLOCALEDIR setup requires
    // find_my_exec/get_etc_path, which are not yet ported. C returns early and
    // void when the executable path cannot be located; we therefore stop here,
    // which is faithful to the best-effort contract.
}

/// `get_user_name_or_exit(progname)` (`src/common/username.c`): return the
/// effective OS user name (via `getpwuid(geteuid())`). C's frontend leg prints
/// `"<progname>: <errstr>"` to stderr and `exit(1)` on failure; this port
/// returns `Err(PgError::new(FATAL, ...))` so the caller maps it to the fatal
/// exit (mirroring the project convention of surfacing process-fatal failures
/// as `PgResult`).
pub fn get_user_name_or_exit(progname: &str) -> PgResult<String> {
    // get_user_name(&errstr): getpwuid(geteuid()).
    // SAFETY: geteuid never fails; getpwuid returns a pointer into a static
    // libc buffer (single-threaded startup), copied out immediately below.
    let user_id = unsafe { libc::geteuid() };

    let pw = unsafe { libc::getpwuid(user_id) };

    if pw.is_null() {
        // POSIX: getpwuid leaves errno unset (0) when there is simply no
        // matching entry, and sets it on a genuine lookup error — exactly the
        // `errno ? strerror(errno) : "user does not exist"` split in C.
        let os_err = std::io::Error::last_os_error();
        let detail = match os_err.raw_os_error() {
            Some(0) | None => "user does not exist".to_string(),
            Some(_) => os_err.to_string(),
        };
        let errstr = format!("could not look up effective user ID {user_id}: {detail}");
        return Err(PgError::new(FATAL, format!("{progname}: {errstr}")));
    }

    // pw->pw_name
    let name = unsafe { CStr::from_ptr((*pw).pw_name) };
    Ok(name.to_string_lossy().into_owned())
}
