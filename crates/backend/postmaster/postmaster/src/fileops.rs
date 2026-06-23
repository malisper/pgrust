//! Postmaster-owned file writes from `PostmasterMain`: `CreateOptsFile`
//! (`postmaster.opts`) and the optional `external_pid_file` write.
//!
//! Both are `postmaster.c`'s own function bodies (the C `CreateOptsFile` static
//! and the inline `external_pid_file` block in `PostmasterMain`). The cwd at
//! this point is the data directory (`ChangeToDataDir` already ran), so the
//! relative `postmaster.opts` path resolves there, exactly as in C.

use std::io::Write;

use utils_error::ereport;
use types_error::LOG;

use crate::helpers::here;

const OPTS_FILE: &str = "postmaster.opts";

/// C: `static bool CreateOptsFile(int argc, char *argv[], char *fullprogname)`.
///
/// Write the postmaster invocation to `postmaster.opts`: the program path
/// (`argv[0]`) followed by each remaining arg quoted. `argv` is the full
/// argument vector (`argv[0]` is `fullprogname`/`my_exec_path`).
pub fn create_opts_file(argv: Vec<String>) -> bool {
    // C: fp = fopen(OPTS_FILE, "w")
    let mut fp = match std::fs::File::create(OPTS_FILE) {
        Ok(f) => f,
        Err(_) => {
            let _ = ereport(LOG)
                .errmsg(format!("could not create file \"{OPTS_FILE}\""))
                .finish(here("CreateOptsFile"));
            return false;
        }
    };

    // C: fprintf(fp, "%s", fullprogname); for (i=1..argc) fprintf(fp, " \"%s\"", argv[i]); fputs("\n").
    let fullprogname = argv.first().map(String::as_str).unwrap_or("");
    let mut line = String::from(fullprogname);
    for a in argv.iter().skip(1) {
        line.push_str(&format!(" \"{a}\""));
    }
    line.push('\n');

    if fp.write_all(line.as_bytes()).is_err() || fp.flush().is_err() {
        let _ = ereport(LOG)
            .errmsg(format!("could not write file \"{OPTS_FILE}\""))
            .finish(here("CreateOptsFile"));
        return false;
    }

    true
}

/// C (PostmasterMain): the `if (external_pid_file) { ... }` block.
///
/// If the `external_pid_file` GUC is set, write `MyProcPid` to it and make it
/// world-readable. The `on_proc_exit(unlink_external_pid_file)` registration is
/// handled by the proc-exit owner; this is the write half. No-op when unset.
pub fn maybe_write_external_pid_file() {
    let path = match guc_tables::vars::external_pid_file.read() {
        Some(p) if !p.is_empty() => p,
        _ => return,
    };

    let pid = init_small_seams::my_proc_pid::call();
    match std::fs::File::create(&path) {
        Ok(mut f) => {
            // C: fprintf(fpidfile, "%d\n", MyProcPid);
            let _ = writeln!(f, "{pid}");
            let _ = f.flush();
            drop(f);
            // C: chmod(external_pid_file, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH) == 0644.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644));
            }
        }
        Err(_) => {
            let _ = ereport(LOG)
                .errmsg(format!("could not write external PID file \"{path}\""))
                .finish(here("PostmasterMain"));
        }
    }
}
