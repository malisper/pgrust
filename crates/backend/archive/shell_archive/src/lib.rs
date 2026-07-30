//! shell_archive.c. ArchiveModuleCallbacks is pgarch's ArchiveModule enum
//! (shell is the only in-core provider); check_configured returns errdetail text.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use elog::{elog, ereport};
use types_error::{ErrorLocation, PgResult, DEBUG1, DEBUG3, FATAL, LOG};

use percentrepl::replace_percent_placeholders;

const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_ARCHIVE_COMMAND: u32 = PG_WAIT_IPC + 2;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn archive_command() -> String {
    guc_tables::vars::XLogArchiveCommand.read().unwrap_or_default()
}

/// `None` = configured; `Some` = the arch_module_check_errdetail text.
pub fn shell_archive_configured() -> Option<String> {
    if !archive_command().is_empty() {
        return None;
    }
    Some("\"archive_command\" is not set.".to_string())
}

pub fn shell_archive_file(file: &str, path: Option<&str>) -> PgResult<bool> {
    let xlogarchcmd = replace_percent_placeholders(
        &archive_command(),
        "archive_command",
        &[('f', Some(file)), ('p', path)],
    )?;

    ereport(DEBUG3)
        .errmsg_internal(format!("executing archive command \"{xlogarchcmd}\""))
        .finish(loc("shell_archive_file"))?;

    waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_ARCHIVE_COMMAND);
    let rc = wait_error::system(&xlogarchcmd);
    waitevent_seams::pgstat_report_wait_end::call();

    if rc != 0 {
        // FATAL does not return: errfinish proc_exits the archiver thread.
        let (lev, msg) = classify_archive_failure(rc);
        ereport(lev)
            .errmsg(msg)
            .errdetail(format!("The failed archive command was: {xlogarchcmd}"))
            .finish(loc("shell_archive_file"))?;
        return Ok(false);
    }

    elog(DEBUG1, format!("archived write-ahead log file \"{file}\""))?;
    Ok(true)
}

pub fn classify_archive_failure(rc: i32) -> (types_error::ErrorLevel, String) {
    let lev = if wait_error::wait_result_is_any_signal(rc, true) { FATAL } else { LOG };
    let msg = if wait_error::WIFEXITED(rc) {
        format!("archive command failed with exit code {}", wait_error::WEXITSTATUS(rc))
    } else if wait_error::WIFSIGNALED(rc) {
        format!(
            "archive command was terminated by signal {}: {}",
            wait_error::WTERMSIG(rc),
            wait_error::pg_strsignal(wait_error::WTERMSIG(rc))
        )
    } else {
        format!("archive command exited with unrecognized status {rc}")
    };
    (lev, msg)
}

pub fn shell_archive_shutdown() -> PgResult<()> {
    elog(DEBUG1, "archiver process shutting down")
}

pub fn init_seams() {}

#[cfg(test)]
mod tests;
