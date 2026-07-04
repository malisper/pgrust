#![allow(clippy::result_large_err)]

use elog::ereport;
use types_error::{ErrorLocation, PgResult, ERRCODE_CONFIG_FILE_ERROR, ERROR, FATAL, LOG};

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("be-secure-common.c", 0, funcname)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn errno() -> i32 {
    unsafe { *libc::__error() }
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
fn errno() -> i32 {
    unsafe { *libc::__errno_location() }
}

// replace_percent_placeholders (common/percentrepl.c), "p"-only instance;
// homed here until common-percentrepl lands.
fn replace_percent_placeholder(
    string: &str,
    guc_name: &str,
    prompt: &str,
) -> PgResult<String> {
    let mut result = String::with_capacity(string.len() + prompt.len());
    let mut it = string.chars();
    while let Some(c) = it.next() {
        if c != '%' {
            result.push(c);
            continue;
        }
        match it.next() {
            Some('%') => result.push('%'),
            Some('p') => result.push_str(prompt),
            Some(other) => {
                return ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "invalid value for parameter \"{guc_name}\": \"{string}\""
                    ))
                    .errdetail(format!("String contains unexpected placeholder \"%{other}\"."))
                    .finish(loc("replace_percent_placeholders"))
                    .map(|()| String::new());
            }
            None => {
                return ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "invalid value for parameter \"{guc_name}\": \"{string}\""
                    ))
                    .errdetail("String ends unexpectedly after escape character \"%\".")
                    .finish(loc("replace_percent_placeholders"))
                    .map(|()| String::new());
            }
        }
    }
    Ok(result)
}

// wait_result_to_str (common/wait_error.c); homed here until common-wait_error
// lands.
fn wait_result_to_str(exitstatus: i32) -> String {
    if libc::WIFEXITED(exitstatus) {
        match libc::WEXITSTATUS(exitstatus) {
            126 => "command not executable".to_string(),
            127 => "command not found".to_string(),
            code => format!("child process exited with exit code {code}"),
        }
    } else if libc::WIFSIGNALED(exitstatus) {
        let sig = libc::WTERMSIG(exitstatus);
        let name = unsafe {
            let p = libc::strsignal(sig);
            if p.is_null() {
                None
            } else {
                std::ffi::CStr::from_ptr(p).to_str().ok().map(str::to_owned)
            }
        };
        format!(
            "child process was terminated by signal {sig}: {}",
            name.unwrap_or_else(|| "unrecognized signal".to_string())
        )
    } else {
        format!("child process exited with unrecognized status {exitstatus}")
    }
}

fn pg_strip_crlf(buf: &mut [u8], mut len: usize) -> usize {
    while len > 0 && (buf[len - 1] == b'\n' || buf[len - 1] == b'\r') {
        len -= 1;
        buf[len] = 0;
    }
    len
}

pub fn run_ssl_passphrase_command(
    prompt: &str,
    is_server_start: bool,
    buf: &mut [u8],
) -> PgResult<usize> {
    let loglevel = if is_server_start { ERROR } else { LOG };
    assert!(!buf.is_empty());
    buf[0] = 0;

    let ssl_passphrase_command = guc_tables::vars::ssl_passphrase_command
        .read()
        .unwrap_or_default();
    let command =
        replace_percent_placeholder(&ssl_passphrase_command, "ssl_passphrase_command", prompt)?;

    let fh = fd::OpenPipeStream(&command, "r")?;
    if fh < 0 {
        ereport(loglevel)
            .with_saved_errno(errno())
            .errcode_for_file_access()
            .errmsg(format!("could not execute command \"{command}\": %m"))
            .finish(loc("run_ssl_passphrase_command"))?;
        return Ok(0);
    }

    let len = match fd::PipeStreamGets(fh, buf) {
        Ok(n) => n,
        Err(e) => {
            buf.fill(0);
            let _ = fd::ClosePipeStream(fh);
            ereport(loglevel)
                .with_saved_errno(e)
                .errcode_for_file_access()
                .errmsg(format!("could not read from command \"{command}\": %m"))
                .finish(loc("run_ssl_passphrase_command"))?;
            return Ok(0);
        }
    };

    let pclose_rc = fd::ClosePipeStream(fh)?;
    if pclose_rc == -1 {
        buf.fill(0);
        ereport(loglevel)
            .with_saved_errno(errno())
            .errcode_for_file_access()
            .errmsg("could not close pipe to external command: %m")
            .finish(loc("run_ssl_passphrase_command"))?;
        return Ok(0);
    } else if pclose_rc != 0 {
        buf.fill(0);
        let reason = wait_result_to_str(pclose_rc);
        ereport(loglevel)
            .errcode_for_file_access()
            .errmsg(format!("command \"{command}\" failed"))
            .errdetail_internal(reason)
            .finish(loc("run_ssl_passphrase_command"))?;
        return Ok(0);
    }

    Ok(pg_strip_crlf(buf, len))
}

pub fn check_ssl_key_file_permissions(ssl_key_file: &str, is_server_start: bool) -> PgResult<bool> {
    let loglevel = if is_server_start { FATAL } else { LOG };

    let cpath = std::ffi::CString::new(ssl_key_file).unwrap();
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: cpath is a valid NUL-terminated path, st is writable.
    if unsafe { libc::stat(cpath.as_ptr(), &mut st) } != 0 {
        ereport(loglevel)
            .with_saved_errno(errno())
            .errcode_for_file_access()
            .errmsg(format!("could not access private key file \"{ssl_key_file}\": %m"))
            .finish(loc("check_ssl_key_file_permissions"))?;
        return Ok(false);
    }

    if st.st_mode & libc::S_IFMT != libc::S_IFREG {
        ereport(loglevel)
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(format!("private key file \"{ssl_key_file}\" is not a regular file"))
            .finish(loc("check_ssl_key_file_permissions"))?;
        return Ok(false);
    }

    let euid = unsafe { libc::geteuid() };
    if st.st_uid != euid && st.st_uid != 0 {
        ereport(loglevel)
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(format!(
                "private key file \"{ssl_key_file}\" must be owned by the database user or root"
            ))
            .finish(loc("check_ssl_key_file_permissions"))?;
        return Ok(false);
    }

    let mode = st.st_mode as u32;
    let irwxg = libc::S_IRWXG as u32;
    let irwxo = libc::S_IRWXO as u32;
    let iwgrp = libc::S_IWGRP as u32;
    let ixgrp = libc::S_IXGRP as u32;
    if (st.st_uid == euid && mode & (irwxg | irwxo) != 0)
        || (st.st_uid == 0 && mode & (iwgrp | ixgrp | irwxo) != 0)
    {
        ereport(loglevel)
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg(format!(
                "private key file \"{ssl_key_file}\" has group or world access"
            ))
            .errdetail("File must have permissions u=rw (0600) or less if owned by the database user, or permissions u=rw,g=r (0640) or less if owned by root.")
            .finish(loc("check_ssl_key_file_permissions"))?;
        return Ok(false);
    }

    Ok(true)
}

#[cfg(test)]
mod tests;
