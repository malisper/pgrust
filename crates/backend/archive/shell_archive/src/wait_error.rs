//! src/common/wait_error.c, hosted here until common-extra-srv-batch4 lands;
//! system(3) via std::process, raw wait word preserved.

pub fn system(command: &str) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    match std::process::Command::new("/bin/sh").arg("-c").arg(command).status() {
        Ok(status) => status.into_raw(),
        Err(_) => -1,
    }
}

pub fn WIFEXITED(status: i32) -> bool {
    libc::WIFEXITED(status)
}

pub fn WEXITSTATUS(status: i32) -> i32 {
    libc::WEXITSTATUS(status)
}

pub fn WIFSIGNALED(status: i32) -> bool {
    libc::WIFSIGNALED(status)
}

pub fn WTERMSIG(status: i32) -> i32 {
    libc::WTERMSIG(status)
}

pub fn pg_strsignal(signum: i32) -> String {
    // SAFETY: strsignal returns a process-lifetime static string (or NULL).
    let p = unsafe { libc::strsignal(signum) };
    if p.is_null() {
        return "unrecognized signal".to_string();
    }
    // SAFETY: non-NULL NUL-terminated string from libc.
    unsafe { std::ffi::CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

fn strerror_now() -> String {
    let errnum = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    // SAFETY: strerror returns a process-lifetime static string.
    let p = unsafe { libc::strerror(errnum) };
    if p.is_null() {
        return format!("error {errnum}");
    }
    // SAFETY: non-NULL NUL-terminated string from libc.
    unsafe { std::ffi::CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

pub fn wait_result_to_str(exitstatus: i32) -> String {
    if exitstatus == -1 {
        strerror_now()
    } else if WIFEXITED(exitstatus) {
        match WEXITSTATUS(exitstatus) {
            126 => "command not executable".to_string(),
            127 => "command not found".to_string(),
            code => format!("child process exited with exit code {code}"),
        }
    } else if WIFSIGNALED(exitstatus) {
        format!(
            "child process was terminated by signal {}: {}",
            WTERMSIG(exitstatus),
            pg_strsignal(WTERMSIG(exitstatus))
        )
    } else {
        format!("child process exited with unrecognized status {exitstatus}")
    }
}

pub fn wait_result_is_signal(exit_status: i32, signum: i32) -> bool {
    if WIFSIGNALED(exit_status) && WTERMSIG(exit_status) == signum {
        return true;
    }
    if WIFEXITED(exit_status) && WEXITSTATUS(exit_status) == 128 + signum {
        return true;
    }
    false
}

pub fn wait_result_is_any_signal(exit_status: i32, include_command_not_found: bool) -> bool {
    if WIFSIGNALED(exit_status) {
        return true;
    }
    if WIFEXITED(exit_status)
        && WEXITSTATUS(exit_status) > (if include_command_not_found { 125 } else { 128 })
    {
        return true;
    }
    false
}
