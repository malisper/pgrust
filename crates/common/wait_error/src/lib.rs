//! src/common/wait_error.c

#![allow(non_snake_case)]

// Not from wait_error.c: a system(3) wrapper colocated here since every
// caller immediately feeds its raw wait status into wait_result_to_str et al.
#[cfg(not(target_family = "wasm"))]
pub fn system(command: &str) -> i32 {
    system_tracked(command, |_| {}, |_| {})
}

// The shell leads its own process group, as C's setsid'd child does, so the
// kill(-pid) leg of signal_child reaches the command's descendants; the hooks
// let the caller register the child for that signal fan-out.
#[cfg(not(target_family = "wasm"))]
pub fn system_tracked(command: &str, spawned: impl FnOnce(u32), reaped: impl FnOnce(u32)) -> i32 {
    use std::os::unix::process::{CommandExt, ExitStatusExt};
    let mut child = match std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .process_group(0)
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return -1,
    };
    let pid = child.id();
    spawned(pid);
    let rc = match child.wait() {
        Ok(status) => status.into_raw(),
        Err(_) => -1,
    };
    reaped(pid);
    rc
}

#[cfg(target_family = "wasm")]
pub fn system_tracked(_command: &str, _spawned: impl FnOnce(u32), _reaped: impl FnOnce(u32)) -> i32 {
    -1
}

// wasm32: WASI p1 has no processes; system(3) fails as C's would when
// fork/exec is unavailable (-1, errno path).
#[cfg(target_family = "wasm")]
pub fn system(_command: &str) -> i32 {
    -1
}

// wasm32 arms: the libc crate for wasi exposes no W* macros; these are the
// classic POSIX bit forms (identical to glibc/wasi-libc definitions), pure
// arithmetic on the status word.
#[cfg(not(target_family = "wasm"))]
pub fn WIFEXITED(status: i32) -> bool {
    libc::WIFEXITED(status)
}
#[cfg(target_family = "wasm")]
pub fn WIFEXITED(status: i32) -> bool {
    status & 0x7f == 0
}

#[cfg(not(target_family = "wasm"))]
pub fn WEXITSTATUS(status: i32) -> i32 {
    libc::WEXITSTATUS(status)
}
#[cfg(target_family = "wasm")]
pub fn WEXITSTATUS(status: i32) -> i32 {
    (status >> 8) & 0xff
}

#[cfg(not(target_family = "wasm"))]
pub fn WIFSIGNALED(status: i32) -> bool {
    libc::WIFSIGNALED(status)
}
#[cfg(target_family = "wasm")]
pub fn WIFSIGNALED(status: i32) -> bool {
    ((status & 0x7f) + 1) >> 1 > 0
}

#[cfg(not(target_family = "wasm"))]
pub fn WTERMSIG(status: i32) -> i32 {
    libc::WTERMSIG(status)
}
#[cfg(target_family = "wasm")]
pub fn WTERMSIG(status: i32) -> i32 {
    status & 0x7f
}

#[cfg(not(target_family = "wasm"))]
pub fn pg_strsignal(signum: i32) -> String {
    // SAFETY: strsignal returns a process-lifetime static string (or NULL).
    let p = unsafe { libc::strsignal(signum) };
    if p.is_null() {
        return "unrecognized signal".to_string();
    }
    // SAFETY: non-NULL NUL-terminated string from libc.
    unsafe { std::ffi::CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

// wasm32: no strsignal in the wasi libc crate; C's NULL fallback string.
#[cfg(target_family = "wasm")]
pub fn pg_strsignal(_signum: i32) -> String {
    "unrecognized signal".to_string()
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

pub fn wait_result_to_exit_code(exit_status: i32) -> i32 {
    if exit_status == -1 {
        return -1;
    }
    if WIFEXITED(exit_status) {
        return WEXITSTATUS(exit_status);
    }
    if WIFSIGNALED(exit_status) {
        return 128 + WTERMSIG(exit_status);
    }
    -1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exited(code: i32) -> i32 {
        code << 8
    }

    fn signaled(sig: i32) -> i32 {
        sig
    }

    // The shell traps SIGTERM and is waiting on its forked `sleep`, so it
    // only exits promptly if kill(-pid) reaches the sleep: the shell must
    // lead its own process group.
    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn system_tracked_child_leads_its_own_process_group() {
        let started = std::time::Instant::now();
        let seen = std::sync::Mutex::new((0u32, 0u32));
        let rc = system_tracked(
            "trap 'exit 7' TERM; sleep 30; exit 1",
            |pid| {
                seen.lock().unwrap().0 = pid;
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_millis(500));
                    // SAFETY: the shell's own group, created by process_group(0).
                    unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGTERM) };
                });
            },
            |pid| seen.lock().unwrap().1 = pid,
        );
        assert!(WIFEXITED(rc), "status {rc:#x}");
        assert_eq!(WEXITSTATUS(rc), 7);
        assert!(started.elapsed() < std::time::Duration::from_secs(20));
        let (spawned, reaped) = *seen.lock().unwrap();
        assert!(spawned != 0 && spawned == reaped);
    }

    #[test]
    fn exit_code_classification() {
        assert_eq!(wait_result_to_str(exited(0)), "child process exited with exit code 0");
        assert_eq!(wait_result_to_str(exited(126)), "command not executable");
        assert_eq!(wait_result_to_str(exited(127)), "command not found");
    }

    #[test]
    fn signal_classification() {
        assert!(WIFSIGNALED(signaled(libc::SIGTERM)));
        assert_eq!(WTERMSIG(signaled(libc::SIGTERM)), libc::SIGTERM);
    }

    #[test]
    fn is_signal_matches_direct_and_shell_form() {
        assert!(wait_result_is_signal(signaled(libc::SIGTERM), libc::SIGTERM));
        assert!(wait_result_is_signal(exited(128 + libc::SIGTERM), libc::SIGTERM));
        assert!(!wait_result_is_signal(exited(1), libc::SIGTERM));
    }

    #[test]
    fn is_any_signal_includes_shell_not_found_when_requested() {
        assert!(wait_result_is_any_signal(exited(127), true));
        assert!(!wait_result_is_any_signal(exited(127), false));
        assert!(wait_result_is_any_signal(exited(129), false));
        assert!(wait_result_is_any_signal(signaled(libc::SIGKILL), false));
    }

    #[test]
    fn exit_code_roundtrip() {
        assert_eq!(wait_result_to_exit_code(exited(3)), 3);
        assert_eq!(wait_result_to_exit_code(signaled(libc::SIGTERM)), 128 + libc::SIGTERM);
        assert_eq!(wait_result_to_exit_code(-1), -1);
    }
}
