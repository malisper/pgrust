//! The fatal-signal reporter end to end: a child process (this test binary
//! re-executed with PGRUST_CRASH_SIGNALS_CHILD set) installs the handler
//! and dies — by a real null-pointer fault and by a kill-sent SIGSEGV (the
//! C3-F3 handled-and-returned path) — and the parent asserts the C-shaped
//! FATAL line, the raw frame dump behind it, and death by that signal
//! (crash_signals.rs; docs/design/crash-restart-gap.md §3).

use std::process::Command;

const CHILD_ENV: &str = "PGRUST_CRASH_SIGNALS_CHILD";

fn run_child(test_name: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .output()
        .expect("re-exec test binary")
}

fn frame_lines(stderr: &str) -> Vec<&str> {
    stderr.lines().filter(|l| l.starts_with("pgrust: frame ")).collect()
}

fn assert_crash_report(out: &std::process::Output, sig: i32, name: &str) {
    use std::os::unix::process::ExitStatusExt;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.signal(),
        Some(sig),
        "child must die by {name}, got {:?}; stderr:\n{stderr}",
        out.status
    );
    let fatal = stderr
        .lines()
        .find(|l| l.starts_with("pgrust: FATAL: server process was terminated by signal "))
        .unwrap_or_else(|| panic!("no FATAL line; stderr:\n{stderr}"));
    assert!(
        fatal.starts_with(&format!(
            "pgrust: FATAL: server process was terminated by signal {sig} ({name}), fault address 0x"
        )),
        "FATAL line format: {fatal}"
    );
    let hex = fatal.rsplit("0x").next().unwrap();
    assert!(!hex.is_empty() && hex.chars().all(|c| c.is_ascii_hexdigit()), "fault address: {fatal}");

    let anchor = stderr
        .lines()
        .find(|l| l.starts_with("pgrust: crash handler at 0x"))
        .unwrap_or_else(|| panic!("no crash-handler anchor line; stderr:\n{stderr}"));
    assert!(
        anchor.contains(", pc 0x") && anchor.contains(", fp 0x"),
        "anchor must carry the ucontext registers on Linux/macOS: {anchor}"
    );
    let frames = frame_lines(&stderr);
    assert!(!frames.is_empty(), "no frame lines; stderr:\n{stderr}");
    assert!(frames.len() <= 64, "frame cap: {}", frames.len());
    for (i, l) in frames.iter().enumerate() {
        let rest = l.strip_prefix(&format!("pgrust: frame {i}: 0x")).unwrap_or_else(|| panic!("frame line {l}"));
        assert!(!rest.is_empty() && rest.chars().all(|c| c.is_ascii_hexdigit()), "frame hex: {l}");
        let v = u64::from_str_radix(rest, 16).unwrap();
        assert!(v >= 0x1_0000, "frame below the zero page: {l}");
    }
    let end = stderr
        .lines()
        .find(|l| l.starts_with("pgrust: end of frames ("))
        .unwrap_or_else(|| panic!("no end-of-frames line; stderr:\n{stderr}"));
    assert_eq!(end, &format!("pgrust: end of frames ({})", frames.len()));
    // The dump is emitted BEFORE the re-raise: nothing of ours follows it.
    assert!(
        stderr.rfind("pgrust: end of frames").unwrap() > stderr.find("pgrust: frame 0:").unwrap(),
        "ordering: {stderr}"
    );
}

// ---- children -------------------------------------------------------------

#[test]
fn crash_child_segv_fault() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }
    postmaster::crash_signals::install_crash_signal_reporter();
    // A real fault, so the ucontext carries the faulting pc and a live
    // frame-pointer chain (never inlined away: volatile through null).
    let p = std::hint::black_box(core::ptr::null_mut::<u64>());
    unsafe { core::ptr::write_volatile(p, 1) };
    unreachable!("write through null returned");
}

#[test]
fn crash_child_segv_sent() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }
    postmaster::crash_signals::install_crash_signal_reporter();
    // Kill-sent: no faulting instruction; the restored disposition (Rust's
    // stack-overflow reporter under the test harness) handles-and-returns,
    // so this is the C3-F3 forced-SIG_DFL path.
    unsafe { libc::raise(libc::SIGSEGV) };
    std::thread::sleep(std::time::Duration::from_secs(5));
    unreachable!("kill-sent SIGSEGV was swallowed");
}

#[test]
fn crash_child_abort() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }
    postmaster::crash_signals::install_crash_signal_reporter();
    std::process::abort();
}

// ---- parents --------------------------------------------------------------

#[test]
fn segv_fault_logs_fatal_line_and_frames() {
    let out = run_child("crash_child_segv_fault");
    assert_crash_report(&out, libc::SIGSEGV, "SIGSEGV");
    let stderr = String::from_utf8_lossy(&out.stderr);
    let fatal = stderr.lines().find(|l| l.contains("terminated by signal")).unwrap();
    assert!(fatal.ends_with("fault address 0x0"), "null-deref fault address: {fatal}");
}

#[test]
fn segv_sent_dies_via_forced_default_and_logs_frames() {
    let out = run_child("crash_child_segv_sent");
    assert_crash_report(&out, libc::SIGSEGV, "SIGSEGV");
}

#[test]
fn abort_logs_fatal_line_and_frames() {
    let out = run_child("crash_child_abort");
    assert_crash_report(&out, libc::SIGABRT, "SIGABRT");
}
