//! `backend-postmaster-fork-process` — the `fork()` chokepoint.
//!
//! Faithful port of `src/backend/postmaster/fork_process.c` (PostgreSQL 18.3),
//! the `#ifndef WIN32` definition. [`fork_process`] is a simple wrapper on top
//! of `fork()` that does not handle the `EXEC_BACKEND` case (Unix-only primary
//! path; this tree uses real `fork()` per the process-model decision). It is
//! the single place the postmaster — and, through `launch_backend.c`'s
//! `postmaster_child_launch`, every postmaster child — crosses into a new
//! process.
//!
//! The structure mirrors the C exactly:
//!
//!  * flush stdio just before the fork (`fflush(NULL)`), so buffered output is
//!    not duplicated into both processes;
//!  * block the postmaster's normal signals across the fork
//!    (`sigprocmask(SIG_SETMASK, &BlockSig, &save_mask)`) and capture the prior
//!    mask, so the child can install its own handlers before unblocking
//!    (avoiding a race where it runs the postmaster's handler and misses a
//!    control signal);
//!  * `result = fork();` and branch on the result;
//!  * in the child (`result == 0`): set `MyProcPid = getpid()`; apply the
//!    optional OOM-score adjustment driven by the `PG_OOM_ADJUST_FILE` /
//!    `PG_OOM_ADJUST_VALUE` environment variables (all errors ignored); and
//!    reinitialize strong-random state (`pg_strong_random_init()`);
//!  * in the parent: restore the saved signal mask
//!    (`sigprocmask(SIG_SETMASK, &save_mask, NULL)`);
//!  * return the `fork()` result (`0` in child, child pid in parent, `-1` on
//!    error).
//!
//! ## Compile-time variants
//!
//! The C build this tree mirrors does not define `WIN32` (so the whole
//! function exists) nor `LINUX_PROFILE` (so the `getitimer`/`setitimer`
//! save/restore of the profiling timer is compiled out); neither path is
//! ported, matching the active C compilation.
//!
//! ## Genuine reuse / external boundary
//!
//!  * `&BlockSig` / signal-mask install → [`libpq_pqsignal`]'s owned
//!    [`libpq_pqsignal::SignalMasks`] (the backend-private
//!    `BlockSig`/`UnBlockSig` globals) plus direct `libc::sigprocmask`.
//!  * `MyProcPid = getpid()` → [`init_small::globals::SetMyProcPid`]
//!    fed by `libc::getpid()`.
//!  * `pg_strong_random_init()` → the
//!    [`pg_strong_random_seams::pg_strong_random_init`] seam, whose owner
//!    (`port-pg-strong-random`) is not ported yet; the call panics loudly until
//!    that owner lands. It is a documented no-op for every supported randomness
//!    source, but is still a genuine cross-unit call, so it is left as a
//!    seam-and-panic rather than silently stubbed here.
//!  * `fflush(NULL)`, `fork()`, `getpid()`, `open`/`write`/`close`, and
//!    `getenv` are genuinely-external OS facilities reached directly via `libc`
//!    / `std`, exactly as the C reaches libc.

#![allow(non_snake_case)]

use libpq_pqsignal::signal_masks;
use init_small::globals::SetMyProcPid;
use types_core::pid_t;

/// Install this crate's seams: the `fork_process` chokepoint consumed by
/// `launch_backend.c`'s `postmaster_child_launch`.
pub fn init_seams() {
    fork_process_seams::fork_process::set(fork_process);
}

/// Wrapper for `fork()`. Return values are the same as those for `fork()`:
/// `-1` if the fork failed, `0` in the child process, and the PID of the child
/// in the parent process. Signals are blocked while forking, so the child must
/// unblock.
///
/// C: `pid_t fork_process(void)` (`fork_process.c`, the `#ifndef WIN32`
/// definition).
pub fn fork_process() -> pid_t {
    // Flush stdio channels just before fork, to avoid double-output problems.
    //   fflush(NULL);
    // libc's fflush(NULL) flushes every open stdio stream; Rust's std streams
    // are line/unbuffered for stderr and lock-guarded for stdout, but the C
    // backend writes through libc stdio, so flush that the same way the C does.
    unsafe {
        libc::fflush(core::ptr::null_mut());
    }

    // We start postmaster children with signals blocked. This allows them to
    // install their own handlers before unblocking, to avoid races where they
    // might run the postmaster's handler and miss an important control signal.
    // With more analysis this could potentially be relaxed.
    //   sigprocmask(SIG_SETMASK, &BlockSig, &save_mask);
    let masks = signal_masks();
    let mut save_mask: libc::sigset_t = unsafe { core::mem::zeroed() };
    // SAFETY: `block_sig()` points to a valid, initialized sigset_t, and
    // `save_mask` is a valid out-parameter for the prior mask.
    unsafe {
        libc::sigprocmask(libc::SIG_SETMASK, masks.block_sig(), &mut save_mask);
    }

    //   result = fork();
    // SAFETY: fork() has no preconditions; this is the deliberate process
    // split. Per the process-model decision, this is the real fork() primary
    // path (Unix/darwin).
    let result: pid_t = unsafe { libc::fork() };

    if result == 0 {
        // fork succeeded, in child
        //   MyProcPid = getpid();
        // SAFETY: getpid() has no preconditions.
        SetMyProcPid(unsafe { libc::getpid() });

        // By default, Linux tends to kill the postmaster in out-of-memory
        // situations, because it blames the postmaster for the sum of child
        // process sizes *including shared memory*. Therefore it's often a good
        // idea to protect the postmaster by setting its OOM score adjustment
        // negative. Since the adjustment is inherited by child processes, this
        // would ordinarily mean that all the postmaster's children are equally
        // protected against OOM kill, which is not such a good idea. So we
        // provide this code to allow the children to change their OOM score
        // adjustments again. Both the file name to write to and the value to
        // write are controlled by environment variables, which can be set by
        // the same startup script that did the original adjustment.
        //
        //   oomfilename = getenv("PG_OOM_ADJUST_FILE");
        //   if (oomfilename != NULL) { ... }
        adjust_oom_score();

        // do post-fork initialization for random number generation
        //   pg_strong_random_init();
        pg_strong_random_seams::pg_strong_random_init::call();
    } else {
        // in parent, restore signal mask
        //   sigprocmask(SIG_SETMASK, &save_mask, NULL);
        // SAFETY: `save_mask` was filled by the sigprocmask above.
        unsafe {
            libc::sigprocmask(libc::SIG_SETMASK, &save_mask, core::ptr::null_mut());
        }
    }

    result
}

/// The child-side OOM-score-adjustment block of `fork_process`.
///
/// C:
/// ```c
/// oomfilename = getenv("PG_OOM_ADJUST_FILE");
/// if (oomfilename != NULL)
/// {
///     int fd = open(oomfilename, O_WRONLY, 0);
///     if (fd >= 0)
///     {
///         const char *oomvalue = getenv("PG_OOM_ADJUST_VALUE");
///         int rc;
///         if (oomvalue == NULL)   /* supply a useful default */
///             oomvalue = "0";
///         rc = write(fd, oomvalue, strlen(oomvalue));
///         (void) rc;
///         close(fd);
///     }
/// }
/// ```
///
/// All errors are deliberately ignored, exactly as in the C. We use `open()`
/// (not buffered stdio) to control the open flags, since some Linux security
/// environments reject anything but `O_WRONLY`.
fn adjust_oom_score() {
    //   oomfilename = getenv("PG_OOM_ADJUST_FILE");
    let oomfilename = match std::env::var_os("PG_OOM_ADJUST_FILE") {
        Some(name) => name,
        None => return,
    };

    // Convert to a NUL-terminated path for open(2). A path containing an
    // embedded NUL cannot match any real file; C's getenv yields a
    // NUL-terminated string and open would simply fail, so mirror that by
    // bailing out (the equivalent of the open() failing with all errors
    // ignored).
    use std::os::unix::ffi::OsStrExt;
    let bytes = oomfilename.as_os_str().as_bytes();
    if bytes.contains(&0) {
        return;
    }
    let mut path: Vec<u8> = Vec::with_capacity(bytes.len() + 1);
    path.extend_from_slice(bytes);
    path.push(0);

    //   int fd = open(oomfilename, O_WRONLY, 0);
    // SAFETY: `path` is a valid NUL-terminated C string for the duration of
    // the call.
    let fd = unsafe { libc::open(path.as_ptr() as *const libc::c_char, libc::O_WRONLY, 0) };

    //   if (fd >= 0)
    if fd >= 0 {
        //   const char *oomvalue = getenv("PG_OOM_ADJUST_VALUE");
        //   if (oomvalue == NULL)   /* supply a useful default */
        //       oomvalue = "0";
        let oomvalue = std::env::var_os("PG_OOM_ADJUST_VALUE");
        let oombytes: &[u8] = match &oomvalue {
            Some(v) => v.as_os_str().as_bytes(),
            None => b"0",
        };

        //   rc = write(fd, oomvalue, strlen(oomvalue));
        //   (void) rc;
        // SAFETY: writing `oombytes.len()` bytes from a valid slice to an open
        // fd; the result is intentionally ignored, as in the C.
        unsafe {
            let _rc = libc::write(
                fd,
                oombytes.as_ptr() as *const libc::c_void,
                oombytes.len(),
            );
        }

        //   close(fd);
        // SAFETY: `fd` is a valid open descriptor.
        unsafe {
            libc::close(fd);
        }
    }
}
