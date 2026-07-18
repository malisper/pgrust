//! fdnb: nonblocking-mode EMULATION over blocking fd duplexes — the shared
//! layer under fd-based transport providers (pqcomm_stdio today; any future
//! fd-duplex provider reuses it instead of re-growing the pattern).
//!
//! Extracted from pqcomm_stdio per the wasm-net-seam review ledger
//! (notes/wasm-net-seam-lane.md §8) whose N1/N2 latents were MUST-FIX the
//! moment a provider's consumers could exercise the noblock arms — which the
//! P4 sim-net increment's tests are the first to do. The fixes live HERE,
//! once, with their own red-scenario tests, not per provider:
//!
//! **N1 (poll-then-BLOCKING-write)**: POLLOUT on a pipe does NOT license a
//! blocking write of an arbitrary buffer — the kernel only guarantees room
//! for a bounded amount, and a blocking write of more TRANSFERS WHAT FITS
//! AND THEN BLOCKS (pipes never return early in blocking mode). Fix:
//! [`write_noblock`] caps each post-POLLOUT write at [`NB_WRITE_CAP`] =
//! `PIPE_BUF` — the amount whose writability POLLOUT is specified to convey
//! on pipes (POSIX poll(): "Normal data may be written without blocking";
//! Linux `pipe_poll` reports writable at ≥ one page = PIPE_BUF; BSDs at
//! ≥ PIPE_BUF free). Short writes are the seam contract's normal vocabulary
//! (pqcomm::internal_flush_buffer loops; pq_flush_if_writable returns).
//!
//! **N2 (POLLNVAL/poll-error → "not ready")**: the old probe mapped a poll
//! error or a dead fd (POLLNVAL) to "no data", so a noblock read spun on
//! EWOULDBLOCK forever where the socket arm would surface EOF/error. Fix:
//! [`poll_ready`] reports READY on poll error and on POLLNVAL — falling
//! through to the actual read/write surfaces the real errno (EBADF) or EOF
//! exactly like a socket does.
//!
//! Determinism ledger: the libc::read/write (+ poll/fcntl) sites below are
//! the raw-IO surface OF fd-based transport providers, the same standing as
//! be_secure's libc::recv/send rows — they live BEHIND the §2.4 seam; the
//! sim-net provider is buffer-backed and never executes them.

/// Per-write cap for the emulated-noblock write arm (the N1 fix): the
/// largest amount POLLOUT is specified to make writable without blocking on
/// a pipe. `PIPE_BUF` per platform (Linux 4096, macOS 512); 512 (the POSIX
/// floor) where libc does not export it.
#[cfg(not(target_family = "wasm"))]
pub const NB_WRITE_CAP: usize = libc::PIPE_BUF;
#[cfg(target_family = "wasm")]
pub const NB_WRITE_CAP: usize = 512;

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn errno() -> i32 {
    // SAFETY: __error returns this thread's valid errno location.
    unsafe { *libc::__error() }
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
fn errno() -> i32 {
    // SAFETY: __errno_location returns this thread's valid errno location.
    unsafe { *libc::__errno_location() }
}

#[cfg(target_family = "wasm")]
fn errno() -> i32 {
    // wasi-libc's errno is thread-local storage exposed as __errno_location.
    // SAFETY: same contract as the unix arms.
    unsafe { *libc::__errno_location() }
}

/// Zero-timeout readiness probe. On wasm32-wasip1, wasi-libc implements
/// poll(2) over poll_oneoff.
///
/// N2 semantics: a poll ERROR and a dead fd (POLLNVAL) both report READY —
/// the caller's read/write then observes the real errno (EBADF/…) or EOF,
/// matching the socket arm's behavior on a dead descriptor. POLLHUP/POLLERR
/// likewise count as ready so the op observes EOF/EPIPE.
pub fn poll_ready(fd: i32, events: i16) -> bool {
    let mut pfd = libc::pollfd { fd, events, revents: 0 };
    // SAFETY: pfd is a valid pollfd for the duration of the call; nfds = 1.
    let rc = unsafe { libc::poll(&mut pfd, 1, 0) };
    if rc < 0 {
        // Poll itself failed: fall through to the op to surface the error
        // (N2 fix — was "not ready" = eternal EWOULDBLOCK).
        return true;
    }
    rc > 0
        && (pfd.revents & (events | libc::POLLHUP | libc::POLLERR | libc::POLLNVAL)) != 0
}

/// One emulated-noblock READ attempt against a blocking fd: zero-timeout
/// poll, then a read that is guaranteed not to block (POLLIN ⇒ ≥ 1 byte or
/// EOF; error/POLLNVAL falls through per N2 and the read surfaces errno).
///
/// Returns the raw `(ssize, errno)` pair (errno only meaningful when
/// ssize < 0). Not-ready is `(-1, EWOULDBLOCK)`. EINTR handling (interrupt
/// seams) stays with the provider.
pub fn read_noblock(fd: i32, buf: &mut [u8]) -> (isize, i32) {
    if !poll_ready(fd, libc::POLLIN) {
        return (-1, libc::EWOULDBLOCK);
    }
    // SAFETY: buf is valid writable memory of buf.len() bytes.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    (n as isize, errno())
}

/// One emulated-noblock WRITE attempt against a blocking fd: zero-timeout
/// poll, then a write CAPPED at [`NB_WRITE_CAP`] bytes — the N1 fix. The
/// uncapped arm could block indefinitely: POLLOUT on a pipe guarantees only
/// bounded room, and a blocking write of more transfers what fits and then
/// parks until the peer drains.
///
/// Returns the raw `(ssize, errno)` pair; short writes are expected (the
/// caller loops or returns per the noblock contract). Not-ready is
/// `(-1, EWOULDBLOCK)`.
pub fn write_noblock(fd: i32, buf: &[u8]) -> (isize, i32) {
    if !poll_ready(fd, libc::POLLOUT) {
        return (-1, libc::EWOULDBLOCK);
    }
    let cap = buf.len().min(NB_WRITE_CAP);
    // SAFETY: buf[..cap] is valid readable memory.
    let n = unsafe { libc::write(fd, buf.as_ptr().cast(), cap) };
    (n as isize, errno())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn mk_pipe() -> (i32, i32) {
        let mut fds = [0i32; 2];
        // SAFETY: fds is a valid 2-int buffer.
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        (fds[0], fds[1])
    }

    fn close(fd: i32) {
        // SAFETY: fd is a descriptor this test owns.
        unsafe { libc::close(fd) };
    }

    /// Fill a pipe to capacity without blocking (temporary O_NONBLOCK).
    fn fill_pipe(wfd: i32) -> usize {
        // SAFETY: fcntl F_GETFL/F_SETFL on an owned fd.
        let flags = unsafe { libc::fcntl(wfd, libc::F_GETFL) };
        assert!(flags >= 0);
        assert!(unsafe { libc::fcntl(wfd, libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0);
        let chunk = [0xAAu8; 4096];
        let mut total = 0;
        loop {
            // SAFETY: chunk is valid readable memory.
            let n = unsafe { libc::write(wfd, chunk.as_ptr().cast(), chunk.len()) };
            if n <= 0 {
                break;
            }
            total += n as usize;
        }
        assert!(unsafe { libc::fcntl(wfd, libc::F_SETFL, flags) } >= 0);
        total
    }

    #[test]
    fn read_noblock_empty_pipe_is_ewouldblock_then_data() {
        let (r, w) = mk_pipe();
        let mut buf = [0u8; 16];
        let (n, e) = read_noblock(r, &mut buf);
        assert_eq!((n, e), (-1, libc::EWOULDBLOCK));
        // SAFETY: writing an owned pipe fd.
        assert_eq!(unsafe { libc::write(w, b"hi".as_ptr().cast(), 2) }, 2);
        let (n, _) = read_noblock(r, &mut buf);
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"hi");
        close(r);
        close(w);
    }

    #[test]
    fn read_noblock_closed_writer_is_eof_not_wouldblock() {
        let (r, w) = mk_pipe();
        close(w);
        let mut buf = [0u8; 16];
        // POLLHUP counts as ready; the read observes EOF (0), never an
        // eternal EWOULDBLOCK.
        let (n, _) = read_noblock(r, &mut buf);
        assert_eq!(n, 0);
        close(r);
    }

    /// N2 red scenario: the fd is DEAD (closed). The old probe mapped
    /// POLLNVAL to "not ready" = EWOULDBLOCK forever; the fix reports ready
    /// and the op surfaces EBADF.
    ///
    /// The dead fd is pinned to a HIGH descriptor number first (dup2):
    /// sibling tests run in parallel and their pipe(2) calls re-mint
    /// lowest-available fds, which would resurrect a just-closed low fd and
    /// race this test's "definitely invalid" premise.
    #[test]
    fn n2_dead_fd_surfaces_ebadf_not_eternal_wouldblock() {
        let (r, w) = mk_pipe();
        let dead = 700;
        // SAFETY: dup2 onto an unused high slot this test owns.
        assert_eq!(unsafe { libc::dup2(r, dead) }, dead);
        close(r);
        close(w);
        close(dead);
        assert!(poll_ready(dead, libc::POLLIN), "POLLNVAL must report ready (N2)");
        let mut buf = [0u8; 4];
        let (n, e) = read_noblock(dead, &mut buf);
        assert_eq!(n, -1);
        assert_eq!(e, libc::EBADF);
        let (n, e) = write_noblock(dead, b"x");
        assert_eq!(n, -1);
        assert_eq!(e, libc::EBADF);
    }

    /// N1 red scenario: pipe filled to capacity — write_noblock must return
    /// EWOULDBLOCK promptly, never park. A watchdog thread proves
    /// non-blocking behavior wall-clock (the pre-fix arm hangs here when
    /// poll misreports or the buffer exceeds the licensed room).
    #[test]
    fn n1_full_pipe_write_returns_not_blocks() {
        let (r, w) = mk_pipe();
        let filled = fill_pipe(w);
        assert!(filled > 0);
        let (tx, rx) = std::sync::mpsc::channel();
        let big = vec![0x55u8; 1 << 20];
        std::thread::spawn(move || {
            let (n, e) = write_noblock(w, &big);
            tx.send((n, e)).unwrap();
            close(w);
        });
        let (n, e) = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("write_noblock BLOCKED on a full pipe (N1 regression)");
        assert_eq!((n, e), (-1, libc::EWOULDBLOCK));
        close(r);
    }

    /// N1 boundary: pipe NEARLY full (free room < the request, ≥ 1 byte).
    /// The capped write must return a short count promptly instead of
    /// transferring-what-fits-then-parking, which is what an uncapped
    /// blocking write does on a pipe.
    #[test]
    fn n1_nearly_full_pipe_short_write_returns_not_blocks() {
        let (r, w) = mk_pipe();
        let filled = fill_pipe(w);
        // Drain one cap's worth so poll reports writable but a large write
        // could not fully fit.
        let mut sink = vec![0u8; NB_WRITE_CAP.max(4096)];
        // SAFETY: reading an owned pipe fd into valid memory.
        let drained = unsafe { libc::read(r, sink.as_mut_ptr().cast(), sink.len()) };
        assert!(drained > 0);
        let (tx, rx) = std::sync::mpsc::channel();
        let big = vec![0x66u8; 1 << 20];
        std::thread::spawn(move || {
            let mut wrote = 0usize;
            // Emulated-noblock flush loop, exactly the consumer shape
            // (internal_flush_buffer): loop until EWOULDBLOCK.
            loop {
                let (n, e) = write_noblock(w, &big[wrote..]);
                if n > 0 {
                    wrote += n as usize;
                    continue;
                }
                tx.send((wrote, n, e)).unwrap();
                break;
            }
            close(w);
        });
        let (wrote, n, e) = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("capped noblock flush loop BLOCKED (N1 regression)");
        assert!(wrote <= drained as usize, "cannot write more than the drained room");
        assert_eq!((n, e), (-1, libc::EWOULDBLOCK));
        assert!(filled > 0);
        close(r);
    }

    #[test]
    fn write_cap_is_bounded_per_call() {
        let (r, w) = mk_pipe();
        let big = vec![0x77u8; NB_WRITE_CAP * 4];
        let (n, _) = write_noblock(w, &big);
        assert!(n > 0);
        assert!(
            (n as usize) <= NB_WRITE_CAP,
            "single noblock write exceeded the POLLOUT-licensed cap"
        );
        close(r);
        close(w);
    }
}
