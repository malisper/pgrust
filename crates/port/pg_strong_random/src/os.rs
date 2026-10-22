//! `OsEntropy` — the raw OS entropy internals, moved VERBATIM from the
//! pre-seam `lib.rs` (DST P2 contract §2.1). This file is the sanctioned
//! entropy-funnel internals (s2.3): the only place in the tree allowed to
//! touch raw OS entropy, ledgered in `crates/_support/seams_init/tests/lint-determinism.allow`.
//!
//! C's ladder is OpenSSL/Win32/dev-urandom; no OpenSSL here, so the primary
//! arm is getentropy(2) (same kernel CSPRNG), C's dev-urandom arm as
//! fallback.

use crate::EntropySource;

/// ZST; product builds monomorphize `ActiveEntropy = OsEntropy` (law 0.1).
pub(crate) struct OsEntropy;

impl OsEntropy {
    pub(crate) const fn new() -> Self {
        OsEntropy
    }
}

impl EntropySource for OsEntropy {
    #[inline]
    fn fill(&self, buf: &mut [u8]) -> bool {
        // getentropy caps a request at 256 bytes.
        for chunk in buf.chunks_mut(256) {
            #[cfg(not(target_env = "musl"))]
            // SAFETY: chunk is a live writable buffer of chunk.len() <= 256 bytes.
            let rc = unsafe { libc::getentropy(chunk.as_mut_ptr().cast(), chunk.len()) };
            // musl has getentropy(3) since 1.1.20, but the libc crate binds it
            // for glibc only, so do what glibc's and musl's getentropy do over
            // getrandom(2) (same kernel CSPRNG).
            #[cfg(target_env = "musl")]
            let rc = if getentropy_loop(chunk, getrandom) {
                0
            } else {
                -1
            };
            if rc != 0 {
                return dev_urandom(buf);
            }
        }
        true
    }
}

/// One `getrandom(2)` call with no flags: bytes written, or the errno.
#[cfg(target_env = "musl")]
fn getrandom(buf: &mut [u8]) -> Result<usize, i32> {
    // SAFETY: buf is a live writable buffer of buf.len() bytes.
    let n = unsafe { libc::getrandom(buf.as_mut_ptr().cast(), buf.len(), 0) };
    usize::try_from(n).map_err(|_| std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
}

/// getentropy(3) semantics over a getrandom-shaped call, as glibc and musl
/// implement them: fill all of `buf` or fail. `EINTR` retries (an
/// interrupted wait for the entropy pool must not reach the dev-urandom
/// fallback, which does not wait for it); a short read continues from where
/// it stopped; any other error, or a zero-byte read, fails.
#[cfg(target_env = "musl")]
fn getentropy_loop(buf: &mut [u8], mut call: impl FnMut(&mut [u8]) -> Result<usize, i32>) -> bool {
    let mut p = 0;
    while p < buf.len() {
        match call(&mut buf[p..]) {
            Ok(0) => return false,
            Ok(n) => p += n,
            Err(libc::EINTR) => {}
            Err(_) => return false,
        }
    }
    true
}

#[cold]
fn dev_urandom(buf: &mut [u8]) -> bool {
    // SAFETY: the path is a static NUL-terminated string.
    let f = unsafe { libc::open(c"/dev/urandom".as_ptr(), libc::O_RDONLY, 0) };
    if f == -1 {
        return false;
    }
    let mut p = 0;
    while p < buf.len() {
        // SAFETY: writes at most buf.len()-p bytes into the live tail of buf.
        let res = unsafe { libc::read(f, buf[p..].as_mut_ptr().cast(), buf.len() - p) };
        if res <= 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            // SAFETY: f is the fd opened above.
            unsafe { libc::close(f) };
            return false;
        }
        p += res as usize;
    }
    // SAFETY: f is the fd opened above.
    unsafe { libc::close(f) };
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    // Fallback-path parity (contract §2.1): the dev-urandom arm fills, and
    // fills differently per call, just like the getentropy arm.
    #[test]
    fn urandom_arm() {
        let mut a = [0u8; 300];
        let mut b = [0u8; 300];
        assert!(dev_urandom(&mut a));
        assert!(dev_urandom(&mut b));
        assert_ne!(&a[..], &[0u8; 300][..]);
        assert_ne!(&a[..], &b[..]);
    }

    // The musl arm keeps getentropy's contract: EINTR retries and a short
    // read resumes, rather than falling back to dev-urandom.
    #[cfg(target_env = "musl")]
    #[test]
    fn getentropy_loop_retries_eintr_and_short_reads() {
        let mut script = vec![Err(libc::EINTR), Ok(3), Err(libc::EINTR), Ok(5)].into_iter();
        let mut buf = [0u8; 8];
        assert!(getentropy_loop(&mut buf, |b| {
            let r = script.next().unwrap();
            if let Ok(n) = r {
                b[..n].fill(0xAB);
            }
            r
        }));
        assert_eq!(buf, [0xAB; 8]);
        assert!(script.next().is_none());
    }

    #[cfg(target_env = "musl")]
    #[test]
    fn getentropy_loop_fails_on_other_errors() {
        let mut buf = [0u8; 8];
        assert!(!getentropy_loop(&mut buf, |_| Err(libc::ENOSYS)));
        assert!(!getentropy_loop(&mut buf, |_| Ok(0)));
    }

    #[test]
    fn os_fill_via_trait() {
        let src = OsEntropy::new();
        let mut a = [0u8; 64];
        assert!(src.fill(&mut a));
        assert_ne!(a, [0u8; 64]);
    }
}
