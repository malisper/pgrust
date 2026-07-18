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
            // SAFETY: chunk is a live writable buffer of chunk.len() <= 256 bytes.
            let rc = unsafe { libc::getentropy(chunk.as_mut_ptr().cast(), chunk.len()) };
            if rc != 0 {
                return dev_urandom(buf);
            }
        }
        true
    }
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

    #[test]
    fn os_fill_via_trait() {
        let src = OsEntropy::new();
        let mut a = [0u8; 64];
        assert!(src.fill(&mut a));
        assert_ne!(a, [0u8; 64]);
    }
}
