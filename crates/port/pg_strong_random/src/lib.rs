// C's ladder is OpenSSL/Win32/dev-urandom; no OpenSSL here, so the primary
// arm is getentropy(2) (same kernel CSPRNG), C's dev-urandom arm as fallback.

pub fn pg_strong_random_init() {}

#[must_use]
pub fn pg_strong_random(buf: &mut [u8]) -> bool {
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

    #[test]
    fn fills_and_varies() {
        let mut a = [0u8; 64];
        let mut b = [0u8; 64];
        assert!(pg_strong_random(&mut a));
        assert!(pg_strong_random(&mut b));
        assert_ne!(a, b);
        assert_ne!(a, [0u8; 64]);
    }

    #[test]
    fn urandom_arm() {
        let mut a = [0u8; 300];
        assert!(dev_urandom(&mut a));
        assert_ne!(&a[..], &[0u8; 300][..]);
    }

    #[test]
    fn large_request_chunks() {
        let mut a = vec![0u8; 700];
        assert!(pg_strong_random(&mut a));
        assert!(a.iter().any(|&b| b != 0));
    }
}
