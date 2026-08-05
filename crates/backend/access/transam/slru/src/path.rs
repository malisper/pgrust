use core::fmt::{self, Write};

const DIR_CAP: usize = 64;
const PATH_CAP: usize = 96;

#[derive(Clone, Copy)]
pub struct SlruDir {
    buf: [u8; DIR_CAP],
    len: u8,
}

impl SlruDir {
    pub(crate) fn new(subdir: &str) -> Self {
        // C: `char Dir[64]` filled by strlcpy; SLRU subdirs are ASCII literals.
        assert!(subdir.is_ascii());
        let n = subdir.len().min(DIR_CAP - 1);
        let mut buf = [0u8; DIR_CAP];
        buf[..n].copy_from_slice(&subdir.as_bytes()[..n]);
        Self { buf, len: n as u8 }
    }

    pub fn as_str(&self) -> &str {
        // ASCII invariant established in new().
        core::str::from_utf8(&self.buf[..self.len as usize]).expect("non-ASCII SLRU dir")
    }
}

impl fmt::Display for SlruDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Stack-built segment path (C: `char path[MAXPGPATH]` + snprintf); dir(≤63)
/// + '/' + ≤15 hex digits always fits, and the untouched zeroed tail keeps it
/// NUL-terminated for unlink(2).
pub struct SlruPath {
    buf: [u8; PATH_CAP],
    len: u8,
}

impl SlruPath {
    pub(crate) fn new() -> Self {
        Self {
            buf: [0u8; PATH_CAP],
            len: 0,
        }
    }

    pub fn as_str(&self) -> &str {
        core::str::from_utf8(&self.buf[..self.len as usize]).expect("non-ASCII SLRU path")
    }

    pub(crate) fn as_c_ptr(&self) -> *const u8 {
        debug_assert!((self.len as usize) < PATH_CAP);
        self.buf.as_ptr()
    }
}

impl Write for SlruPath {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let len = self.len as usize;
        if len + s.len() >= PATH_CAP {
            return Err(fmt::Error);
        }
        self.buf[len..len + s.len()].copy_from_slice(s.as_bytes());
        self.len = (len + s.len()) as u8;
        Ok(())
    }
}

impl fmt::Display for SlruPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
