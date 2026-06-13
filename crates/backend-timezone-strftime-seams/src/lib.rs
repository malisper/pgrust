//! Seam declarations for the `backend-timezone-strftime` unit
//! (`src/timezone/strftime.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `pg_strftime(s, maxsize, format, t)` (`timezone/strftime.c`) — format
    /// a broken-down time into the caller-supplied buffer, mirroring the C
    /// fill-a-buffer contract. `buf.len()` plays the role of `maxsize - 1`
    /// (the C NUL terminator is not stored). Returns the number of bytes
    /// written, or `0` if the rendering did not fit (the C error return).
    pub fn pg_strftime(buf: &mut [u8], format: &str, t: &types_pgtime::pg_tm) -> usize
);
