//! Seam declarations for the `backend-timezone-strftime` unit
//! (`src/timezone/strftime.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `pg_strftime(s, maxsize, format, t)` (`timezone/strftime.c`) — format
    /// a broken-down time. The C buffer-and-maxsize contract is marshaled as
    /// an owned `String` of the full rendering; the caller imposes its own
    /// buffer cap (as the C caller's `maxsize` would).
    pub fn pg_strftime(format: &str, t: &types_pgtime::pg_tm) -> ::std::string::String
);
