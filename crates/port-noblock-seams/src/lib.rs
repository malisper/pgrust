//! Seam declarations for the `port-noblock` unit (`src/port/noblock.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `bool pg_set_noblock(pgsocket sock)` — put the socket into
    /// non-blocking mode. Returns `true` on success. Never ereports.
    pub fn pg_set_noblock(sock: types_core::pgsocket) -> bool
);
