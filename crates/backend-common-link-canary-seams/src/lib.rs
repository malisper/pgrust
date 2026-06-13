//! Seam declarations for the `common-link-canary` unit
//! (`src/common/link-canary.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `pg_link_canary_is_frontend()` (link-canary.c): whether the canary
    /// compiled into the *frontend* copy of this file got linked in (a build
    /// misconfiguration). Pure constant return; infallible.
    pub fn pg_link_canary_is_frontend() -> bool
);
