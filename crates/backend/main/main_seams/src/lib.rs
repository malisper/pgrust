//! Seam declarations for the `backend-main-main` unit (`main/main.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use ::types_startup::DispatchOption;

seam_core::seam!(
    /// `parse_dispatch_option(name)` (main.c): map a must-be-first option name
    /// to its [`DispatchOption`]; an unmatched name yields
    /// `DISPATCH_POSTMASTER`. Pure string lookup; infallible.
    pub fn parse_dispatch_option(name: &str) -> DispatchOption
);
