//! Seam declarations for the `backend-postmaster-pmchild` unit
//! (`postmaster/pmchild.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `InitPostmasterChildSlots()` (pmchild.c): allocate the postmaster's
    /// per-child-slot array from the configured backend counts. Called in
    /// bootstrap/single-process mode too (sizing only); infallible.
    pub fn init_postmaster_child_slots()
);
