//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`utils/init/miscinit.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `process_shared_preload_libraries_in_progress` (miscinit.c) — whether
    /// the backend is currently inside the `shared_preload_libraries`
    /// initialization window. A backend-local global read.
    pub fn process_shared_preload_libraries_in_progress() -> bool
);
