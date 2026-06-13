//! Port of the `backend-utils-init-small` unit:
//! `src/backend/utils/init/globals.c` (the backend-global variable
//! declarations) and `src/backend/utils/init/usercontext.c` (run code as a
//! different database user).

#![allow(non_snake_case)]

pub mod globals;
pub mod usercontext;

pub use usercontext::{RestoreUserContext, SwitchToUntrustedUser};

/// Install this unit's seams (`backend-utils-init-small-seams`).
pub fn init_seams() {
    backend_utils_init_small_seams::work_mem::set(globals::work_mem);
    backend_utils_init_small_seams::max_worker_processes::set(globals::max_worker_processes);
    backend_utils_init_small_seams::max_parallel_workers::set(globals::max_parallel_workers);
    backend_utils_init_small_seams::is_under_postmaster::set(globals::IsUnderPostmaster);
    backend_utils_init_small_seams::is_postmaster_environment::set(globals::IsPostmasterEnvironment);
}

#[cfg(test)]
mod tests;
