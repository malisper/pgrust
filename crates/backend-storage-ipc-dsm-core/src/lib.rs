//! `backend-storage-ipc-dsm-core` — dynamic shared memory management plus
//! exit-time cleanup.
//!
//! Covers three C files:
//!
//! * `storage/ipc/dsm_impl.c` ([`dsm_impl`]) — the low-level OS primitives
//!   (POSIX shm, System V shm, mmap'd files; the Windows implementation is
//!   not ported, matching the repo's established platform scope).
//! * `storage/ipc/dsm.c` ([`dsm`]) — the reference-counted convenience layer:
//!   the shared control segment, backend-local segment descriptors, pinning,
//!   and on-detach callbacks.
//! * `storage/ipc/ipc.c` ([`ipc`]) — `proc_exit`/`shmem_exit` and the on-exit
//!   callback lists. `ipc.c` lives here because it and `dsm.c` are mutually
//!   recursive (`shmem_exit` hard-codes a call to `dsm_backend_shutdown`;
//!   `dsm_postmaster_startup` registers itself with `on_shmem_exit`), and
//!   because this unit installs the pending `backend-storage-ipc-dsm-core-seams`
//!   declarations (`proc_exit`, `on_proc_exit`, `on_shmem_exit`).
//!
//! The C `dsm_segment *` becomes a stable [`dsm::DsmSegmentId`] naming a
//! descriptor in a backend-local (`thread_local`) list, plus a
//! [`dsm::DsmSegment`] RAII guard standing in for the `ResourceOwner`
//! bookkeeping per `docs/query-lifecycle-raii.md` (resowner.c is not ported;
//! `ResourceOwnerRememberDSM`/`ForgetDSM` become guard construction/drop, and
//! `ResOwnerReleaseDSM` is the guard's `Drop`).
//!
//! The control segment and the preallocated main-region pages are genuinely
//! shared memory and are accessed through raw pointers under
//! `DynamicSharedMemoryControlLock`, exactly as in C.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

pub mod dsm;
pub mod dsm_impl;
pub mod ipc;

/// Reusable in-process DSM control-segment bring-up for unit tests. Gated
/// behind the `test-bringup` feature so the substrate-owner dependencies it
/// wires (the merged LWLock manager + FreePageManager owners) are pulled in
/// only for test builds, never for the production dependency graph. Enabled
/// automatically under `cfg(test)` for this crate's own gating test.
#[cfg(any(test, feature = "test-bringup"))]
pub mod test_bringup;

/// Install this crate's implementations into the seam crates it owns, plus
/// its GUC option array and storage variables into the GUC tables' slots.
pub fn init_seams() {
    use backend_utils_misc_guc_tables::{option_sets, vars, GucVarAccessors};

    backend_storage_ipc_dsm_core_seams::dsm_detach_all::set(|| {
        dsm::dsm_detach_all().expect("dsm_detach_all failed")
    });

    // `dsm_estimate_size()` is infallible in-crate (`usize`); the seam carries
    // the `add_size`/`mul_size` overflow `ereport` surface, so wrap as `Ok`.
    backend_storage_ipc_dsm_core_seams::dsm_estimate_size::set(|| Ok(dsm::dsm_estimate_size()));
    backend_storage_ipc_dsm_core_seams::dsm_shmem_init::set(dsm::dsm_shmem_init);
    // `dsm_postmaster_startup(PGShmemHeader *shim)` reads the global
    // `MaxBackends` (globals.c); the in-crate port takes it explicitly per the
    // no-ambient-global rule, so resolve it through the init-small seam.
    backend_storage_ipc_dsm_core_seams::dsm_postmaster_startup::set(|shim| {
        let max_backends = backend_utils_init_small_seams::max_backends::call();
        dsm::dsm_postmaster_startup(shim, max_backends)
    });

    backend_storage_ipc_dsm_core_seams::proc_exit::set(ipc::proc_exit);
    backend_storage_ipc_dsm_core_seams::on_proc_exit::set(ipc::on_proc_exit);
    backend_storage_ipc_dsm_core_seams::on_shmem_exit::set(ipc::on_shmem_exit);
    backend_storage_ipc_dsm_core_seams::before_shmem_exit::set(ipc::before_shmem_exit);
    backend_storage_ipc_dsm_core_seams::on_exit_reset::set(ipc::on_exit_reset);
    backend_storage_ipc_dsm_core_seams::check_on_shmem_exit_lists_are_empty::set(
        ipc::check_on_shmem_exit_lists_are_empty,
    );

    option_sets::dynamic_shared_memory_options.install(dsm_impl::DYNAMIC_SHARED_MEMORY_OPTIONS);
    vars::dynamic_shared_memory_type.install(GucVarAccessors {
        get: dsm_impl::dynamic_shared_memory_type,
        set: dsm_impl::set_dynamic_shared_memory_type,
    });
    vars::min_dynamic_shared_memory.install(GucVarAccessors {
        get: dsm_impl::min_dynamic_shared_memory,
        set: dsm_impl::set_min_dynamic_shared_memory,
    });
}

#[cfg(test)]
mod tests;
