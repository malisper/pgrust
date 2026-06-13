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
//!   because this unit installs the pending `backend-storage-ipc-seams`
//!   declarations (`proc_exit`).
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

/// Install this crate's implementations into the seam crates it owns, plus
/// its GUC option array and storage variables into the GUC tables' slots.
pub fn init_seams() {
    use backend_utils_misc_guc_tables::{option_sets, vars, GucVarAccessors};

    backend_storage_ipc_seams::proc_exit::set(ipc::proc_exit);

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
