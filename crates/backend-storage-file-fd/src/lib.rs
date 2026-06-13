#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the project-wide `PgResult` (== `Result<_,
// PgError>`); `PgError` is a large owned struct, so the un-boxed `Err` variant
// trips `clippy::result_large_err`. The un-boxed return is the project's error
// contract, so accept the lint crate-wide.
#![allow(clippy::result_large_err)]
// SCAFFOLD STAGE: private LRU/free-list/desc-table helpers are declared with
// `todo!()` bodies ahead of the public entry points that will call them; they
// are intentionally unused until the families' logic lands.
#![allow(dead_code)]

//! `backend-storage-file-fd` — port of `src/backend/storage/file/fd.c`.
//!
//! `fd.c` is the virtual file descriptor (VFD) layer: a per-backend pool of
//! kernel file handles managed as an LRU ring so a backend can hold far more
//! logical files open than the OS fd limit allows. A [`File`] value is an
//! *index* into the VFD cache, not a kernel handle.
//!
//! All of fd.c's state — the VFD cache, the LRU ring, the free list, the
//! allocated-descriptor table, temp-file accounting and the temp-tablespace
//! list — is per-backend, so the port keeps it in `thread_local!` state, never
//! in shared statics (see AGENTS.md "Backend-global state").
//!
//! This unit is split into five family modules (decomposition track):
//!
//!  * [`vfd_core`]   — the [`Vfd`] struct + VFD cache / LRU ring / free list,
//!    `AllocateVfd`/`FreeVfd`, `FileAccess`/`FileInvalidate`,
//!    `BasicOpenFile[Perm]`, the external-FD reservation family,
//!    `count_usable_fds`/`set_max_safe_fds`, `InitFileAccess`, and the
//!    resowner -> RAII `File` ownership glue.
//!  * [`vfd_io`]     — `PathNameOpenFile[Perm]`, `FileClose`, the
//!    `FileReadV`/`FileWriteV`/`FileStartReadV` AIO surface,
//!    `FilePrefetch`/`Writeback`/`Sync`/`Zero`/`Fallocate`/`Size`/`Truncate`,
//!    `FilePathName`/`FileGetRawDesc`.
//!  * [`temp_files`] — `OpenTemporaryFile[InTablespace]`, `TempTablespacePath`,
//!    the `PathName{Create,Delete}Temporary{Dir,File}` family,
//!    `RegisterTemporaryFile`, temp-tablespace state and `temp_file_limit`.
//!  * [`allocated_desc`] — the `allocatedDescs` table, `AllocateFile`/`FreeFile`,
//!    `OpenTransientFile[Perm]`/`CloseTransientFile`,
//!    `OpenPipeStream`/`ClosePipeStream`, `AllocateDir`/`ReadDir`/`FreeDir`,
//!    `closeAllVfds`.
//!  * [`sync_cleanup`] — the `pg_fsync` family, `fsync_fname`/`durable_rename`/
//!    `durable_unlink`, `walkdir`/`SyncDataDirectory`, `RemovePgTempFiles`,
//!    the `AtEOSubXact`/`AtEOXact`/`BeforeShmemExit_Files`/`CleanupTempFiles`
//!    transaction-end cleanup.

pub mod allocated_desc;
pub mod sync_cleanup;
pub mod temp_files;
pub mod vfd_core;
pub mod vfd_io;

/// Install every seam this unit owns.
///
/// The unit owns two seam crates (by C-source coverage of `fd.c`):
/// `backend-storage-file-seams` and `backend-storage-file-fd-seams`. The
/// fd.c-direct inward adapters are installed here, exactly once. NOTE: a number
/// of inward decls in `backend-storage-file-fd-seams` that wrap consumer-side
/// I/O loops (the `relmap_*`, `allocate_file_*`, `open_copy_to_file`/
/// `copy_write_file` families, `access_f_ok`, `read_dir_names*`, `rename_file`/
/// `unlink_file`) are not yet installed — those adapters still need to be
/// authored as thin marshal+delegate wrappers. See `audits/backend-storage-file-fd.md`.
pub fn init_seams() {
    use backend_storage_file_fd_seams as fd_seams;
    use backend_storage_file_seams as file_seams;

    // backend-storage-file-fd-seams — the fd.c-direct inward adapters this unit
    // owns and backs with real fd.c logic.
    fd_seams::make_pg_directory::set(vfd_core::seam_make_pg_directory);
    fd_seams::at_eoxact_files::set(sync_cleanup::AtEOXact_Files);
    fd_seams::at_eosubxact_files::set(sync_cleanup::AtEOSubXact_Files);
    fd_seams::init_file_access::set(vfd_core::seam_init_file_access);
    fd_seams::init_temporary_file_access::set(vfd_core::InitTemporaryFileAccess);
    fd_seams::set_max_safe_fds::set(vfd_core::set_max_safe_fds);
    fd_seams::last_errno::set(vfd_core::seam_last_errno);
    fd_seams::access_f_ok::set(vfd_core::seam_access_f_ok);
    fd_seams::unlink_file::set(vfd_core::seam_unlink_file);
    fd_seams::rename_file::set(vfd_core::seam_rename_file);

    // backend-storage-file-seams
    file_seams::with_allocated_dir::set(allocated_desc::with_allocated_dir);
    file_seams::open_transient_file::set(allocated_desc::seam_open_transient_file);
    file_seams::close_transient_file::set(allocated_desc::seam_close_transient_file);
    file_seams::reserve_external_fd::set(vfd_core::ReserveExternalFD);
    file_seams::release_external_fd::set(vfd_core::ReleaseExternalFD);
}
