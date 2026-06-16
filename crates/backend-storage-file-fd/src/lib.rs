#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the project-wide `PgResult` (== `Result<_,
// PgError>`); `PgError` is a large owned struct, so the un-boxed `Err` variant
// trips `clippy::result_large_err`. The un-boxed return is the project's error
// contract, so accept the lint crate-wide.
#![allow(clippy::result_large_err)]
// Some private LRU/free-list/desc-table helpers and platform-conditional paths
// (e.g. the non-Linux `do_syncfs` fall-through) are unreferenced on a given
// build configuration; allow dead_code crate-wide so the full fd.c surface stays
// present without per-item cfg gating.
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
pub mod seams;
pub mod sync_cleanup;
pub mod temp_files;
pub mod vfd_core;
pub mod vfd_io;

/// Install every seam this unit owns.
///
/// The unit owns two seam crates (by C-source coverage of `fd.c`):
/// `backend-storage-file-seams` and `backend-storage-file-fd-seams`. Every decl
/// in both is installed here, exactly once, as a thin marshal+delegate over the
/// already-ported fd.c logic in the family modules (and [`seams`] for the
/// cross-family adapters).
pub fn init_seams() {
    use backend_storage_file_fd_seams as fd_seams;
    use backend_storage_file_seams as file_seams;

    // -- backend-storage-file-fd-seams --------------------------------------

    // file_perm.c: SetDataDirectoryCreatePerm (owns the create-mode globals).
    backend_storage_file_fileperm_seams::set_data_directory_create_perm::set(
        vfd_core::set_data_directory_create_perm,
    );

    // GUC / init / errno glue.
    fd_seams::make_pg_directory::set(vfd_core::seam_make_pg_directory);
    fd_seams::init_file_access::set(vfd_core::seam_init_file_access);
    fd_seams::init_temporary_file_access::set(vfd_core::InitTemporaryFileAccess);
    fd_seams::set_max_safe_fds::set(vfd_core::set_max_safe_fds);
    fd_seams::last_errno::set(vfd_core::seam_last_errno);
    fd_seams::access_f_ok::set(vfd_core::seam_access_f_ok);
    fd_seams::unlink_file::set(vfd_core::seam_unlink_file);
    fd_seams::rename_file::set(vfd_core::seam_rename_file);

    // Transaction-end cleanup.
    fd_seams::at_eoxact_files::set(sync_cleanup::AtEOXact_Files);
    fd_seams::at_eosubxact_files::set(sync_cleanup::AtEOSubXact_Files);

    // sync / rename / existence primitives.
    fd_seams::pg_fsync::set(seams::seam_pg_fsync);
    fd_seams::fsync_fname::set(seams::seam_fsync_fname);
    fd_seams::pg_file_exists::set(seams::seam_pg_file_exists);
    fd_seams::basic_open_file::set(seams::basic_open_file);
    fd_seams::basic_open_file_flags::set(seams::basic_open_file_flags);

    // Transient-fd API (i32 contract).
    fd_seams::open_transient_file::set(seams::open_transient_file_i32);
    fd_seams::close_transient_file::set(seams::close_transient_file_i32);
    fd_seams::transient_read::set(seams::transient_read);
    fd_seams::transient_write::set(seams::transient_write);
    fd_seams::pg_pread::set(seams::pg_pread);
    fd_seams::pg_pwrite::set(seams::pg_pwrite);
    fd_seams::pg_pwrite_transient::set(seams::pg_pwrite_transient);
    fd_seams::pg_ftruncate_transient::set(seams::pg_ftruncate_transient);
    fd_seams::close_fd::set(seams::close_fd);
    fd_seams::pg_pwrite_zeros::set(seams::pg_pwrite_zeros);

    // Directory / tree helpers (decls owned here; logic in `seams`).
    fd_seams::rmtree::set(seams::rmtree);
    fd_seams::path_is_dir::set(seams::path_is_dir);
    fd_seams::read_dir_names::set(seams::read_dir_names);
    fd_seams::read_dir_names_logged::set(seams::read_dir_names_logged);
    fd_seams::get_dirent_type::set(seams::get_dirent_type);

    // AllocateFile-based reads/writes (snapmgr, timeline).
    fd_seams::allocate_file_write::set(seams::allocate_file_write);
    fd_seams::create_empty_file::set(seams::create_empty_file);
    fd_seams::allocate_file_read::set(seams::allocate_file_read);
    fd_seams::read_file_or_absent::set(seams::read_file_or_absent);
    fd_seams::file_exists::set(seams::file_exists);

    // relmapper load/store steps.
    fd_seams::relmap_read_file::set(seams::relmap_read_file);
    fd_seams::relmap_write_temp::set(seams::relmap_write_temp);
    fd_seams::relmap_durable_rename::set(seams::relmap_durable_rename);

    // COPY TO stream family.
    fd_seams::open_copy_to_file::set(seams::open_copy_to_file);
    fd_seams::open_pipe_stream_write::set(seams::open_pipe_stream_write);
    fd_seams::copy_write_file::set(seams::copy_write_file);
    fd_seams::free_file::set(seams::free_file);
    fd_seams::close_pipe_to_program::set(seams::close_pipe_to_program);
    fd_seams::open_pipe_stream_read::set(seams::open_pipe_stream_read);
    fd_seams::pipe_read_line::set(seams::pipe_read_line);
    fd_seams::close_pipe_stream::set(seams::close_pipe_stream);
    fd_seams::stdout_stream::set(seams::stdout_stream);

    // VFD temp-file API consumed by buffile.c.
    fd_seams::open_temporary_file::set(temp_files::OpenTemporaryFile);
    fd_seams::file_close::set(vfd_io::seam_file_close);
    fd_seams::file_read::set(vfd_io::seam_file_read);
    fd_seams::file_write::set(vfd_io::seam_file_write);
    fd_seams::file_size::set(vfd_io::FileSize);
    fd_seams::file_truncate::set(vfd_io::FileTruncate);
    fd_seams::file_path_name::set(vfd_io::FilePathName);
    fd_seams::path_name_open_file::set(seams::path_name_open_file);
    fd_seams::lstat_mtime::set(seams::lstat_mtime);

    // -- backend-storage-file-seams -----------------------------------------
    file_seams::with_allocated_dir::set(allocated_desc::with_allocated_dir);
    file_seams::open_transient_file::set(allocated_desc::seam_open_transient_file);
    file_seams::close_transient_file::set(allocated_desc::seam_close_transient_file);
    file_seams::reserve_external_fd::set(vfd_core::ReserveExternalFD);
    file_seams::release_external_fd::set(vfd_core::ReleaseExternalFD);
    file_seams::acquire_external_fd::set(vfd_core::AcquireExternalFD);
    file_seams::pg_fsync::set(seams::seam_pg_fsync);
    file_seams::fsync_fname::set(seams::seam_fsync_fname);
    file_seams::data_sync_elevel::set(seams::seam_data_sync_elevel);
    file_seams::durable_rename::set(seams::seam_durable_rename);
}
