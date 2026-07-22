#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend-storage-file-fd` — src/backend/storage/file/fd.c: the per-backend
//! virtual file descriptor cache (LRU ring over kernel fds), temp files, the
//! allocated-descriptor table, and the fsync/durability helpers.

pub mod buffile;
pub mod copydir;
pub mod desc;
pub mod fileset;
pub mod reinit;
pub mod io;
pub mod sync;
pub mod temp;
pub mod vfd;

pub use buffile::{
    BufFile, BufFileCreateFileSet, BufFileCreateTemp, BufFileDeleteFileSet, BufFileOpenFileSet,
    BufFileOpenFileSetMaybe, PrepareTempTablespaces,
};
pub use fileset::FileSet;
pub use copydir::{copy_file, copydir, directory_is_empty, pg_mkdir_p, rmtree};
pub use desc::{
    closeAllVfds, with_allocated_dir, with_allocated_stdio, AllocateDir, AllocateFile,
    ClosePipeStream, CloseTransientFile, FreeDir, FreeFile, OpenPipeStream, OpenTransientFile,
    OpenTransientFilePerm, PipeStreamGets, ReadDir, ReadDirExtended, TransientFileRawFd,
};
pub use io::{
    pg_file_size_raw, pg_pread, pg_pwrite, FileClose, FileFallocate, FileGetRawDesc, FileGetRawFlags,
    FileGetRawMode, FilePathName, FilePrefetch, FileRead, FileReadV, FileSize,
    FileRawDescForAio, FileStartBufferRead, FileStartReadV, FileSync, FileTruncate, FileWrite,
    FileWriteV,
    FileWriteback, FileZero, PathNameOpenFile, PathNameOpenFilePerm,
};
pub use sync::{
    data_sync_elevel, durable_rename, durable_unlink, fsync_fname, fsync_fname_ext,
    looks_like_temp_rel_name, pg_close, pg_fdatasync, pg_file_exists, pg_flush_data, pg_fstat,
    pg_fsync, pg_fsync_no_writethrough, pg_fsync_writethrough, pg_lstat, pg_readlink, pg_rename,
    pg_stat, pg_truncate, pg_unlink, read_whole_file, write_whole_file, AtEOSubXact_Files,
    AtEOXact_Files, BeforeShmemExit_Files, RemovePgTempFiles, RemovePgTempFilesInDir,
    SyncDataDirectory,
};
// The VFS's plain stat carrier, for the pg_stat/pg_lstat callers.
pub use ::vfs::FileInfo;
// The shared errno TLS cell (DST P1 errno contract): fenced callers read the
// raw errno after fd-crate calls through these, not via std::io::Error.
pub use ::vfs::{get_errno, set_errno};
pub use temp::{
    GetNextTempTableSpace, GetTempTablespaces, OpenTemporaryFile, PathNameCreateTemporaryDir,
    PathNameCreateTemporaryFile, PathNameDeleteTemporaryDir, PathNameDeleteTemporaryFile,
    PathNameOpenTemporaryFile, SetTempTablespaces, TempTablespacePath, TempTablespacesAreSet,
};
pub use vfd::resowner::FILE_RESOWNER_DESC;
pub use vfd::{
    io_direct_flags, max_files_per_process, max_safe_fds, set_io_direct_flags, set_max_safe_fds,
    AcquireExternalFD, BasicOpenFile, BasicOpenFilePerm, InitFileAccess, InitTemporaryFileAccess,
    MakePGDirectory, ReattachRetainedFileAccess, ReleaseExternalFD, ReserveExternalFD,
};

pub fn init_seams() {
    file_seams::open_transient_file::set(desc::OpenTransientFile);
    file_seams::basic_open_file::set(|name, flags| {
        vfd::BasicOpenFile(name, flags).expect("BasicOpenFile: fd table poisoned")
    });
    file_seams::close_transient_file::set(desc::CloseTransientFile);
    file_seams::pg_fsync::set(sync::pg_fsync);
    file_seams::fsync_fname::set(|fname, isdir| sync::fsync_fname(fname, isdir));
    file_seams::data_sync_elevel::set(sync::data_sync_elevel);
    file_seams::with_allocated_dir::set(desc::with_allocated_dir);

    // fd.c owns these GUC variables' backing storage (fd.c:146-171).
    {
        use guc_tables::{hooks, vars, GucHookExtra, GucVarAccessors};

        vars::max_files_per_process.install(GucVarAccessors {
            get: vfd::max_files_per_process,
            set: vfd::set_max_files_per_process,
        });
        vars::data_sync_retry.install(GucVarAccessors {
            get: vfd::data_sync_retry,
            set: vfd::set_data_sync_retry,
        });
        vars::recovery_init_sync_method.install(GucVarAccessors {
            get: vfd::recovery_init_sync_method,
            set: vfd::set_recovery_init_sync_method,
        });
        vars::file_extend_method.install(GucVarAccessors {
            get: vfd::file_extend_method,
            set: vfd::set_file_extend_method,
        });
        // C home is copydir.c; storage hosted with the other fd-owned GUCs.
        vars::file_copy_method.install(GucVarAccessors {
            get: vfd::file_copy_method,
            set: vfd::set_file_copy_method,
        });
        // C home is tablespace.c; hosted here until that unit ports so
        // PrepareTempTablespaces sees SET values (non-empty stays loud there).
        vars::temp_tablespaces.install(GucVarAccessors {
            get: vfd::temp_tablespaces_guc,
            set: vfd::set_temp_tablespaces_guc,
        });

        hooks::check_debug_io_direct.install(|newval, extra, _source| {
            let flags = vfd::check_debug_io_direct(newval.as_deref().unwrap_or(""))?;
            *extra = Some(Box::new(flags) as GucHookExtra);
            Ok(true)
        });
        hooks::assign_debug_io_direct.install(|_newval, extra| {
            let flags = extra
                .and_then(|e| e.downcast_ref::<i32>())
                .copied()
                .expect("assign_debug_io_direct requires check_debug_io_direct's extra");
            vfd::assign_debug_io_direct(flags);
        });

        // pgrust.resource_counters (PGC_INTERNAL, computed): the value shown
        // comes from the show hook; the stored string is never consulted, so
        // the backing is a constant empty and SETs (only the boot-default
        // write can reach it) are dropped.
        vars::pgrust_resource_counters.install(GucVarAccessors {
            get: || Some(String::new()),
            set: |_| {},
        });
        hooks::show_resource_counters.install(vfd::show_resource_counters);
    }
}

#[cfg(test)]
mod tests;

// AIO engine unported => no in-flight AIO can reference any fd; skipping the
// uninstalled seam is C's empty-drain arm.
pub(crate) fn pgaio_closing_fd_if_engine_present(fd: i32) {
    if aio_seams::pgaio_closing_fd::is_installed() {
        aio_seams::pgaio_closing_fd::call(fd);
    }
}
