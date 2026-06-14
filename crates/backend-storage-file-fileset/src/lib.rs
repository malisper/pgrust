//! `backend-storage-file-fileset` — a faithful port of
//! `src/backend/storage/file/fileset.c`.
//!
//! FileSets provide a temporary namespace (think directory) so that files can be
//! discovered by name. They are used when temporary files need to be
//! opened/closed multiple times and the underlying files must survive across
//! transactions (e.g. the parallel-sort / parallel-hash `SharedFileSet`).
//!
//! A FileSet is, under the covers, one directory per configured temp tablespace,
//! named `pgsql_tmp<creator_pid>.<number>.fileset`; the files within it live at
//! `<that dir>/<name>` and are distributed over the tablespaces by hashing the
//! file name.
//!
//! This is a thin layer over the already-ported `fd` temp-file / temp-tablespace
//! API (`PathNameCreateTemporaryFile` / `PathNameOpenTemporaryFile` /
//! `PathNameDeleteTemporaryFile` / `PathNameCreateTemporaryDir` /
//! `PathNameDeleteTemporaryDir` / `TempTablespacePath` / `GetTempTablespaces`,
//! called directly on the sibling `backend-storage-file-fd` crate), the
//! `hash_any` from `common-hashfn`, and the backend globals `MyProcPid` /
//! `MyDatabaseTableSpace` (reached through `backend-utils-init-small-seams`).
//! The one genuinely-external edge `fd` does not own — `PrepareTempTablespaces()`
//! from `commands/tablespace.c` — crosses the
//! `backend-commands-tablespace-seams::prepare_temp_tablespaces` seam.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the project-wide `PgResult` (== `Result<_,
// PgError>`); `PgError` is a large owned struct, so the un-boxed `Err` variant
// trips `clippy::result_large_err`. The un-boxed return is the project's error
// contract, so accept the lint crate-wide rather than scatter per-fn `#[allow]`s.
#![allow(clippy::result_large_err)]

use std::sync::atomic::{AtomicU32, Ordering};

use backend_commands_tablespace_seams::prepare_temp_tablespaces;
use backend_storage_file_fd::temp_files::{
    GetTempTablespaces, PathNameCreateTemporaryDir, PathNameCreateTemporaryFile,
    PathNameDeleteTemporaryFile, PathNameOpenTemporaryFile, TempTablespacePath,
};
use backend_utils_init_small_seams::{my_database_table_space, my_proc_pid};
use common_hashfn::hash_bytes;
use types_core::Oid;
use types_error::PgResult;
use types_storage::file::File;
use types_storage::fileset::{FileSet, FILESET_MAX_TABLESPACES};

mod seams;
pub use seams::init_seams;

/// `InvalidOid` (`postgres_ext.h`).
const INVALID_OID: Oid = 0;

/// `PG_TEMP_FILE_PREFIX` (`storage/fd.h`).
use types_storage::file::PG_TEMP_FILE_PREFIX;

/// `static uint32 counter` in `FileSetInit` (fileset.c:54).
///
/// In C this is a function-local `static`, i.e. one counter shared by every
/// `FileSetInit` call in the process. Kept here as a single process-global
/// atomic with the same semantics (read the current value into `number`, then
/// advance `(counter + 1) % INT_MAX`). A relaxed RMW is sufficient: the value is
/// purely a uniquifier for the directory name and has no ordering relationship
/// with any other state.
static COUNTER: AtomicU32 = AtomicU32::new(0);

/// `INT_MAX` (`<limits.h>`) — the modulus the C counter wraps at (fileset.c:58).
const INT_MAX: u32 = i32::MAX as u32;

/// Initialize a space for temporary files. (`FileSetInit`, fileset.c:51-86.)
///
/// This API can be used by a shared fileset as well as when the temporary files
/// are used only by a single backend but need to be opened/closed multiple times
/// and to survive across transactions. The callers are expected to explicitly
/// remove such files via [`FileSetDelete`]/[`FileSetDeleteAll`]. Files are
/// distributed over the tablespaces configured in `temp_tablespaces`.
pub fn FileSetInit(fileset: &mut FileSet) -> PgResult<()> {
    // fileset.c:56-58 -- creator_pid = MyProcPid; number = counter;
    //                    counter = (counter + 1) % INT_MAX.
    fileset.creator_pid = my_proc_pid::call();
    fileset.number = COUNTER
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |counter| {
            Some((counter + 1) % INT_MAX)
        })
        .expect("the update closure always returns Some");

    // fileset.c:60-64 -- Capture the tablespace OIDs so that all backends agree
    // on them.
    prepare_temp_tablespaces::call()?;
    fileset.ntablespaces =
        GetTempTablespaces(&mut fileset.tablespaces[..FILESET_MAX_TABLESPACES]);
    if fileset.ntablespaces == 0 {
        // fileset.c:65-70 -- If the GUC is empty, use the current database's
        // default tablespace.
        fileset.tablespaces[0] = my_database_table_space::call();
        fileset.ntablespaces = 1;
    } else {
        // fileset.c:71-85 -- An entry of InvalidOid means "use the default
        // tablespace for the current database". Replace that now so all users of
        // the FileSet agree on what to do.
        for i in 0..fileset.ntablespaces as usize {
            if fileset.tablespaces[i] == INVALID_OID {
                fileset.tablespaces[i] = my_database_table_space::call();
            }
        }
    }
    Ok(())
}

/// Create a new file in the given set. (`FileSetCreate`, fileset.c:91-114.)
pub fn FileSetCreate(fileset: &FileSet, name: &str) -> PgResult<File> {
    let path = FilePath(fileset, name);
    // fileset.c:98 -- PathNameCreateTemporaryFile(path, false): a `<= 0` return
    // (here `Ok(File(<= 0))`) is a non-fatal "could not create", which makes us
    // try to create the directory on demand and retry.
    let file = PathNameCreateTemporaryFile(&path, false)?;
    if file.0 <= 0 {
        // fileset.c:101-110 -- create the directory on demand, then retry with
        // error_on_failure = true.
        let tablespace = ChooseTablespace(fileset, name);
        let tempdirpath = TempTablespacePath(tablespace);
        let filesetpath = FileSetPath(fileset, tablespace);
        PathNameCreateTemporaryDir(&tempdirpath, &filesetpath)?;
        return PathNameCreateTemporaryFile(&path, true);
    }
    Ok(file)
}

/// Open a file that was created with [`FileSetCreate`]. (`FileSetOpen`,
/// fileset.c:116-128.)
pub fn FileSetOpen(fileset: &FileSet, name: &str, mode: i32) -> PgResult<File> {
    let path = FilePath(fileset, name);
    PathNameOpenTemporaryFile(&path, mode)
}

/// Delete a file that was created with [`FileSetCreate`]. Returns `true` if the
/// file existed, `false` if it did not. (`FileSetDelete`, fileset.c:135-144.)
pub fn FileSetDelete(fileset: &FileSet, name: &str, error_on_failure: bool) -> PgResult<bool> {
    let path = FilePath(fileset, name);
    PathNameDeleteTemporaryFile(&path, error_on_failure)
}

/// Delete all files in the set. (`FileSetDeleteAll`, fileset.c:149-165.)
pub fn FileSetDeleteAll(fileset: &FileSet) -> PgResult<()> {
    // fileset.c:160-164 -- Delete the directory we created in each tablespace.
    // Doesn't fail because we use this in error-cleanup paths, but can generate a
    // LOG message on IO error.
    for i in 0..fileset.ntablespaces as usize {
        let dirpath = FileSetPath(fileset, fileset.tablespaces[i]);
        backend_storage_file_fd::temp_files::PathNameDeleteTemporaryDir(&dirpath)?;
    }
    Ok(())
}

/// Build the path for the directory holding the files backing a FileSet in a
/// given tablespace. (`FileSetPath`, fileset.c:171-180.)
fn FileSetPath(fileset: &FileSet, tablespace: Oid) -> String {
    // fileset.c:176-179 -- "<tempdirpath>/<PG_TEMP_FILE_PREFIX><creator_pid>.
    //                       <number>.fileset".
    let tempdirpath = TempTablespacePath(tablespace);
    format!(
        "{tempdirpath}/{PG_TEMP_FILE_PREFIX}{}.{}.fileset",
        // C casts creator_pid to `unsigned long` for the `%lu` format; the value
        // is a non-negative pid, so the decimal rendering is identical.
        fileset.creator_pid as i64 as u64,
        fileset.number
    )
}

/// Determine which tablespace a given temporary file belongs in.
/// (`ChooseTablespace`, fileset.c:185-191.)
fn ChooseTablespace(fileset: &FileSet, name: &str) -> Oid {
    // fileset.c:188-190 -- hash_any(name, strlen(name)) % ntablespaces.
    let hash = hash_bytes(name.as_bytes());
    fileset.tablespaces[(hash % fileset.ntablespaces as u32) as usize]
}

/// Compute the full path of a file in a FileSet. (`FilePath`, fileset.c:196-203.)
fn FilePath(fileset: &FileSet, name: &str) -> String {
    // fileset.c:201-202 -- "<dirpath>/<name>", where dirpath is the
    // per-tablespace FileSet directory chosen by hashing `name`.
    let dirpath = FileSetPath(fileset, ChooseTablespace(fileset, name));
    format!("{dirpath}/{name}")
}
