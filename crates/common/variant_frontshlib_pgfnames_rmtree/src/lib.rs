//! Port of PostgreSQL's `src/common/pgfnames.c` and `src/common/rmtree.c`
//! (the FRONTEND/shared-library variant: plain `opendir`/`readdir`/`closedir`
//! and `opendir`-based `rmtree`, as opposed to the backend `AllocateDir`
//! variant that lives in `backend-storage-file-fd`).
//!
//! - [`pgfnames`] lists the names of the objects in a directory, excluding
//!   `.` and `..`.
//! - [`pgfnames_cleanup`] frees that list (a no-op move in Rust — the owned
//!   `Vec` frees on drop, mirroring the C `pfree` loop).
//! - [`rmtree`] removes a directory tree recursively.
//!
//! ## Memory model
//!
//! The C builds the working `filenames`/`dirnames` arrays with
//! `palloc`/`repalloc` in the current memory context, and `pstrdup`s each entry
//! name into that same context; `pgfnames`'s result is handed back to the
//! caller (who later `pgfnames_cleanup`s it). Mirroring that, [`pgfnames`]
//! charges its growing name list to the caller-supplied [`::mcx::Mcx`] via a
//! fallible [`::mcx::PgVec`] (so `palloc` OOM is a recoverable [`PgError`], not an
//! abort) and returns it. [`rmtree`]'s deferred-subdirectory list is a
//! function-local context whose [`::mcx::PgVec`] is reclaimed by scope drop — one
//! open directory handle at a time, exactly like the C.
//!
//! ## Error reporting
//!
//! The C reports directory open/read/close and unlink/rmdir problems at
//! `WARNING` (frontend `pg_log_warning`, backend `elog(WARNING, ...)`) and
//! signals overall failure through the return value (`NULL` for `pgfnames`,
//! `false` for `rmtree`); it never `ERROR`s except via `palloc` OOM. Following
//! that contract:
//! - [`pgfnames`] returns `Ok(None)` when the directory cannot be opened (the C
//!   warning + `NULL`), `Ok(Some(list))` on success, and `Err` only on the
//!   `palloc`/`repalloc` OOM path.
//! - [`rmtree`] returns `false` if there was any problem, `true` otherwise.

use std::ffi::OsString;
use std::fs;
use std::io;

use mcx::{Mcx, MemoryContext, PgString, PgVec};
use ::types_error::PgResult;

/// The list of names returned by [`pgfnames`], charged to the caller's context.
pub type PgFileNames<'mcx> = PgVec<'mcx, PgString<'mcx>>;

/// `pgfnames(path)` (`common/pgfnames.c`).
///
/// Returns the names of the objects in `path`, excluding `.` and `..`, each
/// `pstrdup`'d into `mcx`. The C logs a warning and returns `NULL` when the
/// directory cannot be opened; here that is `Ok(None)`. Read and close
/// failures are logged (here: surfaced through [`io`] kinds at the warning
/// sites the C had) but, like the C, do not discard the partial list. `Err` is
/// the `palloc`/`repalloc` OOM path.
pub fn pgfnames<'mcx>(mcx: Mcx<'mcx>, path: &str) -> PgResult<Option<PgFileNames<'mcx>>> {
    // dir = opendir(path); if (dir == NULL) { pg_log_warning(...); return NULL; }
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(_error) => {
            // C: pg_log_warning("could not open directory \"%s\": %m", path);
            return Ok(None);
        }
    };

    // filenames = palloc(fnsize * sizeof(char *)); grows by repalloc.
    // 200 entries "enough for many small dbs", matching the C initial fnsize.
    let mut filenames: PgFileNames<'mcx> = ::mcx::vec_with_capacity_in(mcx, 200)?;

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            // C: `if (errno) pg_log_warning("could not read directory ...")`
            // after the loop, then still returns the names collected so far.
            Err(_error) => break,
        };
        let name = entry.file_name();
        if name != "." && name != ".." {
            // filenames[numnames++] = pstrdup(file->d_name);
            // The repalloc-doubling is handled by PgVec's fallible growth, which
            // charges the spine to `mcx` and surfaces OOM as Err (palloc abort
            // analog made recoverable).
            push_name(mcx, &mut filenames, &name)?;
        }
    }

    Ok(Some(filenames))
}

/// `pstrdup(file->d_name)` into `mcx`, then `filenames[numnames++] = ...` with
/// the fallible (`repalloc`) growth. Reserves one slot fallibly first so the
/// spine charge/OOM is accounted before the push.
fn push_name<'mcx>(
    mcx: Mcx<'mcx>,
    filenames: &mut PgFileNames<'mcx>,
    name: &OsString,
) -> PgResult<()> {
    // repalloc growth: reserve a slot fallibly (charges the spine to mcx).
    if filenames.len() == filenames.capacity() {
        let want = filenames.capacity().saturating_mul(2).max(1);
        let request = want.saturating_mul(core::mem::size_of::<PgString<'mcx>>());
        ::mcx::check_alloc_size(request)?;
        filenames
            .try_reserve(want - filenames.len())
            .map_err(|_| mcx.oom(request))?;
    }
    // pstrdup of the entry name into mcx.
    let mut s = PgString::new_in(mcx);
    s.try_push_str(&name.to_string_lossy()).map_err(|_| {
        mcx.oom(name.len())
    })?;
    filenames.push(s);
    Ok(())
}

/// `pgfnames_cleanup(filenames)` (`common/pgfnames.c`).
///
/// The C `pfree`s each name and then the array. In Rust the owned list (and
/// each [`PgString`] within it) reclaims its context charge on drop, so this
/// simply consumes the list.
pub fn pgfnames_cleanup(_filenames: PgFileNames<'_>) {}

/// `rmtree(path, rmtopdir)` (`common/rmtree.c`).
///
/// Delete a directory tree recursively. Everything under `path` is removed, and
/// the top directory itself when `rmtopdir` is true. Returns `true` on success,
/// `false` if there was any problem (the C logs the details at `WARNING` as it
/// goes; processing continues so the tree is removed as completely as possible).
pub fn rmtree(path: &str, rmtopdir: bool) -> bool {
    // The C builds a per-level deferred-subdirectory array with palloc/repalloc
    // in the current context; mirror that with a function-local context whose
    // PgVec is reclaimed by scope drop. Recursion creates a fresh context per
    // level — one open directory handle at a time, exactly like the C.
    let ctx = MemoryContext::new("rmtree");
    rmtree_in(ctx.mcx(), path, rmtopdir)
}

fn rmtree_in(mcx: Mcx<'_>, path: &str, rmtopdir: bool) -> bool {
    let mut result = true;

    // dir = OPENDIR(path); if (dir == NULL) { pg_log_warning(...); return false; }
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(_error) => {
            // pg_log_warning("could not open directory \"%s\": %m", path);
            return false;
        }
    };

    // dirnames = palloc(sizeof(char *) * dirnames_capacity);  (cap 8)
    // OOM here is the palloc abort path: report failure and stop, like C would
    // have ERRORed out of the whole rmtree.
    let mut dirnames: PgVec<'_, String> = match ::mcx::vec_with_capacity_in(mcx, 8) {
        Ok(v) => v,
        Err(_) => return false,
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_error) => {
                // C's `while (errno=0, (de = readdir(dir)))` exits the loop when
                // readdir returns NULL on a read error, then `if (errno != 0)`
                // warns and sets result=false. A read error therefore STOPS
                // processing remaining entries — match that with `break`.
                result = false;
                break;
            }
        };
        let name = entry.file_name();
        // C `readdir` yields "."/"..": skip them. `read_dir` already omits them
        // on every supported platform, but match the C explicitly.
        if name == "." || name == ".." {
            continue;
        }

        // snprintf(pathbuf, ..., "%s/%s", path, de->d_name);
        let pathbuf = format!("{path}/{}", name.to_string_lossy());

        // get_dirent_type(pathbuf, de, look_through_symlinks=false, LOG):
        // file_type() is lstat-based on Unix, so a symlink (even to a dir) is
        // NOT classified as a directory and falls to the unlink branch — never
        // followed, matching the C contract.
        match entry.file_type() {
            Ok(ft) if ft.is_dir() => {
                // PGFILETYPE_DIR: defer recursion until this directory handle
                // is dropped, to avoid using more than one fd at a time. The
                // repalloc-doubling is PgVec's fallible growth.
                if dirnames.try_reserve(1).is_err() {
                    // palloc abort path.
                    result = false;
                    continue;
                }
                // dirnames[dirnames_size++] = pstrdup(pathbuf);
                dirnames.push(pathbuf);
            }
            Ok(_) => {
                // default: if (unlink(pathbuf) != 0 && errno != ENOENT) warn.
                if let Err(e) = fs::remove_file(&pathbuf) {
                    if e.kind() != io::ErrorKind::NotFound {
                        // pg_log_warning("could not remove file ...");
                        result = false;
                    }
                }
            }
            Err(_e) => {
                // PGFILETYPE_ERROR: already logged, press on (result unchanged
                // here, matching C which only warns in get_dirent_type and does
                // not flip result for the classify error itself).
            }
        }
    }

    // CLOSEDIR(dir) happens here as `entries` is dropped at end of loop.

    // Now recurse into the subdirectories we found.
    for dirname in dirnames.iter() {
        // Fresh per-level context (one open dir handle at a time).
        let ctx = MemoryContext::new("rmtree");
        if !rmtree_in(ctx.mcx(), dirname, true) {
            result = false;
        }
    }

    if rmtopdir {
        // if (rmdir(path) != 0) pg_log_warning(...);
        if fs::remove_dir(path).is_err() {
            result = false;
        }
    }

    // pfree(dirnames): reclaimed by scope drop.
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::fs::File;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "pgrust-pgfnames-rmtree-{name}-{}-{nanos}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn path_str(&self) -> String {
            self.path.to_string_lossy().into_owned()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn names(ctx: &MemoryContext, path: &str) -> BTreeSet<String> {
        pgfnames(ctx.mcx(), path)
            .unwrap()
            .unwrap()
            .iter()
            .map(|s| s.as_str().to_string())
            .collect()
    }

    #[test]
    fn pgfnames_returns_directory_entries_without_dot_entries() {
        let temp = TempDir::new("names");
        File::create(temp.path().join("alpha")).unwrap();
        fs::create_dir(temp.path().join("nested")).unwrap();

        let ctx = MemoryContext::new("test");
        assert_eq!(
            names(&ctx, &temp.path_str()),
            BTreeSet::from(["alpha".to_string(), "nested".to_string()])
        );
    }

    #[test]
    fn pgfnames_reports_missing_directory_as_none() {
        let temp = TempDir::new("missing-names");
        let missing = temp.path().join("missing");

        let ctx = MemoryContext::new("test");
        assert!(pgfnames(ctx.mcx(), &missing.to_string_lossy())
            .unwrap()
            .is_none());
    }

    #[test]
    fn pgfnames_cleanup_consumes_owned_names() {
        let ctx = MemoryContext::new("test");
        let temp = TempDir::new("cleanup");
        File::create(temp.path().join("a")).unwrap();
        let list = pgfnames(ctx.mcx(), &temp.path_str()).unwrap().unwrap();
        pgfnames_cleanup(list);
    }

    #[test]
    fn pgfnames_charge_released_after_drop() {
        let temp = TempDir::new("charge");
        File::create(temp.path().join("a")).unwrap();
        File::create(temp.path().join("b")).unwrap();

        let ctx = MemoryContext::new("charge-gate");
        {
            let list = pgfnames(ctx.mcx(), &temp.path_str()).unwrap().unwrap();
            assert!(ctx.used() > 0, "spine must be charged while alive");
            drop(list);
        }
        assert_eq!(ctx.used(), 0, "no charge may leak after teardown");
    }

    #[test]
    fn rmtree_removes_files_subdirectories_and_top_directory() {
        let temp = TempDir::new("remove-top");
        fs::create_dir(temp.path().join("child")).unwrap();
        File::create(temp.path().join("child").join("inside")).unwrap();
        File::create(temp.path().join("root-file")).unwrap();
        let root = temp.path_str();

        assert!(rmtree(&root, true));
        assert!(!Path::new(&root).exists());
    }

    #[test]
    fn rmtree_can_leave_top_directory() {
        let temp = TempDir::new("keep-top");
        fs::create_dir(temp.path().join("child")).unwrap();
        File::create(temp.path().join("child").join("inside")).unwrap();
        File::create(temp.path().join("root-file")).unwrap();

        assert!(rmtree(&temp.path_str(), false));
        assert!(temp.path().is_dir());
        let ctx = MemoryContext::new("test");
        assert!(names(&ctx, &temp.path_str()).is_empty());
    }

    #[test]
    fn rmtree_reports_missing_top_directory() {
        let temp = TempDir::new("missing-tree");
        let missing = temp.path().join("missing");

        assert!(!rmtree(&missing.to_string_lossy(), true));
    }

    #[cfg(unix)]
    #[test]
    fn rmtree_unlinks_directory_symlink_without_following_it() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new("symlink");
        let target = TempDir::new("symlink-target");
        File::create(target.path().join("kept")).unwrap();
        symlink(target.path(), temp.path().join("link")).unwrap();

        assert!(rmtree(&temp.path_str(), false));
        assert!(temp.path().is_dir());
        assert!(target.path().join("kept").exists());
    }
}
