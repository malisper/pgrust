use ::elog::ereport;
use ::types_error::{PgResult, ERROR, WARNING};

use crate::desc::{CloseTransientFile, OpenTransientFile, TransientFileRawFd};
use crate::sync::{fsync_fname, pg_flush_data};
use crate::vfd::{get_errno, loc, set_errno, MakePGDirectory};

const FILE_COPY_METHOD_COPY: i32 = 0;

const COPY_BUF_SIZE: usize = 8 * 8192;
#[cfg(target_os = "macos")]
const FLUSH_DISTANCE: i64 = 32 * 1024 * 1024;
#[cfg(not(target_os = "macos"))]
const FLUSH_DISTANCE: i64 = 1024 * 1024;

fn entry_names(dir: &str) -> PgResult<Vec<String>> {
    let mut names = Vec::new();
    crate::desc::with_allocated_dir(dir, &mut |name| {
        if name != "." && name != ".." {
            names.push(name.to_owned());
        }
        Ok(false)
    })?;
    Ok(names)
}

pub fn copydir(fromdir: &str, todir: &str, recurse: bool) -> PgResult<()> {
    if MakePGDirectory(todir) != 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not create directory \"{todir}\": %m"))
            .finish(loc("copydir"))
            .unwrap_err());
    }

    if guc_tables::vars::file_copy_method.read() != FILE_COPY_METHOD_COPY {
        panic!("file_copy_method=clone not ported: land clone_file (copydir.c)");
    }

    for name in entry_names(fromdir)? {
        postgres_seams::check_for_interrupts::call()?;
        let fromfile = format!("{fromdir}/{name}");
        let tofile = format!("{todir}/{name}");
        // get_dirent_type(look_through_symlinks=false): lstat.
        let md = std::fs::symlink_metadata(&fromfile).map_err(|e| {
            ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{fromfile}\": %m"))
                .finish(loc("copydir"))
                .unwrap_err()
        })?;
        if md.is_dir() {
            if recurse {
                copydir(&fromfile, &tofile, true)?;
            }
        } else if md.is_file() {
            copy_file(&fromfile, &tofile)?;
        }
    }

    if !init_small::globals::enableFsync() {
        return Ok(());
    }

    // Be paranoid here and fsync all files to ensure the copy is really done.
    for name in entry_names(todir)? {
        let tofile = format!("{todir}/{name}");
        let md = std::fs::symlink_metadata(&tofile).map_err(|e| {
            ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{tofile}\": %m"))
                .finish(loc("copydir"))
                .unwrap_err()
        })?;
        if md.is_file() {
            fsync_fname(&tofile, false)?;
        }
    }

    // It's important to fsync the destination directory itself as individual
    // file fsyncs don't guarantee that the directory entry for the file is
    // synced.
    fsync_fname(todir, true)?;
    Ok(())
}

pub fn copy_file(fromfile: &str, tofile: &str) -> PgResult<()> {
    let mut buffer = vec![0u8; COPY_BUF_SIZE];

    let srcfd = OpenTransientFile(fromfile, libc::O_RDONLY)?;
    if srcfd < 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{fromfile}\": %m"))
            .finish(loc("copy_file"))
            .unwrap_err());
    }
    let dstfd = OpenTransientFile(tofile, libc::O_RDWR | libc::O_CREAT | libc::O_EXCL)?;
    if dstfd < 0 {
        let en = get_errno();
        let _ = CloseTransientFile(srcfd);
        return Err(ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{tofile}\": %m"))
            .finish(loc("copy_file"))
            .unwrap_err());
    }

    let src_raw = TransientFileRawFd(srcfd).expect("live transient fd");
    let dst_raw = TransientFileRawFd(dstfd).expect("live transient fd");

    let mut offset: i64 = 0;
    let mut flush_offset: i64 = 0;
    loop {
        postgres_seams::check_for_interrupts::call()?;

        if offset - flush_offset >= FLUSH_DISTANCE {
            pg_flush_data(dst_raw, flush_offset, offset - flush_offset)?;
            flush_offset = offset;
        }

        // SAFETY: read(2) into the live buffer on a caller-owned descriptor.
        let nbytes =
            unsafe { libc::read(src_raw, buffer.as_mut_ptr().cast(), COPY_BUF_SIZE) };
        if nbytes < 0 {
            return Err(ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{fromfile}\": %m"))
                .finish(loc("copy_file"))
                .unwrap_err());
        }
        if nbytes == 0 {
            break;
        }
        set_errno(0);
        // SAFETY: write(2) of the just-read prefix.
        if unsafe { libc::write(dst_raw, buffer.as_ptr().cast(), nbytes as usize) } != nbytes {
            if get_errno() == 0 {
                set_errno(libc::ENOSPC);
            }
            return Err(ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not write to file \"{tofile}\": %m"))
                .finish(loc("copy_file"))
                .unwrap_err());
        }
        offset += nbytes as i64;
    }

    if offset > flush_offset {
        pg_flush_data(dst_raw, flush_offset, offset - flush_offset)?;
    }

    if CloseTransientFile(dstfd) != 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{tofile}\": %m"))
            .finish(loc("copy_file"))
            .unwrap_err());
    }
    if CloseTransientFile(srcfd) != 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{fromfile}\": %m"))
            .finish(loc("copy_file"))
            .unwrap_err());
    }
    Ok(())
}

// rmtree (common/rmtree.c): returns false if any operation failed (with a
// WARNING), true on full success.
pub fn rmtree(path: &str, rmtopdir: bool) -> PgResult<bool> {
    let names = match entry_names(path) {
        Ok(n) => n,
        Err(_) => return Ok(false),
    };
    let mut result = true;
    for name in names {
        let full = format!("{path}/{name}");
        let md = match std::fs::symlink_metadata(&full) {
            Ok(md) => md,
            // PGFILETYPE_ERROR arm: log and press on, result stays true.
            Err(e) => {
                ereport(WARNING)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errmsg(format!("could not stat file \"{full}\": %m"))
                    .finish(loc("rmtree"))?;
                continue;
            }
        };
        if md.is_dir() {
            if !rmtree(&full, true)? {
                result = false;
            }
        } else if let Err(e) = std::fs::remove_file(&full) {
            if e.kind() != std::io::ErrorKind::NotFound {
                ereport(WARNING)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errmsg(format!("could not remove file \"{full}\": %m"))
                    .finish(loc("rmtree"))?;
                result = false;
            }
        }
    }
    if rmtopdir {
        if let Err(e) = std::fs::remove_dir(path) {
            ereport(WARNING)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errmsg(format!("could not remove directory \"{path}\": %m"))
                .finish(loc("rmtree"))?;
            result = false;
        }
    }
    Ok(result)
}

// directory_is_empty (commands/tablespace.c) — hosted here with the other
// directory walks until the tablespace unit lands.
pub fn directory_is_empty(path: &str) -> PgResult<bool> {
    let mut empty = true;
    crate::desc::with_allocated_dir(path, &mut |name| {
        if name != "." && name != ".." {
            empty = false;
            return Ok(true);
        }
        Ok(false)
    })?;
    Ok(empty)
}

// pg_mkdir_p (port/path.c) shape: create path and missing parents with
// pg_dir_create_mode.
pub fn pg_mkdir_p(path: &str) -> PgResult<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(crate::vfd::pg_dir_create_mode())
        .create(path)
        .map_err(|e| {
            ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not create directory \"{path}\": %m"))
                .finish(loc("pg_mkdir_p"))
                .unwrap_err()
        })
}
