use ::elog::ereport;
use ::types_error::{PgResult, ERROR, WARNING};

use crate::desc::{CloseTransientFile, OpenTransientFile, TransientFileRawFd};
use crate::sync::{fsync_fname, pg_flush_data};
use crate::vfd::{cpath, get_errno, loc, set_errno, MakePGDirectory};

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

    // Invariant, not a live arm: "clone" is absent from file_copy_method's
    // GUC options until clone_file (copydir.c) ports, so no accepted setting
    // reaches here non-copy.
    if crate::vfd::file_copy_method() != FILE_COPY_METHOD_COPY {
        panic!("file_copy_method=clone not ported: land clone_file (copydir.c)");
    }

    for name in entry_names(fromdir)? {
        postgres_seams::check_for_interrupts::call()?;
        let fromfile = format!("{fromdir}/{name}");
        let tofile = format!("{todir}/{name}");
        // get_dirent_type(look_through_symlinks=false): lstat.
        let mut md = vfs::FileInfo::zeroed();
        if vfs::lstat(&cpath(&fromfile), &mut md) != 0 {
            return Err(ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{fromfile}\": %m"))
                .finish(loc("copydir"))
                .unwrap_err());
        }
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
        let mut md = vfs::FileInfo::zeroed();
        if vfs::lstat(&cpath(&tofile), &mut md) != 0 {
            return Err(ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{tofile}\": %m"))
                .finish(loc("copydir"))
                .unwrap_err());
        }
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

        // Positional IO at the tracked offset (`offset` already advances in
        // lockstep with C's sequential read/write; both fds are regular files
        // opened here, so pread/pwrite move the same bytes).
        let nbytes = vfs::pread(src_raw, &mut buffer, offset as libc::off_t);
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
        if vfs::pwrite(dst_raw, &buffer[..nbytes as usize], offset as libc::off_t) != nbytes {
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
        let cfull = cpath(&full);
        let mut md = vfs::FileInfo::zeroed();
        if vfs::lstat(&cfull, &mut md) != 0 {
            // PGFILETYPE_ERROR arm: log and press on, result stays true.
            ereport(WARNING)
                .with_saved_errno(get_errno())
                .errmsg(format!("could not stat file \"{full}\": %m"))
                .finish(loc("rmtree"))?;
            continue;
        }
        if md.is_dir() {
            if !rmtree(&full, true)? {
                result = false;
            }
        } else if vfs::unlink(&cfull) != 0 {
            let en = get_errno();
            if en != libc::ENOENT {
                ereport(WARNING)
                    .with_saved_errno(en)
                    .errmsg(format!("could not remove file \"{full}\": %m"))
                    .finish(loc("rmtree"))?;
                result = false;
            }
        }
    }
    if rmtopdir {
        if vfs::rmdir(&cpath(path)) != 0 {
            ereport(WARNING)
                .with_saved_errno(get_errno())
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
// pg_dir_create_mode (DirBuilder-recursive semantics: existing dirs at any
// level are fine, existing non-dir surfaces the mkdir errno).
pub fn pg_mkdir_p(path: &str) -> PgResult<()> {
    fn mkdir_p_inner(path: &str, mode: u32) -> Result<(), i32> {
        let cp = cpath(path);
        if vfs::mkdir(&cp, mode as libc::mode_t) == 0 {
            return Ok(());
        }
        let en = get_errno();
        let exists_as_dir = |p: &std::ffi::CStr| {
            let mut info = vfs::FileInfo::zeroed();
            vfs::stat(p, &mut info) == 0 && info.is_dir()
        };
        match en {
            libc::EEXIST | libc::EISDIR => {
                if exists_as_dir(&cp) {
                    Ok(())
                } else {
                    Err(en)
                }
            }
            libc::ENOENT => {
                let parent = std::path::Path::new(path)
                    .parent()
                    .and_then(std::path::Path::to_str)
                    .filter(|p| !p.is_empty() && *p != path);
                let Some(parent) = parent else { return Err(en) };
                mkdir_p_inner(parent, mode)?;
                if vfs::mkdir(&cp, mode as libc::mode_t) == 0 {
                    return Ok(());
                }
                let en = get_errno();
                if (en == libc::EEXIST || en == libc::EISDIR) && exists_as_dir(&cp) {
                    Ok(())
                } else {
                    Err(en)
                }
            }
            _ => Err(en),
        }
    }

    mkdir_p_inner(path, crate::vfd::pg_dir_create_mode()).map_err(|en| {
        ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not create directory \"{path}\": %m"))
            .finish(loc("pg_mkdir_p"))
            .unwrap_err()
    })
}
