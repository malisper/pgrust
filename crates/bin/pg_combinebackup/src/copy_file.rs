//! C: src/bin/pg_combinebackup/copy_file.c — copy entire files with an
//! optional running checksum.
//!
//! The portable block copy (--copy, the default) is a faithful port. The
//! platform fast paths are implemented where the syscalls are cleanly
//! available: --clone via copyfile(COPYFILE_CLONE_FORCE) on macOS and the
//! FICLONE ioctl on Linux; --copy-file-range via copy_file_range(2) on
//! Linux (other platforms get C's exact "not supported on this platform"
//! error, which C also emits when built without those syscalls). --link is
//! link(2) everywhere. The WIN32 CopyFile strategy is not ported.

use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use manifest::{pg_checksum_type_name, PgChecksumContext, PgChecksumType};

use crate::flog::{errno_message, log_debug, pg_fatal};
use crate::BLCKSZ;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CopyMethod {
    Clone,
    Copy,
    CopyFileRange,
    Link,
}

/// Open dst with O_WRONLY|O_CREAT|O_EXCL (O_RDWR when `rdwr`) and
/// pg_file_create_mode, like every output-file open in the C tool.
pub fn open_excl_create(path: &Path, rdwr: bool) -> std::io::Result<File> {
    let mut opts = std::fs::OpenOptions::new();
    if rdwr {
        opts.read(true);
    }
    opts.write(true)
        .create_new(true)
        .mode(crate::fsutil::pg_file_create_mode())
        .open(path)
}

/// Close a file, surfacing the close(2) error (drop would swallow it).
pub fn close_file(file: File) -> std::io::Result<()> {
    use std::os::fd::IntoRawFd;
    let fd = file.into_raw_fd();
    // SAFETY: fd was just released from the File and is closed exactly once.
    if unsafe { libc::close(fd) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// C: copy_file — dispatch on the copy method; in dry-run mode only verify
/// the source can be opened and emit the debug message.
pub fn copy_file(
    src: &Path,
    dst: &Path,
    checksum_ctx: &mut PgChecksumContext,
    copy_method: CopyMethod,
    dry_run: bool,
) {
    if dry_run {
        match File::open(src) {
            Ok(f) => drop(f),
            Err(e) => pg_fatal!(
                "could not open file \"{}\": {}",
                src.display(),
                errno_message(&e)
            ),
        }
    }

    let strategy_name = match copy_method {
        CopyMethod::Clone => Some("clone"),
        CopyMethod::Copy => None, /* simple block-by-block copy */
        CopyMethod::CopyFileRange => Some("copy_file_range"),
        CopyMethod::Link => Some("link"),
    };

    if dry_run {
        if let Some(name) = strategy_name {
            log_debug(&format!(
                "would copy \"{}\" to \"{}\" using strategy {}",
                src.display(),
                dst.display(),
                name
            ));
        } else {
            log_debug(&format!(
                "would copy \"{}\" to \"{}\"",
                src.display(),
                dst.display()
            ));
        }
        return;
    }

    if let Some(name) = strategy_name {
        log_debug(&format!(
            "copying \"{}\" to \"{}\" using strategy {}",
            src.display(),
            dst.display(),
            name
        ));
    } else if checksum_ctx.checksum_type() == PgChecksumType::None {
        log_debug(&format!("copying \"{}\" to \"{}\"", src.display(), dst.display()));
    } else {
        log_debug(&format!(
            "copying \"{}\" to \"{}\" and checksumming with {}",
            src.display(),
            dst.display(),
            pg_checksum_type_name(checksum_ctx.checksum_type())
        ));
    }

    match copy_method {
        CopyMethod::Clone => copy_file_clone(src, dst, checksum_ctx),
        CopyMethod::Copy => copy_file_blocks(src, dst, checksum_ctx),
        CopyMethod::CopyFileRange => copy_file_by_range(src, dst, checksum_ctx),
        CopyMethod::Link => copy_file_link(src, dst, checksum_ctx),
    }
}

/// C: checksum_file — read src and feed it to the checksum, for strategies
/// that don't move the bytes through userspace.
fn checksum_file(src: &Path, checksum_ctx: &mut PgChecksumContext) {
    if checksum_ctx.checksum_type() == PgChecksumType::None {
        return;
    }
    let mut f = match File::open(src) {
        Ok(f) => f,
        Err(e) => pg_fatal!(
            "could not open file \"{}\": {}",
            src.display(),
            errno_message(&e)
        ),
    };
    let mut buffer = vec![0u8; 50 * BLCKSZ];
    loop {
        match f.read(&mut buffer) {
            Ok(0) => break,
            Ok(rb) => checksum_ctx.update(&buffer[..rb]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => pg_fatal!(
                "could not read file \"{}\": {}",
                src.display(),
                errno_message(&e)
            ),
        }
    }
}

/// C: copy_file_blocks — 50-block read/write loop with inline checksum.
fn copy_file_blocks(src: &Path, dst: &Path, checksum_ctx: &mut PgChecksumContext) {
    let mut src_f = match File::open(src) {
        Ok(f) => f,
        Err(e) => pg_fatal!(
            "could not open file \"{}\": {}",
            src.display(),
            errno_message(&e)
        ),
    };
    let mut dst_f = match open_excl_create(dst, false) {
        Ok(f) => f,
        Err(e) => pg_fatal!(
            "could not open file \"{}\": {}",
            dst.display(),
            errno_message(&e)
        ),
    };

    let mut buffer = vec![0u8; 50 * BLCKSZ];
    let mut offset: u64 = 0;
    loop {
        let rb = match src_f.read(&mut buffer) {
            Ok(0) => break,
            Ok(rb) => rb,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => pg_fatal!(
                "could not read from file \"{}\": {}",
                src.display(),
                errno_message(&e)
            ),
        };
        // C: a single write(2); short writes are fatal with a distinct text.
        match dst_f.write(&buffer[..rb]) {
            Ok(wb) if wb == rb => {}
            Ok(wb) => pg_fatal!(
                "could not write to file \"{}\", offset {}: wrote {} of {}",
                dst.display(),
                offset,
                wb,
                rb
            ),
            Err(e) => pg_fatal!(
                "could not write to file \"{}\": {}",
                dst.display(),
                errno_message(&e)
            ),
        }
        checksum_ctx.update(&buffer[..rb]);
        offset += rb as u64;
    }
}

/// C: copy_file_clone.
#[cfg(target_os = "macos")]
fn copy_file_clone(src: &Path, dest: &Path, checksum_ctx: &mut PgChecksumContext) {
    // C: copyfile(src, dest, NULL, COPYFILE_CLONE_FORCE).
    const COPYFILE_CLONE_FORCE: u32 = 1 << 13;
    unsafe extern "C" {
        // copyfile_state_t is opaque; we pass NULL.
        fn copyfile(
            from: *const libc::c_char,
            to: *const libc::c_char,
            state: *mut libc::c_void,
            flags: u32,
        ) -> libc::c_int;
    }
    let csrc = crate::fsutil::cstring(src);
    let cdst = crate::fsutil::cstring(dest);
    // SAFETY: NUL-terminated paths, NULL state.
    let rc = unsafe { copyfile(csrc.as_ptr(), cdst.as_ptr(), std::ptr::null_mut(), COPYFILE_CLONE_FORCE) };
    if rc < 0 {
        let e = std::io::Error::last_os_error();
        pg_fatal!(
            "error while cloning file \"{}\" to \"{}\": {}",
            src.display(),
            dest.display(),
            errno_message(&e)
        );
    }
    checksum_file(src, checksum_ctx);
}

/// C: copy_file_clone (Linux FICLONE branch).
#[cfg(target_os = "linux")]
fn copy_file_clone(src: &Path, dest: &Path, checksum_ctx: &mut PgChecksumContext) {
    use std::os::fd::AsRawFd;
    const FICLONE: libc::c_ulong = 0x40049409;

    let src_f = match File::open(src) {
        Ok(f) => f,
        Err(e) => pg_fatal!(
            "could not open file \"{}\": {}",
            src.display(),
            errno_message(&e)
        ),
    };
    let dest_f = match open_excl_create(dest, true) {
        Ok(f) => f,
        Err(e) => pg_fatal!(
            "could not create file \"{}\": {}",
            dest.display(),
            errno_message(&e)
        ),
    };
    // SAFETY: both fds are open; FICLONE takes the source fd as argument.
    if unsafe { libc::ioctl(dest_f.as_raw_fd(), FICLONE, src_f.as_raw_fd()) } < 0 {
        let e = std::io::Error::last_os_error();
        let _ = std::fs::remove_file(dest);
        pg_fatal!(
            "error while cloning file \"{}\" to \"{}\": {}",
            src.display(),
            dest.display(),
            errno_message(&e)
        );
    }
    checksum_file(src, checksum_ctx);
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn copy_file_clone(_src: &Path, _dest: &Path, _checksum_ctx: &mut PgChecksumContext) {
    pg_fatal!("file cloning not supported on this platform");
}

/// C: copy_file_by_range.
#[cfg(target_os = "linux")]
fn copy_file_by_range(src: &Path, dest: &Path, checksum_ctx: &mut PgChecksumContext) {
    use std::os::fd::AsRawFd;

    let src_f = match File::open(src) {
        Ok(f) => f,
        Err(e) => pg_fatal!(
            "could not open file \"{}\": {}",
            src.display(),
            errno_message(&e)
        ),
    };
    let dest_f = match open_excl_create(dest, true) {
        Ok(f) => f,
        Err(e) => pg_fatal!(
            "could not create file \"{}\": {}",
            dest.display(),
            errno_message(&e)
        ),
    };
    loop {
        // SAFETY: fds open; NULL offsets use and update the file positions.
        let nbytes = unsafe {
            libc::copy_file_range(
                src_f.as_raw_fd(),
                std::ptr::null_mut(),
                dest_f.as_raw_fd(),
                std::ptr::null_mut(),
                libc::ssize_t::MAX as usize,
                0,
            )
        };
        if nbytes < 0 {
            let e = std::io::Error::last_os_error();
            pg_fatal!(
                "error while copying file range from \"{}\" to \"{}\": {}",
                src.display(),
                dest.display(),
                errno_message(&e)
            );
        }
        if nbytes == 0 {
            break;
        }
    }
    checksum_file(src, checksum_ctx);
}

#[cfg(not(target_os = "linux"))]
fn copy_file_by_range(_src: &Path, _dest: &Path, _checksum_ctx: &mut PgChecksumContext) {
    pg_fatal!("copy_file_range not supported on this platform");
}

/// C: copy_file_link.
fn copy_file_link(src: &Path, dest: &Path, checksum_ctx: &mut PgChecksumContext) {
    if let Err(e) = std::fs::hard_link(src, dest) {
        pg_fatal!(
            "could not create link from \"{}\" to \"{}\": {}",
            src.display(),
            dest.display(),
            errno_message(&e)
        );
    }
    checksum_file(src, checksum_ctx);
}
