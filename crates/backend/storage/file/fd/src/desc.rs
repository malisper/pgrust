use std::fs::File as StdFile;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};

use ::elog::ereport;
use ::types_core::SubTransactionId;
use ::types_error::{ErrorLevel, PgResult, ERRCODE_INSUFFICIENT_RESOURCES, ERROR, LOG, WARNING};
use ::types_storage::{Dir, DirEnt, FD_MINFREE};

use crate::vfd::{self, get_errno, loc, set_errno, with_fd, FdState};

pub(crate) enum AllocatedHandle {
    // DST P1 carve-out: buffered stdio streams (AllocateFile fopen) and pipes
    // (OpenPipeStream popen) stay posix-only in P1 — out of Vfs scope.
    File(StdFile),
    Dir(Option<vfs::VfsDirIter>),
    // vfs-minted, so the holder must be the vfs-close guard (finding F1b):
    // an unwind/thread-exit drop releases into the active vfs namespace,
    // never posix-side.
    RawFd(vfs::VfsFd),
    Pipe(PipeHandle),
}

pub(crate) struct PipeHandle {
    pub child: std::process::Child,
    pub stdout: Option<std::process::ChildStdout>,
    pub stdin: Option<std::process::ChildStdin>,
}

pub(crate) struct AllocateDesc {
    pub create_subid: SubTransactionId,
    pub desc: AllocatedHandle,
}

pub(crate) fn reserveAllocatedDesc(fd: &mut FdState) -> bool {
    let num = vfd::occupied_descs(fd);

    if num < fd.max_allocated_descs {
        return true;
    }

    // First allocation: FD_MINFREE / 3 slots (set_max_safe_fds may not have
    // run yet). C's malloc-failure ERROR collapses: Vec growth aborts on OOM.
    if fd.max_allocated_descs == 0 {
        let new_max = FD_MINFREE / 3;
        fd.allocated_descs.reserve_exact(new_max as usize);
        fd.max_allocated_descs = new_max;
        return true;
    }

    // Cap at max_safe_fds / 3 so allocated descriptors can't hog all the FDs.
    let new_max = vfd::max_safe_fds() / 3;
    if new_max > fd.max_allocated_descs {
        fd.allocated_descs.reserve_exact((new_max - num) as usize);
        fd.max_allocated_descs = new_max;
        return true;
    }

    false
}

fn exceeded_max_descs(what: &str, name: &str, func: &'static str) -> PgResult<()> {
    let max = with_fd(|fd| fd.max_allocated_descs);
    ereport(ERROR)
        .errcode(ERRCODE_INSUFFICIENT_RESOURCES)
        .errmsg(format!(
            "exceeded maxAllocatedDescs ({max}) while trying to {what} \"{name}\""
        ))
        .finish(loc(func))
}

fn out_of_fds_log() -> PgResult<()> {
    let en = get_errno();
    ereport(LOG)
        .with_saved_errno(en)
        .errcode(ERRCODE_INSUFFICIENT_RESOURCES)
        .errmsg("out of file descriptors: %m; release and retry")
        .finish(loc("AllocateFile"))
}

fn current_subid() -> SubTransactionId {
    xact_seams::get_current_sub_transaction_id::call()
}

// C compacts and re-finds by FILE*; index handles need stable slots, so the
// slot is tombstoned instead (trailing tombstones trimmed). Returns the
// underlying close result.
pub(crate) fn FreeDesc(index: i32) -> i32 {
    let desc = with_fd(|fd| {
        let desc = fd.allocated_descs[index as usize].take().expect("FreeDesc: occupied slot");
        while matches!(fd.allocated_descs.last(), Some(None)) {
            fd.allocated_descs.pop();
        }
        desc
    });
    match desc.desc {
        AllocatedHandle::File(file) => {
            // DST P1 carve-out boundary (contract §1.1, Ruling 3 Class C):
            // this fd was minted posix-side by open_stdio (the fopen plane,
            // permanently out of Vfs scope), so it must close posix-side too
            // — vfs::close is reserved for vfs-minted fds. Under sim, SimVfs
            // EBADFs foreign fds below SIM_FD_BASE (the domain tripwire that
            // caught the original misroute; the fd then leaked). Allowlist
            // row retained under the P0 fencing lint with the fopen sites.
            // Live descriptor released from its guard; closed once here.
            let raw = file.into_raw_fd();
            // SAFETY: into_raw_fd transferred ownership; closed exactly once.
            unsafe { libc::close(raw) }
        }
        AllocatedHandle::Pipe(pipe) => pclose(pipe),
        AllocatedHandle::Dir(dir) => {
            drop(dir);
            0
        }
        AllocatedHandle::RawFd(file) => {
            crate::pgaio_closing_fd_if_engine_present(file.as_raw_fd());
            // Live descriptor released from its guard (disarmed); closed
            // exactly once here.
            let raw = file.into_raw();
            vfs::close(raw)
        }
    }
}


// First tombstoned slot, else append (stable indices; see FreeDesc).
fn install_desc(fd: &mut FdState, desc: AllocateDesc) -> i32 {
    match fd.allocated_descs.iter().position(Option::is_none) {
        Some(i) => {
            fd.allocated_descs[i] = Some(desc);
            i as i32
        }
        None => {
            fd.allocated_descs.push(Some(desc));
            (fd.allocated_descs.len() - 1) as i32
        }
    }
}

// C contract: table index >= 0, or -1 with errno set on fopen failure.
pub fn AllocateFile(name: &str, mode: &str) -> PgResult<i32> {
    if !with_fd(reserveAllocatedDesc) {
        exceeded_max_descs("open file", name, "AllocateFile")?;
    }

    with_fd(vfd::ReleaseLruFiles)?;

    loop {
        match open_stdio(name, mode) {
            Ok(file) => {
                let create_subid = current_subid();
                return with_fd(|fd| {
                    Ok(install_desc(
                        fd,
                        AllocateDesc { create_subid, desc: AllocatedHandle::File(file) },
                    ))
                });
            }
            Err(en) => {
                if en == libc::EMFILE || en == libc::ENFILE {
                    set_errno(en);
                    out_of_fds_log()?;
                    if with_fd(vfd::ReleaseLruFile)? {
                        continue;
                    }
                }
                set_errno(en);
                return Ok(-1);
            }
        }
    }
}

pub fn FreeFile(index: i32) -> PgResult<i32> {
    let found = with_fd(|fd| {
        matches!(
            fd.allocated_descs.get(index as usize),
            Some(Some(AllocateDesc { desc: AllocatedHandle::File(_), .. }))
        )
    });
    if found {
        return Ok(FreeDesc(index));
    }

    ereport(WARNING)
        .errmsg("file passed to FreeFile was not obtained from AllocateFile")
        .finish(loc("FreeFile"))?;
    Ok(-1)
}

pub fn OpenTransientFile(file_name: &str, file_flags: i32) -> PgResult<i32> {
    OpenTransientFilePerm(file_name, file_flags, vfd::pg_file_create_mode())
}

// C contract: the raw kernel fd, or -1 with errno set on open failure.
pub fn OpenTransientFilePerm(file_name: &str, file_flags: i32, file_mode: u32) -> PgResult<i32> {
    if !with_fd(reserveAllocatedDesc) {
        exceeded_max_descs("open file", file_name, "OpenTransientFilePerm")?;
    }

    with_fd(|fd| {
        vfd::ReleaseLruFiles(fd)?;

        let raw = vfd::BasicOpenFilePermInternal(fd, file_name, file_flags, file_mode)?;
        if raw >= 0 {
            install_desc(
                fd,
                AllocateDesc {
                    create_subid: current_subid(),
                    // SAFETY: freshly vfs-opened descriptor now owned by the
                    // table's guard.
                    desc: AllocatedHandle::RawFd(unsafe { vfs::VfsFd::from_raw(raw) }),
                },
            );
        }
        Ok(raw)
    })
}

// C contract: close(2)'s result.
pub fn CloseTransientFile(fd_to_close: i32) -> i32 {
    let index = with_fd(|fd| {
        for i in (0..fd.allocated_descs.len()).rev() {
            if let Some(AllocateDesc { desc: AllocatedHandle::RawFd(f), .. }) =
                &fd.allocated_descs[i]
            {
                if f.as_raw_fd() == fd_to_close {
                    return Some(i as i32);
                }
            }
        }
        None
    });
    if let Some(index) = index {
        return FreeDesc(index);
    }

    let _ = ereport(WARNING)
        .errmsg("fd passed to CloseTransientFile was not obtained from OpenTransientFile")
        .finish(loc("CloseTransientFile"));

    crate::pgaio_closing_fd_if_engine_present(fd_to_close);
    // Caller asserts ownership of this descriptor, as in C.
    vfs::close(fd_to_close)
}

// C contract: table index >= 0, or -1 with errno set on popen failure.
pub fn OpenPipeStream(command: &str, mode: &str) -> PgResult<i32> {
    if !with_fd(reserveAllocatedDesc) {
        exceeded_max_descs("execute command", command, "OpenPipeStream")?;
    }

    with_fd(vfd::ReleaseLruFiles)?;

    // C flushes stdio and flips SIGPIPE to SIG_DFL around popen; a Command
    // child gets default dispositions already, so no signal dance is needed.
    loop {
        match popen(command, mode) {
            Ok(pipe) => {
                let create_subid = current_subid();
                return with_fd(|fd| {
                    Ok(install_desc(
                        fd,
                        AllocateDesc { create_subid, desc: AllocatedHandle::Pipe(pipe) },
                    ))
                });
            }
            Err(en) => {
                if en == libc::EMFILE || en == libc::ENFILE {
                    set_errno(en);
                    out_of_fds_log()?;
                    if with_fd(vfd::ReleaseLruFile)? {
                        continue;
                    }
                }
                set_errno(en);
                return Ok(-1);
            }
        }
    }
}

// fgets(buf, size, fh) over an OpenPipeStream handle: fills up to
// buf.len()-1 bytes, stops after '\n'. Ok(0) is EOF-without-bytes (C NULL
// with !ferror); Err(errno) is C NULL with ferror set.
pub fn PipeStreamGets(index: i32, buf: &mut [u8]) -> Result<usize, i32> {
    use std::io::Read;

    with_fd(|fd| {
        let Some(Some(desc)) = fd.allocated_descs.get_mut(index as usize) else {
            return Err(libc::EBADF);
        };
        let AllocatedHandle::Pipe(pipe) = &mut desc.desc else {
            return Err(libc::EBADF);
        };
        let Some(out) = pipe.stdout.as_mut() else {
            return Err(libc::EBADF);
        };
        let mut n = 0usize;
        while n + 1 < buf.len() {
            let mut byte = [0u8; 1];
            match out.read(&mut byte) {
                Ok(0) => break,
                Ok(_) => {
                    buf[n] = byte[0];
                    n += 1;
                    if byte[0] == b'\n' {
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.raw_os_error().unwrap_or(libc::EIO)),
            }
        }
        if n < buf.len() {
            buf[n] = 0;
        }
        Ok(n)
    })
}

// C contract: pclose()'s wait status, -1 on a stream we don't track.
pub fn ClosePipeStream(index: i32) -> PgResult<i32> {
    let found = with_fd(|fd| {
        matches!(
            fd.allocated_descs.get(index as usize),
            Some(Some(AllocateDesc { desc: AllocatedHandle::Pipe(_), .. }))
        )
    });
    if found {
        return Ok(FreeDesc(index));
    }

    ereport(WARNING)
        .errmsg("file passed to ClosePipeStream was not obtained from OpenPipeStream")
        .finish(loc("ClosePipeStream"))?;
    Ok(-1)
}

// C contract: Ok(None) mirrors a NULL DIR* with errno set; the following
// ReadDir reports the failure.
pub fn AllocateDir(dirname: &str) -> PgResult<Option<Dir>> {
    if !with_fd(reserveAllocatedDesc) {
        exceeded_max_descs("open directory", dirname, "AllocateDir")?;
    }

    with_fd(vfd::ReleaseLruFiles)?;

    let cdir = vfd::cpath(dirname);
    loop {
        match vfs::read_dir(&cdir) {
            Ok(iter) => {
                let create_subid = current_subid();
                return with_fd(|fd| {
                    Ok(Some(install_desc(
                        fd,
                        AllocateDesc { create_subid, desc: AllocatedHandle::Dir(Some(iter)) },
                    )))
                });
            }
            Err(en) => {
                if en == libc::EMFILE || en == libc::ENFILE {
                    set_errno(en);
                    out_of_fds_log()?;
                    if with_fd(vfd::ReleaseLruFile)? {
                        continue;
                    }
                }
                set_errno(en);
                return Ok(None);
            }
        }
    }
}

pub fn ReadDir(dir: Option<Dir>, dirname: &str) -> PgResult<Option<DirEnt>> {
    ReadDirExtended(dir, dirname, ERROR)
}

pub fn ReadDirExtended(
    dir: Option<Dir>,
    dirname: &str,
    elevel: ErrorLevel,
) -> PgResult<Option<DirEnt>> {
    let index = match dir {
        None => {
            ereport(elevel)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open directory \"{dirname}\": %m"))
                .finish(loc("ReadDirExtended"))?;
            return Ok(None);
        }
        Some(index) => index,
    };

    let next = with_fd(|fd| match fd.allocated_descs[index as usize].as_mut() {
        Some(AllocateDesc { desc: AllocatedHandle::Dir(iter), .. }) => {
            iter.as_mut().and_then(Iterator::next)
        }
        _ => None,
    });

    match next {
        Some(Ok(d_name)) => Ok(Some(DirEnt { d_name })),
        Some(Err(en)) => {
            ereport(elevel)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg(format!("could not read directory \"{dirname}\": %m"))
                .finish(loc("ReadDirExtended"))?;
            Ok(None)
        }
        None => Ok(None),
    }
}

pub fn FreeDir(dir: Option<Dir>) -> PgResult<i32> {
    let index = match dir {
        None => return Ok(0),
        Some(index) => index,
    };

    let found = with_fd(|fd| {
        matches!(
            fd.allocated_descs.get(index as usize),
            Some(Some(AllocateDesc { desc: AllocatedHandle::Dir(_), .. }))
        )
    });
    if found {
        return Ok(FreeDesc(index));
    }

    ereport(WARNING)
        .errmsg("dir passed to FreeDir was not obtained from AllocateDir")
        .finish(loc("FreeDir"))?;
    Ok(-1)
}

pub fn closeAllVfds() -> PgResult<()> {
    with_fd(|fd| {
        if fd.size_vfd_cache() > 0 {
            debug_assert!(vfd::FileIsNotOpen(fd, 0));
            for i in 1..fd.size_vfd_cache() as i32 {
                if !vfd::FileIsNotOpen(fd, i) {
                    vfd::LruDelete(fd, i)?;
                }
            }
        }
        Ok(())
    })
}

// The `with_allocated_dir` seam: AllocateDir + ReadDir + FreeDir as one owned
// walk. `f` returns Ok(true) to stop early; the walk returns the last
// callback value (false once the directory is exhausted).
pub fn with_allocated_dir(
    dirname: &str,
    f: &mut dyn FnMut(&str) -> PgResult<bool>,
) -> PgResult<bool> {
    let dir = AllocateDir(dirname)?;

    let mut last = false;
    loop {
        let ent = match ReadDir(dir, dirname) {
            Ok(Some(ent)) => ent,
            Ok(None) => break,
            Err(e) => {
                let _ = FreeDir(dir);
                return Err(e);
            }
        };

        match f(&ent.d_name) {
            Ok(stop) => {
                last = stop;
                if stop {
                    break;
                }
            }
            Err(e) => {
                let _ = FreeDir(dir);
                return Err(e);
            }
        }
    }

    FreeDir(dir)?;
    Ok(last)
}

// DST P1 carve-out: the buffered-stream (fopen) plane stays posix-only in P1;
// stdio is permanently out of Vfs scope (contract §1.1). Allowlist row
// retained under the P0 fencing lint.
fn open_stdio(name: &str, mode: &str) -> Result<StdFile, i32> {
    use std::fs::OpenOptions;

    // fopen mode map for the modes the backend passes (optional trailing 'b').
    let m = mode.trim_end_matches('b');
    let mut opts = OpenOptions::new();
    match m {
        "r" => opts.read(true),
        "w" => opts.write(true).create(true).truncate(true),
        "a" => opts.append(true).create(true),
        "r+" => opts.read(true).write(true),
        "w+" => opts.read(true).write(true).create(true).truncate(true),
        "a+" => opts.read(true).append(true).create(true),
        _ => opts.read(true),
    };
    opts.open(name).map_err(|e| e.raw_os_error().unwrap_or(0))
}

fn popen(command: &str, mode: &str) -> Result<PipeHandle, i32> {
    use std::process::{Command, Stdio};

    let reading = mode.starts_with('r');
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg(command);
    if reading {
        cmd.stdout(Stdio::piped());
    } else {
        cmd.stdin(Stdio::piped());
    }
    match cmd.spawn() {
        Ok(mut child) => {
            let stdout = child.stdout.take();
            let stdin = child.stdin.take();
            Ok(PipeHandle {
                child,
                stdout,
                stdin,
            })
        }
        Err(e) => Err(e.raw_os_error().unwrap_or(0)),
    }
}

// pclose: close our pipe end (child sees EOF/SIGPIPE), wait, and return the
// raw wait(2) status word.
fn pclose(mut pipe: PipeHandle) -> i32 {
    drop(pipe.stdout.take());
    drop(pipe.stdin.take());
    match pipe.child.wait() {
        Ok(status) => {
            if let Some(code) = status.code() {
                (code & 0xff) << 8
            } else {
                #[cfg(not(target_family = "wasm"))]
                {
                    use std::os::unix::process::ExitStatusExt;
                    status.signal().unwrap_or(0) & 0x7f
                }
                // wasm32: WASI has no processes/signals; unreachable (the
                // popen arm can never spawn), keep the C -1 failure word.
                #[cfg(target_family = "wasm")]
                {
                    -1
                }
            }
        }
        Err(_) => -1,
    }
}

// Reach the buffered stream behind an AllocateFile index (the C callers'
// direct `FILE *` reads/writes).
pub fn with_allocated_stdio<R>(index: i32, f: impl FnOnce(&mut StdFile) -> R) -> Option<R> {
    with_fd(|fd| match fd.allocated_descs.get_mut(index as usize) {
        Some(Some(AllocateDesc {
            desc: AllocatedHandle::File(file),
            ..
        })) => Some(f(file)),
        _ => None,
    })
}

// Raw kernel fd behind a transient-file value (fstat-style callers).
// Raw-fd escape hatch (DST P1 contract §1.1 carve-out): the returned kernel
// descriptor bypasses the VFS. Sim-scope callers must not use it.
pub fn TransientFileRawFd(fd_value: i32) -> Option<RawFd> {
    with_fd(|fd| {
        fd.allocated_descs.iter().rev().flatten().find_map(|d| match &d.desc {
            AllocatedHandle::RawFd(f) if f.as_raw_fd() == fd_value => Some(f.as_raw_fd()),
            _ => None,
        })
    })
}
