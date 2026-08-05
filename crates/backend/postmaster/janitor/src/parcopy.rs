//! Parallel batch-copy helper for the janitor's deferred-copy mints (F1,
//! test-views.md mint-strategy addendum): fans a batch's per-member
//! FILE_COPY directory copies across a small worker pool while EVERY piece
//! of backend-thread-bound state stays on the janitor thread.
//!
//! Worker-thread safety analysis (the scout verdict this module answers):
//! `fd::copydir` is NOT worker-safe — transient-fd bookkeeping, the
//! file_copy_method/enableFsync GUC mirrors, subtransaction ids, interrupt
//! flags, and the elog stack are all thread-locals of the backend thread.
//! Workers here therefore:
//! - touch NO pgrust TLS at all: plain `std::fs`/`libc` path-based
//!   syscalls only (errno is natively per-thread);
//! - receive the GUC-dependent knobs (copy method, fsync, create modes) as
//!   plain values SNAPSHOTTED on the janitor thread before spawning;
//! - never ereport: the first failure is parked as raw (path, errno, op)
//!   in a mutex and raised as a normal `ereport(ERROR)` by the janitor
//!   thread after every worker stopped;
//! - poll a shared abort flag between files AND inside the copy loops at
//!   C's own CFI cadence (per 64KB buffered chunk / per 1MB clone chunk;
//!   the macOS copyfile arm is C's own uninterruptible-single-call shape),
//!   set by the first failing worker OR by the janitor thread when
//!   `check_for_interrupts` trips while it babysits the pool (interrupts
//!   are serviced ONLY on the janitor thread — worker CFI would read that
//!   worker's never-set TLS flags);
//! - are scoped (`std::thread::scope`): they cannot outlive the copy
//!   phase, and a worker panic is caught (`catch_unwind`) and converted
//!   into the same abort-the-batch error, never a janitor-loop panic.
//!
//! Directory fsyncs stay on the janitor thread (`fd::fsync_fname`, which
//! is transient-fd machinery): they run once per destination directory
//! AFTER the workers finished, matching `copydir`'s copy-files /
//! fsync-files / fsync-dir order. Per-FILE fsyncs run on the workers' own
//! raw fds.
//!
//! The unit of fan-out is one destination directory (member x tablespace),
//! matching the charter ("the per-member directory copies fan out"); file
//! enumeration happens on the janitor thread (plain read_dir — this module
//! is compiled live-only, see lib.rs).
//!
//! Known divergence from fd::copy_file, on purpose: the worker copy loop
//! issues no incremental pg_flush_data hints (FLUSH_DISTANCE) — those are
//! writeback-pacing advice, not durability; durability is the per-file
//! fsync (workers) + per-directory fsync (janitor thread) pair, kept
//! 1:1 with copydir's — INCLUDING the primitive: workers reproduce
//! pg_fsync's routing (plain fsync unless fsync_writethrough is active,
//! snapshotted into CopyKnobs), never std File::sync_all, whose macOS
//! F_FULLFSYNC would silently upgrade (and ~70x-slow-down) every per-file
//! fsync versus the serial path this replaces.

use std::ffi::CString;
use std::io::Read as _;
use std::os::unix::ffi::OsStrExt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use elog::ereport;
use types_error::{PgResult, ERROR};

/// One destination directory to materialize: `dst` is created by the
/// worker (pg_dir_create_mode), then every (src, dst) file pair is copied.
pub(crate) struct CopyDirJob {
    pub dst_dir: String,
    /// (absolute source path, absolute destination path) per top-level
    /// regular file — `copydir(recurse = false)`'s exact file population.
    pub files: Vec<(String, String)>,
}

/// Copy-phase telemetry for the batch witness line.
#[derive(Debug)]
pub(crate) struct CopyStats {
    pub dirs: usize,
    pub files: usize,
    pub threads: usize,
    /// Wall time of the whole parallel phase.
    pub wall_ns: u64,
    /// Sum of the workers' busy time. This upper-bounds THREAD UTILIZATION
    /// (never-idle workers sum to ~threads x wall by construction) and is
    /// NOT a serial baseline: concurrent workers self-contend for the
    /// device and pay their own per-file fsyncs, so busy/wall says how
    /// well the pool kept busy, nothing about what the pre-F1 inline
    /// serial path would have cost. A real before/after needs a genuine
    /// serial control run (the review that renamed this field caught a
    /// self-referential "speedup" quote built from it).
    pub busy_ns: u64,
}

/// The knobs workers need, snapshotted ON THE JANITOR THREAD from their
/// thread-local GUC mirrors (a worker reading them would see boot
/// defaults — the scout's hazard #1/#2).
#[derive(Clone, Copy)]
pub(crate) struct CopyKnobs {
    clone_files: bool,
    fsync: bool,
    /// pg_fsync's ROUTING, not just its on/off: plain fsync(2) unless
    /// wal_sync_method=fsync_writethrough was installed at snapshot time
    /// (macOS F_FULLFSYNC). Workers must NOT use std File::sync_all, which
    /// is F_FULLFSYNC unconditionally on macOS — a per-file device-flush
    /// upgrade the replaced fd::copydir path never pays by default (and a
    /// measured ~70x per-file cost on the dev box).
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    writethrough: bool,
    file_mode: u32,
    dir_mode: u32,
}

/// First failure wins; everything else aborts fast.
#[derive(Debug)]
struct CopyFailure {
    op: &'static str,
    path: String,
    errno: i32,
    panic: bool,
}

/// Snapshot the copy knobs. MUST be called on the janitor (backend)
/// thread: the sources are thread-local GUC mirrors.
pub(crate) fn snapshot_knobs() -> CopyKnobs {
    CopyKnobs {
        clone_files: fd::vfd::file_copy_method() == fd::copydir::FILE_COPY_METHOD_CLONE,
        fsync: init_small::globals::enableFsync(),
        writethrough: fd::sync::fsync_writethrough_active(),
        file_mode: fd::vfd::pg_file_create_mode(),
        dir_mode: fd::vfd::pg_dir_create_mode(),
    }
}

/// Enumerate one source directory into a CopyDirJob (janitor thread only:
/// plain read_dir, but the caller context — batch transaction — is
/// thread-bound anyway). Mirrors `copydir`'s population: top-level entries,
/// lstat, regular files only (subdirectories are not recursed — database
/// directories contain only files; symlinks and specials are skipped).
pub(crate) fn enumerate_dir_job(src_dir: &str, dst_dir: &str) -> PgResult<CopyDirJob> {
    let mut files = Vec::new();
    let entries = std::fs::read_dir(src_dir).map_err(|e| {
        ereport(ERROR)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not open directory \"{src_dir}\": %m"))
            .into_error()
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not read directory \"{src_dir}\": %m"))
                .into_error()
        })?;
        let name = entry.file_name();
        if name.as_bytes() == b"." || name.as_bytes() == b".." {
            continue;
        }
        let md = std::fs::symlink_metadata(entry.path()).map_err(|e| {
            ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not stat file \"{}\": %m",
                    entry.path().display()
                ))
                .into_error()
        })?;
        if md.is_file() {
            let name = name.to_string_lossy();
            files.push((format!("{src_dir}/{name}"), format!("{dst_dir}/{name}")));
        }
    }
    Ok(CopyDirJob { dst_dir: dst_dir.to_owned(), files })
}

/// Run every job across `workers` scoped threads (clamped to the job
/// count). Returns stats on full success; on any failure (worker error,
/// worker panic, or an interrupt observed by the janitor thread) every
/// worker is stopped via the abort flag, all are joined, and the FIRST
/// failure is raised as an ordinary ereport(ERROR) on the calling thread —
/// the caller (mint_batch_body) then aborts the batch onto the serial
/// fallback. Destination-directory fsyncs run here, on the calling thread,
/// after all workers finished.
pub(crate) fn copy_dirs_parallel(
    jobs: Vec<CopyDirJob>,
    workers: usize,
    knobs: CopyKnobs,
) -> PgResult<CopyStats> {
    let dirs = jobs.len();
    let files: usize = jobs.iter().map(|j| j.files.len()).sum();
    let threads = workers.min(dirs).max(1);

    let queue: Mutex<Vec<CopyDirJob>> = Mutex::new(jobs);
    let abort = AtomicBool::new(false);
    let failure: Mutex<Option<CopyFailure>> = Mutex::new(None);
    let busy_ns = AtomicU64::new(0);

    let t0 = std::time::Instant::now();
    let mut interrupt_err: Option<Box<types_error::PgError>> = None;

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            handles.push(scope.spawn(|| {
                let started = std::time::Instant::now();
                let caught = catch_unwind(AssertUnwindSafe(|| {
                    worker_loop(&queue, &abort, &failure, knobs);
                }));
                if caught.is_err() {
                    park_failure(
                        &failure,
                        &abort,
                        CopyFailure {
                            op: "copy worker",
                            path: String::new(),
                            errno: 0,
                            panic: true,
                        },
                    );
                }
                busy_ns.fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }));
        }
        // Babysit: interrupts are serviced HERE (the only thread whose
        // interrupt TLS is real). A trip stops the workers and surfaces
        // after they are joined.
        while handles.iter().any(|h| !h.is_finished()) {
            // is_installed guard: unit tests run without the seam; live
            // servers always have it.
            if interrupt_err.is_none() && postgres_seams::check_for_interrupts::is_installed() {
                if let Err(e) = postgres_seams::check_for_interrupts::call() {
                    abort.store(true, Ordering::Release);
                    interrupt_err = Some(e);
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        // Scope end joins every worker (all finished — the loop above saw
        // them so). Panics were caught inside the workers.
        for h in handles {
            let _ = h.join();
        }
    });
    let wall_ns = t0.elapsed().as_nanos() as u64;

    if let Some(e) = interrupt_err {
        return Err(e);
    }
    if let Some(f) = failure.into_inner().unwrap_or_else(|e| e.into_inner()) {
        if f.panic {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_INTERNAL_ERROR)
                .errmsg("batch copy worker panicked; aborting the mint batch".to_string())
                .into_error()
                .into());
        }
        return Err(ereport(ERROR)
            .with_saved_errno(f.errno)
            .errcode_for_file_access()
            .errmsg(format!("could not {} \"{}\": %m", f.op, f.path))
            .into_error()
            .into());
    }

    // NOTE: destination-DIRECTORY fsyncs (copydir's tail) are the
    // caller's: mint.rs runs fd::fsync_fname(dst, true) per directory on
    // the janitor thread after this returns — transient-fd machinery that
    // must not run on workers. Per-FILE fsyncs already ran here.
    Ok(CopyStats { dirs, files, threads, wall_ns, busy_ns: busy_ns.into_inner() })
}

fn worker_loop(
    queue: &Mutex<Vec<CopyDirJob>>,
    abort: &AtomicBool,
    failure: &Mutex<Option<CopyFailure>>,
    knobs: CopyKnobs,
) {
    loop {
        if abort.load(Ordering::Acquire) {
            return;
        }
        let job = {
            let mut q = queue.lock().unwrap_or_else(|e| e.into_inner());
            q.pop()
        };
        let Some(job) = job else { return };
        if let Err(f) = copy_one_dir(&job, abort, knobs) {
            park_failure(failure, abort, f);
            return;
        }
    }
}

fn park_failure(failure: &Mutex<Option<CopyFailure>>, abort: &AtomicBool, f: CopyFailure) {
    let mut slot = failure.lock().unwrap_or_else(|e| e.into_inner());
    if slot.is_none() {
        *slot = Some(f);
    }
    abort.store(true, Ordering::Release);
}

fn copy_one_dir(job: &CopyDirJob, abort: &AtomicBool, knobs: CopyKnobs) -> Result<(), CopyFailure> {
    let cdst = cstr(&job.dst_dir)?;
    // MakePGDirectory's shape: one mkdir with pg_dir_create_mode; any
    // failure (EEXIST included — the dst must not pre-exist) is fatal to
    // the batch.
    // SAFETY: mkdir(2) on a NUL-terminated path.
    if unsafe { libc::mkdir(cdst.as_ptr(), knobs.dir_mode as libc::mode_t) } != 0 {
        return Err(CopyFailure {
            op: "create directory",
            path: job.dst_dir.clone(),
            errno: last_errno(),
            panic: false,
        });
    }
    for (src, dst) in &job.files {
        if abort.load(Ordering::Acquire) {
            return Ok(()); // someone else already failed; stop quietly
        }
        copy_one_file(src, dst, abort, knobs)?;
    }
    Ok(())
}

/// One file. Abort responsiveness matches C's cadence: the buffered loop
/// polls the flag per 64KB chunk (copy_file's per-chunk CFI) and the Linux
/// clone loop per 1MB (clone_file's); a poll that trips abandons the file
/// mid-copy — the partial dst is an aborting batch's orphan, removed by
/// cleanup_orphaned_datadirs with the rest of the directory. The macOS
/// copyfile() arm is ONE uninterruptible syscall per file — C's own
/// HAVE_COPYFILE shape (no CFI inside it there either), so that per-file
/// latency floor is inherited, not a regression.
fn copy_one_file(
    src: &str,
    dst: &str,
    abort: &AtomicBool,
    knobs: CopyKnobs,
) -> Result<(), CopyFailure> {
    if knobs.clone_files {
        clone_file_raw(src, dst, abort, knobs)?;
        if knobs.fsync {
            let f = std::fs::File::open(dst).map_err(|e| io_failure("open file", dst, &e))?;
            fsync_file_raw(&f, dst, knobs)?;
        }
        return Ok(());
    }
    let mut from = std::fs::File::open(src).map_err(|e| io_failure("open file", src, &e))?;
    let mut to = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(knobs.file_mode)
            .open(dst)
            .map_err(|e| io_failure("create file", dst, &e))?
    };
    let mut buf = vec![0u8; 8 * 8192];
    loop {
        if abort.load(Ordering::Acquire) {
            return Ok(()); // batch is aborting; partial dst is its orphan
        }
        let n = from.read(&mut buf).map_err(|e| io_failure("read file", src, &e))?;
        if n == 0 {
            break;
        }
        std::io::Write::write_all(&mut to, &buf[..n])
            .map_err(|e| io_failure("write to file", dst, &e))?;
    }
    if knobs.fsync {
        fsync_file_raw(&to, dst, knobs)?;
    }
    Ok(())
}

/// pg_fsync's exact semantics, TLS-free (the worker version of
/// `fd::sync::pg_fsync`): callers gate on knobs.fsync (enableFsync), so
/// this always issues the syscall — plain fsync(2), EINTR-retried, unless
/// wal_sync_method=fsync_writethrough was active at snapshot time (the
/// macOS F_FULLFSYNC arm). NOT std File::sync_all: that is F_FULLFSYNC
/// unconditionally on macOS, a durability-primitive upgrade over the
/// serial path this module replaces (CopyKnobs.writethrough's rationale).
fn fsync_file_raw(f: &std::fs::File, path: &str, knobs: CopyKnobs) -> Result<(), CopyFailure> {
    use std::os::unix::io::AsRawFd;
    let fd = f.as_raw_fd();
    #[cfg(target_os = "macos")]
    if knobs.writethrough {
        // SAFETY: F_FULLFSYNC on a live fd owned by `f` (pg_fsync_writethrough's arm).
        if unsafe { libc::fcntl(fd, libc::F_FULLFSYNC) } < 0 {
            return Err(CopyFailure {
                op: "fsync file",
                path: path.to_owned(),
                errno: last_errno(),
                panic: false,
            });
        }
        return Ok(());
    }
    #[cfg(not(target_os = "macos"))]
    let _ = knobs; // writethrough is macOS-only (pg_fsync's HAVE_FSYNC_WRITETHROUGH arm)
    loop {
        // SAFETY: fsync(2) on a live fd owned by `f`.
        if unsafe { libc::fsync(fd) } == 0 {
            return Ok(());
        }
        let en = last_errno();
        if en != libc::EINTR {
            return Err(CopyFailure {
                op: "fsync file",
                path: path.to_owned(),
                errno: en,
                panic: false,
            });
        }
    }
}

// clone_file's live arms, path-based and TLS-free (the worker versions of
// copydir.rs's clone_file — same syscalls, raw error transport, and the
// same dst create mode: the Linux arm applies knobs.file_mode where the
// serial path's OpenTransientFile applies pg_file_create_mode; the macOS
// COPYFILE_CLONE clones the source's own mode, as copydir's arm does).
#[cfg(target_os = "macos")]
fn clone_file_raw(
    src: &str,
    dst: &str,
    _abort: &AtomicBool,
    _knobs: CopyKnobs,
) -> Result<(), CopyFailure> {
    let csrc = cstr(src)?;
    let cdst = cstr(dst)?;
    // ONE uninterruptible copyfile(3) per file — C's HAVE_COPYFILE arm is
    // the same single call (no CFI inside), so this per-file abort-latency
    // floor is inherited C shape, not a worker-pool regression (the
    // copy_one_file doc's cadence note).
    // SAFETY: copyfile(3) on two NUL-terminated paths; NULL clone state.
    let rc = unsafe {
        libc::copyfile(
            csrc.as_ptr(),
            cdst.as_ptr(),
            std::ptr::null_mut(),
            libc::COPYFILE_CLONE_FORCE,
        )
    };
    if rc < 0 {
        return Err(CopyFailure {
            op: "clone file",
            path: src.to_owned(),
            errno: last_errno(),
            panic: false,
        });
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn clone_file_raw(
    src: &str,
    dst: &str,
    abort: &AtomicBool,
    knobs: CopyKnobs,
) -> Result<(), CopyFailure> {
    use std::os::unix::io::AsRawFd;
    let from = std::fs::File::open(src).map_err(|e| io_failure("open file", src, &e))?;
    let to = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(knobs.file_mode)
            .open(dst)
            .map_err(|e| io_failure("create file", dst, &e))?
    };
    loop {
        // Per-1MB abort poll: clone_file's own per-chunk CFI cadence.
        if abort.load(Ordering::Acquire) {
            return Ok(()); // batch is aborting; partial dst is its orphan
        }
        // SAFETY: copy_file_range(2) on two live fds owned above; NULL
        // offsets advance both positions.
        let n = unsafe {
            libc::copy_file_range(
                from.as_raw_fd(),
                std::ptr::null_mut(),
                to.as_raw_fd(),
                std::ptr::null_mut(),
                1024 * 1024,
                0,
            )
        };
        if n < 0 {
            let en = last_errno();
            if en == libc::EINTR {
                continue;
            }
            return Err(CopyFailure {
                op: "clone file",
                path: src.to_owned(),
                errno: en,
                panic: false,
            });
        }
        if n == 0 {
            return Ok(());
        }
    }
}

fn io_failure(op: &'static str, path: &str, e: &std::io::Error) -> CopyFailure {
    CopyFailure { op, path: path.to_owned(), errno: e.raw_os_error().unwrap_or(0), panic: false }
}

fn cstr(s: &str) -> Result<CString, CopyFailure> {
    CString::new(s).map_err(|_| CopyFailure {
        op: "encode path",
        path: s.to_owned(),
        errno: libc::EINVAL,
        panic: false,
    })
}

fn last_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn knobs() -> CopyKnobs {
        // Test knobs bypass snapshot_knobs on purpose: unit tests have no
        // backend TLS to snapshot (which is the whole point of the split).
        CopyKnobs {
            clone_files: false,
            fsync: false,
            writethrough: false,
            file_mode: 0o600,
            dir_mode: 0o700,
        }
    }

    fn mkfile(path: &std::path::Path, contents: &[u8]) {
        std::fs::write(path, contents).unwrap();
    }

    fn scratch(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "pgrust_parcopy_{tag}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// End-to-end success across multiple jobs and threads: every file
    /// lands byte-identical, stats add up.
    #[test]
    fn parallel_copy_moves_all_files() {
        let root = scratch("ok");
        let mut jobs = Vec::new();
        for j in 0..5 {
            let src = root.join(format!("src{j}"));
            std::fs::create_dir(&src).unwrap();
            for f in 0..4 {
                mkfile(&src.join(format!("f{f}")), format!("j{j}f{f}").as_bytes());
            }
            jobs.push(
                enumerate_dir_job(
                    src.to_str().unwrap(),
                    root.join(format!("dst{j}")).to_str().unwrap(),
                )
                .unwrap(),
            );
        }
        let stats = copy_dirs_parallel(jobs, 3, knobs()).unwrap();
        assert_eq!((stats.dirs, stats.files, stats.threads), (5, 20, 3));
        for j in 0..5 {
            for f in 0..4 {
                let got = std::fs::read(root.join(format!("dst{j}/f{f}"))).unwrap();
                assert_eq!(got, format!("j{j}f{f}").into_bytes());
            }
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Failure propagation: a poisoned job (pre-existing destination
    /// directory — the mkdir EEXIST arm) fails the whole call with the
    /// path and errno in the raised error; the worker error reaches the
    /// caller as an ordinary PgError.
    #[test]
    fn worker_failure_aborts_and_surfaces() {
        let root = scratch("fail");
        let src = root.join("src");
        std::fs::create_dir(&src).unwrap();
        mkfile(&src.join("f0"), b"x");
        let poisoned_dst = root.join("dst_taken");
        std::fs::create_dir(&poisoned_dst).unwrap(); // squatter
        let job =
            enumerate_dir_job(src.to_str().unwrap(), poisoned_dst.to_str().unwrap()).unwrap();
        let err = copy_dirs_parallel(vec![job], 2, knobs()).unwrap_err();
        assert!(
            err.message().contains("create directory")
                && err.message().contains("dst_taken"),
            "error names the op and path: {}",
            err.message()
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The dst create mode is the SNAPSHOTTED knob, not a hardcoded 0600
    /// (group-access clusters run pg_file_create_mode=0640): buffered arm
    /// pinned here; the Linux clone arm shares the same OpenOptions
    /// .mode(knobs.file_mode) call. 0o400 is umask-proof (no sane umask
    /// clears the owner-read bit), so the assert is exact.
    #[test]
    fn dst_files_carry_snapshotted_mode() {
        use std::os::unix::fs::PermissionsExt;
        let root = scratch("mode");
        let src = root.join("src");
        std::fs::create_dir(&src).unwrap();
        mkfile(&src.join("f0"), b"x");
        let dst = root.join("dst");
        let job = enumerate_dir_job(src.to_str().unwrap(), dst.to_str().unwrap()).unwrap();
        let mut k = knobs();
        k.file_mode = 0o400;
        copy_dirs_parallel(vec![job], 1, k).unwrap();
        let mode = std::fs::metadata(dst.join("f0")).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o400, "dst mode is knobs.file_mode, not a hardcoded 0600");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// In-loop abort poll (C's per-chunk CFI cadence): a tripped flag stops
    /// copy_one_file BEFORE the next chunk — a pre-tripped flag yields an
    /// empty partial dst (created, zero bytes), never a completed copy; the
    /// partial is the aborting batch's orphan by contract.
    #[test]
    fn abort_flag_stops_mid_file() {
        let root = scratch("midabort");
        let src = root.join("src");
        std::fs::create_dir(&src).unwrap();
        mkfile(&src.join("f0"), &vec![7u8; 3 * 64 * 1024]);
        let dst = root.join("f0.out");
        let abort = AtomicBool::new(true); // already tripped
        copy_one_file(
            src.join("f0").to_str().unwrap(),
            dst.to_str().unwrap(),
            &abort,
            knobs(),
        )
        .unwrap();
        assert_eq!(
            std::fs::metadata(&dst).unwrap().len(),
            0,
            "no chunk may be copied after the abort flag is up"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Abort-flag propagation (the charter's third unit test): a worker
    /// observing the abort flag stops BETWEEN files — a pre-set flag means
    /// the job's directory is created (mkdir precedes the loop) but no
    /// file is copied; and park_failure keeps the FIRST failure only while
    /// raising the flag for everyone.
    #[test]
    fn abort_flag_stops_copying_between_files() {
        let root = scratch("abort");
        let src = root.join("src");
        std::fs::create_dir(&src).unwrap();
        for f in 0..8 {
            mkfile(&src.join(format!("f{f}")), b"payload");
        }
        let dst = root.join("dst");
        let job = enumerate_dir_job(src.to_str().unwrap(), dst.to_str().unwrap()).unwrap();
        let abort = AtomicBool::new(true); // already tripped
        copy_one_dir(&job, &abort, knobs()).unwrap();
        assert!(dst.is_dir(), "mkdir precedes the abort check");
        assert_eq!(
            std::fs::read_dir(&dst).unwrap().count(),
            0,
            "no file may be copied after the abort flag is up"
        );

        // park_failure: first failure wins, flag goes up.
        let failure: Mutex<Option<CopyFailure>> = Mutex::new(None);
        let flag = AtomicBool::new(false);
        park_failure(
            &failure,
            &flag,
            CopyFailure { op: "one", path: "p1".into(), errno: 1, panic: false },
        );
        park_failure(
            &failure,
            &flag,
            CopyFailure { op: "two", path: "p2".into(), errno: 2, panic: false },
        );
        assert!(flag.load(Ordering::Acquire));
        let kept = failure.into_inner().unwrap().unwrap();
        assert_eq!((kept.op, kept.errno), ("one", 1), "first failure wins");
        let _ = std::fs::remove_dir_all(&root);
    }
}
