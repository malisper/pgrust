use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Once;

use ::types_storage::File;

use crate::vfd::{self, with_fd};

static SETUP: Once = Once::new();
// Serializes the tests that chdir into a scratch data directory.
static CWD: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn enter_datadir(dir: &str) -> std::sync::MutexGuard<'static, ()> {
    let guard = CWD.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    std::fs::create_dir_all(format!("{dir}/base/pgsql_tmp")).unwrap();
    std::env::set_current_dir(dir).unwrap();
    // Sim has no cwd: relative paths resolve against "/" (sim.rs norm_path),
    // so the datadir-relative temp tree must exist at the sim root. fd's
    // ENOENT retry (temp.rs OpenTemporaryFileInTablespace) only mkdirs the
    // pgsql_tmp leaf, never `base` — a real datadir always has `base`.
    #[cfg(pgrust_sim)]
    vfs_mkdir_p("base/pgsql_tmp");
    guard
}

// ---------------------------------------------------------------------------
// DST P4 fixture routing (P1 Ruling 3 Class B, `# pending: sim-fixture-routing`):
// fixtures are built and asserted through the ACTIVE vfs, so setup, ops, and
// asserts share one filesystem domain under both cfgs. Under the default cfg
// these helpers hit PosixVfs — byte-for-byte the same real-fs behavior the old
// std::fs fixtures had; under `--cfg pgrust_sim` they hit the thread-local
// SimVfs tree the fd ops actually run against.
// ---------------------------------------------------------------------------

fn cpath(path: &str) -> std::ffi::CString {
    std::ffi::CString::new(path).unwrap()
}

/// `mkdir -p` through the active vfs (EEXIST tolerated per component).
fn vfs_mkdir_p(path: &str) {
    let mut prefix = String::new();
    for comp in path.split('/') {
        if comp.is_empty() {
            continue;
        }
        if !prefix.is_empty() || path.starts_with('/') {
            prefix.push('/');
        }
        prefix.push_str(comp);
        let rc = vfs::mkdir(&cpath(&prefix), 0o700);
        assert!(
            rc == 0 || vfs::get_errno() == libc::EEXIST,
            "vfs_mkdir_p({prefix}): errno {}",
            vfs::get_errno()
        );
    }
}

/// Create/replace a file with `data` through the active vfs.
fn vfs_write_file(path: &str, data: &[u8]) {
    let fd = vfs::open(
        &cpath(path),
        libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC,
        0o600,
    );
    assert!(fd >= 0, "vfs_write_file open({path}): errno {}", vfs::get_errno());
    if !data.is_empty() {
        assert_eq!(vfs::pwrite(fd, data, 0), data.len() as isize, "{path}");
    }
    assert_eq!(vfs::close(fd), 0);
}

/// Whole-file read through the active vfs.
fn vfs_read_file(path: &str) -> Vec<u8> {
    let fd = vfs::open(&cpath(path), libc::O_RDONLY, 0);
    assert!(fd >= 0, "vfs_read_file open({path}): errno {}", vfs::get_errno());
    let size = vfs::file_size(fd);
    assert!(size >= 0, "{path}");
    let mut buf = vec![0u8; size as usize];
    if size > 0 {
        assert_eq!(vfs::pread(fd, &mut buf, 0), size as isize, "{path}");
    }
    assert_eq!(vfs::close(fd), 0);
    buf
}

/// stat() through the active vfs — the same-domain `Path::exists`.
fn vfs_path_exists(path: &str) -> bool {
    let mut info = vfs::FileInfo::zeroed();
    vfs::stat(&cpath(path), &mut info) == 0
}

// Per-thread CHECK_FOR_INTERRUPTS() count (each #[test] runs on its own
// thread, and nothing here crosses threads): lets tests witness which copy
// engine actually ran — copy_file checks once per 64KiB chunk, clone_file at
// most once per 1MiB chunk.
thread_local! {
    static INTERRUPT_CHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn interrupt_checks() -> u64 {
    INTERRUPT_CHECKS.with(std::cell::Cell::get)
}

// Per-thread record of every pgstat_report_wait_start(wait_event_info) the
// fd ops made: the witness for the WaitEventIO codes C passes (BufFile
// read/write, copy_file read/write/copy).
thread_local! {
    static WAIT_EVENTS: std::cell::RefCell<Vec<u32>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn take_wait_events() -> Vec<u32> {
    WAIT_EVENTS.with(|v| std::mem::take(&mut *v.borrow_mut()))
}

// Per-thread capture of every server-log-bound report (elog emit hook):
// (level, message) pairs, for the LOG/DEBUG witnesses below.
thread_local! {
    static LOG_LINES: std::cell::RefCell<Vec<(types_error::ErrorLevel, String)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn capture_log_line(err: &types_error::PgError, _output_to_server: &mut bool) {
    LOG_LINES.with(|v| v.borrow_mut().push((err.level(), err.message().to_string())));
}

fn take_log_lines() -> Vec<(types_error::ErrorLevel, String)> {
    LOG_LINES.with(|v| std::mem::take(&mut *v.borrow_mut()))
}

// wait_event_types.h WaitEventIO codes (PG_WAIT_IO | index in name order).
const PG_WAIT_IO: u32 = 0x0A00_0000;
const WE_BUFFILE_READ: u32 = PG_WAIT_IO + 6;
const WE_BUFFILE_WRITE: u32 = PG_WAIT_IO + 7;
const WE_COPY_FILE_READ: u32 = PG_WAIT_IO + 15;
const WE_COPY_FILE_WRITE: u32 = PG_WAIT_IO + 16;

fn setup() {
    SETUP.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        crate::init_seams();

        xact_seams::get_current_sub_transaction_id::set(|| 1);
        postgres_seams::check_for_interrupts::set(|| {
            INTERRUPT_CHECKS.with(|c| c.set(c.get() + 1));
            Ok(())
        });
        aio_seams::pgaio_closing_fd::set(|_| {});
        aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
        waitevent_seams::pgstat_report_wait_start::set(|ev| {
            WAIT_EVENTS.with(|v| v.borrow_mut().push(ev));
        });
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pgstat_seams::pgstat_report_tempfile::set(|_| {});
    });
    vfd::InitFileAccess();
}

fn scratch_dir(tag: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "pgrust_fd_test_{}_{tag}_{n}",
        std::process::id()
    ));
    // Real-fs side stays: the posix carve-outs (AllocateFile stdio fopen,
    // OpenPipeStream popen) resolve scratch paths on the real filesystem
    // under BOTH cfgs (contract §1.1 — stdio/pipes are out of Vfs scope).
    std::fs::create_dir_all(&dir).unwrap();
    // Mirror the scratch dir into the active vfs namespace so vfs-routed
    // fixtures and fd ops resolve; pure EEXIST no-ops under the default cfg.
    vfs_mkdir_p(dir.to_str().unwrap());
    dir.to_str().unwrap().to_owned()
}

fn open_rw(path: &str) -> File {
    let f = crate::io::PathNameOpenFile(path, libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC)
        .unwrap();
    assert!(f.0 > 0, "open failed: {path}");
    f
}

#[test]
fn vfd_open_write_read_close_roundtrip() {
    setup();
    let dir = scratch_dir("roundtrip");
    let path = format!("{dir}/a");

    let f = open_rw(&path);
    assert_eq!(crate::io::FileWrite(f, b"hello", 0, 0).unwrap(), 5);
    let mut buf = [0u8; 5];
    assert_eq!(crate::io::FileRead(f, &mut buf, 0, 0).unwrap(), 5);
    assert_eq!(&buf, b"hello");
    assert_eq!(crate::io::FileSize(f).unwrap(), 5);
    assert_eq!(crate::io::FilePathName(f), path);

    crate::io::FileClose(f).unwrap();
    assert!(vfs_path_exists(&path));
}

#[test]
#[cfg(target_os = "linux")]
fn dio_companion_fd_lifecycle() {
    setup();
    let dir = scratch_dir("dio");
    let path = format!("{dir}/d");
    let f = open_rw(&path);
    assert_eq!(crate::io::FileWrite(f, &[7u8; 8192], 0, 0).unwrap(), 8192);

    let ext_before = vfd::num_external_fds();
    let raw = with_fd(|fd| vfd::FileAccessDio(fd, f.0)).unwrap();
    if raw < 0 {
        // tmpfs refuses O_DIRECT; the failure must latch, never retry-loop.
        assert!(with_fd(|fd| fd.vfd_cache[f.0 as usize].dio_failed));
        crate::io::FileClose(f).unwrap();
        return;
    }
    assert_eq!(vfd::num_external_fds(), ext_before + 1);
    let again = with_fd(|fd| vfd::FileAccessDio(fd, f.0)).unwrap();
    assert_eq!(raw, again, "companion fd must be cached, not reopened");

    crate::io::FileClose(f).unwrap();
    assert_eq!(vfd::num_external_fds(), ext_before);
    // Slot reuse must not inherit dio state.
    let f2 = open_rw(&format!("{dir}/e"));
    assert_eq!(f.0, f2.0);
    assert!(with_fd(|fd| !fd.vfd_cache[f2.0 as usize].dio_failed
        && fd.vfd_cache[f2.0 as usize].fd_dio.is_none()));
    crate::io::FileClose(f2).unwrap();
}

#[test]
fn vfd_slot_recycled_through_free_list() {
    setup();
    let dir = scratch_dir("recycle");

    let f1 = open_rw(&format!("{dir}/one"));
    crate::io::FileClose(f1).unwrap();
    let f2 = open_rw(&format!("{dir}/two"));
    assert_eq!(f1.0, f2.0);
    crate::io::FileClose(f2).unwrap();
}

#[test]
fn lru_evicts_and_reopens_transparently() {
    setup();
    let dir = scratch_dir("lru");

    // Force the LRU to evict aggressively: every open must close another.
    let saved = vfd::max_safe_fds();
    vfd::set_max_safe_fds_value(1);

    let files: Vec<File> = (0..8)
        .map(|i| {
            let f = open_rw(&format!("{dir}/f{i}"));
            assert_eq!(crate::io::FileWrite(f, format!("data{i}").as_bytes(), 0, 0).unwrap(), 5);
            f
        })
        .collect();

    let open_now = with_fd(|fd| fd.nfile);
    assert!(open_now <= 1, "nfile = {open_now}");

    // Reads from evicted VFDs must reopen via LruInsert with saved flags.
    for (i, &f) in files.iter().enumerate() {
        let mut buf = [0u8; 5];
        assert_eq!(crate::io::FileRead(f, &mut buf, 0, 0).unwrap(), 5);
        assert_eq!(buf, format!("data{i}").as_bytes());
    }

    for f in files {
        crate::io::FileClose(f).unwrap();
    }
    vfd::set_max_safe_fds_value(saved);
}

#[test]
fn vfd_cache_grows_in_doubling_steps() {
    setup();
    let dir = scratch_dir("grow");

    let files: Vec<File> = (0..40).map(|i| open_rw(&format!("{dir}/g{i}"))).collect();
    let size = with_fd(|fd| fd.size_vfd_cache());
    assert!(size >= 41, "cache size {size}");
    assert_eq!(with_fd(|fd| fd.size_vfd_cache()), 64);

    for f in files {
        crate::io::FileClose(f).unwrap();
    }
}

#[test]
fn temp_file_deleted_at_close_and_counted() {
    setup();
    let dir = scratch_dir("temp");
    let _cwd = enter_datadir(&dir);

    with_fd(|fd| fd.temporary_files_allowed = true);
    let f = crate::temp::OpenTemporaryFile(true).unwrap();
    assert!(f.0 > 0);
    let path = crate::io::FilePathName(f);
    assert_eq!(crate::io::FileWrite(f, &[7u8; 2048], 0, 0).unwrap(), 2048);
    assert_eq!(with_fd(|fd| fd.temporary_files_size), 2048);
    assert!(vfs_path_exists(&path));

    crate::io::FileClose(f).unwrap();
    assert!(!vfs_path_exists(&path));
    assert_eq!(with_fd(|fd| fd.temporary_files_size), 0);
}

#[test]
fn temp_file_limit_enforced_with_sqlstate() {
    setup();
    let dir = scratch_dir("limit");
    let _cwd = enter_datadir(&dir);

    with_fd(|fd| fd.temporary_files_allowed = true);
    let f = crate::temp::OpenTemporaryFile(true).unwrap();

    let saved = guc_tables::vars::temp_file_limit.read();
    guc_tables::vars::temp_file_limit.write(1);
    let err = crate::io::FileWrite(f, &[0u8; 2048], 0, 0).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED);
    guc_tables::vars::temp_file_limit.write(saved);

    crate::io::FileClose(f).unwrap();
}

#[test]
fn buffile_delete_unknown_is_elog_xx000() {
    setup();
    let dir = scratch_dir("bfdel");
    let _cwd = enter_datadir(&dir);
    with_fd(|fd| fd.temporary_files_allowed = true);

    let fs = crate::fileset::FileSet::init().unwrap();
    let err = crate::buffile::BufFileDeleteFileSet(&fs, "missing", false).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INTERNAL_ERROR);
    assert!(
        err.message().contains("could not delete unknown BufFile \"missing\""),
        "message={}",
        err.message()
    );
}

#[test]
fn file_truncate_adjusts_temp_accounting() {
    setup();
    let dir = scratch_dir("trunc");
    let _cwd = enter_datadir(&dir);

    with_fd(|fd| fd.temporary_files_allowed = true);
    let f = crate::temp::OpenTemporaryFile(true).unwrap();
    assert_eq!(crate::io::FileWrite(f, &[1u8; 4096], 0, 0).unwrap(), 4096);
    assert_eq!(crate::io::FileTruncate(f, 1024, 0).unwrap(), 0);
    assert_eq!(with_fd(|fd| fd.temporary_files_size), 1024);
    crate::io::FileClose(f).unwrap();
}

#[test]
fn transient_files_track_and_close() {
    setup();
    let dir = scratch_dir("transient");
    let path = format!("{dir}/t");
    vfs_write_file(&path, b"x");

    let occupied = || with_fd(|fd| crate::vfd::occupied_descs(fd));
    let before = occupied();
    let fd1 = crate::desc::OpenTransientFile(&path, libc::O_RDWR).unwrap();
    assert!(fd1 >= 0);
    assert_eq!(occupied(), before + 1);
    assert_eq!(crate::desc::TransientFileRawFd(fd1), Some(fd1));
    assert_eq!(crate::desc::CloseTransientFile(fd1), 0);
    assert_eq!(occupied(), before);

    let missing = crate::desc::OpenTransientFile(&format!("{dir}/absent"), libc::O_RDONLY).unwrap();
    assert_eq!(missing, -1);
    assert_eq!(vfd::get_errno(), libc::ENOENT);
}

#[test]
fn durable_rename_and_unlink() {
    setup();
    let dir = scratch_dir("durable");
    let old = format!("{dir}/old");
    let new = format!("{dir}/new");
    vfs_write_file(&old, b"payload");
    vfs_write_file(&new, b"stale");

    assert_eq!(crate::sync::durable_rename(&old, &new, ::types_error::LOG).unwrap(), 0);
    assert!(!vfs_path_exists(&old));
    assert_eq!(vfs_read_file(&new), b"payload");

    assert_eq!(crate::sync::durable_unlink(&new, ::types_error::LOG).unwrap(), 0);
    assert!(!vfs_path_exists(&new));

    assert_eq!(crate::sync::durable_unlink(&new, ::types_error::LOG).unwrap(), -1);
}

/// Fsyncgate elevel routing (fd.c:4001): data_sync_elevel promotes any
/// level to PANIC unless data_sync_retry=on (GUC default off,
/// guc_tables.c:2102) — a failed data fsync must never be retried at a
/// catchable level. Every mapping-file / segment / SLRU fsync site routes
/// through this; the default MUST stay PANIC.
#[test]
fn data_sync_elevel_panics_unless_retry_guc() {
    setup();
    assert!(!vfd::data_sync_retry(), "GUC boot default is off");
    assert_eq!(crate::sync::data_sync_elevel(::types_error::ERROR), ::types_error::PANIC);
    assert_eq!(crate::sync::data_sync_elevel(::types_error::WARNING), ::types_error::PANIC);
    vfd::set_data_sync_retry(true);
    assert_eq!(crate::sync::data_sync_elevel(::types_error::ERROR), ::types_error::ERROR);
    vfd::set_data_sync_retry(false);
}

/// fsync_fname (fd.c:3846) raises at data_sync_elevel(ERROR): with the GUC
/// at default a failure must escalate to backend exit (PanicExitThread
/// unwind), never return a catchable Err — the guard the rewriteheap
/// mapping-file sites (rewriteheap.c:923/1238/1252) now rely on.
#[test]
fn fsync_fname_failure_escalates_to_thread_exit() {
    setup();
    let dir = scratch_dir("fsyncfname_panic");
    let missing = format!("{dir}/does_not_exist");
    let r = std::panic::catch_unwind(|| crate::sync::fsync_fname(&missing, false));
    match r {
        Err(payload) => assert!(
            payload.is::<::types_error::PanicExitThread>(),
            "fsync_fname failure must unwind PanicExitThread"
        ),
        Ok(inner) => panic!("fsync_fname failure returned control at default GUC: {inner:?}"),
    }
    // With data_sync_retry=on the same failure is an ordinary Err.
    vfd::set_data_sync_retry(true);
    let r = std::panic::catch_unwind(|| crate::sync::fsync_fname(&missing, false));
    vfd::set_data_sync_retry(false);
    match r {
        Ok(inner) => assert!(inner.is_err(), "retry=on demotes to a catchable ERROR"),
        Err(_) => panic!("retry=on must not panic"),
    }
}

#[test]
fn allocate_dir_walks_entries() {
    setup();
    let dir = scratch_dir("dirwalk");
    for name in ["alpha", "beta", "gamma"] {
        vfs_write_file(&format!("{dir}/{name}"), b"");
    }

    let d = crate::desc::AllocateDir(&dir).unwrap();
    assert!(d.is_some());
    let mut seen = Vec::new();
    while let Some(ent) = crate::desc::ReadDir(d, &dir).unwrap() {
        if ent.d_name != "." && ent.d_name != ".." {
            seen.push(ent.d_name);
        }
    }
    crate::desc::FreeDir(d).unwrap();
    seen.sort();
    assert_eq!(seen, ["alpha", "beta", "gamma"]);

    let mut via_seam = Vec::new();
    crate::desc::with_allocated_dir(&dir, &mut |name| {
        if name != "." && name != ".." {
            via_seam.push(name.to_owned());
        }
        Ok(false)
    })
    .unwrap();
    via_seam.sort();
    assert_eq!(via_seam, ["alpha", "beta", "gamma"]);

    let none = crate::desc::AllocateDir(&format!("{dir}/absent")).unwrap();
    assert!(none.is_none());
    let err = crate::desc::ReadDir(none, "absent").unwrap_err();
    assert!(err.message().contains("could not open directory"));
}

#[test]
fn eoxact_closes_flagged_vfds_and_descs() {
    setup();
    let dir = scratch_dir("eoxact");
    let path = format!("{dir}/x");

    let f = open_rw(&path);
    with_fd(|fd| {
        fd.vfd_cache[f.0 as usize].fdstate |= vfd::FD_CLOSE_AT_EOXACT;
        fd.have_xact_temporary_files = true;
    });
    let td = crate::desc::OpenTransientFile(&path, libc::O_RDWR).unwrap();
    assert!(td >= 0);

    crate::sync::AtEOXact_Files(false).unwrap();

    with_fd(|fd| {
        assert!(fd.vfd_cache[f.0 as usize].file_name.is_none());
        assert!(fd.allocated_descs.is_empty());
        assert!(!fd.have_xact_temporary_files);
    });
}

#[test]
fn subxact_reassigns_or_frees_descs() {
    setup();
    let dir = scratch_dir("subxact");
    let path = format!("{dir}/s");
    vfs_write_file(&path, b"x");

    let td = crate::desc::OpenTransientFile(&path, libc::O_RDWR).unwrap();
    let idx = with_fd(|fd| fd.allocated_descs.iter().rposition(Option::is_some).unwrap());
    with_fd(|fd| fd.allocated_descs[idx].as_mut().unwrap().create_subid = 7);

    crate::sync::AtEOSubXact_Files(true, 7, 3);
    assert_eq!(with_fd(|fd| fd.allocated_descs[idx].as_ref().unwrap().create_subid), 3);

    crate::sync::AtEOSubXact_Files(false, 3, 1);
    assert!(crate::desc::TransientFileRawFd(td).is_none());
}

#[test]
fn temp_rel_name_matcher_matches_c() {
    setup();
    for ok in ["t1_2", "t123_456", "t1_2_fsm", "t1_2_vm.3", "t1_2.0", "t1_2_init.42"] {
        assert!(crate::sync::looks_like_temp_rel_name(ok), "{ok}");
    }
    for bad in ["x1_2", "t_2", "t1", "t1_", "t1_2_", "t1_2_main", "t1_2.", "t1_2_bogus", "t1_2x"] {
        assert!(!crate::sync::looks_like_temp_rel_name(bad), "{bad}");
    }
}

#[test]
fn temp_tablespace_path_shapes() {
    setup();
    assert_eq!(crate::temp::TempTablespacePath(0), "base/pgsql_tmp");
    assert_eq!(crate::temp::TempTablespacePath(1663), "base/pgsql_tmp");
    assert_eq!(crate::temp::TempTablespacePath(1664), "base/pgsql_tmp");
    assert_eq!(
        crate::temp::TempTablespacePath(16385),
        format!(
            "pg_tblspc/16385/{}/pgsql_tmp",
            ::types_storage::TABLESPACE_VERSION_DIRECTORY
        )
    );
}

#[test]
fn temp_tablespace_list_round_robin() {
    setup();
    assert!(!crate::temp::TempTablespacesAreSet());
    crate::temp::SetTempTablespaces(&[42]);
    assert!(crate::temp::TempTablespacesAreSet());
    assert_eq!(crate::temp::GetNextTempTableSpace(), 42);
    assert_eq!(crate::temp::GetNextTempTableSpace(), 42);

    let mut out = [0; 4];
    assert_eq!(crate::temp::GetTempTablespaces(&mut out), 1);
    assert_eq!(out[0], 42);

    crate::sync::AtEOXact_Files(true).unwrap();
    assert!(!crate::temp::TempTablespacesAreSet());
    assert_eq!(crate::temp::GetNextTempTableSpace(), ::types_core::InvalidOid);
}

#[test]
fn remove_pg_temp_files_in_dir_filters_prefix() {
    setup();
    let dir = scratch_dir("rmtemp");
    vfs_write_file(&format!("{dir}/pgsql_tmp123.0"), b"x");
    vfs_mkdir_p(&format!("{dir}/pgsql_tmp_sub"));
    vfs_write_file(&format!("{dir}/pgsql_tmp_sub/anything"), b"x");
    vfs_write_file(&format!("{dir}/keepme"), b"x");

    crate::sync::RemovePgTempFilesInDir(&dir, false, false).unwrap();

    assert!(!vfs_path_exists(&format!("{dir}/pgsql_tmp123.0")));
    assert!(!vfs_path_exists(&format!("{dir}/pgsql_tmp_sub")));
    assert!(vfs_path_exists(&format!("{dir}/keepme")));

    crate::sync::RemovePgTempFilesInDir(&format!("{dir}/absent"), true, false).unwrap();
}

#[test]
fn check_debug_io_direct_parses_flag_list() {
    setup();
    use ::types_storage::{IO_DIRECT_DATA, IO_DIRECT_WAL, IO_DIRECT_WAL_INIT};
    assert_eq!(vfd::check_debug_io_direct(""), Ok(0));
    assert_eq!(vfd::check_debug_io_direct("data"), Ok(IO_DIRECT_DATA));
    assert_eq!(
        vfd::check_debug_io_direct("data, WAL, wal_init"),
        Ok(IO_DIRECT_DATA | IO_DIRECT_WAL | IO_DIRECT_WAL_INIT)
    );
    // Err = the GUC_check_errdetail text (fd.c:4029/4046).
    assert_eq!(
        vfd::check_debug_io_direct("bogus"),
        Err("Invalid option \"bogus\".".to_string())
    );

    // SplitGUCList semantics, verified against postgres:18.3 (2026-07-31):
    // 'data wal' -> FATAL: invalid value for parameter "debug_io_direct":
    // "data wal" / DETAIL: Invalid list syntax in parameter
    // "debug_io_direct".  (whitespace does NOT separate unquoted items)
    let syntax = Err("Invalid list syntax in parameter \"debug_io_direct\".".to_string());
    assert_eq!(vfd::check_debug_io_direct("data wal"), syntax);
    // 'data,,wal' -> same FATAL/DETAIL (empty items are a syntax error).
    assert_eq!(vfd::check_debug_io_direct("data,,wal"), syntax);
    assert!(vfd::check_debug_io_direct("data,").is_err());
    // '"data",wal' -> server started (double-quoted items are legal).
    assert_eq!(
        vfd::check_debug_io_direct("\"data\",wal"),
        Ok(IO_DIRECT_DATA | IO_DIRECT_WAL)
    );
    // 'data,wal ' -> server started (trailing whitespace trimmed).
    assert_eq!(
        vfd::check_debug_io_direct("data,wal "),
        Ok(IO_DIRECT_DATA | IO_DIRECT_WAL)
    );
}

#[test]
fn external_fd_reservation_caps_at_a_third() {
    setup();
    let saved = vfd::max_safe_fds();
    vfd::set_max_safe_fds_value(9);
    let baseline = vfd::num_external_fds();

    assert!(crate::vfd::AcquireExternalFD().unwrap());
    assert!(crate::vfd::AcquireExternalFD().unwrap());
    assert!(crate::vfd::AcquireExternalFD().unwrap());
    assert!(!crate::vfd::AcquireExternalFD().unwrap());
    assert_eq!(vfd::get_errno(), libc::EMFILE);

    while vfd::num_external_fds() > baseline {
        crate::vfd::ReleaseExternalFD();
    }
    vfd::set_max_safe_fds_value(saved);
}

#[test]
fn pipe_stream_round_trip() {
    setup();
    let idx = crate::desc::OpenPipeStream("exit 3", "r").unwrap();
    assert!(idx >= 0);
    let status = crate::desc::ClosePipeStream(idx).unwrap();
    assert_eq!(status, 3 << 8);
}

// A backend thread's blocked signal mask must not reach the popen child:
// C's kill(-pid, SIGTERM) from pg_terminate_backend kills a COPY PROGRAM.
#[test]
fn pipe_stream_child_starts_with_signals_unblocked() {
    setup();
    let mut set: libc::sigset_t = unsafe { std::mem::zeroed() };
    let mut old: libc::sigset_t = unsafe { std::mem::zeroed() };
    // SAFETY: thread-local mask change, restored below.
    unsafe {
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, &mut old);
    }
    let idx = crate::desc::OpenPipeStream("kill -TERM $$; echo survived", "r").unwrap();
    // SAFETY: restores the mask saved above.
    unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &old, std::ptr::null_mut()) };
    assert!(idx >= 0);
    let mut buf = [0u8; 64];
    assert_eq!(crate::desc::PipeStreamRead(idx, &mut buf).unwrap(), 0);
    assert_eq!(crate::desc::ClosePipeStream(idx).unwrap(), libc::SIGTERM);
}

#[test]
fn pipe_stream_read_drains_child_stdout() {
    setup();
    let idx = crate::desc::OpenPipeStream("printf 'ab\\ncd\\n'", "r").unwrap();
    assert!(idx >= 0);
    let mut buf = [0u8; 64];
    // Greedy fill (fread semantics): short only at EOF.
    let n = crate::desc::PipeStreamRead(idx, &mut buf).unwrap();
    assert_eq!(&buf[..n], b"ab\ncd\n");
    assert_eq!(crate::desc::PipeStreamRead(idx, &mut buf).unwrap(), 0);
    assert_eq!(crate::desc::ClosePipeStream(idx).unwrap(), 0);
}

#[test]
fn pipe_stream_write_feeds_child_stdin() {
    setup();
    // Child asserts the bytes it receives; a mismatch surfaces as exit 9
    // in the pclose status word.
    let idx =
        crate::desc::OpenPipeStream("test \"$(cat)\" = 'payload' || exit 9", "w").unwrap();
    assert!(idx >= 0);
    crate::desc::PipeStreamWrite(idx, b"payload").unwrap();
    assert_eq!(crate::desc::ClosePipeStream(idx).unwrap(), 0);
}

#[test]
fn pipe_stream_write_epipe_when_child_quit() {
    setup();
    let idx = crate::desc::OpenPipeStream("exit 0", "w").unwrap();
    assert!(idx >= 0);
    // Wait for the child to be gone, then write until the pipe breaks.
    std::thread::sleep(std::time::Duration::from_millis(200));
    let payload = [0u8; 65536];
    let mut saw_epipe = false;
    for _ in 0..4 {
        if let Err(en) = crate::desc::PipeStreamWrite(idx, &payload) {
            assert_eq!(en, libc::EPIPE);
            saw_epipe = true;
            break;
        }
    }
    assert!(saw_epipe, "write to a dead child's pipe never returned EPIPE");
    assert_eq!(crate::desc::ClosePipeStream(idx).unwrap(), 0);
}

#[test]
fn pwrite_zeros_buffer_is_io_aligned() {
    // O_DIRECT contract (c.h PGIOAlignedBlock, common/file_utils.c
    // pg_pwrite_zeros): the zero buffer handed to pwritev must be
    // PG_IO_ALIGN_SIZE-aligned or the kernel EINVALs every FileZero-backed
    // zero-extension under debug_io_direct=data. macOS has no O_DIRECT, so
    // the EINVAL itself is Linux-only; the alignment property is testable
    // everywhere.
    let addr = crate::io::ZBUFFER.0.as_ptr() as usize;
    assert_eq!(addr % ::types_storage::bufpage::PG_IO_ALIGN_SIZE, 0);
    assert_eq!(
        core::mem::align_of::<crate::io::IoAlignedBlock>(),
        ::types_storage::bufpage::PG_IO_ALIGN_SIZE
    );
}

#[test]
fn file_zero_and_fallocate_extend() {
    setup();
    let dir = scratch_dir("zero");
    let f = open_rw(&format!("{dir}/z"));
    assert_eq!(crate::io::FileZero(f, 0, 16384, 0).unwrap(), 0);
    assert_eq!(crate::io::FileSize(f).unwrap(), 16384);
    assert_eq!(crate::io::FileFallocate(f, 16384, 8192, 0).unwrap(), 0);
    assert_eq!(crate::io::FileSize(f).unwrap(), 24576);
    crate::io::FileClose(f).unwrap();
}

#[test]
fn allocate_file_stdio_modes() {
    setup();
    let dir = scratch_dir("stdio");
    let path = format!("{dir}/s");

    let w = crate::desc::AllocateFile(&path, "w").unwrap();
    assert!(w >= 0);
    crate::desc::with_allocated_stdio(w, |f| {
        use std::io::Write;
        f.write_all(b"line").unwrap();
    })
    .unwrap();
    assert_eq!(crate::desc::FreeFile(w).unwrap(), 0);
    assert_eq!(std::fs::read(&path).unwrap(), b"line");

    let missing = crate::desc::AllocateFile(&format!("{dir}/absent"), "r").unwrap();
    assert_eq!(missing, -1);
    assert_eq!(vfd::get_errno(), libc::ENOENT);
}

// DST P1 Ruling 3 Class C regression — the SIM_FD_BASE tripwire that caught
// the FreeDesc misroute, made permanent. AllocateFile mints its fd posix-side
// (open_stdio, the fopen carve-out of contract §1.1), so FreeDesc must close
// it posix-side; routing it through vfs::close makes SimVfs EBADF the foreign
// fd (below SIM_FD_BASE), FreeFile report -1, and the posix fd leak. The
// OpenTransientFile RawFd arm is vfs-minted and correctly stays on vfs::close.
#[cfg(pgrust_sim)]
#[test]
fn allocate_file_stdio_free_closes_posix_side_not_vfs() {
    use std::os::fd::AsRawFd;

    setup();
    let dir = scratch_dir("stdio_sim_tripwire");
    let path = format!("{dir}/s");

    let idx = crate::desc::AllocateFile(&path, "w").unwrap();
    assert!(idx >= 0);
    let raw = crate::desc::with_allocated_stdio(idx, |f| f.as_raw_fd()).unwrap();
    assert!(
        raw < vfs::sim::SIM_FD_BASE,
        "stdio fd must be posix-minted (carve-out), got sim-domain fd {raw}"
    );

    // A vfs::close misroute EBADFs inside SimVfs and surfaces here as -1.
    assert_eq!(
        crate::desc::FreeFile(idx).unwrap(),
        0,
        "FreeDesc routed a posix-minted stdio fd through vfs::close"
    );

    // And the posix-side close really happened (pre-fix the fd leaked).
    assert_eq!(
        unsafe { libc::fcntl(raw, libc::F_GETFD) },
        -1,
        "posix fd {raw} leaked: still open after FreeFile"
    );
}


// DST P4 finding F1b: every RAII holder of a vfs-minted fd must release
// through the SAME Vfs provider that minted it. Pre-guard, the holders were
// plain OwnedFd, whose Drop closes posix-side: any unwind or thread exit with
// a live holder EBADF'd inside the kernel (the sim fd is foreign there), the
// sim fd LEAKED in the sim table, and under debug std's IO-safety check
// aborted the whole test process. P4 fault injection unwinds through exactly
// these states.
//
// Same-thread arm: drop live holders (the FdState-teardown shape, without
// leaving the thread) and prove the fds were released INTO THE SIM NAMESPACE.
#[cfg(pgrust_sim)]
#[test]
fn dropped_holders_release_into_sim_namespace_not_posix() {
    use std::os::fd::AsRawFd;

    setup();
    let dir = scratch_dir("f1b_guard");

    // Holder 1: AllocatedHandle::RawFd (transient desc), vfs-minted.
    let tpath = format!("{dir}/t");
    vfs_write_file(&tpath, b"x");
    let tfd = crate::desc::OpenTransientFile(&tpath, libc::O_RDWR).unwrap();
    assert!(tfd >= vfs::sim::SIM_FD_BASE, "transient fd must be sim-minted");

    // Holder 2: Vfd.fd (VFD cache), vfs-minted.
    let f = open_rw(&format!("{dir}/v"));
    let vraw = with_fd(|fd| {
        fd.vfd_cache[f.0 as usize].fd.as_ref().map(|h| h.as_raw_fd()).unwrap()
    });
    assert!(vraw >= vfs::sim::SIM_FD_BASE, "vfd fd must be sim-minted");

    // Both live sim-side right now.
    let mut info = vfs::FileInfo::zeroed();
    assert_eq!(vfs::fstat(tfd, &mut info), 0);
    assert_eq!(vfs::fstat(vraw, &mut info), 0);

    // The unwind/teardown shape: the holders drop WITHOUT the deliberate
    // close paths (FreeDesc / FileClose) running.
    with_fd(|fd| {
        fd.allocated_descs.clear();
        fd.vfd_cache.clear();
        fd.nfile = 0;
    });

    // Pre-guard this point was unreachable (debug IO-safety abort) or, with
    // ub-checks off, both fds were still open sim-side (leaked). The guard
    // must have released them in the SIM fd table.
    assert_eq!(
        vfs::fstat(tfd, &mut info),
        -1,
        "transient-desc holder leaked its sim fd on drop"
    );
    assert_eq!(vfd::get_errno(), libc::EBADF);
    assert_eq!(vfs::fstat(vraw, &mut info), -1, "Vfd.fd holder leaked its sim fd on drop");
    assert_eq!(vfd::get_errno(), libc::EBADF);
}

// Thread-exit arm — the exact F1 chain: a panic unwinds out of fd code with
// live holders, the thread dies, and the FdState TLS destructor drops them.
// Pre-guard (debug) this aborted the WHOLE test process ("fatal runtime
// error: IO Safety violation"); the guard must keep the abort machinery out
// of the picture regardless of TLS destructor ordering.
#[cfg(pgrust_sim)]
#[test]
fn thread_exit_with_live_vfs_fd_holders_does_not_abort_process() {
    setup();
    let joined = std::thread::spawn(|| {
        setup(); // fresh TLS in this thread: its own FdState + sim universe
        vfs_mkdir_p("f1b_thread");
        vfs_write_file("f1b_thread/t", b"x");
        let tfd = crate::desc::OpenTransientFile("f1b_thread/t", libc::O_RDWR).unwrap();
        assert!(tfd >= vfs::sim::SIM_FD_BASE);
        let f = open_rw("f1b_thread/v");
        assert!(f.0 > 0);
        // P4 fault-injection shape: unwind with both holders live.
        panic!("simulated fault-injection unwind");
    })
    .join();
    assert!(joined.is_err(), "the spawned thread must have panicked");
}

// upstream 6cb307251c5c (18.4): an EINTR from sync_file_range must be
// retried; the gate compared the -1 return value with EINTR, never fired, and
// the hint escalated through data_sync_elevel (PANIC at the GUC default).
#[cfg(all(target_os = "linux", pgrust_sim))]
#[test]
fn flush_data_retries_sync_file_range_on_eintr() {
    use std::sync::Arc;
    use vfs::sim::{FaultDecision, FaultPlan, OpDesc, OpKind, SimVfs};

    struct EintrOnce(Arc<AtomicU32>);
    impl FaultPlan for EintrOnce {
        fn before_op(&mut self, op: &OpDesc<'_>) -> FaultDecision {
            if op.kind == OpKind::FlushRange && self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                FaultDecision::Errno(libc::EINTR)
            } else {
                FaultDecision::Proceed
            }
        }
    }

    setup();
    assert!(init_small::globals::enableFsync(), "witness needs the flush hint enabled");
    let dir = scratch_dir("flush_eintr");
    let path = format!("{dir}/f");
    vfs_write_file(&path, b"payload");
    let fd = vfs::open(&cpath(&path), libc::O_RDWR, 0);
    assert!(fd >= 0, "open: errno {}", vfs::get_errno());

    let flushes = Arc::new(AtomicU32::new(0));
    SimVfs::set_fault_plan(Box::new(EintrOnce(Arc::clone(&flushes))));
    crate::sync::pg_flush_data(fd, 0, 0).unwrap();
    assert_eq!(flushes.load(Ordering::SeqCst), 2, "EINTR must be retried exactly once");
    SimVfs::set_fault_plan(Box::new(vfs::sim::NoFaults));
    assert_eq!(vfs::close(fd), 0);
}

// Test-process-global: resowner seams install once (seam_core forbids
// reinstall); every test that needs an owner goes through here.
fn install_resowner_seams_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        resowner::init_seams();
        ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
    });
}

#[test]
fn buffile_write_seek_read_roundtrip() {
    setup();
    install_resowner_seams_once();
    let owner = resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "buffile-test")
        .unwrap();
    resowner_seams::set_current_resource_owner::call(owner);
    let dir = scratch_dir("buffile");
    let _cwd = enter_datadir(&dir);
    with_fd(|fd| fd.temporary_files_allowed = true);

    let ctx = mcx::MemoryContext::new("buffile-test");
    let mcx = ctx.mcx();
    let mut bf = crate::buffile::BufFileCreateTemp(mcx, false).unwrap();

    // Spans several 8KB buffer loads; per-chunk patterns catch misplaced writes.
    let mut expected = Vec::new();
    for i in 0u32..100 {
        let chunk = vec![(i % 251) as u8; 997];
        bf.write(&chunk).unwrap();
        expected.extend_from_slice(&chunk);
    }
    assert_eq!(bf.tell(), (0, expected.len() as i64));

    assert_eq!(bf.seek(0, 0, crate::buffile::SEEK_SET).unwrap(), 0);
    let mut got = vec![0u8; expected.len()];
    bf.read_exact(&mut got).unwrap();
    assert_eq!(got, expected);

    // EOF: read_maybe_eof returns 0, plain read returns short.
    let mut tail = [0u8; 8];
    assert_eq!(bf.read_maybe_eof(&mut tail, true).unwrap(), 0);

    // Overwrite mid-file through a dirty-buffer backwards seek.
    assert_eq!(bf.seek(0, 10_000, crate::buffile::SEEK_SET).unwrap(), 0);
    bf.write(&[0xAB; 16]).unwrap();
    assert_eq!(bf.seek(0, 9_990, crate::buffile::SEEK_SET).unwrap(), 0);
    let mut window = [0u8; 40];
    bf.read_exact(&mut window).unwrap();
    assert_eq!(&window[10..26], &[0xAB; 16]);
    assert_eq!(&window[..10], &expected[9_990..10_000]);
    assert_eq!(&window[26..], &expected[10_016..10_030]);

    // Relative seek; (1, 0) legally aliases end-of-segment-0; segment 2 is EOF.
    assert_eq!(bf.seek(0, 0, crate::buffile::SEEK_CUR).unwrap(), 0);
    assert_eq!(bf.tell(), (0, 10_030));
    assert_eq!(bf.seek(1, 0, crate::buffile::SEEK_SET).unwrap(), 0);
    assert_eq!(bf.seek(2, 0, crate::buffile::SEEK_SET).unwrap(), -1);

    bf.close().unwrap();
}

#[test]
fn parse_filename_for_nontemp_relation_shapes() {
    use crate::reinit::parse_filename_for_nontemp_relation as parse;
    use types_core::ForkNumber::*;
    assert_eq!(parse("16384"), Some((16384, MAIN_FORKNUM, 0)));
    assert_eq!(parse("16384_init"), Some((16384, INIT_FORKNUM, 0)));
    assert_eq!(parse("16384_fsm"), Some((16384, FSM_FORKNUM, 0)));
    assert_eq!(parse("16384_vm.3"), Some((16384, VISIBILITYMAP_FORKNUM, 3)));
    assert_eq!(parse("16384.2"), Some((16384, MAIN_FORKNUM, 2)));
    // Leading zeroes, zero values, trailing junk, unknown forks all reject.
    assert_eq!(parse("016384"), None);
    assert_eq!(parse("0"), None);
    assert_eq!(parse("16384_"), None);
    assert_eq!(parse("16384_initx"), None);
    assert_eq!(parse("16384.02"), None);
    assert_eq!(parse("16384.2x"), None);
    assert_eq!(parse("t5_16384"), None);
    assert_eq!(parse("pg_filenode.map"), None);
    assert_eq!(parse("99999999999999999999"), None);
}

// Worker FATAL mid-sort ordering (ts-extract grouped-agg, P2): proc_exit's abort
// cleanup frees the spill VFDs, then the ProcExitThread unwind drops the
// Tuplesort, whose tapeset close reaches BufFile::close with a dirty buffer.
// The close must be a no-op, never a write through dead Files.
#[test]
fn buffile_close_after_proc_exit_cleanup_is_inert() {
    setup();
    install_resowner_seams_once();
    let owner = resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "p2s-test")
        .unwrap();
    resowner_seams::set_current_resource_owner::call(owner);
    let dir = scratch_dir("procexitclose");
    let _cwd = enter_datadir(&dir);
    with_fd(|fd| fd.temporary_files_allowed = true);

    let ctx = mcx::MemoryContext::new("procexitclose");
    let mut bf = crate::buffile::BufFileCreateTemp(ctx.mcx(), false).unwrap();
    bf.write(&[0x5A; 4096]).unwrap(); // dirty write buffer, never flushed

    // The abort resowner release closes and frees the temp-file VFDs while
    // the BufFile still references them.
    let files: Vec<::types_storage::File> =
        with_fd(|fd| {
            (1..fd.size_vfd_cache() as i32)
                .filter(|&i| fd.vfd_cache[i as usize].file_name.is_some())
                .map(::types_storage::File)
                .collect()
        });
    assert!(!files.is_empty());
    for f in &files {
        crate::io::FileClose(*f).unwrap();
    }

    ::elog::config::set_proc_exit_inprogress(true);
    let closed = bf.close();
    ::elog::config::set_proc_exit_inprogress(false);
    closed.unwrap();
}

// Double FileClose must not push a slot onto the freelist twice — aliased
// slots hand the same VFD to two files (silent cross-file corruption).
#[test]
fn file_close_is_idempotent_no_freelist_aliasing() {
    setup();
    let dir = scratch_dir("dblclose");
    let _cwd = enter_datadir(&dir);
    with_fd(|fd| fd.temporary_files_allowed = true);

    let f = crate::temp::OpenTemporaryFile(true).unwrap();
    crate::io::FileClose(f).unwrap();
    crate::io::FileClose(f).unwrap();

    let a = crate::temp::OpenTemporaryFile(true).unwrap();
    let b = crate::temp::OpenTemporaryFile(true).unwrap();
    assert_ne!(a.0, b.0, "freelist aliased two live files onto one VFD slot");
    crate::io::FileClose(a).unwrap();
    crate::io::FileClose(b).unwrap();
}

// The motivating stable-slot regression: the old swap_remove registry moved
// the last desc into the freed slot, so freeing the LOWER of two live
// AllocateFile indices left the higher handle aliased to the wrong desc.
#[test]
fn allocated_desc_indices_stable_across_out_of_order_free() {
    setup();
    let dir = scratch_dir("descstable");
    let pa = format!("{dir}/a");
    let pb = format!("{dir}/b");
    std::fs::write(&pa, b"aaa").unwrap();
    std::fs::write(&pb, b"bbb").unwrap();

    let a = crate::desc::AllocateFile(&pa, "r").unwrap();
    let b = crate::desc::AllocateFile(&pb, "r").unwrap();
    assert!(a < b);

    crate::desc::FreeFile(a).unwrap();

    let read = crate::desc::with_allocated_stdio(b, |f| {
        use std::io::Read;
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        s
    })
    .expect("higher handle resolves after freeing the lower");
    assert_eq!(read, "bbb");

    crate::desc::FreeFile(b).unwrap();
    with_fd(|fd| assert!(fd.allocated_descs.is_empty()));
}

// The 100-warehouse OLTP write bank vu64 finding (notes/fdcap-lane.md): many pg_subtrans SLRU
// segment fds held open at once by one backend. A thread left at the
// FD_MINFREE boot default (never handed the postmaster's set_max_safe_fds
// probe) freezes maxAllocatedDescs at FD_MINFREE/3 = 16; with the probed
// value applied, reserveAllocatedDesc scales the cap to max_safe_fds/3 and
// the refusal only fires at the real bound (C fd.c reserveAllocatedDesc).
// Posix-only: opens real scratch files, absent from the empty SimVfs
// namespace (the vfs posix-battery fencing precedent).
#[cfg(not(pgrust_sim))]
#[test]
fn transient_fd_cap_scales_with_max_safe_fds() {
    setup();
    let dir = scratch_dir("fdcap");

    let saved = vfd::max_safe_fds();
    assert_eq!(
        saved,
        ::types_storage::FD_MINFREE,
        "a fresh thread boots at the FD_MINFREE default"
    );

    let path_of = |i: usize| format!("{dir}/seg{i:04}");
    for i in 0..41 {
        std::fs::write(path_of(i), b"x").unwrap();
    }

    let mut open_fds = Vec::new();
    for i in 0..16 {
        let fd = crate::desc::OpenTransientFile(&path_of(i), libc::O_RDONLY).unwrap();
        assert!(fd >= 0, "open {i} failed");
        open_fds.push(fd);
    }

    // 17th simultaneous open on the un-inherited default: the ladder's error.
    let err = crate::desc::OpenTransientFile(&path_of(16), libc::O_RDONLY).unwrap_err();
    assert!(
        err.message()
            .contains("exceeded maxAllocatedDescs (16) while trying to open file"),
        "{err:?}"
    );

    // The postmaster's probed max_safe_fds arrives (launch_backend Inherited):
    // the same open now succeeds and the cap scales to max_safe_fds/3.
    vfd::set_max_safe_fds_value(120);
    for i in 16..40 {
        let fd = crate::desc::OpenTransientFile(&path_of(i), libc::O_RDONLY).unwrap();
        assert!(fd >= 0, "open {i} failed after the cap scaled");
        open_fds.push(fd);
    }

    // The guard still holds at the scaled bound, with C's message.
    let err = crate::desc::OpenTransientFile(&path_of(40), libc::O_RDONLY).unwrap_err();
    assert!(
        err.message()
            .contains("exceeded maxAllocatedDescs (40) while trying to open file"),
        "{err:?}"
    );

    for fd in open_fds {
        assert_eq!(crate::desc::CloseTransientFile(fd), 0);
    }
    with_fd(|fd| assert_eq!(crate::vfd::occupied_descs(fd), 0));
    vfd::set_max_safe_fds_value(saved);
}

// DST P1 inc-4 fence assert: the spill/temp plane (tuplestore, tuplesort,
// sharedtuplestore, sort_storage, spillset, nodehash) had ZERO raw fs sites
// at the P1 census and must stay that way — all of its IO rides the fd File*
// APIs (and therefore the VFS). A hit here means a raw syscall or std::fs
// call crept into a sim-scoped spill path; route it through fd instead.
#[test]
fn dst_p1_spill_crates_have_zero_raw_fs_sites() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
    let spill_crates = [
        "backend/utils/sort/tuplestore",
        "backend/utils/sort/tuplesort",
        "backend/utils/sort/sharedtuplestore",
        "backend/utils/sort/sort_storage",
        "backend/executor/spillset",
        "backend/executor/nodehash",
    ];
    let needles = [
        "std::fs::",
        "libc::open",
        "libc::close",
        "libc::read",
        "libc::write",
        "libc::pread",
        "libc::pwrite",
        "libc::preadv",
        "libc::pwritev",
        "libc::stat",
        "libc::fstat",
        "libc::lstat",
        "libc::unlink",
        "libc::rename",
        "libc::mkdir",
        "libc::rmdir",
        "libc::lseek",
        "libc::ftruncate",
        "libc::truncate",
        "libc::fsync",
        "libc::fdatasync",
        "libc::fallocate",
        "libc::readlink",
        "libc::access",
    ];

    let mut offenders: Vec<String> = Vec::new();
    for krate in spill_crates {
        let src = root.join(krate).join("src");
        assert!(src.is_dir(), "spill-fence census: missing {src:?}");
        let mut stack = vec![src];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read spill crate src") {
                let path = entry.expect("dirent").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                // Prod sites only: test scaffolding may build fixture dirs.
                let p = path.to_string_lossy().into_owned();
                if p.ends_with("/tests.rs") || p.contains("/tests/") {
                    continue;
                }
                let text = std::fs::read_to_string(&path).expect("read source");
                for (lineno, line) in text.lines().enumerate() {
                    let code = line.trim_start();
                    if code.starts_with("//") {
                        continue;
                    }
                    for needle in needles {
                        if code.contains(needle) {
                            offenders.push(format!(
                                "{}:{}: {}",
                                path.display(),
                                lineno + 1,
                                line.trim()
                            ));
                        }
                    }
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "raw fs sites appeared in the fenced spill/temp crates:\n{}",
        offenders.join("\n")
    );
}

// ---------------------------------------------------------------------------
// GL-FDLIMIT-1: the one-process descriptor budget.
//
// C sizes max_safe_fds for ONE backend process. pgrust runs every session as a
// thread in a single process with a single descriptor table, so the same
// arithmetic hands the whole limit to every session at once — which is how a
// 64-connection run exhausted an 8192-descriptor table and killed connection
// setup. These pin the adapted arithmetic.
// ---------------------------------------------------------------------------

// A generous limit must still produce C's answer, to the descriptor: the
// sharing arm may only ever lower the number.
#[test]
fn generous_limit_reproduces_c_arithmetic() {
    let c_answer = 1000i32.min(1000) - ::types_storage::NUM_RESERVED_FDS;
    // 1M descriptors, 300 live children, 120 sessions: max_files_per_process
    // still binds.
    assert_eq!(
        vfd::compute_max_safe_fds(1000, 1000, Some(1_048_576), 300, 120),
        c_answer
    );
    // Unknown ceiling (no getrlimit — the sim/wasm arm) falls back to C's.
    assert_eq!(vfd::compute_max_safe_fds(1000, 1000, None, 300, 120), c_answer);
    // Single-user boot: one thread, whole budget, C's answer.
    assert_eq!(
        vfd::compute_max_safe_fds(1000, 1000, Some(1_048_576), 1, 1),
        c_answer
    );
}

// A tight limit must be divided, not handed out whole: 120 sessions inside
// 16384 descriptors get a share each, and the total stays inside the limit.
#[test]
fn tight_limit_is_shared_between_sessions() {
    let ceiling = 16_384;
    let children = 300;
    let sessions = 120;
    let got = vfd::compute_max_safe_fds(1000, 1000, Some(ceiling), children, sessions);
    let c_answer = 1000 - ::types_storage::NUM_RESERVED_FDS;
    assert!(got < c_answer, "sharing must lower the budget: {got} vs {c_answer}");
    assert!(got >= ::types_storage::FD_MINFREE, "{got} should still be workable");

    // The whole server has to fit: every session filling its cache, plus every
    // live child's setup descriptors, plus the supervisor's.
    let worst_case = (got + ::types_storage::NUM_RESERVED_FDS) as i64 * sessions as i64
        + vfd::PER_SESSION_SETUP_FDS as i64 * children as i64
        + vfd::POSTMASTER_RESERVED_FDS as i64;
    assert!(
        worst_case <= ceiling as i64,
        "budget oversubscribes the descriptor table: {worst_case} > {ceiling}"
    );
}

// The 8192-descriptor case that broke on raw hardware: the budget must come
// out too small to run, so the server refuses at boot instead of dying at
// connection setup (set_max_safe_fds turns this into the FATAL).
#[test]
fn insufficient_limit_reports_below_the_floor() {
    let got = vfd::compute_max_safe_fds(1000, 1000, Some(8192), 300, 120);
    assert!(
        got < ::types_storage::FD_MINFREE,
        "8192 descriptors cannot serve 120 sessions; got {got}"
    );
}

// The probe still bounds the answer: a machine that cannot even hand out
// max_files_per_process descriptors gets the probed number, not the limit.
#[test]
fn probe_still_bounds_the_budget() {
    assert_eq!(
        vfd::compute_max_safe_fds(200, 1000, Some(1_048_576), 4, 2),
        200 - ::types_storage::NUM_RESERVED_FDS
    );
}

// ---------------------------------------------------------------------------
// copydir / clone_file (copydir.c)
// ---------------------------------------------------------------------------

#[test]
fn file_copy_method_boot_default_is_copy() {
    setup();
    assert_eq!(vfd::file_copy_method(), crate::copydir::FILE_COPY_METHOD_COPY);
}

/// Recursively collect `dir`-relative (path, is_dir) entries through the
/// active vfs, sorted for comparison.
#[cfg(not(pgrust_sim))]
fn vfs_tree_entries(dir: &str, prefix: &str, out: &mut Vec<(String, bool)>) {
    let mut names = Vec::new();
    crate::desc::with_allocated_dir(dir, &mut |name| {
        if name != "." && name != ".." {
            names.push(name.to_owned());
        }
        Ok(false)
    })
    .unwrap();
    names.sort();
    for name in names {
        let full = format!("{dir}/{name}");
        let rel = if prefix.is_empty() { name } else { format!("{prefix}/{name}") };
        let mut md = vfs::FileInfo::zeroed();
        assert_eq!(vfs::lstat(&cpath(&full), &mut md), 0, "{full}");
        out.push((rel.clone(), md.is_dir()));
        if md.is_dir() {
            vfs_tree_entries(&full, &rel, out);
        }
    }
}

/// Byte-compare two directory trees through the active vfs.
#[cfg(not(pgrust_sim))]
fn assert_tree_equal(a: &str, b: &str) {
    let mut ea = Vec::new();
    let mut eb = Vec::new();
    vfs_tree_entries(a, "", &mut ea);
    vfs_tree_entries(b, "", &mut eb);
    assert_eq!(ea, eb, "tree shapes differ: {a} vs {b}");
    for (rel, is_dir) in ea {
        if !is_dir {
            assert_eq!(
                vfs_read_file(&format!("{a}/{rel}")),
                vfs_read_file(&format!("{b}/{rel}")),
                "{rel}"
            );
        }
    }
}

/// A source tree with a multi-block binary file past clone_file's 1MiB
/// chunk (so the copy_file_range loop iterates), an empty file, and a
/// subdirectory for the recurse arm.
#[cfg(not(pgrust_sim))]
fn build_copydir_fixture(src: &str) {
    vfs_mkdir_p(src);
    let big: Vec<u8> = (0..(1024 * 1024 + 4096)).map(|i| (i * 31 % 251) as u8).collect();
    vfs_write_file(&format!("{src}/big"), &big);
    vfs_write_file(&format!("{src}/empty"), b"");
    vfs_mkdir_p(&format!("{src}/sub"));
    vfs_write_file(&format!("{src}/sub/inner"), b"inner payload");
}

#[test]
#[cfg(not(pgrust_sim))]
fn copydir_copies_tree_and_refuses_existing_destination() {
    setup();
    let dir = scratch_dir("copydir_copy");
    let src = format!("{dir}/src");
    build_copydir_fixture(&src);

    let saved = vfd::file_copy_method();
    vfd::set_file_copy_method(crate::copydir::FILE_COPY_METHOD_COPY);
    let dst = format!("{dir}/dst");
    crate::copydir::copydir(&src, &dst, true).unwrap();
    assert_tree_equal(&src, &dst);
    // Destination already exists: MakePGDirectory fails (EEXIST) => ERROR.
    assert!(crate::copydir::copydir(&src, &dst, true).is_err());
    vfd::set_file_copy_method(saved);
}

#[test]
#[cfg(not(pgrust_sim))]
fn copydir_clone_matches_copy() {
    setup();
    let dir = scratch_dir("copydir_clone");
    let src = format!("{dir}/src");
    build_copydir_fixture(&src);

    let saved = vfd::file_copy_method();
    vfd::set_file_copy_method(crate::copydir::FILE_COPY_METHOD_CLONE);
    let cloned = format!("{dir}/cloned");
    let before = interrupt_checks();
    crate::copydir::copydir(&src, &cloned, true).unwrap();
    let clone_checks = interrupt_checks() - before;
    assert_tree_equal(&src, &cloned);
    // Destination already exists: MakePGDirectory fails (EEXIST) => ERROR.
    assert!(crate::copydir::copydir(&src, &cloned, true).is_err());

    // The same tree through the copy arm must land byte-identical.
    vfd::set_file_copy_method(crate::copydir::FILE_COPY_METHOD_COPY);
    let copied = format!("{dir}/copied");
    let before = interrupt_checks();
    crate::copydir::copydir(&src, &copied, true).unwrap();
    let copy_checks = interrupt_checks() - before;
    assert_tree_equal(&src, &copied);
    assert_tree_equal(&cloned, &copied);
    // Byte-equality alone cannot tell the arms apart; the interrupt-check
    // cadence can. copy_file checks once per 64KiB chunk of the >1MiB
    // fixture; clone_file checks at most once per 1MiB chunk (Linux) and
    // never in-file (macOS). Equal counts would mean the CLONE dispatch
    // fell through to copy_file.
    assert!(
        clone_checks < copy_checks,
        "clone arm did not run: {clone_checks} interrupt checks vs {copy_checks} for copy"
    );
    vfd::set_file_copy_method(saved);
}

// DST P4 inc-1: crash-recovery property sweep + red battery (sim-only).
#[cfg(pgrust_sim)]
mod crash_sweep;


// ===========================================================================
// audit-18.6 remediation b031 (backend/storage/file): C-parity witnesses.
// ===========================================================================

// The fileset BufFile scaffold shared by the witnesses: resowner seams, an
// owner, a scratch datadir, temp-file access.
fn fileset_scaffold(tag: &'static str) -> (String, std::sync::MutexGuard<'static, ()>) {
    setup();
    install_resowner_seams_once();
    let owner = resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, tag).unwrap();
    resowner_seams::set_current_resource_owner::call(owner);
    let dir = scratch_dir(tag);
    let cwd = enter_datadir(&dir);
    with_fd(|fd| fd.temporary_files_allowed = true);
    (dir, cwd)
}

// The "<tempdir>/pgsql_tmp<pid>.<n>.fileset" directory component of a
// fileset path, so a test can rebuild the set's directory in another
// tablespace.
fn fileset_dir_component(path: &str) -> String {
    path.split('/')
        .find(|c| c.starts_with("pgsql_tmp") && c.ends_with(".fileset"))
        .unwrap_or_else(|| panic!("no fileset component in {path}"))
        .to_string()
}

// fileset.c:83-99 FileSetInit: an empty temp_tablespaces GUC (or an
// InvalidOid entry, which PrepareTempTablespaces stores for the database's
// own tablespace) means MyDatabaseTableSpace — a database living in a
// non-default tablespace keeps its filesets there, never in base/pgsql_tmp.
#[test]
fn fileset_init_places_files_in_the_database_tablespace() {
    setup();
    let saved = init_small::globals::MyDatabaseTableSpace();
    init_small::globals::SetMyDatabaseTableSpace(16385);
    let tblspc_tmp = crate::temp::TempTablespacePath(16385);
    assert!(tblspc_tmp.starts_with("pg_tblspc/16385/"));

    // Empty GUC arm.
    crate::sync::AtEOXact_Files(true).unwrap();
    assert!(!crate::temp::TempTablespacesAreSet());
    let fs = crate::fileset::FileSet::init().unwrap();
    let path = fs.name_path("x");
    assert!(
        path.starts_with(&format!("{tblspc_tmp}/pgsql_tmp")),
        "empty temp_tablespaces: fileset path {path} is not under {tblspc_tmp}"
    );
    drop(fs);

    // InvalidOid-entry arm.
    crate::temp::SetTempTablespaces(&[::types_core::InvalidOid]);
    let fs = crate::fileset::FileSet::init().unwrap();
    let path = fs.name_path("x");
    assert!(
        path.starts_with(&format!("{tblspc_tmp}/pgsql_tmp")),
        "InvalidOid temp tablespace entry: fileset path {path} is not under {tblspc_tmp}"
    );
    drop(fs);

    crate::sync::AtEOXact_Files(true).unwrap();
    init_small::globals::SetMyDatabaseTableSpace(saved);
}

// fileset.c:186 ChooseTablespace hashes each FILE name with hash_any; a
// BufFile's segments are distinct names ("<name>.<n>"), so with two temp
// tablespaces the segments of one BufFile spread across both — exactly
// where C puts them.
#[cfg(not(pgrust_sim))]
#[test]
fn fileset_segments_spread_across_tablespaces_by_segment_name_hash() {
    let (_dir, _cwd) = fileset_scaffold("fsspread");
    let spaces: [::types_core::Oid; 2] = [1663, 16385];
    vfs_mkdir_p(&crate::temp::TempTablespacePath(16385));
    crate::temp::SetTempTablespaces(&spaces);
    let fs = crate::fileset::FileSet::init().unwrap();

    // A name whose segments 0 and 1 hash to different tablespaces.
    let pick = |n: &str| (hashfn::hash_bytes(n.as_bytes()) as usize) % 2;
    let name = (b'a'..=b'z')
        .map(|c| (c as char).to_string())
        .find(|n| pick(&format!("{n}.0")) != pick(&format!("{n}.1")))
        .expect("some single-letter name splits its segments");
    let component = fileset_dir_component(&fs.name_path(&name));
    let seg_path = |seg: usize| {
        let ts = spaces[pick(&format!("{name}.{seg}"))];
        format!("{}/{component}/{name}.{seg}", crate::temp::TempTablespacePath(ts))
    };

    let ctx = mcx::MemoryContext::new("fsspread");
    let mut bf = crate::buffile::BufFileCreateFileSet(ctx.mcx(), &fs, &name).unwrap();
    // Straddle the 1GB segment boundary: the flush creates segment 1.
    let boundary = 0x4000_0000;
    assert_eq!(bf.seek(0, boundary - 4, crate::buffile::SEEK_SET).unwrap(), 0);
    bf.write(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
    bf.close().unwrap();

    assert!(vfs_path_exists(&seg_path(0)), "segment 0 missing at {}", seg_path(0));
    assert!(vfs_path_exists(&seg_path(1)), "segment 1 missing at {}", seg_path(1));
    crate::sync::AtEOXact_Files(true).unwrap();
}

// buffile.c:231-256 MakeNewFileSetSegment: creating segment N unlinks a
// leftover segment N+1 (crash-restart debris with the same name) so that
// BufFileOpenFileSet does not count it as part of the new file.
#[test]
fn fileset_segment_create_unlinks_stale_next_segment() {
    let (_dir, _cwd) = fileset_scaffold("fsstale");
    let fs = crate::fileset::FileSet::init().unwrap();
    let base = fs.name_path("s");
    let stale = format!("{base}.1");
    let dir = base.rsplit_once('/').unwrap().0.to_string();
    vfs_mkdir_p(&dir);
    vfs_write_file(&stale, b"debris");
    assert!(vfs_path_exists(&stale));

    let ctx = mcx::MemoryContext::new("fsstale");
    let bf = crate::buffile::BufFileCreateFileSet(ctx.mcx(), &fs, "s").unwrap();
    assert!(
        !vfs_path_exists(&stale),
        "stale segment {stale} survived BufFileCreateFileSet"
    );
    bf.close().unwrap();
    crate::sync::AtEOXact_Files(true).unwrap();
}

// buffile.c:346-349: `could not open temporary file "<name>.0" from BufFile
// "<name>": %m` — the probed segment name and the BufFile name, not a path.
#[test]
fn buffile_open_fileset_missing_names_segment_and_buffile() {
    let (_dir, _cwd) = fileset_scaffold("fsopen");
    let fs = crate::fileset::FileSet::init().unwrap();
    let ctx = mcx::MemoryContext::new("fsopen");
    let err = match crate::buffile::BufFileOpenFileSet(ctx.mcx(), &fs, "nope", true) {
        Err(e) => e,
        Ok(_) => panic!("opened a BufFile that does not exist"),
    };
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_UNDEFINED_FILE);
    assert!(
        err.message()
            .starts_with("could not open temporary file \"nope.0\" from BufFile \"nope\": "),
        "message={}",
        err.message()
    );
}

// buffile.c:631-639: a short exact read on a fileset BufFile names the file
// set: `could not read from file set "<name>": read only %zu of %zu bytes`.
#[test]
fn buffile_short_read_names_the_file_set() {
    let (_dir, _cwd) = fileset_scaffold("fsshort");
    let fs = crate::fileset::FileSet::init().unwrap();
    let ctx = mcx::MemoryContext::new("fsshort");
    let mut bf = crate::buffile::BufFileCreateFileSet(ctx.mcx(), &fs, "r").unwrap();
    bf.write(&[7u8; 10]).unwrap();
    assert_eq!(bf.seek(0, 0, crate::buffile::SEEK_SET).unwrap(), 0);
    let mut got = [0u8; 20];
    let err = bf.read_exact(&mut got).unwrap_err();
    assert_eq!(
        err.message(),
        "could not read from file set \"r\": read only 10 of 20 bytes"
    );
    bf.close().unwrap();
    crate::sync::AtEOXact_Files(true).unwrap();
}

// buffile.c:379-393 BufFileDeleteFileSet: FileSetDelete(..., true) — an
// unlink failure on an EXISTING segment is an ERROR (`could not unlink
// temporary file "%s": %m`), not a LOG followed by "unknown BufFile".
#[test]
fn buffile_delete_fileset_errors_on_unlink_failure() {
    let (_dir, _cwd) = fileset_scaffold("fsdel");
    let fs = crate::fileset::FileSet::init().unwrap();
    // A directory where segment 0 should be: stat succeeds, unlink fails.
    let seg0 = format!("{}.0", fs.name_path("d"));
    vfs_mkdir_p(&seg0);
    let err = crate::buffile::BufFileDeleteFileSet(&fs, "d", false).unwrap_err();
    assert!(
        err.message().starts_with("could not unlink temporary file \""),
        "message={}",
        err.message()
    );
    assert_ne!(err.sqlstate(), ::types_error::ERRCODE_INTERNAL_ERROR);
    let _ = vfs::rmdir(&cpath(&seg0));
}

// buffile.c:779 default: elog(ERROR, "invalid whence: %d") — an ERROR, not
// a thread panic.
#[test]
fn buffile_seek_invalid_whence_is_elog_error() {
    let (_dir, _cwd) = fileset_scaffold("bfwhence");
    let ctx = mcx::MemoryContext::new("bfwhence");
    let mut bf = crate::buffile::BufFileCreateTemp(ctx.mcx(), false).unwrap();
    let err = bf.seek(0, 0, 7).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), "invalid whence: 7");
    bf.close().unwrap();
}

// buffile.c:761: SEEK_CUR's off_t add wraps (-fwrapv) and the wrapped value
// fails the range checks as EOF; no overflow trap, position unchanged.
#[test]
fn buffile_seek_cur_huge_offset_is_eof_not_overflow() {
    let (_dir, _cwd) = fileset_scaffold("bfwrap");
    let ctx = mcx::MemoryContext::new("bfwrap");
    let mut bf = crate::buffile::BufFileCreateTemp(ctx.mcx(), false).unwrap();
    bf.write(&[1u8; 16]).unwrap();
    assert_eq!(bf.seek(0, i64::MAX, crate::buffile::SEEK_CUR).unwrap(), -1);
    assert_eq!(bf.tell(), (0, 16));
    bf.close().unwrap();
}

// buffile.c:951-957 BufFileTruncateFileSet: a segment that cannot be deleted
// reports `could not delete fileset "<name>.<n>": %m` — segment name, errno.
#[cfg(not(pgrust_sim))]
#[test]
fn buffile_truncate_fileset_missing_segment_message() {
    let (_dir, _cwd) = fileset_scaffold("fstrunc");
    let fs = crate::fileset::FileSet::init().unwrap();
    let ctx = mcx::MemoryContext::new("fstrunc");
    let mut bf = crate::buffile::BufFileCreateFileSet(ctx.mcx(), &fs, "u").unwrap();
    let boundary = 0x4000_0000;
    assert_eq!(bf.seek(0, boundary - 4, crate::buffile::SEEK_SET).unwrap(), 0);
    bf.write(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
    // Flush: segment 1 now exists on disk.
    assert_eq!(bf.seek(0, 0, crate::buffile::SEEK_SET).unwrap(), 0);
    let seg1 = format!("{}.1", fs.name_path("u"));
    assert!(vfs_path_exists(&seg1));
    assert_eq!(vfs::unlink(&cpath(&seg1)), 0);

    let err = bf.truncate_fileset(1, 0).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_UNDEFINED_FILE);
    assert_eq!(
        err.message(),
        "could not delete fileset \"u.1\": No such file or directory"
    );
    crate::sync::AtEOXact_Files(true).unwrap();
}

// fd.c:4007-4045 check_debug_io_direct: a rejected value goes through
// GUC_check_errdetail and `return false` — guc.c then raises
// `invalid value for parameter "debug_io_direct": "<val>"` (22023) with the
// DETAIL — instead of surfacing the detail text as an XX000 error.
thread_local! {
    static GUC_DETAILS: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn install_guc_check_errdetail_recorder_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        guc_seams::guc_check_errdetail::set(|d| GUC_DETAILS.with(|v| v.borrow_mut().push(d)));
    });
}

#[test]
fn check_debug_io_direct_rejects_through_guc_check_errdetail() {
    setup();
    install_guc_check_errdetail_recorder_once();
    let hook = guc_tables::hooks::check_debug_io_direct.get();
    let mut extra = None;
    for (val, detail) in [
        ("bogus", "Invalid option \"bogus\"."),
        ("data,,wal", "Invalid list syntax in parameter \"debug_io_direct\"."),
    ] {
        GUC_DETAILS.with(|v| v.borrow_mut().clear());
        let mut nv = Some(val.to_string());
        let r = hook(&mut nv, &mut extra, types_guc::GucSource::PGC_S_FILE);
        assert!(
            matches!(r, Ok(false)),
            "{val}: check hook must return Ok(false) via GUC_check_errdetail, got {r:?}"
        );
        assert_eq!(GUC_DETAILS.with(|v| v.borrow().clone()), vec![detail.to_string()], "{val}");
    }
    let mut nv = Some("data".to_string());
    assert!(matches!(hook(&mut nv, &mut extra, types_guc::GucSource::PGC_S_FILE), Ok(true)));
}

// buffile.c:463/545: FileRead/FileWrite carry WAIT_EVENT_BUFFILE_READ /
// WAIT_EVENT_BUFFILE_WRITE (pg_stat_activity.wait_event BufFileRead/Write).
#[test]
fn buffile_io_reports_buffile_wait_events() {
    let (_dir, _cwd) = fileset_scaffold("bfwait");
    let ctx = mcx::MemoryContext::new("bfwait");
    let mut bf = crate::buffile::BufFileCreateTemp(ctx.mcx(), false).unwrap();
    take_wait_events();
    bf.write(&[3u8; 8192 * 2]).unwrap();
    assert_eq!(bf.seek(0, 0, crate::buffile::SEEK_SET).unwrap(), 0);
    let mut got = [0u8; 100];
    bf.read_exact(&mut got).unwrap();
    let events = take_wait_events();
    assert!(events.contains(&WE_BUFFILE_WRITE), "no BufFileWrite wait event in {events:?}");
    assert!(events.contains(&WE_BUFFILE_READ), "no BufFileRead wait event in {events:?}");
    bf.close().unwrap();
}

// copydir.c:195/205: copy_file reports WAIT_EVENT_COPY_FILE_READ around the
// read and WAIT_EVENT_COPY_FILE_WRITE around the write.
#[cfg(not(pgrust_sim))]
#[test]
fn copy_file_reports_copy_file_wait_events() {
    setup();
    let dir = scratch_dir("cpwait");
    vfs_write_file(&format!("{dir}/src"), &[9u8; 70_000]);
    take_wait_events();
    crate::copydir::copy_file(&format!("{dir}/src"), &format!("{dir}/dst")).unwrap();
    let events = take_wait_events();
    assert!(events.contains(&WE_COPY_FILE_READ), "no CopyFileRead wait event in {events:?}");
    assert!(events.contains(&WE_COPY_FILE_WRITE), "no CopyFileWrite wait event in {events:?}");
    assert_eq!(vfs_read_file(&format!("{dir}/dst")).len(), 70_000);
}

// fd.c:3746 walkdir -> get_dirent_type(subpath, de, process_symlinks, elevel)
// (common/file_utils.c:547): an entry that cannot be stat'd — a dangling
// symlink looked through — logs `could not stat file "%s": %m` at elevel.
#[cfg(not(pgrust_sim))]
#[test]
fn walkdir_logs_unstatable_entries() {
    setup();
    let dir = scratch_dir("walkstat");
    vfs_write_file(&format!("{dir}/plain"), b"x");
    std::os::unix::fs::symlink("nowhere-to-be-found", format!("{dir}/dangling")).unwrap();
    let prev = elog::set_emit_log_hook(Some(capture_log_line));
    take_log_lines();
    crate::sync::walkdir(&dir, crate::sync::WalkAction::PreSync, true, types_error::LOG).unwrap();
    let lines = take_log_lines();
    elog::set_emit_log_hook(prev);
    let want = format!("could not stat file \"{dir}/dangling\": No such file or directory");
    assert!(
        lines.iter().any(|(lvl, m)| *lvl == types_error::LOG && *m == want),
        "expected LOG {want:?}, got {lines:?}"
    );
}

// reinit.c:56/264/314: DEBUG1 "resetting unlogged relations: cleanup %d
// init %d", DEBUG2 "unlinked file \"%s\"", DEBUG2 "copying %s to %s".
#[cfg(not(pgrust_sim))]
#[test]
fn reset_unlogged_relations_emits_debug_lines() {
    setup();
    let dir = scratch_dir("reinitdbg");
    let _cwd = enter_datadir(&dir);
    vfs_mkdir_p("base/1");
    vfs_mkdir_p("pg_tblspc");
    vfs_write_file("base/1/12345_init", b"init-fork");
    vfs_write_file("base/1/12345", b"main-fork");
    let saved_min = elog::config::log_min_messages();
    elog::config::set_log_min_messages(types_error::DEBUG2);
    let prev = elog::set_emit_log_hook(Some(capture_log_line));
    take_log_lines();

    crate::reinit::ResetUnloggedRelations(crate::reinit::UNLOGGED_RELATION_CLEANUP).unwrap();
    let cleanup = take_log_lines();
    crate::reinit::ResetUnloggedRelations(crate::reinit::UNLOGGED_RELATION_INIT).unwrap();
    let init = take_log_lines();

    elog::set_emit_log_hook(prev);
    elog::config::set_log_min_messages(saved_min);

    let has = |lines: &[(types_error::ErrorLevel, String)], lvl, m: &str| {
        lines.iter().any(|(l, s)| *l == lvl && s == m)
    };
    assert!(
        has(&cleanup, types_error::DEBUG1, "resetting unlogged relations: cleanup 1 init 0"),
        "{cleanup:?}"
    );
    assert!(has(&cleanup, types_error::DEBUG2, "unlinked file \"base/1/12345\""), "{cleanup:?}");
    assert!(
        has(&init, types_error::DEBUG1, "resetting unlogged relations: cleanup 0 init 1"),
        "{init:?}"
    );
    assert!(
        has(&init, types_error::DEBUG2, "copying base/1/12345_init to base/1/12345"),
        "{init:?}"
    );
    assert_eq!(vfs_read_file("base/1/12345"), b"init-fork");
}

// ereport_startup_progress (postmaster/startup.h) in fd.c:3794/3826 and
// reinit.c:145/148: once the startup-progress timer fires, the phase logs
// `<phase>, elapsed time: %ld.%02d s, current path: %s`.
thread_local! {
    static STARTUP_PROGRESS_ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn install_startup_progress_seam_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        startup_seams::has_startup_progress_timeout_expired::set(|| {
            if STARTUP_PROGRESS_ARMED.with(std::cell::Cell::get) {
                Some((1, 234_567))
            } else {
                None
            }
        });
    });
}

#[cfg(not(pgrust_sim))]
#[test]
fn startup_progress_lines_during_datadir_sync_and_unlogged_reset() {
    setup();
    install_startup_progress_seam_once();
    let dir = scratch_dir("startprog");
    let _cwd = enter_datadir(&dir);
    vfs_mkdir_p("base/1");
    vfs_mkdir_p("pg_tblspc");
    vfs_write_file("base/1/777_init", b"i");
    vfs_mkdir_p("syncme");
    vfs_write_file("syncme/f", b"y");
    let prev = elog::set_emit_log_hook(Some(capture_log_line));
    STARTUP_PROGRESS_ARMED.with(|c| c.set(true));
    take_log_lines();

    crate::sync::walkdir("syncme", crate::sync::WalkAction::PreSync, false, types_error::LOG).unwrap();
    crate::sync::walkdir("syncme", crate::sync::WalkAction::DatadirFsync, false, types_error::LOG).unwrap();
    crate::reinit::ResetUnloggedRelations(crate::reinit::UNLOGGED_RELATION_INIT).unwrap();
    let lines = take_log_lines();

    STARTUP_PROGRESS_ARMED.with(|c| c.set(false));
    elog::set_emit_log_hook(prev);

    for want in [
        "syncing data directory (pre-fsync), elapsed time: 1.23 s, current path: syncme/f",
        "syncing data directory (fsync), elapsed time: 1.23 s, current path: syncme/f",
        "resetting unlogged relations (init), elapsed time: 1.23 s, current path: base/1",
    ] {
        assert!(
            lines.iter().any(|(lvl, m)| *lvl == types_error::LOG && m == want),
            "expected LOG {want:?}, got {lines:?}"
        );
    }
}

#[test]
fn buffile_segment_rollover_keeps_creation_owner() {
    setup();
    install_resowner_seams_once();
    let saved = resowner::CurrentResourceOwner();
    let owner = resowner::ResourceOwnerCreate(saved, "buffile-owner").unwrap();
    resowner::SetCurrentResourceOwner(owner);
    let dir = scratch_dir("buffile-owner");
    let _cwd = enter_datadir(&dir);
    with_fd(|fd| fd.temporary_files_allowed = true);
    let ctx = mcx::MemoryContext::new("buffile-owner");
    let mut bf = crate::buffile::BufFileCreateTemp(ctx.mcx(), false).unwrap();
    let child = resowner::ResourceOwnerCreate(owner, "buffile-child").unwrap();
    resowner::SetCurrentResourceOwner(child);
    let boundary = 0x4000_0000;
    assert_eq!(bf.seek(0, boundary - 4, crate::buffile::SEEK_SET).unwrap(), 0);
    bf.write(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
    assert_eq!(bf.seek(0, boundary - 4, crate::buffile::SEEK_SET).unwrap(), 0);
    assert_eq!(resowner::CurrentResourceOwner(), child);
    for phase in [
        types_resowner::RESOURCE_RELEASE_BEFORE_LOCKS,
        types_resowner::RESOURCE_RELEASE_LOCKS,
        types_resowner::RESOURCE_RELEASE_AFTER_LOCKS,
    ] {
        resowner::ResourceOwnerRelease(child, phase, false, false).unwrap();
    }
    resowner::SetCurrentResourceOwner(owner);
    resowner::ResourceOwnerDelete(child);
    let mut bytes = [0; 8];
    bf.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, [1, 2, 3, 4, 5, 6, 7, 8]);
    bf.close().unwrap();
    resowner::SetCurrentResourceOwner(saved);
    resowner::ResourceOwnerDelete(owner);
}

// rmtree.c:63: an unopenable directory logs WARNING "could not open
// directory" and reports false (nothing was silently skipped).
#[cfg(not(pgrust_sim))]
#[test]
fn rmtree_warns_on_unopenable_directory() {
    use std::os::unix::fs::PermissionsExt;
    setup();
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let dir = scratch_dir("rmtree_unopenable");
    let sub = format!("{dir}/locked");
    std::fs::create_dir(&sub).unwrap();
    vfs_write_file(&format!("{sub}/inner"), b"x");
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o000)).unwrap();
    let prev = elog::set_emit_log_hook(Some(capture_log_line));
    take_log_lines();
    let ok = crate::copydir::rmtree(&sub, true);
    let lines = take_log_lines();
    elog::set_emit_log_hook(prev);
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(ok.unwrap(), false);
    let want = format!("could not open directory \"{sub}\": Permission denied");
    assert!(
        lines.iter().any(|(lvl, m)| *lvl == types_error::WARNING && *m == want),
        "expected WARNING {want:?}, got {lines:?}"
    );
    assert!(std::path::Path::new(&format!("{sub}/inner")).exists());

    assert!(crate::copydir::rmtree(&dir, true).unwrap());
    assert!(!std::path::Path::new(&dir).exists());
}
