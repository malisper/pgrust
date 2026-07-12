use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::Once;

use ::types_storage::File;

use crate::vfd::{self, with_fd};

static SETUP: Once = Once::new();
static WAL_SYNC_METHOD: AtomicI32 = AtomicI32::new(0);
// Serializes the tests that chdir into a scratch data directory.
static CWD: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn enter_datadir(dir: &str) -> std::sync::MutexGuard<'static, ()> {
    let guard = CWD.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    std::fs::create_dir_all(format!("{dir}/base/pgsql_tmp")).unwrap();
    std::env::set_current_dir(dir).unwrap();
    guard
}

fn setup() {
    SETUP.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        crate::init_seams();

        xact_seams::get_current_sub_transaction_id::set(|| 1);
        aio_seams::pgaio_closing_fd::set(|_| {});
        aio_seams::pgaio_io_start_readv::set(|_, _, _| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pgstat_seams::pgstat_report_tempfile::set(|_| {});
        guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
            get: || WAL_SYNC_METHOD.load(Ordering::Relaxed),
            set: |v| WAL_SYNC_METHOD.store(v, Ordering::Relaxed),
        });
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
    std::fs::create_dir_all(&dir).unwrap();
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
    assert!(std::path::Path::new(&path).exists());
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
    assert!(std::path::Path::new(&path).exists());

    crate::io::FileClose(f).unwrap();
    assert!(!std::path::Path::new(&path).exists());
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
    std::fs::write(&path, b"x").unwrap();

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
    std::fs::write(&old, b"payload").unwrap();
    std::fs::write(&new, b"stale").unwrap();

    assert_eq!(crate::sync::durable_rename(&old, &new, ::types_error::LOG).unwrap(), 0);
    assert!(!std::path::Path::new(&old).exists());
    assert_eq!(std::fs::read(&new).unwrap(), b"payload");

    assert_eq!(crate::sync::durable_unlink(&new, ::types_error::LOG).unwrap(), 0);
    assert!(!std::path::Path::new(&new).exists());

    assert_eq!(crate::sync::durable_unlink(&new, ::types_error::LOG).unwrap(), -1);
}

#[test]
fn allocate_dir_walks_entries() {
    setup();
    let dir = scratch_dir("dirwalk");
    for name in ["alpha", "beta", "gamma"] {
        std::fs::write(format!("{dir}/{name}"), b"").unwrap();
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
    std::fs::write(&path, b"x").unwrap();

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
    std::fs::write(format!("{dir}/pgsql_tmp123.0"), b"x").unwrap();
    std::fs::create_dir(format!("{dir}/pgsql_tmp_sub")).unwrap();
    std::fs::write(format!("{dir}/pgsql_tmp_sub/anything"), b"x").unwrap();
    std::fs::write(format!("{dir}/keepme"), b"x").unwrap();

    crate::sync::RemovePgTempFilesInDir(&dir, false, false).unwrap();

    assert!(!std::path::Path::new(&format!("{dir}/pgsql_tmp123.0")).exists());
    assert!(!std::path::Path::new(&format!("{dir}/pgsql_tmp_sub")).exists());
    assert!(std::path::Path::new(&format!("{dir}/keepme")).exists());

    crate::sync::RemovePgTempFilesInDir(&format!("{dir}/absent"), true, false).unwrap();
}

#[test]
fn check_debug_io_direct_parses_flag_list() {
    setup();
    use ::types_storage::{IO_DIRECT_DATA, IO_DIRECT_WAL, IO_DIRECT_WAL_INIT};
    assert_eq!(vfd::check_debug_io_direct("").unwrap(), 0);
    assert_eq!(vfd::check_debug_io_direct("data").unwrap(), IO_DIRECT_DATA);
    assert_eq!(
        vfd::check_debug_io_direct("data, WAL, wal_init").unwrap(),
        IO_DIRECT_DATA | IO_DIRECT_WAL | IO_DIRECT_WAL_INIT
    );
    let err = vfd::check_debug_io_direct("bogus").unwrap_err();
    assert!(err.message().contains("Invalid option \"bogus\"."));
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

// Worker FATAL mid-sort ordering (ClickBench Q19 P2): proc_exit's abort
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
