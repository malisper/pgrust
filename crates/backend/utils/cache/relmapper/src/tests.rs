use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, Once, OnceLock};

use types_storage::SharedInvalidationMessage;

use super::*;

static SETUP: Once = Once::new();
thread_local! {
    static NEST_LEVEL: std::cell::Cell<i32> = const { std::cell::Cell::new(1) };
}

fn wal_log() -> &'static Mutex<Vec<(u8, u8, Vec<u8>)>> {
    static L: OnceLock<Mutex<Vec<(u8, u8, Vec<u8>)>>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(Vec::new()))
}

fn sinval_log() -> &'static Mutex<Vec<SharedInvalidationMessage>> {
    static L: OnceLock<Mutex<Vec<SharedInvalidationMessage>>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(Vec::new()))
}

fn preserve_log() -> &'static Mutex<Vec<RelFileLocator>> {
    static L: OnceLock<Mutex<Vec<RelFileLocator>>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(Vec::new()))
}

fn redo_dir() -> &'static Mutex<String> {
    static L: OnceLock<Mutex<String>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(String::new()))
}

fn setup() {
    SETUP.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });
        lwlock::CreateLWLocks(false).unwrap();

        pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
        init_small_seams::my_proc_pid::set(|| 4242);
        ipc_seams::proc_exit::set(|_code, _my_pid| -> ! { panic!("test-proc-exit") });
        static WAL_SYNC_METHOD: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);
        guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
            get: || WAL_SYNC_METHOD.load(Ordering::Relaxed),
            set: |v| WAL_SYNC_METHOD.store(v, Ordering::Relaxed),
        });
        aio_seams::pgaio_closing_fd::set(|_| {});
        aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        xact_seams::get_current_transaction_nest_level::set(|| NEST_LEVEL.with(|c| c.get()));
        xact_seams::is_in_parallel_mode::set(|| false);
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        xloginsert_seams::xlog_insert::set(|rmid, info, fragments| {
            let mut data = Vec::new();
            for f in fragments {
                data.extend_from_slice(f);
            }
            wal_log().lock().unwrap().push((rmid, info, data));
            Ok(0x1000)
        });
        transam_xlog_seams::xlog_flush::set(|_| Ok(()));
        sinval_seams::send_shared_invalid_messages::set(|msgs| {
            sinval_log().lock().unwrap().extend_from_slice(msgs);
            Ok(())
        });
        catalog_storage_seams::relation_preserve_storage::set(|rlocator, _at_commit| {
            preserve_log().lock().unwrap().push(rlocator);
        });
        relpath_seams::get_database_path::set(|mcx, _db, _spc| {
            mcx::PgString::from_str_in(&redo_dir().lock().unwrap(), mcx)
        });
    });
    fd::InitFileAccess();
}

// ---------------------------------------------------------------------------
// Fixture routing through the ACTIVE vfs (the p4-fd-fixtures Class-B routing
// precedent): PosixVfs under the default cfg (identical real-fs behavior to
// the old std::fs fixtures), the SimVfs namespace under --cfg pgrust_sim.
// The four former sim reds here were fixture-domain mismatches — fixtures
// minted on the real fs while relmapper's fd-routed IO ran in the sim
// namespace ("pg_filenode.map.tmp: No such file or directory").
// ---------------------------------------------------------------------------

fn cpath(path: &str) -> std::ffi::CString {
    std::ffi::CString::new(path).unwrap()
}

fn vfs_mkdir_p(path: &str) {
    let mut prefix = String::new();
    for comp in path.split('/') {
        if comp.is_empty() {
            continue;
        }
        prefix.push('/');
        prefix.push_str(comp);
        let rc = vfs::mkdir(&cpath(&prefix), 0o700);
        assert!(
            rc == 0 || vfs::get_errno() == libc::EEXIST,
            "vfs_mkdir_p({prefix}): errno {}",
            vfs::get_errno()
        );
    }
}

fn vfs_write_file(path: &str, data: &[u8]) {
    let fd = vfs::open(&cpath(path), libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC, 0o600);
    assert!(fd >= 0, "vfs_write_file open({path}): errno {}", vfs::get_errno());
    if !data.is_empty() {
        assert_eq!(vfs::pwrite(fd, data, 0), data.len() as isize, "{path}");
    }
    assert_eq!(vfs::close(fd), 0);
}

fn vfs_read_file(path: &str) -> Vec<u8> {
    let fd = vfs::open(&cpath(path), libc::O_RDONLY, 0);
    assert!(fd >= 0, "vfs_read_file open({path}): errno {}", vfs::get_errno());
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    let mut off = 0i64;
    loop {
        let n = vfs::pread(fd, &mut buf, off);
        assert!(n >= 0, "vfs_read_file pread({path}): errno {}", vfs::get_errno());
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n as usize]);
        off += n as i64;
    }
    assert_eq!(vfs::close(fd), 0);
    out
}

fn vfs_remove_file(path: &str) {
    assert_eq!(vfs::unlink(&cpath(path)), 0, "vfs_remove_file({path})");
}

/// Per-test scratch dir, present in BOTH domains: minted on the real fs (the
/// default-cfg vfs) and mirrored into the active vfs namespace (a no-op
/// EEXIST walk under the default cfg; the SimVfs mirror under sim).
fn scratch_dir(tag: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("pgrust_relmapper_{}_{tag}_{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let dir = dir.to_str().unwrap().to_owned();
    vfs_mkdir_p(&dir);
    dir
}

fn sample_map(entries: &[(Oid, RelFileNumber)]) -> RelMapFile {
    let mut map = RelMapFile::EMPTY;
    for &(oid, filenum) in entries {
        apply_map_update(&mut map, oid, filenum, true).unwrap();
    }
    map
}

fn write_map_to(dbpath: &str, map: &mut RelMapFile) {
    lock_map(lwlock::LW_EXCLUSIVE).unwrap();
    let res = write_relmap_file(map, false, false, false, InvalidOid, InvalidOid, dbpath);
    unlock_map().unwrap();
    res.unwrap();
}

#[test]
fn image_layout_matches_c() {
    assert_eq!(SIZEOF_RELMAPFILE, 524);
    assert_eq!(OFFSETOF_CRC, 520);
    let map = sample_map(&[(1259, 16384), (2662, 16385)]);
    let bytes = map.as_bytes();
    assert_eq!(&bytes[4..8], &2i32.to_ne_bytes());
    assert_eq!(&bytes[8..12], &1259u32.to_ne_bytes());
    assert_eq!(&bytes[12..16], &16384u32.to_ne_bytes());
    let back = RelMapFile::from_bytes(bytes);
    assert_eq!(back.num_mappings, 2);
    assert_eq!(back.mappings[1], map.mappings[1]);
}

#[test]
fn lookup_prefers_active_updates() {
    setup();
    RelationMapInitialize();
    with_state(|st| {
        st.local_map = sample_map(&[(1259, 100)]);
        st.local_map.magic = RELMAPPER_FILEMAGIC;
        st.active_local_updates = sample_map(&[(1259, 200)]);
    });
    assert_eq!(RelationMapOidToFilenumber(1259, false), 200);
    assert_eq!(RelationMapFilenumberToOid(100, false), 1259);
    assert_eq!(RelationMapOidToFilenumber(1259, true), InvalidRelFileNumber);
    assert_eq!(RelationMapFilenumberToOid(999, false), InvalidOid);
    RelationMapInitialize();
}

#[test]
fn update_map_routes_pending_and_cci_activates() {
    setup();
    RelationMapInitialize();
    RelationMapUpdateMap(1259, 300, false, false).unwrap();
    assert_eq!(RelationMapOidToFilenumber(1259, false), InvalidRelFileNumber);
    AtCCI_RelationMap().unwrap();
    assert_eq!(RelationMapOidToFilenumber(1259, false), 300);

    RelationMapUpdateMap(2662, 301, true, true).unwrap();
    assert_eq!(RelationMapOidToFilenumber(2662, true), 301);

    AtEOXact_RelationMap(false, false).unwrap();
    assert_eq!(RelationMapOidToFilenumber(1259, false), InvalidRelFileNumber);
    assert_eq!(RelationMapOidToFilenumber(2662, true), InvalidRelFileNumber);
}

#[test]
fn update_map_rejects_subxact() {
    setup();
    NEST_LEVEL.with(|c| c.set(2));
    let err = RelationMapUpdateMap(1, 2, false, true).unwrap_err();
    assert!(err
        .to_string()
        .contains("cannot change relation mapping within subtransaction"));
    NEST_LEVEL.with(|c| c.set(1));
}

#[test]
fn remove_mapping_collapses_entry() {
    setup();
    RelationMapInitialize();
    RelationMapUpdateMap(10, 11, false, true).unwrap();
    RelationMapUpdateMap(20, 21, false, true).unwrap();
    RelationMapRemoveMapping(10).unwrap();
    assert_eq!(RelationMapOidToFilenumber(10, false), InvalidRelFileNumber);
    assert_eq!(RelationMapOidToFilenumber(20, false), 21);
    let err = RelationMapRemoveMapping(10).unwrap_err();
    assert!(err.to_string().contains("could not find temporary mapping"));
    RelationMapInitialize();
}

#[test]
fn apply_map_update_bounds() {
    let mut map = RelMapFile::EMPTY;
    for i in 0..MAX_MAPPINGS {
        apply_map_update(&mut map, i as Oid + 1, 1, true).unwrap();
    }
    let err = apply_map_update(&mut map, 9999, 1, true).unwrap_err();
    assert!(err.to_string().contains("ran out of space"));
    let err = apply_map_update(&mut RelMapFile::EMPTY, 5, 6, false).unwrap_err();
    assert!(err.to_string().contains("unmapped relation 5"));
}

#[test]
fn at_prepare_rejects_modified_map() {
    setup();
    RelationMapInitialize();
    AtPrepare_RelationMap().unwrap();
    RelationMapUpdateMap(30, 31, false, true).unwrap();
    let err = AtPrepare_RelationMap().unwrap_err();
    assert!(err.to_string().contains("cannot PREPARE"));
    RelationMapInitialize();
}

#[test]
fn write_read_roundtrip_and_corruption() {
    setup();
    let _l = lock_guard();
    let dir = scratch_dir("rw");
    let mut map = sample_map(&[(1259, 16384), (1249, 16385), (2662, 16386)]);
    write_map_to(&dir, &mut map);
    assert_eq!(map.crc, map.compute_crc());

    let mut got = RelMapFile::EMPTY;
    read_relmap_file(&mut got, &dir, false, ERROR).unwrap();
    assert_eq!(got.as_bytes(), map.as_bytes());
    assert_eq!(
        RelationMapOidToFilenumberForDatabase(&dir, 1249).unwrap(),
        16385
    );
    assert_eq!(
        RelationMapOidToFilenumberForDatabase(&dir, 7777).unwrap(),
        InvalidRelFileNumber
    );

    let path = format!("{dir}/{RELMAPPER_FILENAME}");
    let mut bytes = vfs_read_file(&path);
    bytes[16] ^= 0xFF;
    vfs_write_file(&path, &bytes);
    let err = read_relmap_file(&mut got, &dir, false, ERROR).unwrap_err();
    assert!(err.to_string().contains("incorrect checksum"));

    bytes[0..4].copy_from_slice(&0i32.to_ne_bytes());
    vfs_write_file(&path, &bytes);
    let err = read_relmap_file(&mut got, &dir, false, ERROR).unwrap_err();
    assert!(err.to_string().contains("contains invalid data"));

    vfs_write_file(&path, &bytes[..100]);
    let err = read_relmap_file(&mut got, &dir, false, ERROR).unwrap_err();
    assert!(err.to_string().contains("read 100 of 524"));

    vfs_remove_file(&path);
    let err = read_relmap_file(&mut got, &dir, false, ERROR).unwrap_err();
    assert!(err.to_string().contains("could not open file"));
}

#[test]
fn missing_file_is_fatal_on_load() {
    setup();
    let _l = lock_guard();
    let dir = scratch_dir("fatal");
    init_small::globals::SetDatabasePath(&dir);
    RelationMapInitialize();
    // FATAL is process-fatal: elog routes it to proc_exit (mocked to panic).
    let died = std::panic::catch_unwind(RelationMapInitializePhase3).is_err();
    assert!(died);
}

#[test]
fn commit_writes_wal_sinval_and_preserve() {
    setup();
    let _l = lock_guard();
    let dir = scratch_dir("commit");
    init_small::globals::SetDatabasePath(&dir);
    init_small::globals::SetMyDatabaseId(5);
    init_small::globals::SetMyDatabaseTableSpace(1663);

    let mut initial = sample_map(&[(1259, 16384)]);
    write_map_to(&dir, &mut initial);
    with_state(|st| st.local_map = initial);

    RelationMapUpdateMap(1259, 90000, false, true).unwrap();
    AtEOXact_RelationMap(true, false).unwrap();

    assert_eq!(RelationMapOidToFilenumber(1259, false), 90000);
    let mut on_disk = RelMapFile::EMPTY;
    read_relmap_file(&mut on_disk, &dir, false, ERROR).unwrap();
    assert_eq!(lookup_oid(&on_disk, 1259), Some(90000));

    {
        let wal = wal_log().lock().unwrap();
        let (rmid, info, data) = wal.last().unwrap();
        assert_eq!((*rmid, *info), (RM_RELMAP_ID, XLOG_RELMAP_UPDATE));
        assert_eq!(data.len(), MIN_SIZE_OF_RELMAP_UPDATE + SIZEOF_RELMAPFILE);
        assert_eq!(&data[0..4], &5u32.to_ne_bytes());
        assert_eq!(&data[4..8], &1663u32.to_ne_bytes());
        assert_eq!(&data[8..12], &524i32.to_ne_bytes());
        assert_eq!(&data[12..], on_disk.as_bytes());
    }

    assert!(sinval_log().lock().unwrap().iter().any(|m| matches!(
        m,
        SharedInvalidationMessage::Relmap(r) if r.dbId == 5
    )));
    assert!(preserve_log()
        .lock()
        .unwrap()
        .iter()
        .any(|rl| *rl == RelFileLocator::new(1663, 5, 90000)));
    assert_eq!(init_small::globals::CritSectionCount(), 0);
    RelationMapInitialize();
}

#[test]
fn redo_writes_map_and_sends_sinval() {
    setup();
    let _l = lock_guard();
    let dir = scratch_dir("redo");
    *redo_dir().lock().unwrap() = dir.clone();

    let map = sample_map(&[(1259, 42)]);
    let mut data = Vec::new();
    data.extend_from_slice(&77u32.to_ne_bytes());
    data.extend_from_slice(&1663u32.to_ne_bytes());
    data.extend_from_slice(&(SIZEOF_RELMAPFILE as i32).to_ne_bytes());
    data.extend_from_slice(map.as_bytes());

    let mut record = XLogReaderState::default();
    let mut decoded = xlogreader_seams::DecodedXLogRecord::default();
    decoded.xl_info = XLOG_RELMAP_UPDATE;
    decoded.main_data = data.as_ptr();
    decoded.main_data_len = data.len() as u32;
    record.record = Some(decoded);

    relmap_redo(&mut record).unwrap();

    let mut got = RelMapFile::EMPTY;
    read_relmap_file(&mut got, &dir, false, ERROR).unwrap();
    assert_eq!(lookup_oid(&got, 1259), Some(42));
    assert!(sinval_log().lock().unwrap().iter().any(|m| matches!(
        m,
        SharedInvalidationMessage::Relmap(r) if r.dbId == 77
    )));
}

#[test]
fn serialize_restore_roundtrip() {
    setup();
    RelationMapInitialize();
    RelationMapUpdateMap(50, 51, false, true).unwrap();
    let ser = SerializeRelationMap();
    let err = RestoreRelationMap(&ser).unwrap_err();
    assert!(err.to_string().contains("existing mappings"));
    RelationMapInitialize();
    RestoreRelationMap(&ser).unwrap();
    assert_eq!(RelationMapOidToFilenumber(50, false), 51);
    assert_eq!(EstimateRelationMapSpace(), 1048);
    RelationMapInitialize();
}

#[test]
fn invalidate_skips_unloaded_maps() {
    setup();
    RelationMapInitialize();
    RelationMapInvalidate(true).unwrap();
    RelationMapInvalidate(false).unwrap();
    RelationMapInvalidateAll().unwrap();
}

#[test]
fn bootstrap_finish_writes_both_maps() {
    setup();
    let _l = lock_guard();
    let root = scratch_dir("boot");
    std::fs::create_dir_all(format!("{root}/global")).unwrap();
    std::fs::create_dir_all(format!("{root}/base_5")).unwrap();
    let _cwd = enter_dir(&root);
    // The vfs domain's mirror of the two relative fixture dirs. Under the
    // default cfg the relative paths resolve in the real cwd entered above
    // (the dirs already exist: EEXIST no-op); under sim there is no cwd —
    // relative paths resolve against the sim root, so this mints "/global"
    // and "/base_5" in the SimVfs namespace (the fd-fixtures enter_datadir
    // seeding shape).
    for d in ["global", "base_5"] {
        let rc = vfs::mkdir(&cpath(d), 0o700);
        assert!(
            rc == 0 || vfs::get_errno() == libc::EEXIST,
            "fixture mkdir({d}): errno {}",
            vfs::get_errno()
        );
    }

    miscinit::SetProcessingMode(types_core::ProcessingMode::BootstrapProcessing);
    init_small::globals::SetDatabasePath("base_5");
    init_small::globals::SetMyDatabaseId(5);
    init_small::globals::SetMyDatabaseTableSpace(1663);

    RelationMapInitialize();
    RelationMapUpdateMap(1259, 16384, false, false).unwrap();
    RelationMapUpdateMap(1262, 16385, true, false).unwrap();
    RelationMapFinishBootstrap().unwrap();
    miscinit::SetProcessingMode(types_core::ProcessingMode::InitProcessing);

    let mut shared = RelMapFile::EMPTY;
    read_relmap_file(&mut shared, "global", false, ERROR).unwrap();
    assert_eq!(lookup_oid(&shared, 1262), Some(16385));
    let mut local = RelMapFile::EMPTY;
    read_relmap_file(&mut local, "base_5", false, ERROR).unwrap();
    assert_eq!(lookup_oid(&local, 1259), Some(16384));
    RelationMapInitialize();
}

static LOCK_TESTS: Mutex<()> = Mutex::new(());

// RelationMappingLock is process-global; without a PGPROC the lwlock cannot
// wait, so lock-taking tests are serialized here.
fn lock_guard() -> std::sync::MutexGuard<'static, ()> {
    LOCK_TESTS.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

static CWD: Mutex<()> = Mutex::new(());

fn enter_dir(dir: &str) -> std::sync::MutexGuard<'static, ()> {
    let guard = CWD.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    std::env::set_current_dir(dir).unwrap();
    guard
}
