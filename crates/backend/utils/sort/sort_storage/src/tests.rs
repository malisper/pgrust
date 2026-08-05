use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Once;

use ::mcx::{Mcx, MemoryContext};

use crate::LogicalTapeSet;

static SETUP: Once = Once::new();
static WAL_SYNC_METHOD: AtomicI32 = AtomicI32::new(0);
static CWD: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn enter_datadir(tag: &str) -> std::sync::MutexGuard<'static, ()> {
    let guard = CWD.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = format!(
        "{}/pgrust-sortstorage-{}-{}",
        std::env::temp_dir().display(),
        std::process::id(),
        tag
    );
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(format!("{dir}/base/pgsql_tmp")).unwrap();
    std::env::set_current_dir(&dir).unwrap();
    guard
}

fn setup() {
    SETUP.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        aio_seams::pgaio_closing_fd::set(|_| {});
        aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pgstat_seams::pgstat_report_tempfile::set(|_| {});
        ipc_seams::before_shmem_exit::set(|_, _| Ok(()));
        ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
        resowner::init_seams();
        guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
            get: || WAL_SYNC_METHOD.load(Ordering::Relaxed),
            set: |v| WAL_SYNC_METHOD.store(v, Ordering::Relaxed),
        });
    });
    fd::InitFileAccess();
    let _ = fd::InitTemporaryFileAccess();
    if resowner_seams::current_resource_owner::call().is_null() {
        let owner =
            resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "logtape-test")
                .unwrap();
        resowner_seams::set_current_resource_owner::call(owner);
    }
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("logtape-test")));
    m.mcx()
}

fn record(i: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    v[..8].copy_from_slice(&i.to_ne_bytes());
    for (j, b) in v.iter_mut().enumerate().skip(8) {
        *b = (i as u8).wrapping_add(j as u8);
    }
    v
}

#[test]
fn write_read_roundtrip_multiblock() {
    setup();
    let _cwd = enter_datadir("roundtrip");
    let mcx = leaked_mcx();
    let mut lts = LogicalTapeSet::create(mcx, false).unwrap();

    let ntapes = 4usize;
    let per_tape = 600usize; // ~600 * 96B ≈ 7 blocks per tape
    let tapes: Vec<_> = (0..ntapes).map(|_| lts.create_tape()).collect();
    for (t, &tape) in tapes.iter().enumerate() {
        for i in 0..per_tape {
            let rec = record((t * per_tape + i) as u64, 96);
            lts.write(tape, &rec).unwrap();
        }
    }
    for &tape in &tapes {
        lts.rewind_for_read(tape, 3 * 8192).unwrap();
    }
    for (t, &tape) in tapes.iter().enumerate() {
        for i in 0..per_tape {
            let mut buf = [0u8; 96];
            assert_eq!(lts.read(tape, &mut buf).unwrap(), 96);
            assert_eq!(buf, record((t * per_tape + i) as u64, 96).as_slice());
        }
        let mut buf = [0u8; 1];
        assert_eq!(lts.read(tape, &mut buf).unwrap(), 0, "EOF expected");
    }
    let blocks = lts.blocks();
    assert!(blocks >= 4, "expected multi-block file, got {blocks}");
    lts.close().unwrap();
}

#[test]
fn destructive_read_recycles_blocks() {
    setup();
    let _cwd = enter_datadir("recycle");
    let mcx = leaked_mcx();
    let mut lts = LogicalTapeSet::create(mcx, false).unwrap();

    let t1 = lts.create_tape();
    for i in 0..2000u64 {
        lts.write(t1, &record(i, 64)).unwrap();
    }
    lts.rewind_for_read(t1, 8192).unwrap();
    let blocks_after_t1 = lts.blocks();
    let mut buf = [0u8; 64];
    for i in 0..2000u64 {
        assert_eq!(lts.read(t1, &mut buf).unwrap(), 64);
        assert_eq!(buf, record(i, 64).as_slice());
    }
    lts.close_tape(t1);

    // A second same-sized tape reuses the freed blocks: file barely grows.
    let t2 = lts.create_tape();
    for i in 0..2000u64 {
        lts.write(t2, &record(i, 64)).unwrap();
    }
    lts.rewind_for_read(t2, 8192).unwrap();
    let blocks_after_t2 = lts.blocks();
    assert!(
        blocks_after_t2 <= blocks_after_t1 + 1,
        "no recycling: {blocks_after_t1} -> {blocks_after_t2}"
    );
    for i in 0..2000u64 {
        assert_eq!(lts.read(t2, &mut buf).unwrap(), 64);
        assert_eq!(buf, record(i, 64).as_slice());
    }
    lts.close().unwrap();
}

#[test]
fn freeze_backspace_seek_tell() {
    setup();
    let _cwd = enter_datadir("freeze");
    let mcx = leaked_mcx();
    let mut lts = LogicalTapeSet::create(mcx, false).unwrap();

    let t = lts.create_tape();
    for i in 0..1500u64 {
        lts.write(t, &record(i, 72)).unwrap();
    }
    lts.freeze(t).unwrap();

    let mut buf = [0u8; 72];
    for i in 0..700u64 {
        assert_eq!(lts.read(t, &mut buf).unwrap(), 72);
        assert_eq!(buf, record(i, 72).as_slice());
    }
    let (blk, off) = lts.tell(t).unwrap();

    // Backspace over the last record and re-read it.
    assert_eq!(lts.backspace(t, 72).unwrap(), 72);
    assert_eq!(lts.read(t, &mut buf).unwrap(), 72);
    assert_eq!(buf, record(699, 72).as_slice());

    // Continue to EOF, then seek back to the told position.
    for i in 700..1500u64 {
        assert_eq!(lts.read(t, &mut buf).unwrap(), 72);
        assert_eq!(buf, record(i, 72).as_slice());
    }
    assert_eq!(lts.read(t, &mut buf).unwrap(), 0);

    lts.seek(t, blk, off).unwrap();
    assert_eq!(lts.read(t, &mut buf).unwrap(), 72);
    assert_eq!(buf, record(700, 72).as_slice());

    // Backspace beyond start clamps to the beginning.
    let moved = lts.backspace(t, usize::MAX / 2).unwrap();
    assert!(moved < usize::MAX / 2);
    assert_eq!(lts.read(t, &mut buf).unwrap(), 72);
    assert_eq!(buf, record(0, 72).as_slice());

    // Frozen rewind-for-read re-reads from the start.
    lts.rewind_for_read(t, 0).unwrap();
    assert_eq!(lts.read(t, &mut buf).unwrap(), 72);
    assert_eq!(buf, record(0, 72).as_slice());
    lts.close().unwrap();
}

#[test]
fn prealloc_tapes_interleaved() {
    setup();
    let _cwd = enter_datadir("prealloc");
    let mcx = leaked_mcx();
    let mut lts = LogicalTapeSet::create(mcx, true).unwrap();

    let a = lts.create_tape();
    let b = lts.create_tape();
    for i in 0..1200u64 {
        lts.write(a, &record(i, 80)).unwrap();
        lts.write(b, &record(i + 1_000_000, 80)).unwrap();
    }
    lts.rewind_for_read(a, 8192).unwrap();
    lts.rewind_for_read(b, 8192).unwrap();
    let mut buf = [0u8; 80];
    for i in 0..1200u64 {
        assert_eq!(lts.read(a, &mut buf).unwrap(), 80);
        assert_eq!(buf, record(i, 80).as_slice());
        assert_eq!(lts.read(b, &mut buf).unwrap(), 80);
        assert_eq!(buf, record(i + 1_000_000, 80).as_slice());
    }
    lts.close().unwrap();
}
