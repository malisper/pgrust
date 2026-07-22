use std::sync::atomic::{AtomicU32, Ordering};

use types_core::primitive::{ForkNumber, InvalidBlockNumber, INVALID_PROC_NUMBER};
use types_core::BLCKSZ;
use types_storage::{RelFileLocator, RelFileLocatorBackend};

static SYNC_REQUESTS: AtomicU32 = AtomicU32::new(0);

fn fork_suffix(forknum: ForkNumber) -> &'static str {
    match forknum {
        ForkNumber::MAIN_FORKNUM => "",
        ForkNumber::FSM_FORKNUM => "_fsm",
        ForkNumber::VISIBILITYMAP_FORKNUM => "_vm",
        ForkNumber::INIT_FORKNUM => "_init",
        ForkNumber::InvalidForkNumber => panic!("invalid fork"),
    }
}

fn setup() {
    guc_tables::init_seams();
    elog::init_seams();
    fd::init_seams();
    smgr::init_seams();

    xact_seams::get_current_sub_transaction_id::set(|| 1);
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    pgstat_seams::pgstat_report_tempfile::set(|_| {});

    relpath_seams::relpathbackend::set(|rlocator, _backend, forknum| {
        format!(
            "base/{}/{}{}",
            rlocator.dbOid,
            rlocator.relNumber,
            fork_suffix(forknum)
        )
    });
    sync_seams::register_sync_request::set(|_tag, _ty, _retry| {
        SYNC_REQUESTS.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    });
    tablespace_seams::tablespace_create_dbspace::set(|_, _, _| Ok(()));

    let dir = std::env::temp_dir().join(format!("pgrust_smgr_io_{}", std::process::id()));
    std::fs::create_dir_all(dir.join("base/5")).unwrap();
    std::env::set_current_dir(&dir).unwrap();
    fd::InitFileAccess();
}

#[test]
fn create_extend_write_read_nblocks_roundtrip() {
    setup();
    let key = RelFileLocatorBackend {
        locator: RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: 16384 },
        backend: INVALID_PROC_NUMBER,
    };
    let fork = ForkNumber::MAIN_FORKNUM;

    smgr::smgropen(key.locator, key.backend).unwrap();
    assert!(!smgr::smgrexists(key, fork).unwrap());
    smgr::smgrcreate(key, fork, false).unwrap();
    assert!(smgr::smgrexists(key, fork).unwrap());
    assert_eq!(smgr::smgrnblocks(key, fork).unwrap(), 0);

    let block_a = [0xABu8; BLCKSZ];
    let block_b = [0xCDu8; BLCKSZ];
    smgr::smgrextend(key, fork, 0, &block_a, false).unwrap();
    smgr::smgrextend(key, fork, 1, &block_a, false).unwrap();
    assert_eq!(smgr::smgrnblocks(key, fork).unwrap(), 2);

    smgr::smgrwrite(key, fork, 1, &block_b, false).unwrap();

    let mut readback = [0u8; BLCKSZ];
    smgr::smgrread(key, fork, 0, &mut readback).unwrap();
    assert_eq!(readback, block_a);
    smgr::smgrread(key, fork, 1, &mut readback).unwrap();
    assert_eq!(readback, block_b);

    smgr::smgrzeroextend(key, fork, 2, 3, false).unwrap();
    assert_eq!(smgr::smgrnblocks(key, fork).unwrap(), 5);
    smgr::smgrread(key, fork, 4, &mut readback).unwrap();
    assert_eq!(readback, [0u8; BLCKSZ]);

    assert!(SYNC_REQUESTS.load(Ordering::Relaxed) > 0, "dirty segments must be registered");

    smgr::smgrsettargblock(key, 4);
    assert_eq!(smgr::smgrgettargblock(key), 4);

    // AtEOXact_SMgr destroys the unpinned entry and closes its fds; a fresh
    // open re-derives the size from disk.
    smgr::AtEOXact_SMgr().unwrap();
    assert_eq!(smgr::smgrgettargblock(key), InvalidBlockNumber);
    assert_eq!(smgr_seams::smgr_nblocks::call(key, fork).unwrap(), 5);

    // The installed seam surface: release + destroy paths run end to end.
    smgr_seams::smgr_release_rel_locator::call(key).unwrap();
    assert!(smgr_seams::process_barrier_smgr_release::call().unwrap());
    smgr::AtEOXact_SMgr().unwrap();
    smgr_seams::smgr_create::call(key, ForkNumber::FSM_FORKNUM, false).unwrap();
    assert!(smgr::smgrexists(key, ForkNumber::FSM_FORKNUM).unwrap());
}
