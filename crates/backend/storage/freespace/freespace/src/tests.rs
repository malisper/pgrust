use super::*;
use ::bufmgr_seams::BufferPin;
use ::types_core::{Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use ::types_error::{ErrorLevel, DEBUG1};
use ::types_rel::{FormData_pg_class, LockInfoData, LockRelId, RELKIND_RELATION};
use ::types_storage::buf::buftag;
use ::types_storage::bufpage::PageMut;
use ::types_storage::RelFileLocatorBackend;
use core::ptr::NonNull;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicUsize, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard, Once};

#[repr(align(8))]
struct AlignedPage([u8; BLCKSZ]);

fn fsm_test_page() -> Box<AlignedPage> {
    let mut page = Box::new(AlignedPage([0u8; BLCKSZ]));
    // SAFETY: local BLCKSZ buffer, exclusively owned.
    unsafe { PageMut::from_raw(NonNull::new(page.0.as_mut_ptr()).unwrap()) }.init(0);
    page
}

fn view(page: &mut AlignedPage) -> FsmPage {
    // SAFETY: exclusive borrow of a live init'ed page.
    unsafe { FsmPage::from_raw(NonNull::new(page.0.as_mut_ptr()).unwrap()) }
}

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}

static SERIAL: Mutex<()> = Mutex::new(());
static PAGE_ADDR: AtomicUsize = AtomicUsize::new(0);
static LOCK_STATE: AtomicI32 = AtomicI32::new(0); // 0 none, 1 share, 2 excl
static RELOCKS: AtomicUsize = AtomicUsize::new(0);
static DIRTY_HINTS: AtomicUsize = AtomicUsize::new(0);
static INIT: Once = Once::new();

// Fake FSM fork: physical block n is served as buffer n + 1 from PAGES[n]
// (a page of 0 falls back to PAGE_ADDR, the single-page tests' buffer 1).
static PAGES: Mutex<Vec<usize>> = Mutex::new(Vec::new());
static NFSM: AtomicU32 = AtomicU32::new(0);
static FSM_CACHED: AtomicU32 = AtomicU32::new(InvalidBlockNumber);
static LOGGED: Mutex<Vec<(ErrorLevel, String, Option<String>)>> = Mutex::new(Vec::new());
static INTERRUPT_CALLS: AtomicUsize = AtomicUsize::new(0);
static NEWPAGE_LOGS: AtomicUsize = AtomicUsize::new(0);
static WAL_LEVEL: AtomicI32 = AtomicI32::new(1); // replica
static WAL_LOG_HINTS: AtomicBool = AtomicBool::new(false);

const REL_OID: Oid = 91000;
const TEST_LOCATOR: RelFileLocator = RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: REL_OID };

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn install_page_seams() {
    INIT.call_once(|| {
        bufmgr_seams::buffer_get_page::set(|buf| {
            let table = PAGES.lock().unwrap_or_else(|e| e.into_inner());
            let addr = match table.get((buf - 1) as usize) {
                Some(&a) if a != 0 => a,
                _ => PAGE_ADDR.load(Relaxed),
            };
            NonNull::new(addr as *mut u8).unwrap()
        });
        bufmgr_seams::buffer_get_tag::set(|buf| buftag {
            spcOid: TEST_LOCATOR.spcOid,
            dbOid: TEST_LOCATOR.dbOid,
            relNumber: TEST_LOCATOR.relNumber,
            forkNum: ForkNumber::FSM_FORKNUM,
            blockNum: (buf - 1) as BlockNumber,
        });
        bufmgr_seams::relation_smgr_locator::set(|rel| RelFileLocatorBackend {
            locator: rel.rd_locator.get(),
            backend: INVALID_PROC_NUMBER,
        });
        bufmgr_seams::read_buffer_extended::set(|_rel, fork, blk, mode, _strategy| {
            assert_eq!(fork, ForkNumber::FSM_FORKNUM);
            assert_eq!(mode, ReadBufferMode::ZeroOnError);
            assert!(blk < NFSM.load(Relaxed), "read past the fake FSM fork: block {blk}");
            Ok(blk as Buffer + 1)
        });
        bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
        smgr_seams::smgr_exists::set(|_rloc, fork| {
            assert_eq!(fork, ForkNumber::FSM_FORKNUM);
            Ok(NFSM.load(Relaxed) > 0)
        });
        smgr_seams::smgr_nblocks::set(|_rloc, fork| {
            assert_eq!(fork, ForkNumber::FSM_FORKNUM);
            Ok(NFSM.load(Relaxed))
        });
        smgr_seams::smgr_cached_nblocks::set(|_rloc, fork| match fork {
            ForkNumber::FSM_FORKNUM => FSM_CACHED.load(Relaxed),
            _ => InvalidBlockNumber,
        });
        smgr_seams::smgr_set_cached_nblocks::set(|_rloc, fork, v| {
            if fork == ForkNumber::FSM_FORKNUM {
                FSM_CACHED.store(v, Relaxed);
            }
            Ok(())
        });
        xlogutils_seams::in_recovery::set(|| false);
        transam_xlog_seams::data_checksums_enabled::set(|| false);
        xloginsert_seams::log_newpage_buffer::set(|_buf, _std| {
            NEWPAGE_LOGS.fetch_add(1, Relaxed);
            Ok(0)
        });
        guc_tables::vars::wal_level.install_if_absent(guc_tables::GucVarAccessors {
            get: || WAL_LEVEL.load(Relaxed),
            set: |v| WAL_LEVEL.store(v, Relaxed),
        });
        guc_tables::vars::wal_log_hints.install_if_absent(guc_tables::GucVarAccessors {
            get: || WAL_LOG_HINTS.load(Relaxed),
            set: |v| WAL_LOG_HINTS.store(v, Relaxed),
        });
        // ProcessInterrupts with a pending cancel (postgres.c): the
        // QUERY_CANCELED ereport(ERROR) comes back as the Err.
        postgres_seams::check_for_interrupts::set(|| {
            INTERRUPT_CALLS.fetch_add(1, Relaxed);
            Err(Box::new(PgError::error("canceling statement due to user request")))
        });
        elog_seams::ereport_msg::set(|level, msg, detail| {
            LOGGED.lock().unwrap_or_else(|e| e.into_inner()).push((level, msg, detail));
            Ok(())
        });
        bufmgr_seams::lock_buffer::set(|_buf, mode| {
            match mode {
                bufmgr_seams::BUFFER_LOCK_UNLOCK => {
                    assert!(LOCK_STATE.swap(0, Relaxed) != 0, "unlock without lock");
                }
                bufmgr_seams::BUFFER_LOCK_EXCLUSIVE => {
                    assert_eq!(LOCK_STATE.swap(2, Relaxed), 0, "double lock");
                    RELOCKS.fetch_add(1, Relaxed);
                }
                _ => {
                    assert_eq!(LOCK_STATE.swap(1, Relaxed), 0, "double lock");
                }
            }
            Ok(())
        });
        bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| {
            DIRTY_HINTS.fetch_add(1, Relaxed);
            Ok(())
        });
        bufmgr_seams::release_buffer::set(|_buf| Ok(()));
    });
}

fn search(page: &mut AlignedPage, minvalue: u8, advancenext: bool, excl: bool) -> i32 {
    install_page_seams();
    PAGE_ADDR.store(page.0.as_mut_ptr() as usize, Relaxed);
    LOCK_STATE.store(if excl { 2 } else { 1 }, Relaxed);
    let pin = BufferPin::adopt(1).unwrap();
    let slot = fsm_search_avail(&pin, minvalue, advancenext, excl).unwrap();
    LOCK_STATE.store(0, Relaxed);
    pin.release();
    slot
}

fn take_logged() -> Vec<(ErrorLevel, String, Option<String>)> {
    std::mem::take(&mut *LOGGED.lock().unwrap_or_else(|e| e.into_inner()))
}

/// A fake FSM fork of init'ed, all-zero pages (physical order: root,
/// level-1 page 0, leaf page 0, ...), torn down — with the interrupt flag,
/// lock state and GUC fakes reset — when the guard drops, so a failing
/// witness cannot poison the single-page tests that share the fakes.
struct FsmFork;

fn install_fsm_fork(pages: &mut [Box<AlignedPage>]) -> FsmFork {
    install_page_seams();
    *PAGES.lock().unwrap_or_else(|e| e.into_inner()) =
        pages.iter_mut().map(|p| p.0.as_mut_ptr() as usize).collect();
    NFSM.store(pages.len() as BlockNumber, Relaxed);
    FSM_CACHED.store(InvalidBlockNumber, Relaxed);
    FsmFork
}

impl Drop for FsmFork {
    fn drop(&mut self) {
        PAGES.lock().unwrap_or_else(|e| e.into_inner()).clear();
        NFSM.store(0, Relaxed);
        FSM_CACHED.store(InvalidBlockNumber, Relaxed);
        init_small::globals::SetInterruptPending(false);
        LOCK_STATE.store(0, Relaxed);
        WAL_LEVEL.store(1, Relaxed);
        WAL_LOG_HINTS.store(false, Relaxed);
    }
}

fn test_relation<'mcx>(mcx: mcx::Mcx<'mcx>) -> RelationData<'mcx> {
    use std::rc::Rc;
    use types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};
    let att = FormData_pg_attribute {
        attnum: 1,
        attlen: 4,
        attbyval: true,
        attalign: types_tuple::TYPALIGN_INT,
        attstorage: types_tuple::TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = mcx::PgVec::new_in(mcx);
    let mut compact = mcx::PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    let rd_att = Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    });
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    RelationData {
        rd_locator: Cell::new(TEST_LOCATOR),
        rd_smgr: Default::default(),
        rd_id: REL_OID,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: REL_OID, dbId: 5 } },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: 2,
            relfilenode: REL_OID,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att,
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
        rd_opfamily: mcx::PgVec::new_in(mcx),
        rd_indoption: mcx::PgVec::new_in(mcx),
        rd_indcollation: mcx::PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(true),
        pgstat_link: Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: mcx::PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
    }
}

// freespace.c:892: CHECK_FOR_INTERRUPTS() on every slot of every upper-level
// page, so a pending cancel aborts the tree walk (audit-18.6 b210).
#[test]
fn vacuum_checks_for_interrupts_per_slot() {
    let _s = serial();
    let mut pages = vec![fsm_test_page(), fsm_test_page(), fsm_test_page()];
    let _fork = install_fsm_fork(&mut pages);
    let ctx = mcx::MemoryContext::new("fsm_vacuum");
    let rel = test_relation(ctx.mcx());

    // Nothing pending: the whole tree is walked, ProcessInterrupts never runs.
    INTERRUPT_CALLS.store(0, Relaxed);
    init_small::globals::SetInterruptPending(false);
    FreeSpaceMapVacuum(&rel).unwrap();
    assert_eq!(INTERRUPT_CALLS.load(Relaxed), 0);

    // A pending cancel is raised from the first slot; the walk does not
    // continue to the remaining slots/pages.
    init_small::globals::SetInterruptPending(true);
    let res = FreeSpaceMapVacuum(&rel);
    init_small::globals::SetInterruptPending(false);
    let err = res.expect_err("FreeSpaceMapVacuum ignored a pending interrupt");
    assert!(
        err.message().contains("canceling statement due to user request"),
        "{err:?}"
    );
    assert_eq!(INTERRUPT_CALLS.load(Relaxed), 1);

    // Range form goes through the same loop.
    init_small::globals::SetInterruptPending(true);
    let res = FreeSpaceMapVacuumRange(&rel, 0, 10);
    init_small::globals::SetInterruptPending(false);
    assert!(res.is_err(), "FreeSpaceMapVacuumRange ignored a pending interrupt");
    assert_eq!(INTERRUPT_CALLS.load(Relaxed), 2);
    assert_eq!(LOCK_STATE.load(Relaxed), 0, "content lock leaked across the error");
}

// freespace.c:341: `!InRecovery && RelationNeedsWAL(rel) && XLogHintBitIsNeeded()`
// gates the truncation FPI; RelationNeedsWAL (rel.h) is false under
// wal_level=minimal for a relation created/rewritten in this transaction.
#[test]
fn prepare_truncate_fpi_follows_relation_needs_wal() {
    let _s = serial();
    let mut pages = vec![fsm_test_page(), fsm_test_page(), fsm_test_page()];
    let _fork = install_fsm_fork(&mut pages);
    let ctx = mcx::MemoryContext::new("fsm_truncate");
    let rel = test_relation(ctx.mcx());
    WAL_LEVEL.store(0, Relaxed); // minimal
    WAL_LOG_HINTS.store(true, Relaxed);

    // Truncating to 1 block clears slots 1.. of leaf page 0 (physical block
    // 2); the new FSM length is that block + 1.
    let expect_fpis = |rel: &RelationData<'_>, n: usize, what: &str| {
        NEWPAGE_LOGS.store(0, Relaxed);
        assert_eq!(FreeSpaceMapPrepareTruncateRel(rel, 1).unwrap(), 3);
        assert_eq!(NEWPAGE_LOGS.load(Relaxed), n, "{what}");
        assert_eq!(LOCK_STATE.load(Relaxed), 0);
    };

    // Older permanent relation: RelationNeedsWAL, FPI logged.
    expect_fpis(&rel, 1, "permanent relation from an earlier transaction");
    // Created in this transaction under wal_level=minimal: no WAL at all.
    rel.rd_createSubid.set(1);
    expect_fpis(&rel, 0, "rd_createSubid set under wal_level=minimal");
    rel.rd_createSubid.set(0);
    rel.rd_firstRelfilelocatorSubid.set(1);
    expect_fpis(&rel, 0, "rd_firstRelfilelocatorSubid set under wal_level=minimal");
    // wal_level=replica: XLogIsNeeded, so the same relation is WAL-logged.
    WAL_LEVEL.store(1, Relaxed);
    expect_fpis(&rel, 1, "rd_firstRelfilelocatorSubid set under wal_level=replica");
    // Neither wal_log_hints nor checksums: no FPI regardless.
    WAL_LOG_HINTS.store(false, Relaxed);
    rel.rd_firstRelfilelocatorSubid.set(0);
    expect_fpis(&rel, 0, "XLogHintBitIsNeeded false");
}

// First slot >= min at/after fp_next_slot, wrapping (fsmpage.c's result).
fn model_search(leaves: &[u8; SLOTS_PER_FSM_PAGE as usize], target: i32, min: u8) -> i32 {
    let n = SLOTS_PER_FSM_PAGE as usize;
    let start = if (0..SLOTS_PER_FSM_PAGE).contains(&target) { target as usize } else { 0 };
    for i in 0..n {
        let s = (start + i) % n;
        if leaves[s] >= min {
            return s as i32;
        }
    }
    -1
}

#[test]
fn set_get_and_search_match_model() {
    let _s = serial();
    let mut page = fsm_test_page();
    let mut model = [0u8; SLOTS_PER_FSM_PAGE as usize];
    let mut rng = Lcg(42);

    for round in 0..2000 {
        let slot = (rng.next() % SLOTS_PER_FSM_PAGE as u64) as i32;
        let value = (rng.next() % 256) as u8;
        fsm_set_avail(view(&mut page), slot, value);
        model[slot as usize] = value;

        if round % 50 == 0 {
            let v = view(&mut page);
            assert_eq!(fsm_get_max_avail(v), model.iter().copied().max().unwrap());
            for probe in [0, slot, SLOTS_PER_FSM_PAGE - 1] {
                assert_eq!(fsm_get_avail(v, probe), model[probe as usize]);
            }

            let min = (rng.next() % 256) as u8;
            let next = (rng.next() % (SLOTS_PER_FSM_PAGE as u64 + 7)) as i32;
            v.set_next_slot(next);
            let got = search(&mut page, min, false, true);
            assert_eq!(got, model_search(&model, next, min), "min={min} next={next}");
            if got != -1 {
                assert_eq!(view(&mut page).next_slot(), got);
            }
        }
    }
    assert!(!fsm_rebuild_page(view(&mut page)), "propagation left the tree inconsistent");
}

#[test]
fn search_advancenext_and_wraparound() {
    let _s = serial();
    let mut page = fsm_test_page();
    fsm_set_avail(view(&mut page), 3, 200);
    fsm_set_avail(view(&mut page), SLOTS_PER_FSM_PAGE - 1, 200);

    view(&mut page).set_next_slot(0);
    assert_eq!(search(&mut page, 100, true, true), 3);
    assert_eq!(view(&mut page).next_slot(), 4);

    assert_eq!(search(&mut page, 100, true, true), SLOTS_PER_FSM_PAGE - 1);
    assert_eq!(view(&mut page).next_slot(), SLOTS_PER_FSM_PAGE);
    // Out-of-range hint wraps to slot 0's side on the next call.
    assert_eq!(search(&mut page, 100, true, true), 3);

    assert_eq!(search(&mut page, 201, false, true), -1);
}

#[test]
fn search_torn_page_repairs_under_exclusive_and_restarts() {
    let _s = serial();
    let mut page = fsm_test_page();
    fsm_set_avail(view(&mut page), 7, 90);
    // Corrupt one mid-level node on the path to slot 7 to promise 255.
    let mut nodeno = NON_LEAF_NODES_PER_PAGE + 7;
    for _ in 0..3 {
        nodeno = (nodeno - 1) / 2;
    }
    view(&mut page).set_node(nodeno, 255);
    view(&mut page).set_node(0, 255);
    view(&mut page).set_next_slot(0);

    RELOCKS.store(0, Relaxed);
    DIRTY_HINTS.store(0, Relaxed);
    take_logged();
    // Shared-lock caller: the repair relocks exclusive, rebuilds, restarts,
    // and still finds the genuine slot.
    assert_eq!(search(&mut page, 200, false, false), -1);
    assert!(RELOCKS.load(Relaxed) >= 1, "repair did not take the exclusive lock");
    assert!(DIRTY_HINTS.load(Relaxed) >= 1);
    // fsmpage.c:276: elog(DEBUG1, "fixing corrupt FSM block %u, relation
    // %u/%u/%u") from BufferGetTag, once per repair.
    let logged = take_logged();
    assert_eq!(
        logged,
        vec![(DEBUG1, "fixing corrupt FSM block 0, relation 1663/5/91000".to_string(), None)],
        "{logged:?}"
    );
    assert_eq!(view(&mut page).node(0), 90, "rebuild did not fix the root");
    assert_eq!(search(&mut page, 90, false, true), 7);
}

#[test]
fn truncate_avail_clears_tail() {
    let _s = serial();
    let mut page = fsm_test_page();
    for slot in 0..SLOTS_PER_FSM_PAGE {
        fsm_set_avail(view(&mut page), slot, 10);
    }
    fsm_set_avail(view(&mut page), 5, 250);
    fsm_set_avail(view(&mut page), 100, 251);

    assert!(fsm_truncate_avail(view(&mut page), 100));
    let v = view(&mut page);
    assert_eq!(fsm_get_avail(v, 99), 10);
    assert_eq!(fsm_get_avail(v, 100), 0);
    assert_eq!(fsm_get_avail(v, SLOTS_PER_FSM_PAGE - 1), 0);
    assert_eq!(fsm_get_max_avail(v), 250);
    assert!(!fsm_truncate_avail(view(&mut page), 100));
}

#[test]
fn category_math_matches_c() {
    assert_eq!(fsm_space_avail_to_cat(0), 0);
    assert_eq!(fsm_space_avail_to_cat(31), 0);
    assert_eq!(fsm_space_avail_to_cat(32), 1);
    assert_eq!(fsm_space_avail_to_cat(8127), 253);
    assert_eq!(fsm_space_avail_to_cat(8128), 254);
    assert_eq!(fsm_space_avail_to_cat(8159), 254);
    assert_eq!(fsm_space_avail_to_cat(8160), 255);
    assert_eq!(fsm_space_avail_to_cat(8191), 255);

    assert_eq!(fsm_space_cat_to_avail(0), 0);
    assert_eq!(fsm_space_cat_to_avail(1), 32);
    assert_eq!(fsm_space_cat_to_avail(254), 8128);
    assert_eq!(fsm_space_cat_to_avail(255), 8160);

    assert_eq!(fsm_space_needed_to_cat(0).unwrap(), 1);
    assert_eq!(fsm_space_needed_to_cat(1).unwrap(), 1);
    assert_eq!(fsm_space_needed_to_cat(32).unwrap(), 1);
    assert_eq!(fsm_space_needed_to_cat(33).unwrap(), 2);
    assert_eq!(fsm_space_needed_to_cat(8128).unwrap(), 254);
    assert_eq!(fsm_space_needed_to_cat(8129).unwrap(), 255);
    assert_eq!(fsm_space_needed_to_cat(8160).unwrap(), 255);
    let err = fsm_space_needed_to_cat(8161).unwrap_err();
    assert!(err.message().contains("invalid FSM request size 8161"), "{err:?}");

    // avail_to_cat rounds down: the represented lower bound never overstates.
    for avail in [0usize, 1, 31, 32, 4000, 8128, 8159, 8160, 8191] {
        assert!(
            fsm_space_cat_to_avail(fsm_space_avail_to_cat(avail)) <= avail,
            "avail {avail}"
        );
    }
}

#[test]
fn logical_to_physical_fixtures() {
    let a = |level, logpageno| FSMAddress { level, logpageno };
    assert_eq!(fsm_logical_to_physical(a(2, 0)), 0);
    assert_eq!(fsm_logical_to_physical(a(1, 0)), 1);
    assert_eq!(fsm_logical_to_physical(a(0, 0)), 2);
    assert_eq!(fsm_logical_to_physical(a(0, 1)), 3);
    assert_eq!(fsm_logical_to_physical(a(0, 4068)), 4070);
    assert_eq!(fsm_logical_to_physical(a(1, 1)), 4071);
    assert_eq!(fsm_logical_to_physical(a(0, 4069)), 4072);
}

#[test]
fn physical_addresses_are_dense_dfs() {
    // The physical layout is depth-first: root, then per level-1 subtree the
    // level-1 page followed by its leaves. Two full subtrees = a dense 0..n.
    let mut phys = Vec::new();
    phys.push(fsm_logical_to_physical(FSM_ROOT_ADDRESS));
    for l1 in 0..2 {
        phys.push(fsm_logical_to_physical(FSMAddress { level: 1, logpageno: l1 }));
        for leaf in 0..SLOTS_PER_FSM_PAGE {
            phys.push(fsm_logical_to_physical(FSMAddress {
                level: 0,
                logpageno: l1 * SLOTS_PER_FSM_PAGE + leaf,
            }));
        }
    }
    let mut sorted = phys.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), phys.len(), "duplicate physical blocks");
    assert_eq!(sorted, (0..phys.len() as BlockNumber).collect::<Vec<_>>());
}

#[test]
fn addressing_roundtrips() {
    for heapblk in [0u32, 1, 4068, 4069, 4070, 16_555_961, 4_294_967_294] {
        let (addr, slot) = fsm_get_location(heapblk);
        assert_eq!(addr.level, FSM_BOTTOM_LEVEL);
        assert_eq!(fsm_get_heap_blk(addr, slot), heapblk);

        let (parent, pslot) = fsm_get_parent(addr);
        assert_eq!(fsm_get_child(parent, pslot), addr);
        let (root, rslot) = fsm_get_parent(parent);
        assert_eq!(root.level, FSM_ROOT_LEVEL);
        assert_eq!(root.logpageno, 0);
        assert_eq!(fsm_get_child(root, rslot), parent);
    }
}
