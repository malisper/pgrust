use super::*;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Mutex, MutexGuard, Once};

static IN_RECOVERY: AtomicBool = AtomicBool::new(false);

#[repr(align(4096))]
struct AlignedPage([u8; BLCKSZ]);

fn page_ptr() -> core::ptr::NonNull<u8> {
    static PAGE: AlignedPage = AlignedPage([0; BLCKSZ]);
    core::ptr::NonNull::new(PAGE.0.as_ptr().cast_mut()).unwrap()
}

// Seams install once per process (seam_core's set-once law), so every test
// that reads a page through bufmgr goes through one shared selector: the
// page currently chosen under PAGE_LOCK.
static CURRENT_PAGE: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());
static PAGE_SEAMS: Once = Once::new();
static PAGE_LOCK: Mutex<()> = Mutex::new(());

fn current_page(_: Buffer) -> core::ptr::NonNull<u8> {
    core::ptr::NonNull::new(CURRENT_PAGE.load(Ordering::SeqCst)).expect("test page selected")
}
fn block_zero(_: Buffer) -> types_core::BlockNumber {
    0
}
fn no_fpi() -> i64 {
    0
}

/// Installs the shared page seams and selects `page` for the caller's
/// lifetime of the returned guard.
fn select_page(page: core::ptr::NonNull<u8>) -> MutexGuard<'static, ()> {
    PAGE_SEAMS.call_once(|| {
        bufmgr_seams::buffer_get_page::set(current_page);
        bufmgr_seams::buffer_get_block_number::set(block_zero);
        transam_xlog_seams::wal_usage_fpi::set(no_fpi);
    });
    let guard = PAGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    CURRENT_PAGE.store(page.as_ptr(), Ordering::SeqCst);
    guard
}

fn test_relation<'mcx>(mcx: mcx::Mcx<'mcx>) -> RelationData<'mcx> {
    use std::cell::Cell;
    use std::rc::Rc;
    use types_rel::*;
    use types_tuple::{CompactAttribute, FormData_pg_attribute, TupleDescData};
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
    let mut relname = types_tuple::NameData::default();
    relname.namestrcpy("t");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: 2,
        relfilenode: 1000,
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relisshared: false,
        relpersistence: types_core::RELPERSISTENCE_PERMANENT,
        relkind: RELKIND_RELATION,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: b'd',
        relispartition: false,
        relfrozenxid: 3,
        relminmxid: 1,
    };
    RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: 1000,
        rd_backend: types_core::INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: 1000, dbId: 5 },
        },
        rd_rel,
        rd_att,
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
        rd_opfamily: mcx::PgVec::new_in(mcx),
        rd_indoption: mcx::PgVec::new_in(mcx),
        rd_indcollation: mcx::PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: mcx::PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    }
}

#[test]
fn guard_chain_early_exits() {
    init_seams();
    transam_xlog_seams::recovery_in_progress::set(|| IN_RECOVERY.load(Ordering::SeqCst));
    let _page = select_page(page_ptr());
    let mcx_owner = mcx::MemoryContext::new("t");
    let rel = test_relation(mcx_owner.mcx());

    IN_RECOVERY.store(true, Ordering::SeqCst);
    pruneheap_seams::heap_page_prune_opt::call(&rel, 1).unwrap();

    // Invalid pd_prune_xid: exits before the (uninstalled) vistest seam.
    IN_RECOVERY.store(false, Ordering::SeqCst);
    pruneheap_seams::heap_page_prune_opt::call(&rel, 1).unwrap();
}

fn empty_prstate<'a>() -> super::PruneState<'a> {
    super::PruneState {
        vistest: types_core::GlobalVisStateHandle::new(0),
        mark_unused_now: false,
        freeze: false,
        cutoffs: None,
        pagefrz: super::HeapPageFreeze {
            freeze_required: false,
            FreezePageRelfrozenXid: super::InvalidTransactionId,
            NoFreezePageRelfrozenXid: super::InvalidTransactionId,
            FreezePageRelminMxid: 0,
            NoFreezePageRelminMxid: 0,
        },
        nfrozen: 0,
        frozen: [super::HeapTupleFreeze::default(); super::MaxHeapTuplesPerPage],
        new_prune_xid: super::InvalidTransactionId,
        latest_xid_removed: super::InvalidTransactionId,
        nredirected: 0,
        ndead: 0,
        nunused: 0,
        redirected: [0; super::MaxHeapTuplesPerPage * 2],
        nowdead: [0; super::MaxHeapTuplesPerPage],
        nowunused: [0; super::MaxHeapTuplesPerPage],
        nroot_items: 0,
        root_items: [0; super::MaxHeapTuplesPerPage],
        nheaponly_items: 0,
        heaponly_items: [0; super::MaxHeapTuplesPerPage],
        processed: [false; super::MaxHeapTuplesPerPage + 1],
        htsv: [-1; super::MaxHeapTuplesPerPage + 1],
        ndeleted: 0,
        live_tuples: 0,
        recently_dead_tuples: 0,
        hastup: false,
        lpdead_items: 0,
        all_visible: false,
        all_frozen: false,
        visibility_cutoff_xid: super::InvalidTransactionId,
    }
}

#[test]
fn unexpected_dead_htsv_is_ereport_not_panic() {
    #[repr(align(8))]
    struct AlignedPage([u8; super::BLCKSZ]);
    let mut raw = AlignedPage([0; super::BLCKSZ]);
    let page_nn = core::ptr::NonNull::new(raw.0.as_mut_ptr()).unwrap();
    let mut pm = unsafe { super::PageMut::from_raw(page_nn) };
    pm.init(0);
    let item = [0u8; ::types_tuple::SizeofHeapTupleHeader];
    pm.add_item(&item, 0, ::types_storage::bufpage::PAI_IS_HEAP)
        .expect("one heap item");
    let page = unsafe { super::PageRef::from_raw(page_nn) };

    let mut prstate = empty_prstate();
    prstate.htsv[super::FirstOffsetNumber as usize] = super::HTSV_Result::HEAPTUPLE_DEAD as i8;
    let err = super::heap_prune_record_unchanged_lp_normal(
        page,
        &mut prstate,
        super::FirstOffsetNumber,
    )
    .expect_err("C elog(ERROR) on DEAD in unchanged");
    assert_eq!(err.level, types_error::ERROR);
    assert_eq!(err.sqlstate, types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message, "unexpected HeapTupleSatisfiesVacuum result 0");
}

#[test]
fn prune_refuses_line_pointer_count_beyond_max_heap_tuples_as_data_corrupted() {
    // audit-18.6 w2-068 (verified fp5_t): pd_lower forged so the page carries
    // 292 line pointers, one more than MaxHeapTuplesPerPage. C's
    // PruneState.processed/htsv are [MaxHeapTuplesPerPage + 1] stack arrays
    // (pruneheap.c:86/:98) that the scan loop (pruneheap.c:492-506) indexes
    // one past their end - undefined behaviour with no ereport. pgrust must
    // not panic on the same page: the line-pointer count is refused as an
    // XX001 ERROR before the scan, the bufpage data_corrupted surface.
    const fn page_292() -> AlignedPage {
        let mut p = [0u8; super::BLCKSZ];
        // PageHeaderData: pd_lower @12 = 24 + 292 * 4 = 1192, pd_upper @14 =
        // pd_special @16 = BLCKSZ, pd_pagesize_version @18 = BLCKSZ | 4.
        p[12] = 0xa8;
        p[13] = 0x04;
        p[14] = 0x00;
        p[15] = 0x20;
        p[16] = 0x00;
        p[17] = 0x20;
        p[18] = 0x04;
        p[19] = 0x20;
        AlignedPage(p)
    }
    static PAGE: AlignedPage = page_292();
    let page_nn = core::ptr::NonNull::new(PAGE.0.as_ptr().cast_mut()).unwrap();
    let _page = select_page(page_nn);

    let mcx_owner = mcx::MemoryContext::new("t");
    let rel = test_relation(mcx_owner.mcx());
    // SAFETY: a static, never-written page image.
    let maxoff = unsafe { super::PageRef::from_raw(page_nn) }.max_offset_number();
    assert_eq!(maxoff as usize, super::MaxHeapTuplesPerPage + 1);

    let mut presult = super::PruneFreezeResult::default();
    let mut off_loc: super::OffsetNumber = 0;
    let err = super::heap_page_prune_and_freeze(
        &rel,
        1,
        types_core::GlobalVisStateHandle::new(0),
        super::HEAP_PAGE_PRUNE_MARK_UNUSED_NOW,
        None,
        &mut presult,
        super::PruneReason::PruneVacuumScan,
        &mut off_loc,
        None,
        None,
    )
    .expect_err("292 line pointers on a heap page is a typed corruption refusal");
    assert_eq!(err.level, types_error::ERROR);
    assert_eq!(err.sqlstate, types_error::ERRCODE_DATA_CORRUPTED);
    assert_eq!(
        err.message,
        "corrupted line pointer count: nline = 292, max = 291"
    );
}
