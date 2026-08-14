use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

static IN_RECOVERY: AtomicBool = AtomicBool::new(false);

fn page_ptr() -> core::ptr::NonNull<u8> {
    #[repr(align(4096))]
    struct AlignedPage([u8; BLCKSZ]);
    static PAGE: AlignedPage = AlignedPage([0; BLCKSZ]);
    core::ptr::NonNull::new(PAGE.0.as_ptr().cast_mut()).unwrap()
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
    bufmgr_seams::buffer_get_page::set(|_| page_ptr());
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
