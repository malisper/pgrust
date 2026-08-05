// e2e: heap_insert over a fork-aware fake bufmgr with the real freespace
// crate installed; a filled page recorded in the FSM is reused, not extended.
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, Once};

use heapam::heap_insert;
use mcx::MemoryContext;
use types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, Oid, BLCKSZ, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use types_rel::{FormData_pg_class, LockInfoData, LockRelId, RelationData, RELKIND_RELATION};
use types_storage::bufpage::PageRef;
use types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};
use types_tuple::{HeapTupleData, ItemPointerData};

const REL_OID: Oid = 91000;

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    // buffer id - 1 indexes all three vectors; forks share the id space.
    bufs: Vec<(ForkNumber, BlockNumber, usize)>,
    pins: Vec<i32>,
    locks: Vec<i32>,
    nmain: BlockNumber,
    nfsm: BlockNumber,
    fsm_exists: bool,
    cached_main: BlockNumber,
    cached_fsm: BlockNumber,
    main_extends: usize,
}

static FAKE: Mutex<Fake> = Mutex::new(Fake {
    bufs: Vec::new(),
    pins: Vec::new(),
    locks: Vec::new(),
    nmain: 0,
    nfsm: 0,
    fsm_exists: false,
    cached_main: InvalidBlockNumber,
    cached_fsm: InvalidBlockNumber,
    main_extends: 0,
});

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    f(&mut FAKE.lock().unwrap_or_else(|e| e.into_inner()))
}

fn new_page_buf(f: &mut Fake, fork: ForkNumber, blkno: BlockNumber) -> Buffer {
    let addr = Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize;
    f.bufs.push((fork, blkno, addr));
    f.pins.push(0);
    f.locks.push(0);
    f.bufs.len() as Buffer
}

fn find_buf(f: &Fake, fork: ForkNumber, blkno: BlockNumber) -> Buffer {
    f.bufs
        .iter()
        .position(|&(fk, b, _)| fk == fork && b == blkno)
        .map(|i| i as Buffer + 1)
        .unwrap_or_else(|| panic!("no page for fork {fork:?} block {blkno}"))
}

static INIT: Once = Once::new();

fn install_seams() {
    INIT.call_once(|| {
        freespace::init_seams();

        bufmgr_seams::relation_smgr_locator::set(|rel| RelFileLocatorBackend {
            locator: RelFileLocator::new(1663, 5, rel.rd_id),
            backend: INVALID_PROC_NUMBER,
        });
        bufmgr_seams::read_buffer::set(|_rel, blk| {
            with_fake(|f| {
                let buf = find_buf(f, ForkNumber::MAIN_FORKNUM, blk);
                f.pins[(buf - 1) as usize] += 1;
                Ok(buf)
            })
        });
        bufmgr_seams::read_buffer_extended::set(|_rel, fork, blk, mode, _strategy| {
            assert_eq!(mode, ReadBufferMode::ZeroOnError);
            with_fake(|f| {
                let buf = find_buf(f, fork, blk);
                f.pins[(buf - 1) as usize] += 1;
                Ok(buf)
            })
        });
        bufmgr_seams::buffer_get_block_number::set(|buf| {
            with_fake(|f| f.bufs[(buf - 1) as usize].1)
        });
        bufmgr_seams::buffer_get_page::set(|buf| {
            let addr = with_fake(|f| {
                assert!(f.pins[(buf - 1) as usize] > 0, "page access without pin");
                f.bufs[(buf - 1) as usize].2
            });
            NonNull::new(addr as *mut u8).unwrap()
        });
        bufmgr_seams::release_buffer::set(|buf| {
            with_fake(|f| {
                let p = &mut f.pins[(buf - 1) as usize];
                assert!(*p > 0, "double release of buffer {buf}");
                *p -= 1;
            });
            Ok(())
        });
        bufmgr_seams::incr_buffer_ref_count::set(|buf| {
            with_fake(|f| f.pins[(buf - 1) as usize] += 1);
        });
        bufmgr_seams::lock_buffer::set(|buf, mode| {
            with_fake(|f| {
                let l = &mut f.locks[(buf - 1) as usize];
                match mode {
                    bufmgr_seams::BUFFER_LOCK_UNLOCK => {
                        assert!(*l > 0, "unlock without lock");
                        *l -= 1;
                    }
                    _ => {
                        assert_eq!(*l, 0, "double content lock");
                        *l += 1;
                    }
                }
            });
            Ok(())
        });
        bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
        bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, fork| {
            assert_eq!(fork, ForkNumber::MAIN_FORKNUM);
            with_fake(|f| Ok(f.nmain))
        });
        bufmgr_seams::extend_buffered_rel_by::set(|_rel, fork, _strategy, flags, extend_by| {
            assert_eq!(fork, ForkNumber::MAIN_FORKNUM);
            assert_eq!(extend_by, 1);
            assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
            with_fake(|f| {
                let blkno = f.nmain;
                let buf = new_page_buf(f, ForkNumber::MAIN_FORKNUM, blkno);
                f.pins[(buf - 1) as usize] = 1;
                f.locks[(buf - 1) as usize] = 1;
                f.nmain += 1;
                f.cached_main = f.nmain;
                f.main_extends += 1;
                Ok((buf, 1))
            })
        });
        bufmgr_seams::extend_buffered_rel_to::set(
            |_rlocator, fork, _strategy, flags, extend_to, mode| {
                assert_eq!(fork, ForkNumber::FSM_FORKNUM);
                assert!(flags & bufmgr_seams::EB_CREATE_FORK_IF_NEEDED != 0);
                assert!(flags & bufmgr_seams::EB_CLEAR_SIZE_CACHE != 0);
                assert_eq!(mode, ReadBufferMode::ZeroOnError);
                with_fake(|f| {
                    while f.nfsm < extend_to {
                        let blkno = f.nfsm;
                        new_page_buf(f, ForkNumber::FSM_FORKNUM, blkno);
                        f.nfsm += 1;
                    }
                    f.fsm_exists = true;
                    f.cached_fsm = f.nfsm;
                    let buf = find_buf(f, ForkNumber::FSM_FORKNUM, extend_to - 1);
                    f.pins[(buf - 1) as usize] += 1;
                    Ok(buf)
                })
            },
        );
        bufmgr_seams::extend_buffered_rel_to_rel::set(|rel, fork, strategy, flags, extend_to, mode| {
            bufmgr_seams::extend_buffered_rel_to::call(
                bufmgr_seams::relation_smgr_locator::call(rel),
                fork, strategy, flags, extend_to, mode,
            )
        });

        smgr_seams::smgr_exists::set(|_rloc, fork| {
            assert_eq!(fork, ForkNumber::FSM_FORKNUM);
            with_fake(|f| Ok(f.fsm_exists))
        });
        smgr_seams::smgr_nblocks::set(|_rloc, fork| {
            assert_eq!(fork, ForkNumber::FSM_FORKNUM);
            with_fake(|f| {
                f.cached_fsm = f.nfsm;
                Ok(f.nfsm)
            })
        });
        smgr_seams::smgr_cached_nblocks::set(|_rloc, fork| {
            with_fake(|f| match fork {
                ForkNumber::MAIN_FORKNUM => f.cached_main,
                ForkNumber::FSM_FORKNUM => f.cached_fsm,
                other => panic!("unexpected fork {other:?}"),
            })
        });
        smgr_seams::smgr_set_cached_nblocks::set(|_rloc, fork, v| {
            with_fake(|f| match fork {
                ForkNumber::MAIN_FORKNUM => f.cached_main = v,
                ForkNumber::FSM_FORKNUM => f.cached_fsm = v,
                other => panic!("unexpected fork {other:?}"),
            });
            Ok(())
        });

        xact_seams::get_current_transaction_id::set(|| Ok(100));
        xact_seams::is_in_parallel_mode::set(|| false);
        xact_seams::get_current_transaction_nest_level::set(|| 1);
        xloginsert_seams::xlog_insert_record::set(|_rmid, _info, _flags, _main, _bufs| {
            static LSN: AtomicU64 = AtomicU64::new(0x1000);
            Ok(LSN.fetch_add(8, Relaxed))
        });
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        catalog_seams::is_catalog_relation::set(|_rel| false);
        predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
    });
}

fn test_relation<'mcx>(mcx: mcx::Mcx<'mcx>) -> RelationData<'mcx> {
    use std::rc::Rc;
    use types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};
    let att = FormData_pg_attribute {
        attnum: 1,
        attlen: -1,
        attbyval: false,
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
    RelationData { rd_locator: Cell::new(RelFileLocator::new(1663, 5, REL_OID)), rd_smgr: Default::default(),
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

fn make_tuple(payload: usize) -> HeapTupleData<'static> {
    let t_len = 24 + payload;
    let words = t_len.div_ceil(8);
    // Leaked (test-only): the tuple borrows the buffer for 'static.
    let buf: &'static mut [u64] = Box::leak(vec![0u64; words].into_boxed_slice());
    let img = unsafe { core::slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<u8>(), t_len) };
    img[18..20].copy_from_slice(&1u16.to_ne_bytes()); // natts = 1
    img[22] = 24; // t_hoff
    // SAFETY: 8-aligned leaked image, header-complete, unique.
    unsafe {
        HeapTupleData::from_raw_parts(
            buf.as_mut_ptr().cast::<u8>(),
            t_len as u32,
            ItemPointerData::invalid(),
            0,
        )
    }
}

fn insert(payload: usize) -> ItemPointerData {
    test_boot::boot_wal("fsm_reuse");
    let ctx = MemoryContext::new("fsm_reuse");
    let rel = test_relation(ctx.mcx());
    let mut tup = make_tuple(payload);
    heap_insert(&rel, &mut tup, 7, 0, None).unwrap();
    tup.t_self
}

fn main_page_free_space(blkno: BlockNumber) -> usize {
    let addr = with_fake(|f| f.bufs[(find_buf(f, ForkNumber::MAIN_FORKNUM, blkno) - 1) as usize].2);
    // SAFETY: leaked test page, always live.
    unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.heap_free_space()
}

#[test]
fn insert_reuses_fsm_recorded_page_instead_of_extending() {
    install_seams();

    // Four t_len-1800 tuples fill block 0 to ~950 free; the fifth doesn't
    // fit, so hio's RecordAndGetPageWithFreeSpace bounce records block 0 in a
    // real FSM fork and extends to block 1.
    for off in 1..=4u16 {
        assert_eq!(insert(1776), ItemPointerData::new(0, off)); // t_len 1800
    }
    assert_eq!(with_fake(|f| f.main_extends), 1);
    assert_eq!(insert(1776), ItemPointerData::new(1, 1));
    assert_eq!(with_fake(|f| (f.main_extends, f.nfsm)), (2, 3));

    let ctx = MemoryContext::new("fsm_reuse");
    let rel = test_relation(ctx.mcx());
    let free0 = main_page_free_space(0);
    let recorded0 = freespace::GetRecordedFreeSpace(&rel, 0).unwrap();
    assert_eq!(recorded0, free0 / 32 * 32, "category rounds down");
    assert!(recorded0 >= 800, "block 0 should have ~950 free, got {recorded0}");

    // Fill block 1 (this backend's saved target block) down to < 800 free.
    for off in 2..=4u16 {
        assert_eq!(insert(1776), ItemPointerData::new(1, off));
    }
    assert_eq!(insert(576), ItemPointerData::new(1, 5)); // t_len 600
    assert!(main_page_free_space(1) < 800);

    // Fresh "backend" (thread => no saved target block): the last-block
    // fallback (block 1) is too full; the RecordAndGet bounce finds block 0
    // in the FSM leaf page. No extension.
    let tid = std::thread::spawn(|| insert(776)) // t_len 800
        .join()
        .unwrap();
    assert_eq!(
        types_tuple::ItemPointerGetBlockNumber(&tid),
        0,
        "insert should reuse FSM-recorded block 0"
    );
    assert_eq!(with_fake(|f| f.main_extends), 2, "no relation extension");

    // Block 1's shrunken free space was recorded during the bounce.
    let recorded1 = freespace::GetRecordedFreeSpace(&rel, 1).unwrap();
    assert_eq!(recorded1, main_page_free_space(1) / 32 * 32);
    assert!(recorded1 < 800);

    // Oversized requests mirror C's elog(ERROR).
    let err = freespace::GetPageWithFreeSpace(&rel, 8161).unwrap_err();
    assert!(format!("{err:?}").contains("invalid FSM request size 8161"));

    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });
}
