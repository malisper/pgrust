use super::*;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{
    BlockNumber, Buffer, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use ::types_rel::{FormData_pg_class, LockInfoData, LockRelId, RELKIND_RELATION};
use ::types_scan::sdir::ForwardScanDirection;
use ::types_snapshot::SnapshotType;
use ::types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData, LP_NORMAL, LP_REDIRECT};
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, NameData, TupleDescData, HEAP_HOT_UPDATED,
    HEAP_ONLY_TUPLE, HEAP_XMAX_INVALID,
};
use ::types_slot::TupleSlotKind;
use core::ptr::NonNull;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Mutex, Once};

const INVISIBLE_XMIN: u32 = 999;

struct Fake {
    tables: HashMap<Oid, Vec<Buffer>>,
    pages: Vec<usize>, // page base addresses; index = buffer - 1
    pins: Vec<i32>,
    locks: Vec<i32>,
}

static FAKE: Mutex<Option<Fake>> = Mutex::new(None);
// Seam-backed tests share the fake bufmgr; run them serially.
static SERIAL: Mutex<()> = Mutex::new(());
static INIT: Once = Once::new();
static SURELY_DEAD: AtomicBool = AtomicBool::new(false);
static PRUNE_CALLS: AtomicUsize = AtomicUsize::new(0);
static NEXT_OID: AtomicU32 = AtomicU32::new(23000);

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn fresh_oid() -> Oid {
    NEXT_OID.fetch_add(1, Ordering::Relaxed)
}

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
    f(g.get_or_insert_with(|| Fake {
        tables: HashMap::new(),
        pages: Vec::new(),
        pins: Vec::new(),
        locks: Vec::new(),
    }))
}

fn install_seams() {
    INIT.call_once(|| {
        bufmgr_seams::read_buffer::set(|rel, block| {
            with_fake(|f| {
                let buf = f.tables[&rel.rd_id][block as usize];
                f.pins[(buf - 1) as usize] += 1;
                Ok(buf)
            })
        });
        bufmgr_seams::read_buffer_strategy::set(|rel, block, _strategy| {
            bufmgr_seams::read_buffer::call(rel, block)
        });
        bufmgr_seams::buffer_get_block_number::set(|buf| {
            with_fake(|f| {
                for pages in f.tables.values() {
                    if let Some(i) = pages.iter().position(|b| *b == buf) {
                        return i as BlockNumber;
                    }
                }
                panic!("unknown buffer {buf}")
            })
        });
        bufmgr_seams::buffer_get_page::set(|buf| {
            let addr = with_fake(|f| {
                assert!(f.pins[(buf - 1) as usize] > 0, "page access without pin");
                f.pages[(buf - 1) as usize]
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
        bufmgr_seams::get_access_strategy::set(|_| None);
        bufmgr_seams::free_access_strategy::set(|_| {});
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|rel, _fork| {
            with_fake(|f| Ok(f.tables[&rel.rd_id].len() as BlockNumber))
        });

        heapam_visibility_seams::heap_tuple_satisfies_visibility::set(|htup, _snap, _buf| {
            Ok(htup.t_data().xmin_raw() != INVISIBLE_XMIN)
        });
        heapam_visibility_seams::heap_tuple_satisfies_mvcc_page::set(|htup, _snap, _buf, _memo| {
            Ok(htup.t_data().xmin_raw() != INVISIBLE_XMIN)
        });
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_htup, _vt| {
            Ok(SURELY_DEAD.load(Ordering::Relaxed))
        });
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_hdr| Ok(false));
        // Committed-xmax tuples read as updated (lock path) and dead (dirty).
        heapam_visibility_seams::heap_tuple_satisfies_update::set(|htup, _cid, _buf| {
            let hdr = htup.t_data();
            if hdr.xmin_raw() == INVISIBLE_XMIN {
                Ok(::tableam_vocab::TM_Result::TM_Invisible)
            } else if (hdr.t_infomask & HEAP_XMAX_INVALID) == 0 && hdr.xmax_raw() != 0 {
                Ok(::tableam_vocab::TM_Result::TM_Updated)
            } else {
                Ok(::tableam_vocab::TM_Result::TM_Ok)
            }
        });
        heapam_visibility_seams::heap_tuple_satisfies_dirty::set(|htup, _snap, _buf| {
            let hdr = htup.t_data();
            Ok((hdr.t_infomask & HEAP_XMAX_INVALID) != 0 || hdr.xmax_raw() == 0)
        });
        xact_seams::transaction_id_is_current_transaction_id::set(|_xid| false);

        predicate_seams::check_for_serializable_conflict_out_needed::set(|_rel, _snap| Ok(false));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        predicate_seams::predicate_lock_relation::set(|_rel, _snap| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_rel, _tid, _snap, _xid| Ok(()));

        pruneheap_seams::heap_page_prune_opt::set(|_rel, _buf| {
            PRUNE_CALLS.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
        procarray_seams::global_vis_test_for::set(|_rel| {
            ::types_core::GlobalVisStateHandle::new(0)
        });
    });
}

fn quiesced() {
    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });
}

fn pins_of(rel_oid: Oid, block: usize) -> i32 {
    with_fake(|f| {
        let buf = f.tables[&rel_oid][block];
        f.pins[(buf - 1) as usize]
    })
}

// --- page/tuple builders (heapam test fixture shape) ---

enum Item {
    Tuple(Vec<u8>),
    Redirect(u16),
}

fn tuple_image(xmin: u32, xmax: u32, val: i32) -> Vec<u8> {
    let mut img = vec![0u8; 28];
    img[0..4].copy_from_slice(&xmin.to_ne_bytes());
    img[4..8].copy_from_slice(&xmax.to_ne_bytes());
    img[18..20].copy_from_slice(&1u16.to_ne_bytes()); // natts = 1
    img[20..22].copy_from_slice(&HEAP_XMAX_INVALID.to_ne_bytes());
    img[22] = 24; // t_hoff
    img[24..28].copy_from_slice(&val.to_ne_bytes());
    img
}

fn set_ctid(img: &mut [u8], block: u32, off: u16) {
    img[12..14].copy_from_slice(&((block >> 16) as u16).to_ne_bytes());
    img[14..16].copy_from_slice(&(block as u16).to_ne_bytes());
    img[16..18].copy_from_slice(&off.to_ne_bytes());
}

fn set_infomask(img: &mut [u8], infomask: u16, infomask2_or: u16) {
    let m2 = u16::from_ne_bytes([img[18], img[19]]) | infomask2_or;
    img[18..20].copy_from_slice(&m2.to_ne_bytes());
    img[20..22].copy_from_slice(&infomask.to_ne_bytes());
}

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

fn build_page(items: &[Item]) -> Box<TestPage> {
    let mut page = Box::new(TestPage([0u8; BLCKSZ]));
    let n = items.len();
    let lower = SizeOfPageHeaderData + n * 4;
    let mut upper = BLCKSZ;
    for (i, item) in items.iter().enumerate() {
        let id = match item {
            Item::Tuple(img) => {
                let len = img.len();
                upper = (upper - len) & !7; // MAXALIGN down
                page.0[upper..upper + len].copy_from_slice(img);
                ItemIdData::new(upper as u16, LP_NORMAL, len as u16)
            }
            Item::Redirect(link) => ItemIdData::new(*link, LP_REDIRECT, 0),
        };
        let off = SizeOfPageHeaderData + i * 4;
        // SAFETY: repr(transparent) over u32.
        let raw: u32 = unsafe { core::mem::transmute::<ItemIdData, u32>(id) };
        page.0[off..off + 4].copy_from_slice(&raw.to_ne_bytes());
    }
    page.0[12..14].copy_from_slice(&(lower as u16).to_ne_bytes());
    page.0[14..16].copy_from_slice(&(upper as u16).to_ne_bytes());
    page.0[16..18].copy_from_slice(&(BLCKSZ as u16).to_ne_bytes());
    page.0[18..20].copy_from_slice(&((BLCKSZ as u16) | 4).to_ne_bytes());
    page
}

fn register_table(relid: Oid, pages: Vec<Box<TestPage>>) {
    with_fake(|f| {
        let mut bufs = Vec::new();
        for p in pages {
            let addr = Box::leak(p).0.as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(0);
            f.locks.push(0);
            bufs.push(f.pages.len() as Buffer);
        }
        f.tables.insert(relid, bufs);
    });
}

// --- relation / snapshot / slot fixtures ---

fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        attlen: 4,
        attbyval: true,
        attalign: ::types_tuple::TYPALIGN_INT,
        attstorage: ::types_tuple::TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn test_relation<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: ::tableam_vocab::HEAP_TABLE_AM_OID,
        relfilenode: oid,
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
    };
    let data = ::types_rel::RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: std::cell::Cell::new(true),
        rd_createSubid: std::cell::Cell::new(0),
        rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_droppedSubid: std::cell::Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: oid, dbId: 5 },
        },
        rd_rel,
        rd_att: int4_tupdesc(mcx),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
        pgstat_enabled: std::cell::Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
    };
    Relation::open(data, None)
}

fn mvcc_snapshot<'mcx>(mcx: Mcx<'mcx>) -> Snapshot<'mcx> {
    Some(Rc::new(SnapshotData::sentinel(
        mcx,
        SnapshotType::SNAPSHOT_MVCC,
    )))
}

fn buffer_slot<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> SlotData<'mcx> {
    exectuples::make_tuple_table_slot(
        mcx,
        TupleSlotKind::BufferHeapTuple,
        Some(rel.rd_att.clone()),
    )
}

fn slot_val(slot: &SlotData<'_>) -> i32 {
    let SlotData::BufferHeap(b) = slot else {
        panic!("not a buffer slot")
    };
    let t = b.base.tuple.as_ref().expect("empty slot");
    let hoff = t.t_data().t_hoff as usize;
    // SAFETY: test image built by tuple_image: int4 at t_hoff.
    unsafe { core::ptr::read_unaligned(t.header_ptr().add(hoff).cast::<i32>()) }
}

// --- tests ---

#[test]
fn index_fetch_hot_chain_lifecycle() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();

    // off1: redirect -> off2; off2: invisible, HOT-updated -> off3;
    // off3: visible heap-only tuple, end of chain.
    let mut t2 = tuple_image(INVISIBLE_XMIN, 20, 2);
    set_infomask(&mut t2, 0, HEAP_HOT_UPDATED);
    set_ctid(&mut t2, 0, 3);
    let mut t3 = tuple_image(20, 0, 3);
    set_infomask(&mut t3, HEAP_XMAX_INVALID, HEAP_ONLY_TUPLE);
    set_ctid(&mut t3, 0, 3);
    register_table(
        oid,
        vec![build_page(&[Item::Redirect(2), Item::Tuple(t2), Item::Tuple(t3)])],
    );
    let rel = test_relation(mcx, oid);
    let snap = mvcc_snapshot(mcx);
    let mut slot = buffer_slot(mcx, &rel);

    let mut hscan = heapam_index_fetch_begin(&rel);
    assert!(hscan.xs_cbuf.is_none());

    let prune_before = PRUNE_CALLS.load(Ordering::Relaxed);
    let mut tid = ItemPointerData::new(0, 1);
    let mut call_again = false;
    let mut all_dead = false;
    let found = heapam_index_fetch_tuple(
        mcx,
        &mut hscan,
        &mut tid,
        &snap,
        &mut slot,
        &mut call_again,
        Some(&mut all_dead),
    )
    .unwrap();

    assert!(found);
    // C mutates the caller tid to the resolved HOT member.
    assert_eq!(tid, ItemPointerData::new(0, 3));
    assert!(!call_again); // MVCC snapshot: single visible member
    assert!(!all_dead);
    assert_eq!(slot_val(&slot), 3);
    assert_eq!(slot.base().tts_tid, ItemPointerData::new(0, 3));
    assert_eq!(slot.base().tts_tableOid, oid);
    // Descriptor pin + the slot's own pin (ExecStoreBufferHeapTuple).
    assert_eq!(pins_of(oid, 0), 2);

    // Same page again: ReleaseAndReadBuffer keeps the pin, no re-prune.
    let mut tid2 = ItemPointerData::new(0, 1);
    let mut call_again2 = false;
    let found2 = heapam_index_fetch_tuple(
        mcx,
        &mut hscan,
        &mut tid2,
        &snap,
        &mut slot,
        &mut call_again2,
        None,
    )
    .unwrap();
    assert!(found2);
    assert_eq!(PRUNE_CALLS.load(Ordering::Relaxed), prune_before + 1);

    exectuples::exec_clear_tuple(&mut slot, mcx);
    assert_eq!(pins_of(oid, 0), 1);
    heapam_index_fetch_reset(&mut hscan);
    assert!(hscan.xs_cbuf.is_none());
    assert_eq!(pins_of(oid, 0), 0);

    // end() releases a still-held pin.
    let mut tid3 = ItemPointerData::new(0, 3);
    let mut call_again3 = false;
    heapam_index_fetch_tuple(
        mcx,
        &mut hscan,
        &mut tid3,
        &snap,
        &mut slot,
        &mut call_again3,
        None,
    )
    .unwrap();
    exectuples::exec_clear_tuple(&mut slot, mcx);
    heapam_index_fetch_end(hscan);
    quiesced();
}

#[test]
fn index_fetch_dead_chain_reports_all_dead() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(INVISIBLE_XMIN, 0, 1))])],
    );
    let rel = test_relation(mcx, oid);
    let snap = mvcc_snapshot(mcx);
    let mut slot = buffer_slot(mcx, &rel);

    SURELY_DEAD.store(true, Ordering::Relaxed);
    let mut hscan = heapam_index_fetch_begin(&rel);
    let mut tid = ItemPointerData::new(0, 1);
    let mut call_again = false;
    let mut all_dead = false;
    let found = heapam_index_fetch_tuple(
        mcx,
        &mut hscan,
        &mut tid,
        &snap,
        &mut slot,
        &mut call_again,
        Some(&mut all_dead),
    )
    .unwrap();
    SURELY_DEAD.store(false, Ordering::Relaxed);

    assert!(!found);
    assert!(all_dead);
    assert!(!call_again);
    assert_eq!(tid, ItemPointerData::new(0, 1)); // unchanged on miss
    heapam_index_fetch_end(hscan);
    quiesced();
}

#[test]
fn index_fetch_batch_lifecycle() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();

    let mut t2 = tuple_image(INVISIBLE_XMIN, 0, 2);
    set_infomask(&mut t2, HEAP_XMAX_INVALID, 0);
    register_table(
        oid,
        vec![build_page(&[
            Item::Tuple(tuple_image(20, 0, 1)),
            Item::Tuple(t2),
            Item::Tuple(tuple_image(20, 0, 3)),
        ])],
    );
    let rel = test_relation(mcx, oid);
    let snap = mvcc_snapshot(mcx);
    let mut slot = buffer_slot(mcx, &rel);

    let mut hscan = heapam_index_fetch_begin(&rel);
    let prune_before = PRUNE_CALLS.load(Ordering::Relaxed);
    let first = ItemPointerData::new(0, 1);
    let rest = [ItemPointerData::new(0, 2), ItemPointerData::new(0, 3)];
    heapam_index_fetch_batch_fill(mcx, &mut hscan, &first, &rest, &snap).unwrap();
    assert_eq!(PRUNE_CALLS.load(Ordering::Relaxed), prune_before + 1);

    let mut tid = ItemPointerData::new(0, 1);
    assert!(matches!(
        heapam_index_fetch_batch_next(mcx, &mut hscan, &mut tid, &mut slot),
        BatchFetch::Stored
    ));
    assert_eq!(tid, ItemPointerData::new(0, 1));
    assert_eq!(slot_val(&slot), 1);
    assert_eq!(slot.base().tts_tableOid, oid);

    let mut tid = ItemPointerData::new(0, 2);
    assert!(matches!(
        heapam_index_fetch_batch_next(mcx, &mut hscan, &mut tid, &mut slot),
        BatchFetch::NotVisible { all_dead: false }
    ));

    let mut tid = ItemPointerData::new(0, 3);
    assert!(matches!(
        heapam_index_fetch_batch_next(mcx, &mut hscan, &mut tid, &mut slot),
        BatchFetch::Stored
    ));
    assert_eq!(slot_val(&slot), 3);

    let mut tid = ItemPointerData::new(0, 3);
    assert!(matches!(
        heapam_index_fetch_batch_next(mcx, &mut hscan, &mut tid, &mut slot),
        BatchFetch::Miss
    ));

    heapam_index_fetch_batch_fill(mcx, &mut hscan, &first, &rest, &snap).unwrap();
    let mut tid = ItemPointerData::new(0, 3);
    assert!(matches!(
        heapam_index_fetch_batch_next(mcx, &mut hscan, &mut tid, &mut slot),
        BatchFetch::Miss
    ));
    let mut tid = ItemPointerData::new(0, 1);
    assert!(matches!(
        heapam_index_fetch_batch_next(mcx, &mut hscan, &mut tid, &mut slot),
        BatchFetch::Miss
    ));

    heapam_index_fetch_batch_fill(mcx, &mut hscan, &first, &rest, &snap).unwrap();
    heapam_index_fetch_reset(&mut hscan);
    let mut tid = ItemPointerData::new(0, 1);
    assert!(matches!(
        heapam_index_fetch_batch_next(mcx, &mut hscan, &mut tid, &mut slot),
        BatchFetch::Miss
    ));

    exectuples::exec_clear_tuple(&mut slot, mcx);
    heapam_index_fetch_end(hscan);
    quiesced();
}

#[test]
fn index_fetch_batch_all_dead_entry() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[
            Item::Tuple(tuple_image(INVISIBLE_XMIN, 0, 1)),
            Item::Tuple(tuple_image(20, 0, 2)),
        ])],
    );
    let rel = test_relation(mcx, oid);
    let snap = mvcc_snapshot(mcx);
    let mut slot = buffer_slot(mcx, &rel);

    SURELY_DEAD.store(true, Ordering::Relaxed);
    let mut hscan = heapam_index_fetch_begin(&rel);
    let first = ItemPointerData::new(0, 1);
    let rest = [ItemPointerData::new(0, 2)];
    heapam_index_fetch_batch_fill(mcx, &mut hscan, &first, &rest, &snap).unwrap();
    SURELY_DEAD.store(false, Ordering::Relaxed);

    let mut tid = ItemPointerData::new(0, 1);
    assert!(matches!(
        heapam_index_fetch_batch_next(mcx, &mut hscan, &mut tid, &mut slot),
        BatchFetch::NotVisible { all_dead: true }
    ));
    let mut tid = ItemPointerData::new(0, 2);
    assert!(matches!(
        heapam_index_fetch_batch_next(mcx, &mut hscan, &mut tid, &mut slot),
        BatchFetch::Stored
    ));
    exectuples::exec_clear_tuple(&mut slot, mcx);
    heapam_index_fetch_end(hscan);
    quiesced();
}

#[test]
fn fetch_row_version_transfers_pin() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[
            Item::Tuple(tuple_image(10, 0, 7)),
            Item::Tuple(tuple_image(INVISIBLE_XMIN, 0, 8)),
        ])],
    );
    let rel = test_relation(mcx, oid);
    let snap = mvcc_snapshot(mcx);
    let mut slot = buffer_slot(mcx, &rel);

    let found =
        heapam_fetch_row_version(mcx, &rel, &ItemPointerData::new(0, 1), &snap, &mut slot)
            .unwrap();
    assert!(found);
    // Pin transferred: exactly the slot's pin remains.
    assert_eq!(pins_of(oid, 0), 1);
    assert_eq!(slot_val(&slot), 7);
    assert_eq!(slot.base().tts_tableOid, oid);
    assert_eq!(slot.base().tts_tid, ItemPointerData::new(0, 1));

    // Visible slot content satisfies the snapshot (share lock balanced).
    assert!(heapam_tuple_satisfies_snapshot(&rel, &mut slot, &snap).unwrap());

    exectuples::exec_clear_tuple(&mut slot, mcx);
    assert_eq!(pins_of(oid, 0), 0);

    // Invisible version: not found, nothing pinned.
    let found =
        heapam_fetch_row_version(mcx, &rel, &ItemPointerData::new(0, 2), &snap, &mut slot)
            .unwrap();
    assert!(!found);
    quiesced();
}

#[test]
fn tid_valid_and_latest_tid_on_scan() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))])]);
    let rel = test_relation(mcx, oid);

    let mut scan = ::heapam::heap_beginscan(
        mcx,
        &rel,
        mvcc_snapshot(mcx),
        0,
        PgVec::new_in(mcx),
        None,
        ::tableam_vocab::SO_TYPE_TIDSCAN,
    )
    .unwrap();

    assert!(heapam_tuple_tid_valid(&scan, &ItemPointerData::new(0, 1)));
    assert!(!heapam_tuple_tid_valid(&scan, &ItemPointerData::new(1, 1)));
    assert!(!heapam_tuple_tid_valid(&scan, &ItemPointerData::invalid()));

    let mut tid = ItemPointerData::new(0, 1);
    heapam_tuple_get_latest_tid(&mut scan, &mut tid).unwrap();
    assert_eq!(tid, ItemPointerData::new(0, 1)); // self-pointing ctid: no chase

    ::heapam::heap_endscan(scan).unwrap();
    quiesced();
}

// The flipped tableam read lane end-to-end: beginscan -> getnextslot loop ->
// endscan over the enum dispatch, buffer slot pinning per tuple.
#[test]
fn tableam_seqscan_composition() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![
            build_page(&[
                Item::Tuple(tuple_image(10, 0, 1)),
                Item::Tuple(tuple_image(INVISIBLE_XMIN, 0, 99)),
            ]),
            build_page(&[Item::Tuple(tuple_image(11, 0, 2))]),
        ],
    );
    let rel = test_relation(mcx, oid);
    let mut slot = ::tableam::table_slot_create(mcx, &rel).unwrap();
    assert!(matches!(slot, SlotData::BufferHeap(_)));

    let mut scan =
        ::tableam::table_beginscan(mcx, &rel, mvcc_snapshot(mcx), 0, PgVec::new_in(mcx)).unwrap();

    let mut vals = Vec::new();
    while ::tableam::table_scan_getnextslot(mcx, &mut scan, ForwardScanDirection, &mut slot)
        .unwrap()
    {
        vals.push(slot_val(&slot));
    }
    assert_eq!(vals, vec![1, 2]);

    // TID validity consults the scan's nblocks through the dispatch layer.
    assert!(::tableam::table_tuple_tid_valid(&mut scan, &ItemPointerData::new(1, 1)));
    assert!(!::tableam::table_tuple_tid_valid(&mut scan, &ItemPointerData::new(2, 1)));

    ::tableam::table_endscan(scan).unwrap();
    exectuples::exec_clear_tuple(&mut slot, mcx);
    quiesced();
}

#[test]
fn tuple_lock_chase_dirty_fail_keeps_pin_and_reports_deleted() {
    // keep_buf dirty-fail arm: xmin/t_ctid must be read through the
    // still-held pin (C reads t_data before ReleaseBuffer) -> TM_Deleted.
    use ::tableam_vocab::{
        LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result,
        TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
    };

    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();

    // lp1 updated by committed 200 -> lp2; lp2 deleted by committed 201
    // with self-pointing ctid, so it fails the dirty snapshot.
    let mut v2 = tuple_image(100, 200, 2);
    set_infomask(&mut v2, 0, 0);
    set_ctid(&mut v2, 0, 2);
    let mut v3 = tuple_image(200, 201, 3);
    set_infomask(&mut v3, 0, 0);
    set_ctid(&mut v3, 0, 2);
    register_table(oid, vec![build_page(&[Item::Tuple(v2), Item::Tuple(v3)])]);

    let rel = test_relation(mcx, oid);
    let snap = mvcc_snapshot(mcx);
    let mut slot = buffer_slot(mcx, &rel);
    let mut tmfd = TM_FailureData::default();
    let r = heapam_tuple_lock(
        mcx,
        &rel,
        &ItemPointerData::new(0, 1),
        &snap,
        &mut slot,
        7,
        LockTupleMode::LockTupleExclusive,
        LockWaitPolicy::LockWaitBlock,
        TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
        &mut tmfd,
    )
    .unwrap();
    assert_eq!(r, TM_Result::TM_Deleted);
    assert!(tmfd.traversed);
    quiesced();
}
