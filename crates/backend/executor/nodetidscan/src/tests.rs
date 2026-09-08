// audit-18.6 w2-011 (row nodeTidscan-d312a2fd): TidNext runs
// CHECK_FOR_INTERRUPTS() after every rejected TID (nodeTidscan.c:389), so a
// cancel raised while a long list of invisible/dead TIDs is walked surfaces
// from inside the walk — not after the whole list has been checked. The rig
// serves one fake heap page whose tuples the visibility seam rejects, arms
// InterruptPending, and expects C's 57014 after the FIRST rejected TID.
use super::*;

use core::ptr::NonNull;
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, Once};

use ::mcx::MemoryContext;
use ::types_core::{Buffer, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use ::types_error::ERRCODE_QUERY_CANCELED;
use ::types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, RELKIND_RELATION,
};
use ::types_slot::TupleSlotKind;
use ::types_snapshot::{SnapshotData, SnapshotType};
use ::types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData, LP_NORMAL};
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, NameData, TupleDescData, HEAP_XMAX_INVALID,
    TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

const INT4OID: Oid = 23;

struct Fake {
    tables: HashMap<Oid, Vec<Buffer>>,
    reads: HashMap<Oid, u32>,
    pages: Vec<usize>,
    pins: Vec<i32>,
}

static FAKE: Mutex<Option<Fake>> = Mutex::new(None);
static SERIAL: Mutex<()> = Mutex::new(());
static INIT: Once = Once::new();
static CFI_CALLS: AtomicU32 = AtomicU32::new(0);

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
    f(g.get_or_insert_with(|| Fake {
        tables: HashMap::new(),
        reads: HashMap::new(),
        pages: Vec::new(),
        pins: Vec::new(),
    }))
}

fn install_seams() {
    INIT.call_once(|| {
        bufmgr_seams::read_buffer::set(|rel, block| {
            with_fake(|f| {
                let buf = f.tables[&rel.rd_id][block as usize];
                f.pins[(buf - 1) as usize] += 1;
                *f.reads.entry(rel.rd_id).or_insert(0) += 1;
                Ok(buf)
            })
        });
        bufmgr_seams::release_buffer::set(|buf| {
            with_fake(|f| {
                let p = &mut f.pins[(buf - 1) as usize];
                assert!(*p > 0, "double release of buffer {buf}");
                *p -= 1;
            });
            Ok(())
        });
        bufmgr_seams::buffer_get_block_number::set(|buf| {
            with_fake(|f| {
                for pages in f.tables.values() {
                    if let Some(i) = pages.iter().position(|b| *b == buf) {
                        return i as u32;
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
        bufmgr_seams::incr_buffer_ref_count::set(|buf| {
            with_fake(|f| f.pins[(buf - 1) as usize] += 1);
        });
        bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));

        predicate_seams::predicate_lock_tid::set(|_rel, _tid, _snap, _xid| Ok(()));
        predicate_seams::check_for_serializable_conflict_out_needed::set(|_rel, _snap| Ok(false));

        // Every fetched version fails the time qual: the TID walk rejects
        // each entry and loops (nodeTidscan.c:381-389).
        heapam_visibility_seams::heap_tuple_satisfies_visibility::set(
            |_htup, _snap, _buf| Ok(false),
        );

        // CHECK_FOR_INTERRUPTS() -> ProcessInterrupts (postgres.c): the seam
        // only runs while InterruptPending is set; a pending cancel is 57014.
        postgres_seams::check_for_interrupts::set(|| {
            CFI_CALLS.fetch_add(1, Ordering::SeqCst);
            Err(Box::new(
                PgError::error("canceling statement due to user request")
                    .with_sqlstate(ERRCODE_QUERY_CANCELED),
            ))
        });
    });
}

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

fn tuple_image(val: i32) -> Vec<u8> {
    let mut img = vec![0u8; 28];
    img[0..4].copy_from_slice(&10u32.to_ne_bytes()); // xmin
    img[18..20].copy_from_slice(&1u16.to_ne_bytes()); // natts = 1
    img[20..22].copy_from_slice(&HEAP_XMAX_INVALID.to_ne_bytes());
    img[22] = 24; // t_hoff
    img[24..28].copy_from_slice(&val.to_ne_bytes());
    img
}

fn build_heap_page(vals: &[i32]) -> Box<TestPage> {
    let mut page = Box::new(TestPage([0u8; BLCKSZ]));
    let n = vals.len();
    let lower = SizeOfPageHeaderData + n * 4;
    let mut upper = BLCKSZ;
    for (i, val) in vals.iter().enumerate() {
        let img = tuple_image(*val);
        upper = (upper - img.len()) & !7;
        page.0[upper..upper + img.len()].copy_from_slice(&img);
        let id = ItemIdData::new(upper as u16, LP_NORMAL, img.len() as u16);
        let off = SizeOfPageHeaderData + i * 4;
        // SAFETY: repr(transparent) over u32.
        let raw: u32 = unsafe { core::mem::transmute(id) };
        page.0[off..off + 4].copy_from_slice(&raw.to_ne_bytes());
    }
    page.0[12..14].copy_from_slice(&(lower as u16).to_ne_bytes());
    page.0[14..16].copy_from_slice(&(upper as u16).to_ne_bytes());
    page.0[16..18].copy_from_slice(&(BLCKSZ as u16).to_ne_bytes());
    page.0[18..20].copy_from_slice(&((BLCKSZ as u16) | 4).to_ne_bytes());
    page
}

fn register_pages(relid: Oid, pages: Vec<Box<TestPage>>) {
    with_fake(|f| {
        let mut bufs = Vec::new();
        for p in pages {
            let addr = Box::leak(p).0.as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(0);
            bufs.push(f.pages.len() as Buffer);
        }
        f.tables.insert(relid, bufs);
    });
}

fn heap_reads(heap_oid: Oid) -> u32 {
    with_fake(|f| f.reads.get(&heap_oid).copied().unwrap_or(0))
}

fn quiesced() {
    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
    });
}

fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: INT4OID,
        attlen: 4,
        attbyval: true,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn heap_relation<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: tableam::HEAP_TABLE_AM_OID,
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
    let data = RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId {
                relId: oid,
                dbId: 5,
            },
        },
        rd_rel,
        rd_att: int4_tupdesc(mcx),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
    };
    Relation::open(data, None)
}

static NEXT_OID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(91000);
fn fresh_oid() -> Oid {
    NEXT_OID.fetch_add(1, Ordering::Relaxed)
}

fn with_mcx<R>(f: impl for<'m> FnOnce(Mcx<'m>) -> R) -> R {
    install_seams();
    let ctx = MemoryContext::new("nodetidscan-test");
    f(ctx.mcx())
}

// A Tid Scan over `ntids` TIDs on heap block 0 (all rejected by the
// visibility seam), TID list already evaluated (tid_list_eval's product),
// as TidNext sees it on its first call.
fn rig<'mcx>(mcx: Mcx<'mcx>, ntids: usize) -> (Oid, EStateData<'mcx>, TidScanState<'mcx>) {
    let heap_oid = fresh_oid();
    let vals: Vec<i32> = (1..=ntids as i32).collect();
    register_pages(heap_oid, vec![build_heap_page(&vals)]);
    let rel = heap_relation(mcx, heap_oid);
    let mut estate = EStateData::new_in(mcx);
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap-test")));
    estate.es_snapshot = Some(Rc::new(SnapshotData::sentinel(
        ctx.mcx(),
        SnapshotType::SNAPSHOT_MVCC,
    )));
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ss_ScanTupleSlot = estate
        .exec_init_extra_tuple_slot(Some(rel.rd_att.clone()), TupleSlotKind::BufferHeapTuple);
    let mut tids: PgVec<'mcx, ItemPointerData> = PgVec::new_in(mcx);
    for i in 1..=ntids {
        tids.push(ItemPointerData::new(0, i as u16));
    }
    let state = TidScanState {
        ss: ScanState {
            qual: None,
            ps_ProjInfo: None,
            ps_ExprContext,
            scanrelid: 1,
            ss_currentRelation: Some(rel),
            ss_currentScanDesc: None,
            ss_ScanTupleSlot,
            instr_idx: None,
        },
        tss_isCurrentOf: false,
        tss_TidPtr: -1,
        tss_TidList: Some(tids),
        tss_tidexprs: PgVec::new_in(mcx),
    };
    (heap_oid, estate, state)
}

fn with_cancel_pending<R>(f: impl FnOnce() -> R) -> R {
    CFI_CALLS.store(0, Ordering::SeqCst);
    init_small::globals::SetInterruptPending(true);
    let r = f();
    init_small::globals::SetInterruptPending(false);
    r
}

#[test]
fn tid_walk_over_rejected_tids_is_cancellable() {
    let _g = serial();
    with_mcx(|mcx| {
        let (heap_oid, mut estate, mut state) = rig(mcx, 8);
        let err = with_cancel_pending(|| state.scan_next(&mut estate))
            .expect_err("TidNext must honour a pending cancel (nodeTidscan.c:389)");
        assert_eq!(err.sqlstate(), ERRCODE_QUERY_CANCELED);
        // C checks right after the first rejected TID, before fetching the
        // second: exactly one heap fetch and one ProcessInterrupts call.
        assert_eq!(CFI_CALLS.load(Ordering::SeqCst), 1);
        assert_eq!(heap_reads(heap_oid), 1, "cancel surfaced after the FIRST rejected TID");
        assert_eq!(state.tss_TidPtr, 1);
        estate.exec_reset_tuple_table(false);
        quiesced();
    });
}

#[test]
fn tid_walk_completes_without_a_pending_interrupt() {
    let _g = serial();
    with_mcx(|mcx| {
        let (heap_oid, mut estate, mut state) = rig(mcx, 8);
        CFI_CALLS.store(0, Ordering::SeqCst);
        let found = state.scan_next(&mut estate).expect("no interrupt pending: walk completes");
        assert!(!found, "every TID is rejected by the time qual");
        assert_eq!(heap_reads(heap_oid), 8);
        assert_eq!(CFI_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(state.tss_TidPtr, 8);
        estate.exec_reset_tuple_table(false);
        quiesced();
    });
}
