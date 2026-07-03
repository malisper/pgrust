use super::*;
use ::mcx::MemoryContext;
use ::types_core::{InvalidBuffer, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use ::types_rel::{FormData_pg_class, LockInfoData, LockRelId, RELKIND_RELATION};
use ::types_scan::sdir::{BackwardScanDirection, ForwardScanDirection};
use ::types_snapshot::SnapshotType;
use ::types_storage::bufpage::{
    ItemIdData, SizeOfPageHeaderData, LP_DEAD, LP_NORMAL, LP_REDIRECT, LP_UNUSED,
    PD_ALL_VISIBLE,
};
use ::types_tuple::{NameData, TupleDescData, FormData_pg_attribute, CompactAttribute};
use datum::Datum;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}
static VIS_CALLS: AtomicUsize = AtomicUsize::new(0);
static INIT: Once = Once::new();

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
            VIS_CALLS.fetch_add(1, Ordering::Relaxed);
            Ok(htup.t_data().xmin_raw() != INVISIBLE_XMIN)
        });
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_htup, _vt| Ok(false));
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_hdr| Ok(false));

        predicate_seams::check_for_serializable_conflict_out_needed::set(|_rel, _snap| false);
        predicate_seams::predicate_lock_relation::set(|_rel, _snap| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_rel, _tid, _snap, _xid| Ok(()));

        pruneheap_seams::heap_page_prune_opt::set(|_rel, _buf| Ok(()));
        procarray_seams::global_vis_test_for::set(|_rel| ::types_core::GlobalVisStateHandle::new(0));
    });
}

fn quiesced() {
    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });
}

// --- page/tuple builders ---

enum Item {
    Tuple(Vec<u8>),
    Redirect(u16),
    Dead,
    Unused,
}

fn tuple_image(xmin: u32, xmax: u32, val: i32) -> Vec<u8> {
    let mut img = vec![0u8; 28];
    img[0..4].copy_from_slice(&xmin.to_ne_bytes());
    img[4..8].copy_from_slice(&xmax.to_ne_bytes());
    // t_ctid points at self by default; tests overwrite for update chains.
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
pub struct TestPage([u8; BLCKSZ]);

impl core::ops::Deref for TestPage {
    type Target = [u8; BLCKSZ];
    fn deref(&self) -> &[u8; BLCKSZ] {
        &self.0
    }
}
impl core::ops::DerefMut for TestPage {
    fn deref_mut(&mut self) -> &mut [u8; BLCKSZ] {
        &mut self.0
    }
}

fn build_page(items: &[Item], all_visible: bool) -> Box<TestPage> {
    let mut page = Box::new(TestPage([0u8; BLCKSZ]));
    let n = items.len();
    let mut lower = SizeOfPageHeaderData + n * 4;
    let mut upper = BLCKSZ;
    for (i, item) in items.iter().enumerate() {
        let id = match item {
            Item::Tuple(img) => {
                let len = img.len();
                upper = (upper - len) & !7; // MAXALIGN down
                page[upper..upper + len].copy_from_slice(img);
                ItemIdData::new(upper as u16, LP_NORMAL, len as u16)
            }
            Item::Redirect(link) => ItemIdData::new(*link, LP_REDIRECT, 0),
            Item::Dead => ItemIdData::new(0, LP_DEAD, 0),
            Item::Unused => ItemIdData::new(0, LP_UNUSED, 0),
        };
        let off = SizeOfPageHeaderData + i * 4;
        // SAFETY: repr(transparent) over u32.
        let raw: u32 = unsafe { core::mem::transmute(id) };
        page[off..off + 4].copy_from_slice(&raw.to_ne_bytes());
    }
    let flags: u16 = if all_visible { PD_ALL_VISIBLE } else { 0 };
    page[10..12].copy_from_slice(&flags.to_ne_bytes());
    page[12..14].copy_from_slice(&(lower as u16).to_ne_bytes());
    page[14..16].copy_from_slice(&(upper as u16).to_ne_bytes());
    page[16..18].copy_from_slice(&(BLCKSZ as u16).to_ne_bytes());
    page[18..20].copy_from_slice(&((BLCKSZ as u16) | 4).to_ne_bytes());
    lower = lower.max(SizeOfPageHeaderData);
    let _ = lower;
    page
}

fn register_table(relid: Oid, pages: Vec<Box<TestPage>>) {
    with_fake(|f| {
        let mut bufs = Vec::new();
        for p in pages {
            let addr = Box::leak(p).as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(0);
            f.locks.push(0);
            bufs.push(f.pages.len() as Buffer);
        }
        f.tables.insert(relid, bufs);
    });
}

// --- relation / snapshot fixtures ---

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
        pgstat_enabled: std::cell::Cell::new(true),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_supportinfo: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false,
    };
    Relation::open(data, None)
}

fn mvcc_snapshot<'mcx>(mcx: Mcx<'mcx>) -> Snapshot<'mcx> {
    Some(Rc::new(SnapshotData::sentinel(
        mcx,
        SnapshotType::SNAPSHOT_MVCC,
    )))
}

fn begin_seqscan<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
) -> HeapScanDescData<'mcx> {
    let flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;
    heap_beginscan(mcx, rel, snapshot, 0, PgVec::new_in(mcx), None, flags).unwrap()
}

fn collect_vals(scan: &mut HeapScanDescData<'_>, dir: ScanDirection) -> Vec<(u32, u16, i32)> {
    let mut out = Vec::new();
    while let Some(t) = heap_getnext(scan, dir).unwrap() {
        let val = i32::from_ne_bytes(
            // SAFETY: test tuples are hoff(24)+int4.
            unsafe { core::slice::from_raw_parts(t.getstruct(), 4) }
                .try_into()
                .unwrap(),
        );
        out.push((
            ItemPointerGetBlockNumberNoCheck(&t.t_self),
            t.t_self.ip_posid,
            val,
        ));
    }
    out
}

static NEXT_OID: AtomicUsize = AtomicUsize::new(50000);
fn fresh_oid() -> Oid {
    NEXT_OID.fetch_add(1, Ordering::Relaxed) as Oid
}

// --- tests ---

// The page-borrow kernel: PageRef + on-image tuple over raw memory, hint-bit
// write racing a second reader view. Run under Miri.
#[test]
fn kernel_page_borrow() {
    let mut page = build_page(
        &[
            Item::Tuple(tuple_image(10, 0, 7)),
            Item::Unused,
            Item::Tuple(tuple_image(11, 0, 8)),
        ],
        false,
    );
    let ptr = NonNull::new(page.as_mut_ptr()).unwrap();
    // SAFETY: local BLCKSZ buffer, lives for the test.
    let view = unsafe { PageRef::from_raw(ptr) };
    assert_eq!(view.max_offset_number(), 3);
    assert!(!view.is_all_visible());

    let id1 = view.item_id(1);
    assert!(id1.is_normal());
    let (p1, l1) = view.item_raw(id1);
    // SAFETY: item within the live page.
    let mut t1 = unsafe {
        HeapTupleData::from_raw_parts(p1, l1, ItemPointerData::new(0, 1), 1)
    };
    assert_eq!(t1.t_data().xmin_raw(), 10);
    t1.t_data_mut().set_xmin_committed(); // the tolerated hint-bit store

    assert!(!view.item_id(2).is_normal());
    let id3 = view.item_id(3);
    let (p3, l3) = view.item_raw(id3);
    // SAFETY: as above.
    let t3 = unsafe {
        HeapTupleData::from_raw_parts(p3, l3, ItemPointerData::new(0, 3), 1)
    };
    assert_eq!(t3.t_data().xmin_raw(), 11);
    // A second view still reads the page (and sees the hint bit).
    let view2 = unsafe { PageRef::from_raw(ptr) };
    let (q1, m1) = view2.item_raw(view2.item_id(1));
    let r1 = unsafe { HeapTupleData::from_raw_parts(q1, m1, ItemPointerData::new(0, 1), 1) };
    assert!(r1.t_data().xmin_committed());
}

#[test]
fn seqscan_pagemode_forward_backward_rescan() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![
            build_page(
                &[
                    Item::Tuple(tuple_image(10, 0, 1)),
                    Item::Tuple(tuple_image(10, 0, 2)),
                ],
                true,
            ),
            build_page(
                &[
                    Item::Tuple(tuple_image(10, 0, 3)),
                    Item::Dead,
                    Item::Tuple(tuple_image(10, 0, 4)),
                ],
                true,
            ),
        ],
    );
    let rel = test_relation(mcx, oid);

    let mut scan = begin_seqscan(mcx, &rel, mvcc_snapshot(mcx));
    assert_eq!(scan.rs_nblocks, 2);
    let vals = collect_vals(&mut scan, ForwardScanDirection);
    assert_eq!(vals, vec![(0, 1, 1), (0, 2, 2), (1, 1, 3), (1, 3, 4)]);
    assert_eq!(scan.rs_pgstat_getnext, 4);
    assert_eq!(scan.rs_pgstat_numscans, 1);

    heap_rescan(&mut scan, None, false, false, false, false).unwrap();
    let vals = collect_vals(&mut scan, BackwardScanDirection);
    assert_eq!(vals, vec![(1, 3, 4), (1, 1, 3), (0, 2, 2), (0, 1, 1)]);

    heap_endscan(scan).unwrap();
    quiesced();
}

#[test]
fn all_visible_page_skips_visibility_and_filtered_otherwise() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // all-visible page: the visibility seam must not fire.
    let oid = fresh_oid();
    register_table(oid, vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))], true)]);
    let rel = test_relation(mcx, oid);
    let before = VIS_CALLS.load(Ordering::Relaxed);
    let mut scan = begin_seqscan(mcx, &rel, mvcc_snapshot(mcx));
    assert_eq!(collect_vals(&mut scan, ForwardScanDirection).len(), 1);
    heap_endscan(scan).unwrap();
    assert_eq!(VIS_CALLS.load(Ordering::Relaxed), before);

    // non-all-visible page: invisible xmin filtered by the seam.
    let oid2 = fresh_oid();
    register_table(
        oid2,
        vec![build_page(
            &[
                Item::Tuple(tuple_image(10, 0, 1)),
                Item::Tuple(tuple_image(INVISIBLE_XMIN, 0, 2)),
                Item::Tuple(tuple_image(10, 0, 3)),
            ],
            false,
        )],
    );
    let rel2 = test_relation(mcx, oid2);
    let mut scan = begin_seqscan(mcx, &rel2, mvcc_snapshot(mcx));
    let vals = collect_vals(&mut scan, ForwardScanDirection);
    assert_eq!(vals, vec![(0, 1, 1), (0, 3, 3)]);
    heap_endscan(scan).unwrap();
    assert!(VIS_CALLS.load(Ordering::Relaxed) > before);
    quiesced();
}

fn int4eq(
    _flinfo: Option<&mut ::types_fmgr::FmgrInfo>,
    fcinfo: &mut ::types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(fcinfo.arg_i32(0) == fcinfo.arg_i32(1)))
}

#[test]
fn scan_keys_filter_pagemode_and_lockmode() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(
            &[
                Item::Tuple(tuple_image(10, 0, 5)),
                Item::Tuple(tuple_image(10, 0, 42)),
                Item::Tuple(tuple_image(10, 0, 42)),
            ],
            true,
        )],
    );
    let rel = test_relation(mcx, oid);

    let mut key = PgVec::new_in(mcx);
    key.push(ScanKeyData {
        sk_flags: 0,
        sk_attno: 1,
        sk_strategy: ::types_scan::scankey::BTEqualStrategyNumber,
        sk_subtype: 0,
        sk_collation: 0,
        sk_func: ::types_fmgr::FmgrInfo::new(int4eq, 65, 2, true, false),
        sk_argument: Datum::from_i32(42),
    });

    // pagemode
    let flags = SO_TYPE_SEQSCAN | SO_ALLOW_PAGEMODE;
    let mut scan = heap_beginscan(mcx, &rel, mvcc_snapshot(mcx), 1, key, None, flags).unwrap();
    let vals = collect_vals(&mut scan, ForwardScanDirection);
    assert_eq!(vals, vec![(0, 2, 42), (0, 3, 42)]);
    heap_endscan(scan).unwrap();

    // non-pagemode (SnapshotSelf disables pagemode): heapgettup lane, with
    // the content-lock discipline checked by the fake bufmgr.
    let self_snap: Snapshot<'_> = Some(Rc::new(SnapshotData::sentinel(
        mcx,
        SnapshotType::SNAPSHOT_SELF,
    )));
    let mut key2 = PgVec::new_in(mcx);
    key2.push(ScanKeyData {
        sk_flags: 0,
        sk_attno: 1,
        sk_strategy: ::types_scan::scankey::BTEqualStrategyNumber,
        sk_subtype: 0,
        sk_collation: 0,
        sk_func: ::types_fmgr::FmgrInfo::new(int4eq, 65, 2, true, false),
        sk_argument: Datum::from_i32(42),
    });
    let mut scan =
        heap_beginscan(mcx, &rel, self_snap, 1, key2, None, SO_TYPE_SEQSCAN | SO_ALLOW_PAGEMODE)
            .unwrap();
    assert_eq!(scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE, 0);
    let vals = collect_vals(&mut scan, ForwardScanDirection);
    assert_eq!(vals, vec![(0, 2, 42), (0, 3, 42)]);
    heap_endscan(scan).unwrap();
    quiesced();
}

#[test]
fn advance_block_wraps_and_honors_scanlimits() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        (0..4)
            .map(|i| build_page(&[Item::Tuple(tuple_image(10, 0, i))], true))
            .collect(),
    );
    let rel = test_relation(mcx, oid);
    let mut scan = begin_seqscan(mcx, &rel, mvcc_snapshot(mcx));

    // wraparound from a nonzero start block
    scan.rs_startblock = 2;
    assert_eq!(heapgettup_initial_block(&mut scan, ForwardScanDirection), 2);
    scan.rs_inited = true;
    assert_eq!(heapgettup_advance_block(&mut scan, 2, ForwardScanDirection).unwrap(), 3);
    assert_eq!(heapgettup_advance_block(&mut scan, 3, ForwardScanDirection).unwrap(), 0);
    assert_eq!(heapgettup_advance_block(&mut scan, 1, ForwardScanDirection).unwrap(), InvalidBlockNumber);

    // backward from startblock 2 → 1, 0, wrap to 3, done at startblock
    assert_eq!(heapgettup_advance_block(&mut scan, 1, BackwardScanDirection).unwrap(), 0);
    assert_eq!(heapgettup_advance_block(&mut scan, 0, BackwardScanDirection).unwrap(), 3);
    assert_eq!(heapgettup_advance_block(&mut scan, 2, BackwardScanDirection).unwrap(), InvalidBlockNumber);

    // setscanlimits: numblocks counts down to InvalidBlockNumber
    scan.rs_inited = false;
    scan.rs_base.rs_flags &= !SO_ALLOW_SYNC;
    heap_setscanlimits(&mut scan, 1, 2);
    assert_eq!(heapgettup_initial_block(&mut scan, ForwardScanDirection), 1);
    assert_eq!(heapgettup_advance_block(&mut scan, 1, ForwardScanDirection).unwrap(), 2);
    assert_eq!(heapgettup_advance_block(&mut scan, 2, ForwardScanDirection).unwrap(), InvalidBlockNumber);

    heap_endscan(scan).unwrap();
    quiesced();
}

#[test]
fn tidrange_limits_and_empty_range() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        (0..3)
            .map(|_| build_page(&[Item::Tuple(tuple_image(10, 0, 1))], true))
            .collect(),
    );
    let rel = test_relation(mcx, oid);

    let mut scan = begin_seqscan(mcx, &rel, mvcc_snapshot(mcx));
    scan.rs_base.rs_flags &= !SO_ALLOW_SYNC;
    heap_set_tidrange(
        &mut scan,
        &ItemPointerData::new(1, 2),
        &ItemPointerData::new(2, 1),
    );
    assert_eq!(scan.rs_startblock, 1);
    assert_eq!(scan.rs_numblocks, 2);
    assert_eq!(scan.rs_base.rs_mintid, ItemPointerData::new(1, 2));
    assert_eq!(scan.rs_base.rs_maxtid, ItemPointerData::new(2, 1));
    heap_endscan(scan).unwrap();

    let mut scan = begin_seqscan(mcx, &rel, mvcc_snapshot(mcx));
    scan.rs_base.rs_flags &= !SO_ALLOW_SYNC;
    heap_set_tidrange(
        &mut scan,
        &ItemPointerData::new(2, 5),
        &ItemPointerData::new(1, 1),
    );
    assert_eq!(scan.rs_numblocks, 0);
    assert!(heap_getnext(&mut scan, ForwardScanDirection).unwrap().is_none());
    heap_endscan(scan).unwrap();
    quiesced();
}

#[test]
fn fetch_paths() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(
            &[
                Item::Tuple(tuple_image(10, 0, 7)),
                Item::Dead,
                Item::Tuple(tuple_image(INVISIBLE_XMIN, 0, 8)),
            ],
            false,
        )],
    );
    let rel = test_relation(mcx, oid);
    let snap = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);

    // found
    let r = heap_fetch(&rel, &snap, ItemPointerData::new(0, 1), false).unwrap();
    assert!(r.found);
    let t = r.tuple().unwrap();
    assert_eq!(t.t_data().xmin_raw(), 10);
    assert_eq!(t.t_self, ItemPointerData::new(0, 1));
    drop(t);
    r.pin.unwrap().release();

    // dead line pointer
    let r = heap_fetch(&rel, &snap, ItemPointerData::new(0, 2), false).unwrap();
    assert!(!r.found && r.pin.is_none() && r.tuple().is_none());

    // out-of-range offnum
    let r = heap_fetch(&rel, &snap, ItemPointerData::new(0, 9), false).unwrap();
    assert!(!r.found && r.pin.is_none());

    // fails qual, keep_buf: tuple + pin still returned
    let r = heap_fetch(&rel, &snap, ItemPointerData::new(0, 3), true).unwrap();
    assert!(!r.found);
    assert!(r.tuple().is_some());
    r.pin.unwrap().release();

    // fails qual, no keep_buf
    let r = heap_fetch(&rel, &snap, ItemPointerData::new(0, 3), false).unwrap();
    assert!(!r.found && r.pin.is_none());

    quiesced();
}

#[test]
fn hot_chain_search_and_latest_tid() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();

    // off1: redirect -> off2; off2: invisible, HOT-updated -> off3 (xmax 20);
    // off3: visible, xmin 20, end of chain.
    let mut t2 = tuple_image(INVISIBLE_XMIN, 20, 2);
    set_infomask(&mut t2, 0, ::types_tuple::HEAP_HOT_UPDATED); // xmax valid
    set_ctid(&mut t2, 0, 3);
    let mut t3 = tuple_image(20, 0, 3);
    set_infomask(&mut t3, HEAP_XMAX_INVALID, ::types_tuple::HEAP_ONLY_TUPLE);
    set_ctid(&mut t3, 0, 3);
    register_table(
        oid,
        vec![build_page(&[Item::Redirect(2), Item::Tuple(t2), Item::Tuple(t3)], false)],
    );
    let rel = test_relation(mcx, oid);
    let snap = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);

    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(&rel, 0).unwrap()).unwrap();
    {
        let _lock = pin.lock_share().unwrap();
        let r = heap_hot_search_buffer(
            ItemPointerData::new(0, 1),
            &rel,
            &pin,
            &snap,
            true,
            true,
        )
        .unwrap();
        assert!(r.found);
        assert_eq!(r.tid, ItemPointerData::new(0, 3));
        assert_eq!(r.tuple.as_ref().unwrap().t_data().xmin_raw(), 20);
        assert_eq!(r.all_dead, Some(false));
    }
    pin.release();

    let latest = heap_get_latest_tid(&rel, &snap, ItemPointerData::new(0, 3)).unwrap();
    assert_eq!(latest, ItemPointerData::new(0, 3));
    quiesced();
}

#[test]
fn update_xid_plain_xmax_paths() {
    #[repr(align(8))]
    struct Aligned([u8; 28]);
    for infomask in [0u16, HEAP_XMAX_INVALID] {
        let mut img = tuple_image(10, 77, 1);
        set_infomask(&mut img, infomask, 0);
        let mut aligned = Aligned([0; 28]);
        aligned.0.copy_from_slice(&img);
        // SAFETY: MAXALIGNed local image, header-complete, alive for the borrow.
        let t = unsafe {
            HeapTupleData::from_raw_parts(aligned.0.as_ptr(), 28, ItemPointerData::new(0, 1), 1)
        };
        assert_eq!(HeapTupleHeaderGetUpdateXid(t.t_data()).unwrap(), 77);
    }
}

#[test]
fn buffer_pin_guard_drop_is_abort_path() {
    install_seams();
    let _serial = serial();
    let oid = fresh_oid();
    register_table(oid, vec![build_page(&[], true)]);
    let buf = with_fake(|f| f.tables[&oid][0]);
    with_fake(|f| f.pins[(buf - 1) as usize] += 1);
    {
        let _pin = BufferPin::adopt(buf).unwrap();
    } // Drop releases
    quiesced();
    assert!(BufferPin::adopt(InvalidBuffer).is_none());
}

// --- DML phase 2 ---

use ::tableam_vocab::{LockTupleMode, TM_FailureData, TM_Result, TU_UpdateIndexes};
use ::types_storage::bufpage::PageMut;
use ::types_tuple::{HEAP_KEYS_UPDATED, HEAP_UPDATED};

const FAKE_XID: u32 = 100;

static DML_INIT: Once = Once::new();
static XLOG_RECS: Mutex<Vec<(u8, Vec<u8>, usize)>> = Mutex::new(Vec::new());
static NEXT_LSN: AtomicUsize = AtomicUsize::new(0x1000);

fn install_dml_seams() {
    install_seams();
    DML_INIT.call_once(|| {
        bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
        bufmgr_seams::extend_buffered_rel_by::set(|rel, _fork, _strategy, flags, extend_by| {
            assert_eq!(extend_by, 1);
            assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
            let page = Box::new(TestPage([0u8; BLCKSZ]));
            let rd_id = rel.rd_id;
            Ok(with_fake(|f| {
                let addr = Box::leak(page).as_mut_ptr() as usize;
                f.pages.push(addr);
                f.pins.push(1);
                f.locks.push(1);
                let buf = f.pages.len() as Buffer;
                f.tables.get_mut(&rd_id).unwrap().push(buf);
                (buf, 1)
            }))
        });
        xact_seams::get_current_transaction_id::set(|| Ok(FAKE_XID));
        xact_seams::get_current_command_id::set(|_used| Ok(7));
        xact_seams::is_in_parallel_mode::set(|| false);
        xact_seams::get_current_transaction_nest_level::set(|| 1);
        xact_seams::transaction_id_is_current_transaction_id::set(|xid| xid == FAKE_XID);
        heapam_visibility_seams::heap_tuple_satisfies_update::set(|htup, _cid, _buf| {
            let hdr = htup.t_data();
            if hdr.xmin_raw() == INVISIBLE_XMIN {
                return Ok(TM_Result::TM_Invisible);
            }
            if (hdr.t_infomask & HEAP_XMAX_INVALID) != 0 {
                return Ok(TM_Result::TM_Ok);
            }
            if hdr.xmax_raw() == FAKE_XID {
                Ok(TM_Result::TM_SelfModified)
            } else {
                Ok(TM_Result::TM_BeingModified)
            }
        });
        heapam_visibility_seams::heap_tuple_set_hint_bits::set(|hdr, _buf, infomask, _xid| {
            hdr.t_infomask |= infomask;
            Ok(())
        });
        combocid_seams::heap_tuple_header_adjust_cmax::set(|_hdr, cid| Ok((cid, false)));
        combocid_seams::heap_tuple_header_get_cmax::set(|hdr| hdr.raw_command_id());
        multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
        predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
        freespace_seams::get_page_with_free_space::set(|_rel, _need| Ok(InvalidBlockNumber));
        freespace_seams::record_and_get_page_with_free_space::set(|_rel, _old, _avail, _need| {
            Ok(InvalidBlockNumber)
        });
        freespace_seams::record_page_with_free_space::set(|_rel, _blk, _avail| Ok(()));
        xloginsert_seams::xlog_insert_record::set(|_rmid, info, _flags, main_data, bufs| {
            let mut main = Vec::new();
            for frag in main_data {
                main.extend_from_slice(frag);
            }
            XLOG_RECS.lock().unwrap().push((info, main, bufs.len()));
            Ok(NEXT_LSN.fetch_add(8, Ordering::Relaxed) as u64)
        });
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        catalog_seams::is_catalog_relation::set(|_rel| false);
        snapmgr_seams::transaction_xmin::set(|| FAKE_XID);
    });
}

fn take_xlog() -> Vec<(u8, Vec<u8>, usize)> {
    core::mem::take(&mut *XLOG_RECS.lock().unwrap())
}

fn make_writable_tuple(img: &[u8]) -> HeapTupleData<'static> {
    let words = img.len().div_ceil(8);
    // Leaked (test-only): moving a Box would invalidate the derived pointer.
    let buf: &'static mut [u64] = Box::leak(vec![0u64; words].into_boxed_slice());
    // SAFETY: buf is words*8 >= img.len() writable bytes.
    unsafe {
        core::ptr::copy_nonoverlapping(img.as_ptr(), buf.as_mut_ptr().cast::<u8>(), img.len())
    };
    // SAFETY: 8-aligned leaked image, header-complete, unique.
    unsafe {
        HeapTupleData::from_raw_parts(
            buf.as_mut_ptr().cast::<u8>(),
            img.len() as u32,
            ItemPointerData::invalid(),
            0,
        )
    }
}

fn page_tuple_at(oid: Oid, page_idx: usize, off: u16) -> HeapTupleData<'static> {
    let buf = with_fake(|f| f.tables[&oid][page_idx]);
    let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
    // SAFETY: leaked test page, always live.
    let page = unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) };
    let id = page.item_id(off);
    let (ptr, len) = page.item_raw(id);
    // SAFETY: in-page image.
    unsafe {
        HeapTupleData::from_raw_parts(
            ptr,
            len,
            ItemPointerData::new(page_idx as u32, off),
            oid,
        )
    }
}

#[test]
fn dml_insert_extends_stamps_and_logs() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let mut tup = make_writable_tuple(&tuple_image(0, 0, 41));
    dml::heap_insert(&rel, &mut tup, 7, 0).unwrap();
    assert_eq!(tup.t_self, ItemPointerData::new(0, 1));

    let stored = page_tuple_at(oid, 0, 1);
    assert_eq!(stored.t_data().xmin_raw(), FAKE_XID);
    assert_eq!(stored.t_data().raw_command_id(), 7);
    assert!((stored.t_data().t_infomask & HEAP_XMAX_INVALID) != 0);
    assert_eq!(stored.t_data().t_ctid, tup.t_self);

    let mut tup2 = make_writable_tuple(&tuple_image(0, 0, 42));
    dml::heap_insert(&rel, &mut tup2, 7, 0).unwrap();
    assert_eq!(tup2.t_self, ItemPointerData::new(0, 2));

    let recs = take_xlog();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].0, dml::XLOG_HEAP_INSERT | dml::XLOG_HEAP_INIT_PAGE);
    assert_eq!(recs[0].2, 1);
    assert_eq!(recs[1].0, dml::XLOG_HEAP_INSERT);
    // xl_heap_insert: offnum + flags
    assert_eq!(u16::from_ne_bytes([recs[1].1[0], recs[1].1[1]]), 2);
    assert_eq!(
        hio::relation_get_target_block(&rel),
        0,
        "target-block cache primed"
    );
    quiesced();
}

#[test]
fn dml_delete_stamps_xmax_and_logs() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))], false)],
    );
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let tid = ItemPointerData::new(0, 1);
    let mut tmfd = TM_FailureData::default();
    let r = dml::heap_delete(&rel, &tid, 7, None, true, &mut tmfd, false).unwrap();
    assert_eq!(r, TM_Result::TM_Ok);

    let stored = page_tuple_at(oid, 0, 1);
    assert_eq!(stored.t_data().xmax_raw(), FAKE_XID);
    assert!((stored.t_data().t_infomask & HEAP_XMAX_INVALID) == 0);
    assert!((stored.t_data().t_infomask2 & HEAP_KEYS_UPDATED) != 0);
    assert_eq!(stored.t_data().t_ctid, tid);

    let buf = with_fake(|f| f.tables[&oid][0]);
    let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
    // SAFETY: leaked test page.
    let page = unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) };
    assert_eq!(page.prune_xid(), FAKE_XID);

    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].0, dml::XLOG_HEAP_DELETE);
    assert_eq!(
        u32::from_ne_bytes(recs[0].1[0..4].try_into().unwrap()),
        FAKE_XID
    );
    quiesced();
}

#[test]
fn dml_lock_tuple_stamps_xmax_and_logs() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))], false)],
    );
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let tid = ItemPointerData::new(0, 1);
    let mut tmfd = TM_FailureData::default();
    let (r, pin) = dml::heap_lock_tuple(
        &rel,
        &tid,
        7,
        LockTupleMode::LockTupleExclusive,
        ::tableam_vocab::LockWaitPolicy::LockWaitBlock,
        false,
        &mut tmfd,
    )
    .unwrap();
    assert_eq!(r, TM_Result::TM_Ok);
    drop(pin);

    let stored = page_tuple_at(oid, 0, 1);
    let im = stored.t_data().t_infomask;
    assert_eq!(stored.t_data().xmax_raw(), FAKE_XID);
    assert!((im & HEAP_XMAX_INVALID) == 0);
    assert!((im & ::types_tuple::HEAP_XMAX_EXCL_LOCK) != 0);
    assert!((im & ::types_tuple::HEAP_XMAX_LOCK_ONLY) != 0);
    assert!((stored.t_data().t_infomask2 & HEAP_KEYS_UPDATED) != 0);
    // Lock-only mark resets the forward ctid to self.
    assert_eq!(stored.t_data().t_ctid, tid);

    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].0, dml::XLOG_HEAP_LOCK);
    // xl_heap_lock: xmax(4) offnum(2) infobits_set(1) flags(1)
    assert_eq!(
        u32::from_ne_bytes(recs[0].1[0..4].try_into().unwrap()),
        FAKE_XID
    );
    assert_eq!(u16::from_ne_bytes([recs[0].1[4], recs[0].1[5]]), 1);
    let infobits = recs[0].1[6];
    assert_eq!(
        infobits,
        dml::XLHL_XMAX_LOCK_ONLY | dml::XLHL_XMAX_EXCL_LOCK | dml::XLHL_KEYS_UPDATED
    );
    assert_eq!(recs[0].1[7], 0);
    quiesced();
}

#[test]
fn dml_delete_self_modified_fails_without_wal() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    let mut img = tuple_image(10, FAKE_XID, 1);
    set_infomask(&mut img, 0, 0); // xmax valid: deleted by "us"
    register_table(oid, vec![build_page(&[Item::Tuple(img)], false)]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let tid = ItemPointerData::new(0, 1);
    let mut tmfd = TM_FailureData::default();
    let r = dml::heap_delete(&rel, &tid, 7, None, true, &mut tmfd, false).unwrap();
    assert_eq!(r, TM_Result::TM_SelfModified);
    assert_eq!(tmfd.xmax, FAKE_XID);
    assert!(take_xlog().is_empty());
    quiesced();
}

#[test]
fn dml_hot_update_same_page() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))], false)],
    );
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let otid = ItemPointerData::new(0, 1);
    let mut newtup = make_writable_tuple(&tuple_image(0, 0, 2));
    let mut tmfd = TM_FailureData::default();
    let mut lockmode = LockTupleMode::LockTupleNoKeyExclusive;
    let mut update_indexes = TU_UpdateIndexes::TU_None;
    let r = dml::heap_update(
        &rel,
        &otid,
        &mut newtup,
        7,
        None,
        true,
        &mut tmfd,
        &mut lockmode,
        &mut update_indexes,
    )
    .unwrap();
    assert_eq!(r, TM_Result::TM_Ok);
    assert_eq!(update_indexes, TU_UpdateIndexes::TU_None);
    assert_eq!(newtup.t_self, ItemPointerData::new(0, 2));

    let old = page_tuple_at(oid, 0, 1);
    assert!(old.t_data().is_hot_updated());
    assert_eq!(old.t_data().xmax_raw(), FAKE_XID);
    assert_eq!(old.t_data().t_ctid, newtup.t_self);
    let new = page_tuple_at(oid, 0, 2);
    assert!(new.t_data().is_heap_only());
    assert_eq!(new.t_data().xmin_raw(), FAKE_XID);
    assert!((new.t_data().t_infomask & HEAP_UPDATED) != 0);

    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].0, dml::XLOG_HEAP_HOT_UPDATE);
    assert_eq!(recs[0].2, 1, "same-page update registers one buffer");
    quiesced();
}

#[test]
fn dml_update_moves_to_new_page_when_full() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    let mut filler = tuple_image(10, 0, 0);
    filler.resize(1900, 0);
    register_table(
        oid,
        vec![build_page(
            &[
                Item::Tuple(tuple_image(10, 0, 1)),
                Item::Tuple(filler.clone()),
                Item::Tuple(filler.clone()),
                Item::Tuple(filler.clone()),
                Item::Tuple(filler),
            ],
            false,
        )],
    );
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let otid = ItemPointerData::new(0, 1);
    let mut big = tuple_image(0, 0, 2);
    big.resize(600, 0);
    let mut newtup = make_writable_tuple(&big);
    let mut tmfd = TM_FailureData::default();
    let mut lockmode = LockTupleMode::LockTupleNoKeyExclusive;
    let mut update_indexes = TU_UpdateIndexes::TU_None;
    let r = dml::heap_update(
        &rel,
        &otid,
        &mut newtup,
        7,
        None,
        true,
        &mut tmfd,
        &mut lockmode,
        &mut update_indexes,
    )
    .unwrap();
    assert_eq!(r, TM_Result::TM_Ok);
    assert_eq!(update_indexes, TU_UpdateIndexes::TU_All);
    assert_eq!(newtup.t_self, ItemPointerData::new(1, 1));

    let old = page_tuple_at(oid, 0, 1);
    assert!(!old.t_data().is_hot_updated());
    assert_eq!(old.t_data().t_ctid, newtup.t_self);
    let new = page_tuple_at(oid, 1, 1);
    assert!(!new.t_data().is_heap_only());

    let old_buf = with_fake(|f| f.tables[&oid][0]);
    let addr = with_fake(|f| f.pages[(old_buf - 1) as usize]);
    // SAFETY: leaked test page.
    let page = unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) };
    assert!(page.is_full(), "old page hinted full");

    let recs = take_xlog();
    assert_eq!(recs.len(), 2, "xl_heap_lock then xl_heap_update");
    assert_eq!(recs[0].0, dml::XLOG_HEAP_LOCK);
    assert_eq!(recs[1].0, dml::XLOG_HEAP_UPDATE | dml::XLOG_HEAP_INIT_PAGE);
    assert_eq!(recs[1].2, 2, "cross-page update registers both buffers");
    quiesced();
}

#[test]
fn dml_row_too_big_is_54000() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    let err = hio::RelationGetBufferForTuple(&rel, BLCKSZ, None, 0, None, 0).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    quiesced();
}

#[test]
fn dml_speculative_insert_finish() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let mut tup = make_writable_tuple(&tuple_image(0, 0, 41));
    tup.t_data_mut().set_speculative_token(7);
    dml::heap_insert(&rel, &mut tup, 7, hio::HEAP_INSERT_SPECULATIVE).unwrap();
    let tid = tup.t_self;

    let stored = page_tuple_at(oid, 0, 1);
    assert!(stored.t_data().is_speculative());
    assert_eq!(stored.t_data().speculative_token(), 7);

    dml::heap_finish_speculative(&rel, &tid).unwrap();
    let stored = page_tuple_at(oid, 0, 1);
    assert!(!stored.t_data().is_speculative());
    assert_eq!(stored.t_data().t_ctid, tid);
    assert_eq!(stored.t_data().xmin_raw(), FAKE_XID);

    let recs = take_xlog();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].0, dml::XLOG_HEAP_INSERT | dml::XLOG_HEAP_INIT_PAGE);
    // xl_heap_insert flags carry XLH_INSERT_IS_SPECULATIVE
    assert_eq!(recs[0].1[2] & dml::XLH_INSERT_IS_SPECULATIVE, dml::XLH_INSERT_IS_SPECULATIVE);
    assert_eq!(recs[1].0, dml::XLOG_HEAP_CONFIRM);
    // xl_heap_confirm: offnum
    assert_eq!(u16::from_ne_bytes([recs[1].1[0], recs[1].1[1]]), 1);
    quiesced();
}

#[test]
fn dml_speculative_insert_abort_super_deletes() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let mut tup = make_writable_tuple(&tuple_image(0, 0, 41));
    tup.t_data_mut().set_speculative_token(9);
    dml::heap_insert(&rel, &mut tup, 7, hio::HEAP_INSERT_SPECULATIVE).unwrap();
    let tid = tup.t_self;
    let _ = take_xlog();

    dml::heap_abort_speculative(&rel, &tid).unwrap();
    let stored = page_tuple_at(oid, 0, 1);
    assert_eq!(stored.t_data().xmin_raw(), 0, "xmin invalid: dead to everyone");
    assert!(!stored.t_data().is_speculative());
    assert_eq!(stored.t_data().t_ctid, tid);
    assert!((stored.t_data().t_infomask2 & HEAP_KEYS_UPDATED) == 0);

    let buf = with_fake(|f| f.tables[&oid][0]);
    let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
    // SAFETY: leaked test page.
    let page = unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) };
    assert_eq!(page.prune_xid(), FAKE_XID);

    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].0, dml::XLOG_HEAP_DELETE);
    // xl_heap_delete: xmax(4) offnum(2) infobits(1) flags(1)
    assert_eq!(u32::from_ne_bytes(recs[0].1[0..4].try_into().unwrap()), FAKE_XID);
    assert_eq!(u16::from_ne_bytes([recs[0].1[4], recs[0].1[5]]), 1);
    assert_eq!(recs[0].1[7], dml::XLH_DELETE_IS_SUPER);
    quiesced();
}
