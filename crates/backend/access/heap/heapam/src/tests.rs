use super::*;
use ::mcx::MemoryContext;
use ::types_core::{InvalidBuffer, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use ::types_rel::{FormData_pg_class, LockInfoData, LockRelId, RELKIND_RELATION};
use ::types_scan::sdir::ForwardScanDirection;
use ::types_snapshot::SnapshotType;
use ::types_storage::bufpage::{
    ItemIdData, SizeOfPageHeaderData, LP_DEAD, LP_NORMAL, LP_REDIRECT, LP_UNUSED, PD_ALL_VISIBLE,
};
use ::types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};
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
        transam_xlog_seams::xlog_standby_info_active::set(|| false);
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
        heapam_visibility_seams::heap_tuple_satisfies_mvcc_page::set(|htup, _snap, _buf, _memo| {
            VIS_CALLS.fetch_add(1, Ordering::Relaxed);
            Ok(htup.t_data().xmin_raw() != INVISIBLE_XMIN)
        });
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_htup, _vt| Ok(false));
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_hdr| Ok(false));

        predicate_seams::check_for_serializable_conflict_out_needed::set(|_rel, _snap| Ok(false));
        predicate_seams::predicate_lock_relation::set(|_rel, _snap| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_rel, _tid, _snap, _xid| Ok(()));

        pruneheap_seams::heap_page_prune_opt::set(|_rel, _buf| Ok(()));
        procarray_seams::global_vis_test_for::set(|_rel| {
            ::types_core::GlobalVisStateHandle::new(0)
        });
    });
}

fn quiesced() {
    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(
            f.locks.iter().all(|l| *l == 0),
            "leaked locks: {:?}",
            f.locks
        );
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
    test_relation_replident(mcx, oid, b'd')
}

fn test_relation_replident<'mcx>(mcx: Mcx<'mcx>, oid: Oid, replident: u8) -> Relation<'mcx> {
    test_relation_opts(mcx, oid, replident, false)
}

fn user_catalog_std_options() -> ::types_rel::reloptions::StdRdOptions {
    use ::types_rel::reloptions::*;
    StdRdOptions {
        fillfactor: 100,
        toast_tuple_target: 2032,
        autovacuum: AutoVacOpts {
            enabled: true,
            vacuum_threshold: 50,
            vacuum_max_threshold: -1,
            vacuum_ins_threshold: 1000,
            analyze_threshold: 50,
            vacuum_cost_limit: -1,
            freeze_min_age: -1,
            freeze_max_age: -1,
            freeze_table_age: -1,
            multixact_freeze_min_age: -1,
            multixact_freeze_max_age: -1,
            multixact_freeze_table_age: -1,
            log_min_duration: -1,
            vacuum_cost_delay: -1.0,
            vacuum_scale_factor: 0.2,
            vacuum_ins_scale_factor: 0.2,
            analyze_scale_factor: 0.1,
        },
        user_catalog_table: true,
        parallel_workers: -1,
        vacuum_index_cleanup: STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
        vacuum_truncate: true,
        vacuum_truncate_set: false,
        vacuum_max_eager_freeze_failure_rate: -1.0,
    }
}

fn test_relation_opts<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    replident: u8,
    user_catalog: bool,
) -> Relation<'mcx> {
    test_relation_am(mcx, oid, replident, user_catalog, ::tableam_vocab::HEAP_TABLE_AM_OID)
}

fn test_relation_am<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    replident: u8,
    user_catalog: bool,
    relam: Oid,
) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam,
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
        relreplident: replident,
        relispartition: false,
        relfrozenxid: 3,
        relminmxid: 1,
    };
    let data = ::types_rel::RelationData {
        rd_locator: std::cell::Cell::new(::types_storage::RelFileLocator::new(1663, 5, oid)),
        rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: std::cell::Cell::new(true),
        rd_createSubid: std::cell::Cell::new(0),
        rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_droppedSubid: std::cell::Cell::new(0),
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
        rd_options: user_catalog
            .then(|| Box::new(::types_rel::reloptions::RdOptions::Std(user_catalog_std_options()))),
        pgstat_enabled: std::cell::Cell::new(true),
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
    let mut t1 = unsafe { HeapTupleData::from_raw_parts(p1, l1, ItemPointerData::new(0, 1), 1) };
    assert_eq!(t1.t_data().xmin_raw(), 10);
    t1.t_data_mut().set_xmin_committed(); // the tolerated hint-bit store

    assert!(!view.item_id(2).is_normal());
    let id3 = view.item_id(3);
    let (p3, l3) = view.item_raw(id3);
    // SAFETY: as above.
    let t3 = unsafe { HeapTupleData::from_raw_parts(p3, l3, ItemPointerData::new(0, 3), 1) };
    assert_eq!(t3.t_data().xmin_raw(), 11);
    // A second view still reads the page (and sees the hint bit).
    let view2 = unsafe { PageRef::from_raw(ptr) };
    let (q1, m1) = view2.item_raw(view2.item_id(1));
    let r1 = unsafe { HeapTupleData::from_raw_parts(q1, m1, ItemPointerData::new(0, 1), 1) };
    assert!(r1.t_data().xmin_committed());
}

#[test]
fn seqscan_pagemode_forward_rescan() {
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
    assert_eq!(scan.rs_pgstat_tuples, 4);

    // Rescan replays the same forward walk. (Backward-execution wave B7:
    // the backward collect leg - 4,3,2,1 - retired with the stepping arms.)
    heap_rescan(&mut scan, None, false, false, false, false).unwrap();
    let vals = collect_vals(&mut scan, ForwardScanDirection);
    assert_eq!(vals, vec![(0, 1, 1), (0, 2, 2), (1, 1, 3), (1, 3, 4)]);

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
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))], true)],
    );
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
    let mut scan = heap_beginscan(
        mcx,
        &rel,
        self_snap,
        1,
        key2,
        None,
        SO_TYPE_SEQSCAN | SO_ALLOW_PAGEMODE,
    )
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
    assert_eq!(heapgettup_initial_block(&mut scan), 2);
    scan.rs_inited = true;
    assert_eq!(heapgettup_advance_block(&mut scan, 2).unwrap(), 3);
    assert_eq!(heapgettup_advance_block(&mut scan, 3).unwrap(), 0);
    assert_eq!(
        heapgettup_advance_block(&mut scan, 1).unwrap(),
        InvalidBlockNumber
    );

    // (backward-execution wave B7: the backward walk legs - 1, 0, wrap to 3,
    // done at startblock - retired with the backward stepping arms.)

    // setscanlimits: numblocks counts down to InvalidBlockNumber
    scan.rs_inited = false;
    scan.rs_base.rs_flags &= !SO_ALLOW_SYNC;
    heap_setscanlimits(&mut scan, 1, 2);
    assert_eq!(heapgettup_initial_block(&mut scan), 1);
    assert_eq!(heapgettup_advance_block(&mut scan, 1).unwrap(), 2);
    assert_eq!(
        heapgettup_advance_block(&mut scan, 2).unwrap(),
        InvalidBlockNumber
    );

    heap_endscan(scan).unwrap();
    quiesced();
}

// Lane-v2 parallel-worker safety: two scans sharing one parallel shared
// state, driven through the batched page feed (`heap_getnextpagebatch`),
// must partition the relation's blocks — union == all blocks, disjoint —
// including a nonzero startblock (wraparound arithmetic) and the chunk
// allocator's ramp-down composing with page-at-a-time batching.
#[test]
fn parallel_pagebatch_partitions_blocks_disjointly() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // (nblocks, preset startblock or InvalidBlockNumber → init picks 0).
    // 7 @ start 3: wraparound 3,4,5,6,0,1,2 with chunk_size 1.
    // 4096 @ start 0: chunk_size 2 (nextpower2(4096/2048)) with ramp-down
    // to 1 inside the last 64-chunk window.
    for (nblocks, startblock) in [(7u32, 3u32), (4096, InvalidBlockNumber)] {
        let oid = fresh_oid();
        register_table(
            oid,
            (0..nblocks)
                .map(|i| build_page(&[Item::Tuple(tuple_image(10, 0, i as i32))], true))
                .collect(),
        );
        let rel = test_relation(mcx, oid);

        let pbscan = ParallelBlockTableScanDescData {
            phs_nblocks: nblocks,
            ..Default::default()
        };
        if startblock != InvalidBlockNumber {
            // startblock_init only stores when unset; preset exercises the
            // (nallocated + startblock) % nblocks wraparound arithmetic.
            pbscan.phs_startblock.store(startblock, Ordering::SeqCst);
        }
        let pptr = NonNull::from(&pbscan);
        // No STRAT/SYNC: parallel initscan derives SYNC from phs_syncscan
        // anyway, and the fake bufmgr has no strategy seam.
        let flags = SO_TYPE_SEQSCAN | SO_ALLOW_PAGEMODE;
        let mut scans = [
            heap_beginscan(
                mcx,
                &rel,
                mvcc_snapshot(mcx),
                0,
                PgVec::new_in(mcx),
                Some(pptr),
                flags,
            )
            .unwrap(),
            heap_beginscan(
                mcx,
                &rel,
                mvcc_snapshot(mcx),
                0,
                PgVec::new_in(mcx),
                Some(pptr),
                flags,
            )
            .unwrap(),
        ];
        assert!(scans.iter().all(|s| s.rs_nblocks == nblocks));

        // Uneven interleave (worker 0 drains two pages per turn) so chunk
        // boundaries land mid-turn. Record per-worker blocks + global order.
        let mut blocks: [Vec<BlockNumber>; 2] = [Vec::new(), Vec::new()];
        let mut order: Vec<BlockNumber> = Vec::new();
        let mut ntuples = 0u64;
        let mut done = [false, false];
        while !done[0] || !done[1] {
            for w in 0..2 {
                for _ in 0..=w {
                    if done[w] {
                        continue;
                    }
                    let n = heap_getnextpagebatch(&mut scans[w]).unwrap();
                    if n == 0 {
                        done[w] = true;
                        continue;
                    }
                    blocks[w].push(scans[w].rs_cblock);
                    order.push(scans[w].rs_cblock);
                    ntuples += n as u64;
                }
            }
        }

        // Disjoint within and across workers; union covers every block.
        let mut all = order.clone();
        all.sort_unstable();
        assert_eq!(all, (0..nblocks).collect::<Vec<_>>(), "nblocks={nblocks}");
        assert_eq!(ntuples, nblocks as u64);
        // Both workers actually participated.
        assert!(!blocks[0].is_empty() && !blocks[1].is_empty());

        if nblocks == 4096 {
            // The chunk allocator engaged above size 1 and ramped down to 1
            // by scan end (both scans hit the exhausted-nextpage path last).
            for s in &scans {
                assert_eq!(s.rs_parallelworkerdata.as_ref().unwrap().phsw_chunk_size, 1);
            }
            // chunk_size 2 shows up as same-worker consecutive block pairs.
            assert!(blocks[0].windows(2).any(|p| p[1] == p[0] + 1));
        } else {
            // chunk_size 1: single-threaded, blocks are handed out strictly
            // in shared-counter order — the wrapped sequence from startblock.
            let expect: Vec<BlockNumber> =
                (0..nblocks).map(|i| (i + startblock) % nblocks).collect();
            assert_eq!(order, expect);
        }

        let [a, b] = scans;
        heap_endscan(a).unwrap();
        heap_endscan(b).unwrap();
        quiesced();
    }
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
    assert!(heap_getnext(&mut scan, ForwardScanDirection)
        .unwrap()
        .is_none());
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
        vec![build_page(
            &[Item::Redirect(2), Item::Tuple(t2), Item::Tuple(t3)],
            false,
        )],
    );
    let rel = test_relation(mcx, oid);
    let snap = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);

    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(&rel, 0).unwrap()).unwrap();
    {
        let _lock = pin.lock_share().unwrap();
        let r = heap_hot_search_buffer(ItemPointerData::new(0, 1), &rel, &pin, &snap, true, true)
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
fn hot_chain_cycle_errors_not_hangs() {
    // A crafted page with a 2-tuple t_ctid cycle (off1 <-> off2), both
    // HOT-updated and both invisible, satisfies the xmin/xmax continuity check
    // at every step. Without a bound the walk would spin forever holding the
    // buffer pin; the fix must instead raise a catchable data-corruption error.
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();

    // off1 -> off2 -> off1 -> ... ; xmax == next tuple's xmin == INVISIBLE_XMIN
    // so the chain never tests visible and continuity always holds.
    let mut t1 = tuple_image(INVISIBLE_XMIN, INVISIBLE_XMIN, 1);
    set_infomask(&mut t1, 0, ::types_tuple::HEAP_HOT_UPDATED); // xmax valid
    set_ctid(&mut t1, 0, 2);
    let mut t2 = tuple_image(INVISIBLE_XMIN, INVISIBLE_XMIN, 2);
    set_infomask(
        &mut t2,
        0,
        ::types_tuple::HEAP_HOT_UPDATED | ::types_tuple::HEAP_ONLY_TUPLE,
    );
    set_ctid(&mut t2, 0, 1);
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(t1), Item::Tuple(t2)], false)],
    );
    let rel = test_relation(mcx, oid);
    let snap = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);

    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(&rel, 0).unwrap()).unwrap();
    {
        let _lock = pin.lock_share().unwrap();
        let err = heap_hot_search_buffer(ItemPointerData::new(0, 1), &rel, &pin, &snap, true, true)
            .err()
            .unwrap();
        assert_eq!(err.sqlstate, ::types_error::ERRCODE_DATA_CORRUPTED);
    }
    pin.release();
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
static FSM_VACUUM_RANGES: Mutex<Vec<(BlockNumber, BlockNumber)>> = Mutex::new(Vec::new());
// Waiters on the relation-extension lock seen by RelationAddBlocks (0 = C's
// uncontended case).
static EXT_LOCK_WAITERS: AtomicUsize = AtomicUsize::new(0);
static XLOG_RECS: Mutex<Vec<(u8, Vec<u8>, usize, Vec<(u8, Vec<u8>)>)>> = Mutex::new(Vec::new());
static NEXT_LSN: AtomicUsize = AtomicUsize::new(0x1000);

pub(crate) fn wal_insert_record_hook(
    _rmid: u8,
    info: u8,
    _record_flags: u8,
    main_data: &[&[u8]],
    blocks: &[crate::wal::RegBlock<'_>],
) -> ::types_error::PgResult<::types_core::XLogRecPtr> {
    let mut main = Vec::new();
    for frag in main_data {
        main.extend_from_slice(frag);
    }
    let regs = blocks
        .iter()
        .map(|b| (b.flags, b.bufdata.concat()))
        .collect();
    XLOG_RECS
        .lock()
        .unwrap()
        .push((info, main, blocks.len(), regs));
    if let Some(cb) = WAL_INSPECT.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
        cb(info, blocks);
    }
    Ok(NEXT_LSN.fetch_add(8, Ordering::Relaxed) as u64)
}

#[allow(clippy::type_complexity)]
static WAL_INSPECT: Mutex<
    Option<Box<dyn for<'a, 'b> Fn(u8, &'a [crate::wal::RegBlock<'b>]) + Send>>,
> = Mutex::new(None);

fn install_dml_seams() {
    install_seams();
    DML_INIT.call_once(|| {
        bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
        bufmgr_seams::extend_buffered_rel_by::set(|rel, _fork, _strategy, flags, extend_by| {
            assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
            let rd_id = rel.rd_id;
            Ok(with_fake(|f| {
                let mut first = InvalidBuffer;
                for i in 0..extend_by {
                    let page = Box::new(TestPage([0u8; BLCKSZ]));
                    let addr = Box::leak(page).as_mut_ptr() as usize;
                    f.pages.push(addr);
                    // Only the first page comes back pinned + locked
                    // (EB_LOCK_FIRST); the seam impl drops the other pins.
                    f.pins.push(if i == 0 { 1 } else { 0 });
                    f.locks.push(if i == 0 { 1 } else { 0 });
                    let buf = f.pages.len() as Buffer;
                    if i == 0 {
                        first = buf;
                    }
                    f.tables.get_mut(&rd_id).unwrap().push(buf);
                }
                (first, extend_by)
            }))
        });
        freespace_seams::free_space_map_vacuum_range::set(|_rel, start, end| {
            FSM_VACUUM_RANGES.lock().unwrap().push((start, end));
            Ok(())
        });
        // RelationExtensionLockWaiterCount's LockWaiterCount (lock.c:4824):
        // the test-controlled nRequested of the relation-extension lock.
        lock_seams::lock_waiter_count::set(|_tag| Ok(EXT_LOCK_WAITERS.load(Ordering::Relaxed) as i32));
        // CHECK_FOR_INTERRUPTS() (the seam only runs when InterruptPending is
        // set): a pending cancel comes back as C's 57014.
        postgres_seams::check_for_interrupts::set(|| {
            if CANCEL_PENDING.load(Ordering::Relaxed) {
                return Err(Box::new(
                    PgError::error("canceling statement due to user request")
                        .with_sqlstate(::types_error::ERRCODE_QUERY_CANCELED),
                ));
            }
            Ok(())
        });
        // LockAcquireExtended: never available (the NOWAIT / SKIP LOCKED
        // arms), recording the logLockFailure argument the caller passed.
        lock_seams::lock_acquire_extended::set(
            |tag, _mode, _session, _dont_wait, _report_oom, log_lock_failure| {
                // The heavyweight tuple lock can be made available (a
                // heap_lock_tuple NOWAIT then fails on the xact lock instead).
                if tag.locktag_type == ::types_storage::lock::LOCKTAG_TUPLE
                    && TUPLE_LOCK_AVAILABLE.load(Ordering::Relaxed)
                {
                    return Ok(::types_storage::lock::LOCKACQUIRE_OK);
                }
                LAST_LOG_LOCK_FAILURE.store(log_lock_failure, Ordering::Relaxed);
                Ok(::types_storage::lock::LOCKACQUIRE_NOT_AVAIL)
            },
        );
        lock_seams::lock_release::set(|_tag, _mode, _session| Ok(true));
        // Only the multixact-member conflict question reaches this in the
        // paths under test; every pair conflicts.
        lock_seams::do_lock_modes_conflict::set(|_m1, _m2| true);
        procarray_seams::transaction_id_is_in_progress::set(|xid| {
            Ok(IN_PROGRESS_XIDS.lock().unwrap().contains(&xid))
        });
        multixact_seams::get_multi_xact_id_members::set(
            |_multi, _from_pgupgrade, _is_lock_only, consume| {
                let members = MULTI_MEMBERS.lock().unwrap().clone();
                consume(&members);
                Ok(members.len() as i32)
            },
        );
        // ConditionalLockBuffer: the fake's content lock, taken only when free.
        bufmgr_seams::conditional_lock_buffer::set(|buf| {
            COND_LOCK_CALLS.fetch_add(1, Ordering::Relaxed);
            Ok(with_fake(|f| {
                let l = &mut f.locks[(buf - 1) as usize];
                if *l != 0 {
                    return false;
                }
                *l += 1;
                true
            }))
        });
        // WARNINGs (elog_seams) are captured for inspection.
        ::elog_seams::ereport::set(|err| {
            WARNINGS.lock().unwrap().push(err.message.clone());
            Ok(())
        });
        ::guc_tables::vars::log_lock_failures.install_if_absent(::guc_tables::GucVarAccessors {
            get: || LOG_LOCK_FAILURES_GUC.load(Ordering::Relaxed),
            set: |v| LOG_LOCK_FAILURES_GUC.store(v, Ordering::Relaxed),
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
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        freespace_seams::get_page_with_free_space::set(|_rel, _need| Ok(InvalidBlockNumber));
        freespace_seams::record_and_get_page_with_free_space::set(|_rel, _old, _avail, _need| {
            Ok(InvalidBlockNumber)
        });
        freespace_seams::record_page_with_free_space::set(|_rel, _blk, _avail| Ok(()));
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        catalog_seams::is_catalog_relation::set(|_rel| false);
        catalog_seams::is_toast_relation::set(|_rel| false);
        snapmgr_seams::transaction_xmin::set(|| FAKE_XID);
        transam_xlog_seams::xlog_logical_info_active::set(|| LOGICAL.load(Ordering::Relaxed));
        xact_seams::get_top_transaction_id_if_any::set(|| FAKE_XID);
        combocid_seams::heap_tuple_header_get_cmin::set(|hdr| hdr.raw_command_id());
        relcache_seams::relation_get_index_attr_bitmap::set(|_relid| {
            let mcx = Box::leak(Box::new(MemoryContext::new("test indexattr"))).mcx();
            let mut identity = PgVec::new_in(mcx);
            for a in ID_ATTRS.lock().unwrap().iter() {
                identity.push(*a);
            }
            Ok(Rc::new(relcache_seams::IndexAttrBitmaps {
                hot_blocking: PgVec::new_in(mcx),
                summarized: PgVec::new_in(mcx),
                key: PgVec::new_in(mcx),
                pk: PgVec::new_in(mcx),
                identity,
            }))
        });
    });
}

static LOGICAL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static CANCEL_PENDING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static TUPLE_LOCK_AVAILABLE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static COND_LOCK_CALLS: AtomicUsize = AtomicUsize::new(0);
static WARNINGS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static IN_PROGRESS_XIDS: Mutex<Vec<u32>> = Mutex::new(Vec::new());
static MULTI_MEMBERS: Mutex<Vec<::types_storage::multixact::MultiXactMember>> =
    Mutex::new(Vec::new());
static LAST_LOG_LOCK_FAILURE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static LOG_LOCK_FAILURES_GUC: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static ID_ATTRS: Mutex<Vec<i16>> = Mutex::new(Vec::new());

struct LogicalOn;
impl LogicalOn {
    fn new(id_attrs: &[i16]) -> Self {
        LOGICAL.store(true, Ordering::Relaxed);
        *ID_ATTRS.lock().unwrap() = id_attrs.to_vec();
        LogicalOn
    }
}
impl Drop for LogicalOn {
    fn drop(&mut self) {
        LOGICAL.store(false, Ordering::Relaxed);
        ID_ATTRS.lock().unwrap().clear();
    }
}

fn take_xlog() -> Vec<(u8, Vec<u8>, usize, Vec<(u8, Vec<u8>)>)> {
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
        HeapTupleData::from_raw_parts(ptr, len, ItemPointerData::new(page_idx as u32, off), oid)
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
    dml::heap_insert(&rel, &mut tup, 7, 0, None).unwrap();
    assert_eq!(tup.t_self, ItemPointerData::new(0, 1));

    let stored = page_tuple_at(oid, 0, 1);
    assert_eq!(stored.t_data().xmin_raw(), FAKE_XID);
    assert_eq!(stored.t_data().raw_command_id(), 7);
    assert!((stored.t_data().t_infomask & HEAP_XMAX_INVALID) != 0);
    assert_eq!(stored.t_data().t_ctid, tup.t_self);

    let mut tup2 = make_writable_tuple(&tuple_image(0, 0, 42));
    dml::heap_insert(&rel, &mut tup2, 7, 0, None).unwrap();
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
fn dml_delete_invisible_errmsg_is_c_exact() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(INVISIBLE_XMIN, 0, 1))], false)],
    );
    let rel = test_relation(mcx, oid);
    let mut tmfd = TM_FailureData::default();
    let err = dml::heap_delete(
        &rel,
        &ItemPointerData::new(0, 1),
        7,
        None,
        true,
        &mut tmfd,
        false,
    )
    .err().unwrap();
    assert_eq!(err.message(), "attempted to delete invisible tuple");
    assert_eq!(
        err.sqlstate(),
        ::types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
    );
    quiesced();
}

#[test]
fn dml_update_invisible_errmsg_is_c_exact() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(INVISIBLE_XMIN, 0, 1))], false)],
    );
    let rel = test_relation(mcx, oid);
    let mut newtup = make_writable_tuple(&tuple_image(0, 0, 2));
    let mut tmfd = TM_FailureData::default();
    let mut lockmode = LockTupleMode::LockTupleNoKeyExclusive;
    let mut update_indexes = TU_UpdateIndexes::TU_None;
    let err = dml::heap_update(
        &rel,
        &ItemPointerData::new(0, 1),
        &mut newtup,
        7,
        None,
        true,
        &mut tmfd,
        &mut lockmode,
        &mut update_indexes,
    )
    .err().unwrap();
    assert_eq!(err.message(), "attempted to update invisible tuple");
    assert_eq!(
        err.sqlstate(),
        ::types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
    );
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
    let err = hio::RelationGetBufferForTuple(&rel, BLCKSZ, None, 0, None, 0).err().unwrap();
    assert_eq!(
        err.sqlstate(),
        ::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED
    );
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
    dml::heap_insert(&rel, &mut tup, 7, hio::HEAP_INSERT_SPECULATIVE, None).unwrap();
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
    assert_eq!(
        recs[0].1[2] & dml::XLH_INSERT_IS_SPECULATIVE,
        dml::XLH_INSERT_IS_SPECULATIVE
    );
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
    dml::heap_insert(&rel, &mut tup, 7, hio::HEAP_INSERT_SPECULATIVE, None).unwrap();
    let tid = tup.t_self;
    let _ = take_xlog();

    dml::heap_abort_speculative(&rel, &tid).unwrap();
    let stored = page_tuple_at(oid, 0, 1);
    assert_eq!(
        stored.t_data().xmin_raw(),
        0,
        "xmin invalid: dead to everyone"
    );
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
    assert_eq!(
        u32::from_ne_bytes(recs[0].1[0..4].try_into().unwrap()),
        FAKE_XID
    );
    assert_eq!(u16::from_ne_bytes([recs[0].1[4], recs[0].1[5]]), 1);
    assert_eq!(recs[0].1[7], dml::XLH_DELETE_IS_SUPER);
    quiesced();
}

#[test]
fn index_delete_sort_orders_by_block_then_offset() {
    use ::tableam_vocab::TM_IndexDelete;
    let tid = |b: u32, p: u16| ::types_tuple::ItemPointerData::new(b, p);
    let mut deltids: Vec<TM_IndexDelete> =
        [(7, 3), (1, 2), (7, 1), (0, 5), (1, 1), (2048, 9), (0, 4)]
            .iter()
            .enumerate()
            .map(|(i, &(b, p))| TM_IndexDelete {
                tid: tid(b, p),
                id: i as i16,
            })
            .collect();

    index_delete::index_delete_sort(&mut deltids);

    let got: Vec<(u32, u16)> = deltids
        .iter()
        .map(|d| {
            (
                ::types_tuple::ItemPointerGetBlockNumber(&d.tid),
                ::types_tuple::ItemPointerGetOffsetNumber(&d.tid),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![(0, 4), (0, 5), (1, 1), (1, 2), (7, 1), (7, 3), (2048, 9)]
    );
}

static TOAST_INIT: Once = Once::new();
static TOAST_CALLS: AtomicUsize = AtomicUsize::new(0);

// Stamped small copy in the caller's context (copy-from-toast-readback shape).
fn install_toast_seam() {
    TOAST_INIT.call_once(|| {
        heaptoast_seams::heap_toast_insert_or_update::set(|mcx, _rel, newtup, _oldtup, _opts| {
            TOAST_CALLS.fetch_add(1, Ordering::Relaxed);
            let mut t = ::heaptuple::HeapTuple::alloc_zeroed(mcx, 28)?;
            // SAFETY: 24 header bytes from the live stamped source; 28B image.
            unsafe {
                core::ptr::copy_nonoverlapping(newtup.header_ptr(), t.image_mut().as_mut_ptr(), 24);
            }
            t.image_mut()[24..28].copy_from_slice(&0x7A7A7A7Ai32.to_ne_bytes());
            t.as_tuple_mut().t_tableOid = newtup.t_tableOid;
            Ok(Some(t))
        });
    });
}

fn heap_slot_with<'mcx>(mcx: Mcx<'mcx>, img: &[u8]) -> ::types_slot::SlotData<'mcx> {
    use ::types_slot::TupleSlotKind;
    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(int4_tupdesc(mcx)));
    exectuples::exec_store_heap_tuple(&mut slot, mcx, make_writable_tuple(img));
    slot
}

#[test]
fn multi_insert_places_stamped_tuples() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let mut s1 = heap_slot_with(mcx, &tuple_image(0, 0, 41));
    let mut s2 = heap_slot_with(mcx, &tuple_image(0, 0, 42));
    let mut slots = [&mut s1, &mut s2];
    dml::heap_multi_insert(mcx, &rel, &mut slots, 7, 0, None).unwrap();

    for (i, val) in [(1u16, 41i32), (2, 42)] {
        let stored = page_tuple_at(oid, 0, i);
        assert_eq!(stored.t_data().xmin_raw(), FAKE_XID, "tuple {i} xmin");
        assert_eq!(stored.t_data().raw_command_id(), 7);
        // SAFETY: int4 payload at t_hoff within the placed image.
        let got = unsafe { stored.header_ptr().add(24).cast::<i32>().read_unaligned() };
        assert_eq!(got, val);
        assert_eq!(stored.t_self, ItemPointerData::new(0, i));
    }
    let recs = take_xlog();
    assert_eq!(recs.len(), 1, "one MULTI_INSERT record");
    assert_eq!(
        recs[0].0 & !dml::XLOG_HEAP_INIT_PAGE,
        dml::XLOG_HEAP2_MULTI_INSERT
    );
    quiesced();
}

// ExecFetchSlotHeapTuple's copy arm: a Virtual slot materializes a fresh
// heap-tuple copy for placement (the slot keeps its virtual representation;
// only tts_tid receives the placed TID, as C).
#[test]
fn multi_insert_copies_virtual_slots() {
    use ::types_slot::TupleSlotKind;
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let mut virt =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(int4_tupdesc(mcx)));
    {
        let base = virt.base_mut();
        base.tts_values[0] = Datum::from_i32(77);
        base.tts_isnull[0] = false;
    }
    exectuples::exec_store_virtual_tuple(&mut virt);
    let mut heapish = heap_slot_with(mcx, &tuple_image(0, 0, 41));
    let mut slots = [&mut virt, &mut heapish];
    dml::heap_multi_insert(mcx, &rel, &mut slots, 7, 0, None).unwrap();

    for (i, val) in [(1u16, 77i32), (2, 41)] {
        let stored = page_tuple_at(oid, 0, i);
        assert_eq!(stored.t_data().xmin_raw(), FAKE_XID, "tuple {i} xmin");
        // SAFETY: int4 payload at t_hoff within the placed image.
        let got = unsafe { stored.header_ptr().add(24).cast::<i32>().read_unaligned() };
        assert_eq!(got, val);
        assert_eq!(stored.t_self, ItemPointerData::new(0, i));
    }
    // The virtual slot stays virtual; its tts_tid carries the placed TID.
    assert!(matches!(virt, ::types_slot::SlotData::Virtual(_)));
    assert_eq!(virt.base().tts_tid, ItemPointerData::new(0, 1));
    quiesced();
}

// Drop before RelationPutHeapTuple clobbers xmin (copy-from-toast-readback).
#[test]
fn multi_insert_toast_copy_survives_to_placement() {
    install_dml_seams();
    install_toast_seam();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let mut fat = tuple_image(0, 0, 33);
    fat.resize(2100, 0); // > TOAST_TUPLE_THRESHOLD (2032)
    let calls0 = TOAST_CALLS.load(Ordering::Relaxed);
    let mut s1 = heap_slot_with(mcx, &fat);
    let mut s2 = heap_slot_with(mcx, &tuple_image(0, 0, 44));
    let mut slots = [&mut s1, &mut s2];
    dml::heap_multi_insert(mcx, &rel, &mut slots, 7, 0, None).unwrap();
    assert_eq!(TOAST_CALLS.load(Ordering::Relaxed), calls0 + 1);

    let toasted = page_tuple_at(oid, 0, 1);
    assert_eq!(
        toasted.t_data().xmin_raw(),
        FAKE_XID,
        "toast copy xmin intact at placement"
    );
    assert_eq!(
        toasted.t_len, 28,
        "replacement image placed, not the source"
    );
    // SAFETY: sentinel payload at t_hoff within the placed image.
    let got = unsafe { toasted.header_ptr().add(24).cast::<i32>().read_unaligned() };
    assert_eq!(got, 0x7A7A7A7A);

    let plain = page_tuple_at(oid, 0, 2);
    assert_eq!(plain.t_data().xmin_raw(), FAKE_XID);
    quiesced();
}

// --- COPY FREEZE visibility-map side (heapam.c:2460-2654) ---

// Registered VM "tables" live at rel oid + this offset in the shared fake.
const VM_OID_OFFSET: Oid = 1_000_000;
// MAXALIGN(SizeOfPageHeaderData): first VM map byte; heap block 0's
// all-visible bit is its low bit, all-frozen the next (visibilitymap.c).
const VM_FIRST_MAP_BYTE: usize = 24;
// XLOG_HEAP2_VISIBLE (heapam_xlog.h) — emitted by visibilitymap_set.
const XLOG_HEAP2_VISIBLE: u8 = 0x40;

static VM_INIT: Once = Once::new();

fn install_vm_seams() {
    VM_INIT.call_once(|| {
        bufmgr_seams::relation_smgr_locator::set(|rel| ::types_storage::RelFileLocatorBackend {
            locator: ::types_storage::RelFileLocator {
                spcOid: 1663,
                dbOid: 5,
                relNumber: rel.rd_id,
            },
            backend: ::types_core::INVALID_PROC_NUMBER,
        });
        smgr_seams::smgr_cached_nblocks::set(|rloc, _fork| {
            with_fake(|f| {
                f.tables
                    .get(&(rloc.locator.relNumber + VM_OID_OFFSET))
                    .map_or(0, |t| t.len()) as ::types_core::BlockNumber
            })
        });
        smgr_seams::smgr_exists::set(|_rloc, _fork| Ok(true));
        smgr_seams::smgr_nblocks::set(|rloc, fork| {
            Ok(smgr_seams::smgr_cached_nblocks::call(rloc, fork))
        });
        smgr_seams::smgr_set_cached_nblocks::set(|_rloc, _fork, _v| Ok(()));
        bufmgr_seams::read_buffer_extended::set(|rel, fork, blkno, _mode, _strategy| {
            assert_eq!(fork, ::types_core::ForkNumber::VISIBILITYMAP_FORKNUM);
            with_fake(|f| {
                let buf = f.tables[&(rel.rd_id + VM_OID_OFFSET)][blkno as usize];
                f.pins[(buf - 1) as usize] += 1;
                Ok(buf)
            })
        });
        xlogutils_seams::in_recovery::set(|| false);
        transam_xlog_seams::data_checksums_enabled::set(|| false);
        guc_tables::vars::wal_log_hints.install(guc_tables::GucVarAccessors {
            get: || false,
            set: |_| {},
        });
        // visibilitymap's WAL goes through the xloginsert seam (not this
        // crate's direct wal.rs hook); capture into the same record log so
        // take_xlog sees both records in emission order.
        xloginsert_seams::xlog_insert_record::set(|_rmid, info, _flags, main_data, bufs| {
            let main = main_data.concat();
            let regs = bufs.iter().map(|b| (b.flags, b.bufdata.concat())).collect();
            XLOG_RECS
                .lock()
                .unwrap()
                .push((info, main, bufs.len(), regs));
            Ok(NEXT_LSN.fetch_add(8, Ordering::Relaxed) as u64)
        });
    });
}

fn vm_test_page(first_byte: u8) -> Box<TestPage> {
    let mut page = Box::new(TestPage([0u8; BLCKSZ]));
    // SAFETY: aligned, exclusively owned test page.
    unsafe { PageMut::from_raw(NonNull::new(page.0.as_mut_ptr()).unwrap()) }.init(0);
    page.0[VM_FIRST_MAP_BYTE] = first_byte;
    page
}

fn heap_page_flags_check(oid: Oid, page_idx: usize) -> bool {
    let buf = with_fake(|f| f.tables[&oid][page_idx]);
    let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
    // SAFETY: leaked test page, always live.
    let page = unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) };
    page.is_all_visible()
}

fn vm_first_byte(oid: Oid) -> u8 {
    let buf = with_fake(|f| f.tables[&(oid + VM_OID_OFFSET)][0]);
    let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
    // SAFETY: leaked test page, always live.
    unsafe { *(addr as *const u8).add(VM_FIRST_MAP_BYTE) }
}

// COPY FREEZE onto pages started empty: PD_ALL_VISIBLE + VM all-visible|
// all-frozen at insert time, two WAL records (MULTI_INSERT+INIT carrying
// XLH_INSERT_ALL_FROZEN_SET, then HEAP2_VISIBLE) — heapam.c:2460-2654.
#[test]
fn multi_insert_frozen_sets_vm_bits_and_logs_both_records() {
    install_dml_seams();
    install_vm_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    register_table(oid + VM_OID_OFFSET, vec![vm_test_page(0)]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let mut s1 = heap_slot_with(mcx, &tuple_image(0, 0, 41));
    let mut s2 = heap_slot_with(mcx, &tuple_image(0, 0, 42));
    let mut slots = [&mut s1, &mut s2];
    dml::heap_multi_insert(
        mcx,
        &rel,
        &mut slots,
        7,
        crate::hio::HEAP_INSERT_FROZEN,
        None,
    )
    .unwrap();

    for off in [1u16, 2] {
        let stored = page_tuple_at(oid, 0, off);
        assert!(
            stored.t_data().xmin_frozen(),
            "tuple {off} frozen at insert"
        );
    }
    assert!(
        heap_page_flags_check(oid, 0),
        "PD_ALL_VISIBLE set at insert time"
    );
    assert_eq!(
        vm_first_byte(oid),
        0x03,
        "VM all-visible|all-frozen for block 0"
    );

    let recs = take_xlog();
    assert_eq!(recs.len(), 2, "MULTI_INSERT then HEAP2_VISIBLE");
    assert_eq!(
        recs[0].0,
        dml::XLOG_HEAP2_MULTI_INSERT | dml::XLOG_HEAP_INIT_PAGE
    );
    assert_ne!(
        recs[0].1[0] & dml::XLH_INSERT_ALL_FROZEN_SET,
        0,
        "ALL_FROZEN_SET flag"
    );
    assert_eq!(recs[0].1[0] & dml::XLH_INSERT_ALL_VISIBLE_CLEARED, 0);
    assert_eq!(recs[1].0, XLOG_HEAP2_VISIBLE);
    assert_eq!(
        recs[1].1[4], 0x03,
        "xl_heap_visible.flags = ALL_VISIBLE|ALL_FROZEN"
    );
    assert_eq!(&recs[1].1[0..4], &[0u8; 4], "InvalidTransactionId cutoff");
    quiesced();
}

// Frozen rows appended to a partially-filled all-visible page keep the bit
// (the !HEAP_INSERT_FROZEN guard, heapam.c:2503); no VM traffic, no flags.
#[test]
fn multi_insert_frozen_keeps_all_visible_bit_on_nonempty_page() {
    install_dml_seams();
    install_vm_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(9, 0, 1))], true)],
    );
    register_table(oid + VM_OID_OFFSET, vec![vm_test_page(0x03)]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let mut s1 = heap_slot_with(mcx, &tuple_image(0, 0, 43));
    let mut slots = [&mut s1];
    dml::heap_multi_insert(
        mcx,
        &rel,
        &mut slots,
        7,
        crate::hio::HEAP_INSERT_FROZEN,
        None,
    )
    .unwrap();

    assert!(
        heap_page_flags_check(oid, 0),
        "PD_ALL_VISIBLE survives frozen append"
    );
    assert_eq!(vm_first_byte(oid), 0x03, "VM bits survive frozen append");
    let recs = take_xlog();
    assert_eq!(
        recs.len(),
        1,
        "no HEAP2_VISIBLE for a page not started empty"
    );
    assert_eq!(recs[0].1[0] & dml::XLH_INSERT_ALL_VISIBLE_CLEARED, 0);
    assert_eq!(recs[0].1[0] & dml::XLH_INSERT_ALL_FROZEN_SET, 0);
    quiesced();
}

// Non-frozen rows onto an all-visible page clear PD_ALL_VISIBLE + the VM bits
// and stamp XLH_INSERT_ALL_VISIBLE_CLEARED (heapam.c:2503-2510).
#[test]
fn multi_insert_nonfrozen_clears_all_visible() {
    install_dml_seams();
    install_vm_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(9, 0, 1))], true)],
    );
    register_table(oid + VM_OID_OFFSET, vec![vm_test_page(0x03)]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let mut s1 = heap_slot_with(mcx, &tuple_image(0, 0, 44));
    let mut slots = [&mut s1];
    dml::heap_multi_insert(mcx, &rel, &mut slots, 7, 0, None).unwrap();

    assert!(!heap_page_flags_check(oid, 0), "PD_ALL_VISIBLE cleared");
    assert_eq!(vm_first_byte(oid), 0, "VM bits cleared");
    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_ne!(recs[0].1[0] & dml::XLH_INSERT_ALL_VISIBLE_CLEARED, 0);
    quiesced();
}

// --- logical decoding write side ---

const KEEP_DATA: u8 = ::xloginsert_seams::REGBUF_KEEP_DATA;

#[test]
fn logical_insert_contains_new_tuple_and_keeps_data() {
    install_dml_seams();
    let _serial = serial();
    let _logical = LogicalOn::new(&[]);
    let ctx = MemoryContext::new("t");
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(ctx.mcx(), oid);
    let _ = take_xlog();

    let mut tup = make_writable_tuple(&tuple_image(0, 0, 41));
    dml::heap_insert(&rel, &mut tup, 7, 0, None).unwrap();

    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].0, dml::XLOG_HEAP_INSERT | dml::XLOG_HEAP_INIT_PAGE);
    assert_ne!(recs[0].1[2] & dml::XLH_INSERT_CONTAINS_NEW_TUPLE, 0);
    assert_eq!(recs[0].1[2] & dml::XLH_INSERT_ON_TOAST_RELATION, 0);
    assert_ne!(recs[0].3[0].0 & KEEP_DATA, 0);
    quiesced();
}

#[test]
fn logical_insert_catalog_rel_logs_new_cid() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let oid = fresh_oid();
    let _logical = LogicalOn::new(&[]);
    register_table(oid, vec![]);
    let rel = test_relation_opts(ctx.mcx(), oid, b'd', true);
    let _ = take_xlog();

    let mut tup = make_writable_tuple(&tuple_image(0, 0, 41));
    dml::heap_insert(&rel, &mut tup, 7, 0, None).unwrap();

    let recs = take_xlog();
    assert_eq!(recs.len(), 2);
    let (info, main, nblocks, _) = &recs[0];
    assert_eq!(*info, 0x70);
    assert_eq!(*nblocks, 0);
    assert_eq!(main.len(), 34);
    let f = |r: core::ops::Range<usize>| u32::from_ne_bytes(main[r].try_into().unwrap());
    assert_eq!(f(0..4), FAKE_XID);
    assert_eq!(f(4..8), 7);
    assert_eq!(f(8..12), !0u32);
    assert_eq!(f(12..16), !0u32);
    assert_eq!(f(16..20), 1663);
    assert_eq!(f(20..24), 5);
    assert_eq!(f(24..28), oid);
    assert_eq!(u16::from_ne_bytes(main[32..34].try_into().unwrap()), 1);
    assert_eq!(recs[1].0, dml::XLOG_HEAP_INSERT | dml::XLOG_HEAP_INIT_PAGE);
    // user catalog tables are also logically logged (unlike system catalogs)
    assert_ne!(recs[1].1[2] & dml::XLH_INSERT_CONTAINS_NEW_TUPLE, 0);
    assert_ne!(recs[1].3[0].0 & KEEP_DATA, 0);
    quiesced();
}

#[test]
fn logical_delete_catalog_rel_logs_new_cid_cmax() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let oid = fresh_oid();
    let _logical = LogicalOn::new(&[]);
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))], false)],
    );
    let rel = test_relation_opts(ctx.mcx(), oid, b'd', true);
    let _ = take_xlog();

    let tid = ItemPointerData::new(0, 1);
    let mut tmfd = TM_FailureData::default();
    let r = dml::heap_delete(&rel, &tid, 7, None, true, &mut tmfd, false).unwrap();
    assert_eq!(r, TM_Result::TM_Ok);

    let recs = take_xlog();
    assert_eq!(recs.len(), 2);
    let (info, main, _, _) = &recs[0];
    assert_eq!(*info, 0x70);
    let f = |r: core::ops::Range<usize>| u32::from_ne_bytes(main[r].try_into().unwrap());
    assert_eq!(f(4..8), !0u32);
    assert_eq!(f(8..12), 7);
    assert_eq!(recs[1].0, dml::XLOG_HEAP_DELETE);
    assert_eq!(
        recs[1].1.len(),
        8,
        "empty identity: no replica identity payload"
    );
    quiesced();
}

#[test]
fn logical_delete_full_replident_logs_old_tuple() {
    install_dml_seams();
    let _serial = serial();
    let _logical = LogicalOn::new(&[]);
    let ctx = MemoryContext::new("t");
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))], false)],
    );
    let rel = test_relation_replident(ctx.mcx(), oid, b'f');
    let _ = take_xlog();

    let tid = ItemPointerData::new(0, 1);
    let mut tmfd = TM_FailureData::default();
    dml::heap_delete(&rel, &tid, 7, None, true, &mut tmfd, false).unwrap();

    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    let (info, main, _, _) = &recs[0];
    assert_eq!(*info, dml::XLOG_HEAP_DELETE);
    assert_ne!(main[7] & dml::XLH_DELETE_CONTAINS_OLD_TUPLE, 0);
    assert_eq!(main[7] & dml::XLH_DELETE_CONTAINS_OLD_KEY, 0);
    assert_eq!(main.len(), 8 + 5 + 5);
    let stored = page_tuple_at(oid, 0, 1);
    let hdr = stored.t_data();
    assert_eq!(
        u16::from_ne_bytes(main[8..10].try_into().unwrap()),
        hdr.t_infomask2
    );
    assert_eq!(
        u16::from_ne_bytes(main[10..12].try_into().unwrap()),
        hdr.t_infomask
    );
    assert_eq!(main[12], hdr.t_hoff);
    assert_eq!(i32::from_ne_bytes(main[14..18].try_into().unwrap()), 1);
    quiesced();
}

#[test]
fn logical_delete_identity_index_logs_old_key() {
    install_dml_seams();
    let _serial = serial();
    let _logical = LogicalOn::new(&[1]);
    let ctx = MemoryContext::new("t");
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(10, 0, 7))], false)],
    );
    let rel = test_relation(ctx.mcx(), oid);
    let _ = take_xlog();

    let tid = ItemPointerData::new(0, 1);
    let mut tmfd = TM_FailureData::default();
    dml::heap_delete(&rel, &tid, 7, None, true, &mut tmfd, false).unwrap();

    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    let (_, main, _, _) = &recs[0];
    assert_eq!(main[7] & dml::XLH_DELETE_CONTAINS_OLD_TUPLE, 0);
    assert_ne!(main[7] & dml::XLH_DELETE_CONTAINS_OLD_KEY, 0);
    assert_eq!(main.len(), 8 + 5 + 5);
    assert_eq!(main[12], 24);
    assert_eq!(i32::from_ne_bytes(main[14..18].try_into().unwrap()), 7);
    quiesced();
}

#[test]
fn logical_delete_empty_identity_logs_no_key() {
    install_dml_seams();
    let _serial = serial();
    let _logical = LogicalOn::new(&[]);
    let ctx = MemoryContext::new("t");
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))], false)],
    );
    let rel = test_relation(ctx.mcx(), oid);
    let _ = take_xlog();

    let tid = ItemPointerData::new(0, 1);
    let mut tmfd = TM_FailureData::default();
    dml::heap_delete(&rel, &tid, 7, None, true, &mut tmfd, false).unwrap();

    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].1.len(), 8);
    assert_eq!(
        recs[0].1[7] & (dml::XLH_DELETE_CONTAINS_OLD_TUPLE | dml::XLH_DELETE_CONTAINS_OLD_KEY),
        0
    );
    quiesced();
}

#[test]
fn logical_update_full_replident_logs_flags_and_old_tuple() {
    install_dml_seams();
    let _serial = serial();
    let _logical = LogicalOn::new(&[]);
    let ctx = MemoryContext::new("t");
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))], false)],
    );
    let rel = test_relation_replident(ctx.mcx(), oid, b'f');
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

    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    let (info, main, _, regs) = &recs[0];
    assert_eq!(*info, dml::XLOG_HEAP_HOT_UPDATE);
    let flags = main[7];
    assert_ne!(flags & dml::XLH_UPDATE_CONTAINS_NEW_TUPLE, 0);
    assert_ne!(flags & dml::XLH_UPDATE_CONTAINS_OLD_TUPLE, 0);
    assert_eq!(flags & dml::XLH_UPDATE_CONTAINS_OLD_KEY, 0);
    assert_eq!(main.len(), 14 + 5 + 5);
    assert_eq!(i32::from_ne_bytes(main[20..24].try_into().unwrap()), 1);
    assert_ne!(regs[0].0 & KEEP_DATA, 0);
    assert_eq!(regs[0].1.len(), 5 + 5);
    assert_eq!(i32::from_ne_bytes(regs[0].1[6..10].try_into().unwrap()), 2);
    quiesced();
}

// WS-O inc-2 (single-executor wave 2): heap_end_claim_release — the R3
// zero-pins-at-settle seam. A claim ended EARLY (mid-batch) drops its page
// pin and resets to the drained state; the release is idempotent on a
// drained claim; the scan repositions and drains normally afterwards.
#[test]
fn end_claim_release_drops_midclaim_pin_and_repositions() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        (0..3)
            .map(|i| build_page(&[Item::Tuple(tuple_image(10, 0, i as i32))], true))
            .collect(),
    );
    let rel = test_relation(mcx, oid);
    let mut scan = begin_seqscan(mcx, &rel, mvcc_snapshot(mcx));

    // Claim [0, 3): stage one page batch and STOP (the early-end shape —
    // drain error / abort between segments / shed).
    heap_set_block_range(&mut scan, 0, 3).unwrap();
    assert_eq!(heap_getnextpagebatch(&mut scan).unwrap(), 1);
    assert!(
        scan.rs_cbuf.is_some(),
        "mid-claim: the staged page is pinned"
    );
    heap_end_claim_release(&mut scan);
    assert!(scan.rs_cbuf.is_none(), "end_claim released the pin");
    assert!(!scan.rs_inited, "drained/un-inited state restored");
    with_fake(|f| assert!(f.pins.iter().all(|p| *p == 0), "zero pins at settle"));
    // Idempotent on the (now) drained claim.
    heap_end_claim_release(&mut scan);

    // The next claim positions and drains exactly as before.
    heap_set_block_range(&mut scan, 0, 3).unwrap();
    let mut ntuples = 0u32;
    loop {
        let n = heap_getnextpagebatch(&mut scan).unwrap();
        if n == 0 {
            break;
        }
        ntuples += n;
    }
    assert_eq!(ntuples, 3, "full drain after the released claim");

    heap_endscan(scan).unwrap();
    quiesced();
}

// Inplace-update crash discipline (heapam.c heap_inplace_update_and_unlock):
// WAL is inserted BEFORE the live page mutates, registering a stack copy that
// already carries the post-mutation image (the FPI candidate).
#[test]
fn inplace_update_wal_precedes_page_mutation() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(10, 0, 1))], false)],
    );
    let rel = test_relation(mcx, oid);
    let buf = bufmgr_seams::read_buffer::call(&rel, 0).unwrap();
    bufmgr_seams::lock_buffer::call(buf, bufmgr_seams::BUFFER_LOCK_EXCLUSIVE).unwrap();
    let live = bufmgr_seams::buffer_get_page::call(buf).as_ptr() as usize;

    let tid = ItemPointerData::new(0, 1);
    let dst_off = {
        // SAFETY: pinned above.
        let page = unsafe {
            ::types_storage::bufpage::PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf))
        };
        let (ptr, _len) = page.item_raw(page.item_id(1));
        ptr as usize - live + 24
    };

    #[repr(align(8))]
    struct Aligned([u8; 28]);
    let mut a = Aligned([0; 28]);
    a.0.copy_from_slice(&tuple_image(10, 0, 42));
    // SAFETY: MAXALIGNed local image, header-complete, alive for the borrow.
    let newtup = unsafe { HeapTupleData::from_raw_parts(a.0.as_ptr(), 28, tid, oid) };
    let src = &a.0[24..28];

    let saw = std::sync::Arc::new(AtomicUsize::new(0));
    let saw_cb = saw.clone();
    *WAL_INSPECT.lock().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(move |info, blocks| {
        if info != crate::dml::XLOG_HEAP_INPLACE {
            return;
        }
        assert_eq!(blocks.len(), 1);
        let b = &blocks[0];
        assert_eq!(b.flags, ::xloginsert_seams::REGBUF_STANDARD);
        assert_eq!(b.page.len(), BLCKSZ);
        // Registered image is a copy, not the live page...
        assert_ne!(b.page.as_ptr() as usize, live);
        // ...already carrying the post-mutation bytes...
        assert_eq!(&b.page[dst_off..dst_off + 4], &42i32.to_ne_bytes());
        // ...while the live page is still the pre-image at insert time.
        // SAFETY: live page pinned for the whole test.
        let live_val = unsafe { core::ptr::read((live + dst_off) as *const [u8; 4]) };
        assert_eq!(live_val, 1i32.to_ne_bytes());
        assert_eq!(b.bufdata.concat(), 42i32.to_ne_bytes());
        saw_cb.fetch_add(1, Ordering::Relaxed);
    }));

    let msgs: PgVec<'_, ::types_storage::SharedInvalidationMessage> = PgVec::new_in(mcx);
    crate::inplace::inplace_write_wal_and_page(mcx, &rel, &newtup, buf, src, 24, 4, false, 0, &msgs)
        .unwrap();
    *WAL_INSPECT.lock().unwrap_or_else(|e| e.into_inner()) = None;

    assert_eq!(saw.load(Ordering::Relaxed), 1, "XLOG_HEAP_INPLACE was inserted");
    // SAFETY: live page still registered and pinned.
    let live_val = unsafe { core::ptr::read((live + dst_off) as *const [u8; 4]) };
    assert_eq!(live_val, 42i32.to_ne_bytes(), "page mutated after WAL");

    bufmgr_seams::release_buffer::call(buf).unwrap();
    quiesced();
}

// upstream eabc9a9dd908 (18.5): Include last block in FSM vacuum of bulk
// extended relation. FreeSpaceMapVacuumRange's end is exclusive, so the
// range handed over by RelationAddBlocks must run one past the last block
// it recorded in the FSM.
#[test]
fn bulk_extend_fsm_vacuum_range_covers_last_block() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    FSM_VACUUM_RANGES.lock().unwrap().clear();

    // A bulk-insert state that already grew the relation once extends by
    // four pages at a time; num_pages = 1 keeps only the first out of the FSM.
    let mut bistate = hio::GetBulkInsertState();
    bistate.already_extended_by = 4;
    let pin = hio::RelationGetBufferForTuple(&rel, 64, None, 0, Some(&mut bistate), 1).unwrap();
    assert_eq!(pin.block_number(), 0);
    assert_eq!(with_fake(|f| f.tables[&oid].len()), 4);
    bufmgr_seams::lock_buffer::call(pin.buffer(), bufmgr_seams::BUFFER_LOCK_UNLOCK).unwrap();
    pin.release();
    assert_eq!((bistate.next_free, bistate.last_free), (1, 3));
    hio::ReleaseBulkInsertStatePin(&mut bistate);

    // Blocks 1..=3 went into the FSM; the vacuum must cover block 3 too.
    let ranges = core::mem::take(&mut *FSM_VACUUM_RANGES.lock().unwrap());
    assert_eq!(ranges, vec![(1, 4)]);
    quiesced();
}

// hio.c:272-282 RelationAddBlocks: with a bistate or the FSM available,
// extend_by_pages += extend_by_pages * RelationExtensionLockWaiterCount(rel)
// (audit-18.6 w2-034, a186-candidate-fp-heap-hio-0984fe54151d3897036c-1).
#[test]
fn relation_add_blocks_scales_extension_by_extension_lock_waiters() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    FSM_VACUUM_RANGES.lock().unwrap().clear();

    // Two other backends queue on the extension lock: one requested page
    // becomes 1 + 1 * 2 = 3 pages, the extra two going into the FSM.
    EXT_LOCK_WAITERS.store(2, Ordering::Relaxed);
    let pin = hio::RelationGetBufferForTuple(&rel, 64, None, 0, None, 1);
    EXT_LOCK_WAITERS.store(0, Ordering::Relaxed);
    let pin = pin.unwrap();
    let first_block = pin.block_number();
    let nblocks = with_fake(|f| f.tables[&oid].len());
    // Release before asserting so a failing witness leaves the fake quiesced.
    bufmgr_seams::lock_buffer::call(pin.buffer(), bufmgr_seams::BUFFER_LOCK_UNLOCK).unwrap();
    pin.release();
    let ranges = core::mem::take(&mut *FSM_VACUUM_RANGES.lock().unwrap());
    quiesced();
    assert_eq!(first_block, 0);
    assert_eq!(nblocks, 3, "hio.c:282 extend_by_pages += extend_by_pages * waitcount");
    assert_eq!(ranges, vec![(1, 3)], "blocks 1..=2 were recorded in the FSM");
}

// --- upstream f581fa729d8e (18.5): VM buffers registered in heap records ---

// Block references as the write side registers them, per record:
// (block_id, fork, block, flags).
type BlkRefs = Vec<(u8, ::types_core::ForkNumber, BlockNumber, u8)>;
static BLKREFS: Mutex<Vec<(u8, BlkRefs)>> = Mutex::new(Vec::new());

fn capture_blkrefs() {
    BLKREFS.lock().unwrap().clear();
    *WAL_INSPECT.lock().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(|info, blocks| {
        let refs = blocks
            .iter()
            .map(|b| (b.block_id, b.forknum, b.block, b.flags))
            .collect();
        BLKREFS.lock().unwrap().push((info, refs));
    }));
}

fn take_blkrefs() -> Vec<(u8, BlkRefs)> {
    *WAL_INSPECT.lock().unwrap_or_else(|e| e.into_inner()) = None;
    core::mem::take(&mut *BLKREFS.lock().unwrap())
}

fn page_lsn(oid: Oid, page_idx: usize) -> u64 {
    let buf = with_fake(|f| f.tables[&oid][page_idx]);
    let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
    // SAFETY: leaked test page, always live.
    unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
}

const MAIN: ::types_core::ForkNumber = ::types_core::ForkNumber::MAIN_FORKNUM;
const VM: ::types_core::ForkNumber = ::types_core::ForkNumber::VISIBILITYMAP_FORKNUM;
const STD: u8 = ::xloginsert_seams::REGBUF_STANDARD;

// One all-visible heap page holding tuple (9, 0, 1), VM byte `vm_byte` for it.
fn all_visible_table(vm_byte: u8) -> Oid {
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(&[Item::Tuple(tuple_image(9, 0, 1))], true)],
    );
    register_table(oid + VM_OID_OFFSET, vec![vm_test_page(vm_byte)]);
    oid
}

// An insert that clears the page's VM bits registers the VM page as block
// HEAP_INSERT_BLKREF_VM (flags 0) and stamps it with the record LSN.
#[test]
fn insert_onto_all_visible_page_registers_vm_block() {
    install_dml_seams();
    install_vm_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = all_visible_table(0x03);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();
    capture_blkrefs();

    let mut tup = make_writable_tuple(&tuple_image(0, 0, 41));
    dml::heap_insert(&rel, &mut tup, 7, 0, None).unwrap();
    assert_eq!(tup.t_self, ItemPointerData::new(0, 2));

    assert!(!heap_page_flags_check(oid, 0), "PD_ALL_VISIBLE cleared");
    assert_eq!(vm_first_byte(oid), 0, "VM bits cleared");
    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_ne!(recs[0].1[2] & dml::XLH_INSERT_ALL_VISIBLE_CLEARED, 0);
    assert_eq!(
        take_blkrefs(),
        vec![(
            dml::XLOG_HEAP_INSERT,
            vec![(0, MAIN, 0, STD), (dml::HEAP_INSERT_BLKREF_VM, VM, 0, 0)]
        )]
    );
    let heap_lsn = page_lsn(oid, 0);
    assert_ne!(heap_lsn, 0);
    assert_eq!(page_lsn(oid + VM_OID_OFFSET, 0), heap_lsn, "VM page carries the record LSN");
    quiesced();
}

// PD_ALL_VISIBLE set but the VM bits already clear: the flag is still
// logged (redo re-clears via the fallback path) but no VM block is
// registered and the VM page's LSN does not move.
#[test]
fn insert_with_vm_bits_already_clear_registers_no_vm_block() {
    install_dml_seams();
    install_vm_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = all_visible_table(0x00);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();
    capture_blkrefs();

    let mut tup = make_writable_tuple(&tuple_image(0, 0, 41));
    dml::heap_insert(&rel, &mut tup, 7, 0, None).unwrap();

    assert!(!heap_page_flags_check(oid, 0), "PD_ALL_VISIBLE cleared");
    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_ne!(recs[0].1[2] & dml::XLH_INSERT_ALL_VISIBLE_CLEARED, 0);
    assert_eq!(
        take_blkrefs(),
        vec![(dml::XLOG_HEAP_INSERT, vec![(0, MAIN, 0, STD)])]
    );
    assert_eq!(page_lsn(oid + VM_OID_OFFSET, 0), 0, "untouched VM page keeps its LSN");
    quiesced();
}

#[test]
fn delete_on_all_visible_page_registers_vm_block() {
    install_dml_seams();
    install_vm_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = all_visible_table(0x03);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();
    capture_blkrefs();

    let tid = ItemPointerData::new(0, 1);
    let mut tmfd = TM_FailureData::default();
    let r = dml::heap_delete(&rel, &tid, 7, None, true, &mut tmfd, false).unwrap();
    assert_eq!(r, TM_Result::TM_Ok);

    assert!(!heap_page_flags_check(oid, 0), "PD_ALL_VISIBLE cleared");
    assert_eq!(vm_first_byte(oid), 0, "VM bits cleared");
    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_ne!(recs[0].1[7] & dml::XLH_DELETE_ALL_VISIBLE_CLEARED, 0);
    assert_eq!(
        take_blkrefs(),
        vec![(
            dml::XLOG_HEAP_DELETE,
            vec![(0, MAIN, 0, STD), (dml::HEAP_DELETE_BLKREF_VM, VM, 0, 0)]
        )]
    );
    assert_eq!(page_lsn(oid + VM_OID_OFFSET, 0), page_lsn(oid, 0));
    quiesced();
}

// Same-page (HOT) update: the one heap block is block 0, and the VM page
// covering it is registered as HEAP_UPDATE_BLKREF_VM_OLD.
#[test]
fn hot_update_on_all_visible_page_registers_vm_old() {
    install_dml_seams();
    install_vm_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = all_visible_table(0x03);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();
    capture_blkrefs();

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
    assert_eq!(newtup.t_self, ItemPointerData::new(0, 2));

    assert!(!heap_page_flags_check(oid, 0), "PD_ALL_VISIBLE cleared");
    assert_eq!(vm_first_byte(oid), 0, "VM bits cleared");
    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_eq!(
        recs[0].1[7] & (dml::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED | dml::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED),
        dml::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED
    );
    assert_eq!(
        take_blkrefs(),
        vec![(
            dml::XLOG_HEAP_HOT_UPDATE,
            vec![(0, MAIN, 0, STD), (dml::HEAP_UPDATE_BLKREF_VM_OLD, VM, 0, 0)]
        )]
    );
    assert_eq!(page_lsn(oid + VM_OID_OFFSET, 0), page_lsn(oid, 0));
    quiesced();
}

// Cross-page update off a full all-visible page: the lock-only record
// clears (and registers the VM page for) all-frozen, then the update record
// registers new (0) and old (1) heap blocks plus the old page's VM page as
// HEAP_UPDATE_BLKREF_VM_OLD; the fresh new page has no VM bits to clear.
#[test]
fn cross_page_update_registers_vm_old_after_lock_record() {
    install_dml_seams();
    install_vm_seams();
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
            true,
        )],
    );
    register_table(oid + VM_OID_OFFSET, vec![vm_test_page(0x03)]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();
    capture_blkrefs();

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
    assert_eq!(newtup.t_self, ItemPointerData::new(1, 1));

    assert!(!heap_page_flags_check(oid, 0), "old page PD_ALL_VISIBLE cleared");
    assert_eq!(vm_first_byte(oid), 0, "old block's VM bits cleared");
    let recs = take_xlog();
    assert_eq!(recs.len(), 2, "xl_heap_lock then xl_heap_update");
    assert_eq!(recs[0].1[7], dml::XLH_LOCK_ALL_FROZEN_CLEARED);
    assert_eq!(
        recs[1].1[7] & (dml::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED | dml::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED),
        dml::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED
    );
    assert_eq!(
        take_blkrefs(),
        vec![
            (
                dml::XLOG_HEAP_LOCK,
                vec![(0, MAIN, 0, STD), (dml::HEAP_LOCK_BLKREF_VM, VM, 0, 0)]
            ),
            (
                dml::XLOG_HEAP_UPDATE | dml::XLOG_HEAP_INIT_PAGE,
                vec![
                    (0, MAIN, 1, STD | ::xloginsert_seams::REGBUF_WILL_INIT),
                    (1, MAIN, 0, STD),
                    (dml::HEAP_UPDATE_BLKREF_VM_OLD, VM, 0, 0),
                ]
            ),
        ]
    );
    assert_eq!(page_lsn(oid + VM_OID_OFFSET, 0), page_lsn(oid, 0), "VM page LSN = update record");
    quiesced();
}

// Locking a tuple on an all-frozen page clears only all-frozen and
// registers the VM page as HEAP_LOCK_BLKREF_VM; PD_ALL_VISIBLE survives.
#[test]
fn lock_tuple_clearing_all_frozen_registers_vm_block() {
    install_dml_seams();
    install_vm_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = all_visible_table(0x03);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();
    capture_blkrefs();

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

    assert!(heap_page_flags_check(oid, 0), "PD_ALL_VISIBLE kept");
    assert_eq!(vm_first_byte(oid), 0x01, "all-visible kept, all-frozen cleared");
    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].1[7], dml::XLH_LOCK_ALL_FROZEN_CLEARED);
    assert_eq!(
        take_blkrefs(),
        vec![(
            dml::XLOG_HEAP_LOCK,
            vec![(0, MAIN, 0, STD), (dml::HEAP_LOCK_BLKREF_VM, VM, 0, 0)]
        )]
    );
    assert_eq!(page_lsn(oid + VM_OID_OFFSET, 0), page_lsn(oid, 0));
    quiesced();
}

// All-visible but not all-frozen: nothing to clear, so no flag, no VM block
// and no VM LSN movement -- even though the VM buffer was locked.
#[test]
fn lock_tuple_on_unfrozen_all_visible_page_registers_no_vm_block() {
    install_dml_seams();
    install_vm_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = all_visible_table(0x01);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();
    capture_blkrefs();

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

    assert_eq!(vm_first_byte(oid), 0x01);
    let recs = take_xlog();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].1[7], 0);
    assert_eq!(
        take_blkrefs(),
        vec![(dml::XLOG_HEAP_LOCK, vec![(0, MAIN, 0, STD)])]
    );
    assert_eq!(page_lsn(oid + VM_OID_OFFSET, 0), 0);
    quiesced();
}

// --- audit-18.6 remediation batch b008 (backend/access/heap) witnesses ---

/// Arms a pending query cancel for the duration of a test (InterruptPending
/// + the harness's check_for_interrupts seam), restoring both on drop.
struct CancelPending;
impl CancelPending {
    fn arm() -> Self {
        CANCEL_PENDING.store(true, Ordering::Relaxed);
        init_small::globals::SetInterruptPending(true);
        CancelPending
    }
}
impl Drop for CancelPending {
    fn drop(&mut self) {
        init_small::globals::SetInterruptPending(false);
        CANCEL_PENDING.store(false, Ordering::Relaxed);
    }
}

// heapam.c:2452 heap_multi_insert: CHECK_FOR_INTERRUPTS() at the top of the
// per-page loop, so a pending cancel aborts the batch before any page is
// filled (a186-candidate-fp-heap-heapam-p1-defdc40bdc724bf1744d-1).
#[test]
fn multi_insert_page_loop_checks_for_interrupts() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    let _ = take_xlog();

    let mut s1 = heap_slot_with(mcx, &tuple_image(0, 0, 41));
    let mut s2 = heap_slot_with(mcx, &tuple_image(0, 0, 42));
    let mut slots = [&mut s1, &mut s2];
    let err = {
        let _cancel = CancelPending::arm();
        dml::heap_multi_insert(mcx, &rel, &mut slots, 7, 0, None)
            .expect_err("pending cancel must abort the multi-insert page loop")
    };
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_QUERY_CANCELED);
    // Nothing was placed and nothing was logged.
    assert!(take_xlog().is_empty(), "no MULTI_INSERT record after the cancel");
    with_fake(|f| assert!(f.tables[&oid].is_empty(), "no page was extended"));
}

// heapam.c:1350 heap_getnext: a scan over a relation whose table AM is not
// the heap AM is refused with ERRCODE_FEATURE_NOT_SUPPORTED "only heap AM is
// supported" (a186-candidate-fp-heap-heapam-p1-7b5133974859f30af161-1).
#[test]
fn heap_getnext_refuses_non_heap_table_am() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![build_page(&[Item::Tuple(tuple_image(10, 0, 5))], true)]);
    // An unregistered relam: TableAm::of() is None (not the heap AM).
    let rel = test_relation_am(mcx, oid, b'd', false, 424242);

    let mut scan = begin_seqscan(mcx, &rel, mvcc_snapshot(mcx));
    let err = match heap_getnext(&mut scan, ForwardScanDirection) {
        Ok(_) => panic!("heap_getnext over a non-heap AM must be a catchable error"),
        Err(e) => e,
    };
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message, "only heap AM is supported");
    heap_endscan(scan).unwrap();
    quiesced();
}

// heapam.c HeapKeyTest -> heap_getattr(): scan keys may name system
// attributes (attno < 0); heap_getattr routes them to heap_getsysattr
// (a186-candidate-fp-heap-heapam-p1-132358f78c3a9c75d71f-1).
#[test]
fn heap_key_test_accepts_system_attribute_scan_keys() {
    use ::types_scan::scankey::ScanKeyData;
    use ::types_tuple::htup::TableOidAttributeNumber;
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        vec![build_page(
            &[
                Item::Tuple(tuple_image(10, 0, 5)),
                Item::Tuple(tuple_image(10, 0, 42)),
            ],
            true,
        )],
    );
    let rel = test_relation(mcx, oid);

    let flags = SO_TYPE_SEQSCAN | SO_ALLOW_PAGEMODE;
    for (arg, expected) in [(oid, vec![(0u32, 1u16, 5i32), (0, 2, 42)]), (oid + 1, vec![])] {
        let mut key = PgVec::new_in(mcx);
        key.push(ScanKeyData {
            sk_flags: 0,
            sk_attno: TableOidAttributeNumber as i16,
            sk_strategy: ::types_scan::scankey::BTEqualStrategyNumber,
            sk_subtype: 0,
            sk_collation: 0,
            // tableoid is a 4-byte by-value datum: int4eq compares it exactly.
            sk_func: ::types_fmgr::FmgrInfo::new(int4eq, 65, 2, true, false),
            sk_argument: Datum::from_i32(arg as i32),
        });
        let mut scan =
            heap_beginscan(mcx, &rel, mvcc_snapshot(mcx), 1, key, None, flags).unwrap();
        let vals = collect_vals(&mut scan, ForwardScanDirection);
        assert_eq!(vals, expected, "tableoid = {arg}");
        heap_endscan(scan).unwrap();
    }
    quiesced();
}

// heapam.c:8329 index_delete_check_htid: an index TID whose offset is 0
// (below FirstOffsetNumber) is index corruption, reported as
// ERRCODE_INDEX_CORRUPTED like the other in-passing checks — never a panic
// (a186-candidate-fp-heap-heapam-p4-261af0489cea380aa54f-1).
#[test]
fn index_delete_check_htid_offset_zero_is_index_corruption() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    let page = build_page(&[Item::Tuple(tuple_image(10, 0, 5))], false);
    let rel = test_relation(mcx, oid);
    // The index relation only lends its name to the message.
    let irel = test_relation(mcx, oid + 1);
    // SAFETY: a fully built BLCKSZ page image, exclusively owned here.
    let pref = unsafe { PageRef::from_raw(core::ptr::NonNull::from(&page.0[0])) };
    let maxoff = pref.max_offset_number();
    assert_eq!(maxoff, 1);

    let htid = ItemPointerData::new(0, 0);
    let err = crate::index_delete::index_delete_check_htid(&irel, 7, &pref, maxoff, &htid, 3)
        .expect_err("offset 0 is corruption, not a panic");
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INDEX_CORRUPTED);
    assert_eq!(
        err.message,
        "heap tid from index tuple (0,0) points to unused heap page item at offset 3 of block 7 in index \"t\""
    );
    // Control: a valid offset passes the check.
    let ok_tid = ItemPointerData::new(0, 1);
    crate::index_delete::index_delete_check_htid(&irel, 7, &pref, maxoff, &ok_tid, 3).unwrap();
    let _ = rel;
}

// heapam.c:5521 heap_acquire_tuplock: under LockWaitError the GUC
// log_lock_failures is handed to ConditionalLockTupleTuplock so the lock
// manager logs the failed acquisition
// (a186-candidate-fp-heap-heapam-p3-461071caa12657cb0523-1).
#[test]
fn acquire_tuplock_nowait_passes_log_lock_failures_guc() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(oid, vec![]);
    let rel = test_relation(mcx, oid);
    let tid = ItemPointerData::new(0, 1);

    for guc in [false, true] {
        LOG_LOCK_FAILURES_GUC.store(guc, Ordering::Relaxed);
        LAST_LOG_LOCK_FAILURE.store(!guc, Ordering::Relaxed);
        let mut have_tuple_lock = false;
        let err = dml::heap_acquire_tuplock(
            &rel,
            &tid,
            LockTupleMode::LockTupleExclusive,
            ::tableam_vocab::LockWaitPolicy::LockWaitError,
            &mut have_tuple_lock,
        )
        .expect_err("NOWAIT on an unavailable tuple lock is 55P03");
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_LOCK_NOT_AVAILABLE);
        assert_eq!(
            LAST_LOG_LOCK_FAILURE.load(Ordering::Relaxed),
            guc,
            "log_lock_failures={guc} must reach LockAcquireExtended"
        );
        assert!(!have_tuple_lock);
    }
    LOG_LOCK_FAILURES_GUC.store(false, Ordering::Relaxed);
}

// heapam.c:5232 heap_lock_tuple, LockWaitError with a plain-xid xmax: the
// log_lock_failures GUC is handed to ConditionalXactLockTableWait so the lock
// manager logs the failed acquisition before 55P03
// (a186-verified-fp-heap-heapam-p2-0a40872d6ee3b565bf38-1).
#[test]
fn lock_tuple_nowait_xid_passes_log_lock_failures_guc() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    // xmax 200 (another, running xact), HEAP_XMAX_INVALID clear: the
    // visibility seam answers TM_BeingModified.
    let mut img = tuple_image(10, 200, 1);
    set_infomask(&mut img, 0, 0);
    register_table(oid, vec![build_page(&[Item::Tuple(img)], false)]);
    let rel = test_relation(mcx, oid);
    let tid = ItemPointerData::new(0, 1);

    TUPLE_LOCK_AVAILABLE.store(true, Ordering::Relaxed);
    for guc in [false, true] {
        LOG_LOCK_FAILURES_GUC.store(guc, Ordering::Relaxed);
        LAST_LOG_LOCK_FAILURE.store(!guc, Ordering::Relaxed);
        let mut tmfd = TM_FailureData::default();
        let err = dml::heap_lock_tuple(
            &rel,
            &tid,
            7,
            LockTupleMode::LockTupleExclusive,
            ::tableam_vocab::LockWaitPolicy::LockWaitError,
            false,
            &mut tmfd,
        )
        .err()
        .expect("NOWAIT against a running updater is 55P03");
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_LOCK_NOT_AVAILABLE);
        assert_eq!(
            err.message(),
            std::format!("could not obtain lock on row in relation \"{}\"", rel.name())
        );
        assert_eq!(
            LAST_LOG_LOCK_FAILURE.load(Ordering::Relaxed),
            guc,
            "log_lock_failures={guc} must reach ConditionalXactLockTableWait (heapam.c:5232)"
        );
    }
    TUPLE_LOCK_AVAILABLE.store(false, Ordering::Relaxed);
    LOG_LOCK_FAILURES_GUC.store(false, Ordering::Relaxed);
    quiesced();
}

// heapam.c:5194 heap_lock_tuple, LockWaitError with a MultiXact xmax: the GUC
// reaches each member's ConditionalXactLockTableWait through
// ConditionalMultiXactIdWait
// (a186-verified-fp-heap-heapam-p2-b8f24eb0e170af449fda-1).
#[test]
fn lock_tuple_nowait_multixact_passes_log_lock_failures_guc() {
    use ::types_storage::multixact::{MultiXactMember, MultiXactStatus};
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    // xmax = multixact 500 whose one member (xid 300) holds FOR SHARE.
    let mut img = tuple_image(10, 500, 1);
    set_infomask(&mut img, ::types_tuple::HEAP_XMAX_IS_MULTI, 0);
    register_table(oid, vec![build_page(&[Item::Tuple(img)], false)]);
    let rel = test_relation(mcx, oid);
    let tid = ItemPointerData::new(0, 1);
    *MULTI_MEMBERS.lock().unwrap() =
        vec![MultiXactMember { xid: 300, status: MultiXactStatus::MultiXactStatusForShare }];

    TUPLE_LOCK_AVAILABLE.store(true, Ordering::Relaxed);
    for guc in [false, true] {
        LOG_LOCK_FAILURES_GUC.store(guc, Ordering::Relaxed);
        LAST_LOG_LOCK_FAILURE.store(!guc, Ordering::Relaxed);
        let mut tmfd = TM_FailureData::default();
        let err = dml::heap_lock_tuple(
            &rel,
            &tid,
            7,
            LockTupleMode::LockTupleExclusive,
            ::tableam_vocab::LockWaitPolicy::LockWaitError,
            false,
            &mut tmfd,
        )
        .err()
        .expect("NOWAIT against a conflicting multixact member is 55P03");
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_LOCK_NOT_AVAILABLE);
        assert_eq!(
            LAST_LOG_LOCK_FAILURE.load(Ordering::Relaxed),
            guc,
            "log_lock_failures={guc} must reach the member's ConditionalXactLockTableWait (heapam.c:5194)"
        );
    }
    TUPLE_LOCK_AVAILABLE.store(false, Ordering::Relaxed);
    LOG_LOCK_FAILURES_GUC.store(false, Ordering::Relaxed);
    MULTI_MEMBERS.lock().unwrap().clear();
    quiesced();
}

// heapam.c:5725 compute_new_xmax_infomask: LOCK_ONLY without any lock bit
// (a pg_upgrade'd page) whose xmax is still in progress is WARNING
// "LOCK_ONLY found for Xid in progress %u", then treated as unlocked
// (a186-candidate-fp-heap-heapam-p3-bf8d4c2305d26fa4dd55-1).
#[test]
fn compute_new_xmax_infomask_warns_on_lock_only_without_lock_bits() {
    install_dml_seams();
    let _serial = serial();
    IN_PROGRESS_XIDS.lock().unwrap().push(300);
    WARNINGS.lock().unwrap().clear();

    let (new_xmax, new_infomask, new_infomask2) = dml::compute_new_xmax_infomask(
        300,
        ::types_tuple::HEAP_XMAX_LOCK_ONLY,
        0,
        FAKE_XID,
        LockTupleMode::LockTupleExclusive,
        false,
    )
    .unwrap();
    IN_PROGRESS_XIDS.lock().unwrap().clear();

    assert_eq!(
        WARNINGS.lock().unwrap().as_slice(),
        &["LOCK_ONLY found for Xid in progress 300".to_string()][..],
        "heapam.c:5725 emits the WARNING"
    );
    // The stale locker is dropped: the new xmax is our own exclusive lock.
    assert_eq!(new_xmax, FAKE_XID);
    assert_eq!(
        new_infomask,
        ::types_tuple::HEAP_XMAX_LOCK_ONLY | ::types_tuple::HEAP_XMAX_EXCL_LOCK
    );
    assert_eq!(new_infomask2, HEAP_KEYS_UPDATED);
}

// heapam.c:7187 FreezeMultiXactId: a multixact with two updating members is
// ERRCODE_DATA_CORRUPTED with errdetail_internal naming both updater XIDs
// (a186-candidate-fp-heap-heapam-p3-11d0662592cf21d8afd9-1).
#[test]
fn freeze_multixact_two_updaters_reports_both_xids_in_detail() {
    use ::types_storage::multixact::{MultiXactMember, MultiXactStatus};
    install_dml_seams();
    let _serial = serial();
    *MULTI_MEMBERS.lock().unwrap() = vec![
        MultiXactMember { xid: 300, status: MultiXactStatus::MultiXactStatusUpdate },
        MultiXactMember { xid: 301, status: MultiXactStatus::MultiXactStatusUpdate },
    ];
    IN_PROGRESS_XIDS.lock().unwrap().push(300);
    let cutoffs = ::tableam_vocab::VacuumCutoffs {
        relfrozenxid: 50,
        relminmxid: 1,
        OldestXmin: 250,
        OldestMxact: 1,
        // Members precede FreezeLimit: the multi must be replaced.
        FreezeLimit: 400,
        MultiXactCutoff: 1,
    };
    let mut flags = 0u16;
    let mut pagefrz = crate::freeze::HeapPageFreeze {
        freeze_required: false,
        FreezePageRelfrozenXid: 50,
        FreezePageRelminMxid: 1,
        NoFreezePageRelfrozenXid: 50,
        NoFreezePageRelminMxid: 1,
    };
    let err = crate::freeze::FreezeMultiXactId(
        5,
        ::types_tuple::HEAP_XMAX_IS_MULTI,
        &cutoffs,
        &mut flags,
        &mut pagefrz,
    )
    .err()
    .expect("two updaters are data corruption");
    IN_PROGRESS_XIDS.lock().unwrap().clear();
    MULTI_MEMBERS.lock().unwrap().clear();

    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_DATA_CORRUPTED);
    assert_eq!(err.message(), "multixact 5 has two or more updating members");
    assert_eq!(
        err.detail(),
        Some("First updater XID=300 second updater XID=301."),
        "heapam.c:7187 errdetail_internal"
    );
}

// hio.c:826 RelationGetBufferForTuple: after extending for a tuple that did
// not fit heap_update's old page (otherBuffer), the old page is locked with
// ConditionalLockBuffer first; the newly extended page stays locked (the
// unlock/lock/relock order is only the fallback when that fails)
// (a186-candidate-fp-heap-hio-08d267c0d2586ab4c8aa-1).
#[test]
fn buffer_for_tuple_tries_conditional_lock_on_other_buffer_first() {
    install_dml_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    // Page 0 holds 16 tuples; an 8000-byte tuple cannot fit there.
    let items: Vec<Item> = (0..16).map(|i| Item::Tuple(tuple_image(10, 0, i))).collect();
    register_table(oid, vec![build_page(&items, false)]);
    let rel = test_relation(mcx, oid);
    // heap_update's old page: pinned, and (as C, heapam.c:3960) unlocked
    // before the relation is extended.
    let other = BufferPin::adopt(bufmgr_seams::read_buffer::call(&rel, 0).unwrap()).unwrap();

    COND_LOCK_CALLS.store(0, Ordering::Relaxed);
    let pin = hio::RelationGetBufferForTuple(&rel, 8000, Some(&other), 0, None, 1).unwrap();
    assert_eq!(pin.block_number(), 1, "the relation was extended by one page");
    assert_eq!(
        COND_LOCK_CALLS.load(Ordering::Relaxed),
        1,
        "hio.c:826 ConditionalLockBuffer(otherBuffer) is tried first"
    );
    // Both pages come back exclusively locked.
    with_fake(|f| {
        assert_eq!(f.locks[(other.buffer() - 1) as usize], 1, "old page locked");
        assert_eq!(f.locks[(pin.buffer() - 1) as usize], 1, "new page locked");
    });
    bufmgr_seams::lock_buffer::call(pin.buffer(), bufmgr_seams::BUFFER_LOCK_UNLOCK).unwrap();
    bufmgr_seams::lock_buffer::call(other.buffer(), bufmgr_seams::BUFFER_LOCK_UNLOCK).unwrap();
    drop(pin);
    drop(other);
    quiesced();
}

// pg_stat seq_tup_read under the page-batch feed (sitediff N-2): C's
// pgstat_count_heap_getnext is per tuple RETURNED, so a consumer that stops
// mid-page (LIMIT 1, EXISTS, an RI check's tcount=1) credits only the rows it
// pulled; a page the feed advances past is credited whole.
#[test]
fn pagebatch_credits_rows_stored_then_whole_page_on_advance() {
    install_seams();
    let _serial = serial();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let oid = fresh_oid();
    register_table(
        oid,
        (0..2)
            .map(|p| {
                build_page(
                    &[
                        Item::Tuple(tuple_image(10, 0, p * 3)),
                        Item::Tuple(tuple_image(10, 0, p * 3 + 1)),
                        Item::Tuple(tuple_image(10, 0, p * 3 + 2)),
                    ],
                    true,
                )
            })
            .collect(),
    );
    let rel = test_relation(mcx, oid);
    let mut scan = begin_seqscan(mcx, &rel, mvcc_snapshot(mcx));

    assert_eq!(heap_getnextpagebatch(&mut scan).unwrap(), 3);
    assert_eq!(scan.rs_pgstat_tuples, 0, "staging a page returns nothing yet");
    batch_credit_upto(&mut scan, 1);
    assert_eq!(scan.rs_pgstat_tuples, 1, "LIMIT 1 shape: one row returned");
    batch_credit_upto(&mut scan, 1);
    assert_eq!(scan.rs_pgstat_tuples, 1, "re-crediting a row is a no-op");
    batch_credit_upto(&mut scan, 2);
    assert_eq!(scan.rs_pgstat_tuples, 2);

    // Advancing past the page: every row on it was handed to the qual.
    assert_eq!(heap_getnextpagebatch(&mut scan).unwrap(), 3);
    assert_eq!(scan.rs_pgstat_tuples, 3);
    // Exhaustion credits the last page in full.
    assert_eq!(heap_getnextpagebatch(&mut scan).unwrap(), 0);
    assert_eq!(scan.rs_pgstat_tuples, 6);
    heap_endscan(scan).unwrap();
    quiesced();
}

// Sitediff N-1: an erroring statement never reaches heap_endscan; C's
// counts already sit in rel->pgstat_info and are reported at abort, so the
// scan descriptor's drop drains the batched counters too (and a rescan
// mid-page leaves the unreturned remainder uncounted).
#[test]
fn scan_drop_drains_batched_pgstat_counters() {
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
                    Item::Tuple(tuple_image(10, 0, 3)),
                ],
                true,
            ),
            build_page(&[Item::Tuple(tuple_image(10, 0, 4))], true),
        ],
    );
    let rel = test_relation(mcx, oid);
    let counts = |before: (i64, i64)| {
        let c = ::pgstat::relation::find_tabstat_entry(oid).expect("pending entry");
        (c.numscans - before.0, c.tuples_returned - before.1)
    };
    let before = ::pgstat::relation::find_tabstat_entry(oid)
        .map(|c| (c.numscans, c.tuples_returned))
        .unwrap_or((0, 0));

    // Per-tuple walk, two rows out, then the "error" (plain drop).
    let mut scan = begin_seqscan(mcx, &rel, mvcc_snapshot(mcx));
    assert_eq!(counts(before), (1, 0), "numscans lands at initscan (C)");
    assert!(heap_getnext(&mut scan, ForwardScanDirection).unwrap().is_some());
    assert!(heap_getnext(&mut scan, ForwardScanDirection).unwrap().is_some());
    assert_eq!(scan.rs_pgstat_tuples, 2);
    drop(scan);
    assert_eq!(counts(before), (1, 2), "drop drained tuples_returned into the pending counts");

    // Batch feed stopped mid-page, then rescan: only the pulled row counts;
    // the rescan is a second scan.
    let mut scan = begin_seqscan(mcx, &rel, mvcc_snapshot(mcx));
    assert_eq!(heap_getnextpagebatch(&mut scan).unwrap(), 3);
    batch_credit_upto(&mut scan, 1);
    heap_rescan(&mut scan, None, false, false, false, false).unwrap();
    assert_eq!(counts(before), (3, 2));
    assert_eq!(scan.rs_pgstat_tuples, 1);
    let vals = collect_vals(&mut scan, ForwardScanDirection);
    assert_eq!(vals.len(), 4);
    assert_eq!(scan.rs_pgstat_tuples, 5);
    heap_endscan(scan).unwrap();
    assert_eq!(counts(before), (3, 7), "heap_endscan drains once; the drop after it sees zeros");
    quiesced();
}
