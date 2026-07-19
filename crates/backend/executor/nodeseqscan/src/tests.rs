use super::*;

use core::ptr::NonNull;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Mutex, Once};

use ::datum::Datum;
use ::mcx::MemoryContext;
use ::types_core::{
    Buffer, GlobalVisStateHandle, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::{Plan, Scan, SeqScan};
use ::types_nodes::primnodes::OpExpr;
use ::types_rel::{FormData_pg_class, LockInfoData, LockRelId, RelationData, RELKIND_RELATION};
use ::types_snapshot::{SnapshotData, SnapshotType};
use ::types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData, LP_NORMAL};
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, NameData, PgTypeShape, TupleDescData,
    HEAP_XMAX_INVALID, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};
use executils::EStateData;

const INT4OID: Oid = 23;
const BOOLOID: Oid = 16;
const F_INT4EQ: Oid = 65;
const F_INT4GT: Oid = 147;

struct Fake {
    tables: HashMap<Oid, Vec<Buffer>>,
    pages: Vec<usize>,
    pins: Vec<i32>,
    locks: Vec<i32>,
}

static FAKE: Mutex<Option<Fake>> = Mutex::new(None);
static SERIAL: Mutex<()> = Mutex::new(());
static INIT: Once = Once::new();

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
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
            with_fake(|f| Ok(f.tables[&rel.rd_id].len() as u32))
        });

        heapam_visibility_seams::heap_tuple_satisfies_visibility::set(
            |_htup, _snap, _buf| Ok(true),
        );
        heapam_visibility_seams::heap_tuple_satisfies_mvcc_page::set(
            |_htup, _snap, _buf, _memo| Ok(true),
        );
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_htup, _vt| Ok(false));
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_hdr| Ok(false));

        predicate_seams::check_for_serializable_conflict_out_needed::set(|_rel, _snap| Ok(false));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        predicate_seams::predicate_lock_relation::set(|_rel, _snap| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_rel, _tid, _snap, _xid| Ok(()));

        pruneheap_seams::heap_page_prune_opt::set(|_rel, _buf| Ok(()));
        procarray_seams::global_vis_test_for::set(|_rel| GlobalVisStateHandle::new(0));

        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                BOOLOID => Some(PgTypeShape {
                    typlen: 1,
                    typbyval: true,
                    typalign: b'c' as i8,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
    });
}

fn quiesced() {
    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });
}

fn tuple_image(val: i32) -> Vec<u8> {
    let mut img = vec![0u8; 28];
    img[0..4].copy_from_slice(&10u32.to_ne_bytes()); // xmin
    img[18..20].copy_from_slice(&1u16.to_ne_bytes()); // natts = 1
    img[20..22].copy_from_slice(&HEAP_XMAX_INVALID.to_ne_bytes());
    img[22] = 24; // t_hoff
    img[24..28].copy_from_slice(&val.to_ne_bytes());
    img
}

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

fn build_page(vals: &[i32]) -> Box<TestPage> {
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

fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: INT4OID,
        atttypmod: -1,
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
    let data = RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
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
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    };
    Relation::open(data, None)
}

fn static_mvcc_snapshot() -> Rc<SnapshotData<'static>> {
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap-test")));
    Rc::new(SnapshotData::sentinel(ctx.mcx(), SnapshotType::SNAPSHOT_MVCC))
}

static NEXT_OID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(70000);
fn fresh_oid() -> Oid {
    NEXT_OID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn with_mcx<R>(f: impl for<'m> FnOnce(Mcx<'m>) -> R) -> R {
    install_seams();
    let ctx = MemoryContext::new("nodeseqscan-test");
    f(ctx.mcx())
}

fn mk_seqscan<'mcx>(
    scanrelid: u32,
    targetlist: NodeList<'mcx>,
    qual: NodeList<'mcx>,
) -> SeqScan<'mcx> {
    SeqScan {
        cb_scan_cols: None,
        scan: Scan {
            plan: Plan { targetlist, qual, ..Default::default() },
            scanrelid,
        },
    }
}

fn matching_tlist<'mcx>(mcx: Mcx<'mcx>) -> NodeList<'mcx> {
    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
    NodeList::make1(mcx, tle).unwrap()
}

fn const_tlist<'mcx>(mcx: Mcx<'mcx>, v: i32) -> NodeList<'mcx> {
    let c = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(v), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, c, 1, None, false).unwrap();
    NodeList::make1(mcx, tle).unwrap()
}

fn int4_qual<'mcx>(mcx: Mcx<'mcx>, opfuncid: Oid, konst: i32) -> NodeList<'mcx> {
    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let c = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(konst), false, true).unwrap();
    let args = NodeList::make2(mcx, var, c).unwrap();
    let op = Node::mk(
        mcx,
        OpExpr {
            opno: 0,
            opfuncid,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args,
            location: -1,
        },
    )
    .unwrap();
    NodeList::make1(mcx, op).unwrap()
}

fn setup<'mcx>(
    mcx: Mcx<'mcx>,
    pages: &[&[i32]],
    node: &SeqScan<'mcx>,
) -> (EStateData<'mcx>, SeqScanState<'mcx>) {
    let oid = fresh_oid();
    register_table(oid, pages.iter().map(|vals| build_page(vals)).collect());
    let rel = test_relation(mcx, oid);
    let mut estate = EStateData::new_in(mcx);
    estate.es_snapshot = Some(static_mvcc_snapshot());
    let state = exec_init_seq_scan_rel(mcx, node, &mut estate, rel).unwrap();
    (estate, state)
}

fn drain<'mcx>(node: &mut SeqScanState<'mcx>, estate: &mut EStateData<'mcx>) -> Vec<i32> {
    let mut out = Vec::new();
    while let Some(id) = exec_seq_scan(node, estate).unwrap() {
        let mut isnull = false;
        let v = exectuples::slot_getattr(estate.slot_mut(id), 1, &mut isnull);
        assert!(!isnull);
        out.push(v.as_i32());
    }
    out
}

fn teardown<'mcx>(mut node: SeqScanState<'mcx>, estate: &mut EStateData<'mcx>) {
    exec_end_seq_scan(&mut node).unwrap();
    estate.exec_reset_tuple_table(false);
    quiesced();
}

#[test]
fn plain_scan_returns_all_tuples() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), NodeList::default());
        let (mut estate, mut state) = setup(mcx, &[&[1, 2, 3], &[4, 5]], &node);
        assert_eq!(state.variant(), SeqScanVariant::Plain);
        assert!(state.ss.ss_currentScanDesc.is_none());
        assert_eq!(drain(&mut state, &mut estate), vec![1, 2, 3, 4, 5]);
        assert!(state.ss.ss_currentScanDesc.is_some());
        // C restarts an exhausted scan on the next call (rs_inited reset).
        assert_eq!(drain(&mut state, &mut estate), vec![1, 2, 3, 4, 5]);
        teardown(state, &mut estate);
    });
}

#[test]
fn qual_filters_tuples_through_fused_kernel() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), int4_qual(mcx, F_INT4GT, 3));
        let (mut estate, mut state) = setup(mcx, &[&[1, 2, 3], &[4, 5, 6]], &node);
        assert_eq!(state.variant(), SeqScanVariant::WithQual);
        assert!(matches!(
            state.ss.qual.as_ref().unwrap().kernel(),
            execexpr::Kernel::QualScanVarCmpConst { .. }
        ));
        assert_eq!(drain(&mut state, &mut estate), vec![4, 5, 6]);
        teardown(state, &mut estate);
    });
}

#[test]
fn qual_no_match_returns_none() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), int4_qual(mcx, F_INT4EQ, 99));
        let (mut estate, mut state) = setup(mcx, &[&[1, 2, 3]], &node);
        assert_eq!(drain(&mut state, &mut estate), Vec::<i32>::new());
        teardown(state, &mut estate);
    });
}

#[test]
fn projection_evaluates_targetlist() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, const_tlist(mcx, 42), NodeList::default());
        let (mut estate, mut state) = setup(mcx, &[&[1, 2, 3]], &node);
        assert_eq!(state.variant(), SeqScanVariant::WithProject);
        let result_id = state.ss.ps_ProjInfo.as_ref().unwrap().pi_result_slot;
        assert_ne!(result_id, state.ss.ss_ScanTupleSlot);
        assert_eq!(drain(&mut state, &mut estate), vec![42, 42, 42]);
        teardown(state, &mut estate);
    });
}

#[test]
fn qual_and_projection_compose() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, const_tlist(mcx, 7), int4_qual(mcx, F_INT4EQ, 2));
        let (mut estate, mut state) = setup(mcx, &[&[1, 2, 3], &[2, 5]], &node);
        assert_eq!(state.variant(), SeqScanVariant::WithQualProject);
        assert_eq!(drain(&mut state, &mut estate), vec![7, 7]);
        teardown(state, &mut estate);
    });
}

#[test]
fn rescan_restarts_from_first_tuple() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), NodeList::default());
        let (mut estate, mut state) = setup(mcx, &[&[10, 20, 30]], &node);
        assert!(exec_seq_scan(&mut state, &mut estate).unwrap().is_some());
        assert!(exec_seq_scan(&mut state, &mut estate).unwrap().is_some());
        exec_rescan_seq_scan(&mut state, &mut estate).unwrap();
        assert_eq!(drain(&mut state, &mut estate), vec![10, 20, 30]);
        teardown(state, &mut estate);
    });
}

#[test]
fn rescan_before_first_fetch_is_noop() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), NodeList::default());
        let (mut estate, mut state) = setup(mcx, &[&[8, 9]], &node);
        exec_rescan_seq_scan(&mut state, &mut estate).unwrap();
        assert!(state.ss.ss_currentScanDesc.is_none());
        assert_eq!(drain(&mut state, &mut estate), vec![8, 9]);
        teardown(state, &mut estate);
    });
}

#[test]
fn end_scan_without_fetch_releases_nothing() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), NodeList::default());
        let (mut estate, state) = setup(mcx, &[&[1]], &node);
        teardown(state, &mut estate);
    });
}

fn mk_bloom<'mcx>(mcx: Mcx<'mcx>, admit: &[i32]) -> Rc<::nodehash::ProbeBloom<'mcx>> {
    let mut bf = ::nodehash::ProbeBloom::new_in(mcx, 64.0);
    for v in admit {
        bf.insert(::hashfn::hash_bytes_uint32(*v as u32));
    }
    Rc::new(bf)
}

#[test]
fn bloom_pushdown_filters_and_preserves_order() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), NodeList::default());
        let (mut estate, mut state) = setup(mcx, &[&[1, 2, 3, 4], &[5, 6, 7]], &node);
        state.batch_allowed = true;
        let bf = mk_bloom(mcx, &[2, 5, 7]);
        assert!(seq_scan_set_bloom(&mut state, &mut estate, Some((bf.clone(), 0))).unwrap());
        assert_eq!(state.variant(), SeqScanVariant::PlainBloom);
        let expect: Vec<i32> = (1..=7)
            .filter(|v| bf.test(::hashfn::hash_bytes_uint32(*v as u32)))
            .collect();
        assert!(expect.contains(&2) && expect.contains(&5) && expect.contains(&7));
        assert_eq!(drain(&mut state, &mut estate), expect);
        // Disarm returns the exact Plain drive.
        assert!(!seq_scan_set_bloom(&mut state, &mut estate, None).unwrap());
        assert_eq!(state.variant(), SeqScanVariant::Plain);
        exec_rescan_seq_scan(&mut state, &mut estate).unwrap();
        assert_eq!(drain(&mut state, &mut estate), vec![1, 2, 3, 4, 5, 6, 7]);
        teardown(state, &mut estate);
    });
}

#[test]
fn bloom_all_pass_matches_plain_and_rescan_restarts() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), NodeList::default());
        let (mut estate, mut state) = setup(mcx, &[&[10, 20], &[30]], &node);
        state.batch_allowed = true;
        let bf = mk_bloom(mcx, &[10, 20, 30]);
        assert!(seq_scan_set_bloom(&mut state, &mut estate, Some((bf, 0))).unwrap());
        assert_eq!(drain(&mut state, &mut estate), vec![10, 20, 30]);
        assert!(exec_seq_scan(&mut state, &mut estate).unwrap().is_some());
        exec_rescan_seq_scan(&mut state, &mut estate).unwrap();
        assert_eq!(drain(&mut state, &mut estate), vec![10, 20, 30]);
        teardown(state, &mut estate);
    });
}

#[test]
fn bloom_gate_rejects_nonplain_and_disallowed() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), int4_qual(mcx, F_INT4GT, 0));
        let (mut estate, mut state) = setup(mcx, &[&[1, 2]], &node);
        state.batch_allowed = true;
        let bf = mk_bloom(mcx, &[1]);
        assert!(!seq_scan_set_bloom(&mut state, &mut estate, Some((bf, 0))).unwrap());
        assert_eq!(state.variant(), SeqScanVariant::WithQual);
        teardown(state, &mut estate);
    });
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), NodeList::default());
        let (mut estate, mut state) = setup(mcx, &[&[1, 2]], &node);
        // batch_allowed stays false (EXEC_FLAG_BACKWARD|MARK shape).
        let bf = mk_bloom(mcx, &[1]);
        assert!(!seq_scan_set_bloom(&mut state, &mut estate, Some((bf, 0))).unwrap());
        assert_eq!(state.variant(), SeqScanVariant::Plain);
        assert_eq!(drain(&mut state, &mut estate), vec![1, 2]);
        teardown(state, &mut estate);
    });
}

#[test]
fn bloom_sparse_filter_admits_only_tested_hashes() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), NodeList::default());
        let (mut estate, mut state) = setup(mcx, &[&[3, 4]], &node);
        state.batch_allowed = true;
        // Admit only the raw 0 hash: data rows may pass solely as false
        // positives of that one slot; expected set computed exactly.
        let mut bf = ::nodehash::ProbeBloom::new_in(mcx, 64.0);
        bf.insert(0);
        let bf = Rc::new(bf);
        assert!(seq_scan_set_bloom(&mut state, &mut estate, Some((bf.clone(), 0))).unwrap());
        let expect: Vec<i32> = [3, 4]
            .into_iter()
            .filter(|v| bf.test(::hashfn::hash_bytes_uint32(*v as u32)))
            .collect();
        assert_eq!(drain(&mut state, &mut estate), expect);
        teardown(state, &mut estate);
    });
}

#[test]
fn bloom_adaptive_disarm_on_nonselective_scan() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), NodeList::default());
        let all: Vec<i32> = (0..1600).collect();
        let pages: Vec<&[i32]> = all.chunks(100).collect();
        let (mut estate, mut state) = setup(mcx, &pages, &node);
        state.batch_allowed = true;
        let bf = mk_bloom(mcx, &all);
        assert!(seq_scan_set_bloom(&mut state, &mut estate, Some((bf, 0))).unwrap());
        assert_eq!(drain(&mut state, &mut estate), all);
        // Non-selective filter disarms at a page boundary past 1024 rows;
        // the per-tuple walk resumed without skipping or repeating a row.
        assert_eq!(state.variant(), SeqScanVariant::Plain);
        teardown(state, &mut estate);
    });
}

#[test]
fn bloom_selective_scan_stays_armed() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_seqscan(1, matching_tlist(mcx), NodeList::default());
        let all: Vec<i32> = (0..1600).collect();
        let pages: Vec<&[i32]> = all.chunks(100).collect();
        let (mut estate, mut state) = setup(mcx, &pages, &node);
        state.batch_allowed = true;
        let bf = mk_bloom(mcx, &[5, 500, 1500]);
        assert!(seq_scan_set_bloom(&mut state, &mut estate, Some((bf.clone(), 0))).unwrap());
        let expect: Vec<i32> = all
            .iter()
            .copied()
            .filter(|v| bf.test(::hashfn::hash_bytes_uint32(*v as u32)))
            .collect();
        assert_eq!(drain(&mut state, &mut estate), expect);
        assert_eq!(state.variant(), SeqScanVariant::PlainBloom);
        teardown(state, &mut estate);
    });
}

// --- AGGSEQ-STAGE sub-region ------------------------------------------------

/// `PGRUST_LANE_V2_STAGE_VARWALK` A/B lever (AtomicU8 idiom): both states
/// resolvable in one process; restored to OFF (the default the rest of the
/// suite assumes — knob-OFF = the fixed-width-prefix refusal, today's
/// bytes). The admission conjunction (`force && multi && heap && knob`)
/// gets its teeth from the dualexec corpus + the engagement probe (the
/// unit harness here stages int-only fixtures).
#[test]
fn stage_varwalk_knob_ab() {
    stage_varwalk_set_for_tests(true);
    assert!(stage_varwalk_enabled());
    stage_varwalk_set_for_tests(false);
    assert!(!stage_varwalk_enabled());
}

// --- end AGGSEQ-STAGE sub-region ---------------------------------------------
