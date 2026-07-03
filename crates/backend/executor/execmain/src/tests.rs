use std::sync::Once;

use ::datum::Datum;
use ::executils::EStateData;
use ::mcx::{McxOwned, MemoryContext};
use ::tcop_dest::DestReceiver;
use ::types_dest::CommandDest;
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::plannodes::{PlannedStmt, Result as ResultPlan};
use ::types_portal::{ParamListHandle, QueryEnvHandle};
use ::types_scan::sdir::{ForwardScanDirection, NoMovementScanDirection};
use ::types_tuple::{PgTypeShape, TYPALIGN_CHAR, TYPALIGN_INT, TYPSTORAGE_PLAIN};

use crate::querydesc::{ExecData, ExecTy};
use crate::{exec_init_node, exec_proc_node, exec_re_scan};

const INT4OID: u32 = 23;
const BOOLOID: u32 = 16;

static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        crate::init_seams();
        xact::init_seams();
        backend_status_seams::pgstat_report_query_id::set(|_, _| {});
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
                    typalign: TYPALIGN_CHAR,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
    });
}

fn mk_int4_const(mcx: ::mcx::Mcx<'_>, v: i32) -> Node<'_> {
    Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(v), false, true).unwrap()
}

fn mk_bool_const(mcx: ::mcx::Mcx<'_>, v: bool) -> Node<'_> {
    Node::mk_const(mcx, BOOLOID, -1, 0, 1, Datum::from_bool(v), false, true).unwrap()
}

fn mk_select1_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    resconstantqual: Option<Node<'mcx>>,
) -> &'mcx PlannedStmt<'mcx> {
    let tle = Node::mk_target_entry(mcx, mk_int4_const(mcx, 1), 1, Some("?column?"), false)
        .unwrap();
    let tlist = NodeList::make1(mcx, tle).unwrap();
    let mut result = Node::build::<ResultPlan>(mcx).unwrap();
    result.plan.targetlist = tlist;
    result.resconstantqual = resconstantqual;
    let plan_node = result.seal();
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(plan_node);
    pstmt.seal_ref()
}

fn leaked_mcx() -> ::mcx::Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("execmain-test")));
    m.mcx()
}

#[test]
fn select1_via_seams_returns_one_row() {
    install_seams();
    let mcx = leaked_mcx();
    let pstmt = mk_select1_pstmt(mcx, None);
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        "SELECT 1",
        None,
        None,
        CommandDest::None,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    assert_eq!(
        execmain_seams::query_desc_operation::call(qd),
        CmdType::CMD_SELECT
    );
    let desc = execmain_seams::query_desc_result_tupdesc::call(qd).unwrap();
    assert_eq!(desc.natts, 1);
    assert_eq!(desc.attr(0).atttypid, INT4OID);
    assert_eq!(desc.attr(0).attname.name_str(), b"?column?");

    let mut dest = DestReceiver::DoNothing;
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd), 1);

    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd), 0);

    execmain_seams::executor_run::call(qd, NoMovementScanDirection, 0, &mut dest).unwrap();

    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    assert!(execmain_seams::query_desc_result_tupdesc::call(qd).is_none());
    execmain_seams::free_query_desc::call(qd);
}

fn with_exec_data<R>(
    pstmt: &'static PlannedStmt<'static>,
    f: impl for<'mcx> FnOnce(&mut ExecData<'mcx>, &'mcx PlannedStmt<'mcx>) -> R,
) -> R {
    let mut exec = McxOwned::<ExecTy>::try_new(MemoryContext::new_bump("ExecutorState"), |mcx| {
        Ok(ExecData {
            estate: EStateData::new_in(mcx),
            planstate: None,
        })
    })
    .unwrap();
    // SAFETY: test PlannedStmt lives in a leaked context (see shorten_pstmt).
    let r = exec.with_mut(|data| f(data, unsafe { crate::querydesc::shorten_pstmt(pstmt) }));
    exec.with_mut(|data| data.estate.teardown());
    r
}

#[test]
fn result_node_projects_const_datum() {
    install_seams();
    let mcx = leaked_mcx();
    let pstmt = mk_select1_pstmt(mcx, None);
    with_exec_data(pstmt, |data, pstmt| {
        let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
            .unwrap()
            .unwrap();
        let slot_id = exec_proc_node(&mut ps, &mut data.estate).unwrap().unwrap();
        {
            let base = data.estate.slot(slot_id).base();
            assert_eq!(base.tts_values[0], Datum::from_i32(1));
            assert!(!base.tts_isnull[0]);
        }
        assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_none());

        exec_re_scan(&mut ps, &mut data.estate).unwrap();
        let again = exec_proc_node(&mut ps, &mut data.estate).unwrap();
        assert!(again.is_some());
    });
}

#[test]
fn false_constant_qual_yields_zero_rows() {
    install_seams();
    let mcx = leaked_mcx();
    let qual = Node::mk_list(mcx, NodeList::make1(mcx, mk_bool_const(mcx, false)).unwrap())
        .unwrap();
    let pstmt = mk_select1_pstmt(mcx, Some(qual));
    with_exec_data(pstmt, |data, pstmt| {
        let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
            .unwrap()
            .unwrap();
        assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_none());
        assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_none());
    });
}

#[test]
fn true_constant_qual_yields_one_row() {
    install_seams();
    let mcx = leaked_mcx();
    let qual = Node::mk_list(mcx, NodeList::make1(mcx, mk_bool_const(mcx, true)).unwrap())
        .unwrap();
    let pstmt = mk_select1_pstmt(mcx, Some(qual));
    with_exec_data(pstmt, |data, pstmt| {
        let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
            .unwrap()
            .unwrap();
        assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_some());
        assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_none());
    });
}

#[test]
fn run_with_count_limit_stops_early() {
    install_seams();
    let mcx = leaked_mcx();
    let pstmt = mk_select1_pstmt(mcx, None);
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        "SELECT 1",
        None,
        None,
        CommandDest::None,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    let mut dest = DestReceiver::DoNothing;
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 1, &mut dest).unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd), 1);
    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
}

mod scanfix {
    use core::ptr::NonNull;
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use ::mcx::{Mcx, PgVec};
    use ::types_core::{
        Buffer, GlobalVisStateHandle, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
    };
    use ::types_rel::{
        FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, LOCKMODE,
        RELKIND_RELATION,
    };
    use ::types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData, LP_NORMAL};
    use ::types_tuple::{
        CompactAttribute, FormData_pg_attribute, NameData, TupleDescData, HEAP_XMAX_INVALID,
        TYPALIGN_INT, TYPSTORAGE_PLAIN,
    };

    pub static CLOSED: AtomicUsize = AtomicUsize::new(0);
    pub static ACLCHECKED_RELID: AtomicU32 = AtomicU32::new(0);

    struct Fake {
        tables: HashMap<Oid, Vec<Buffer>>,
        pages: Vec<usize>,
        pins: Vec<i32>,
    }

    static FAKE: Mutex<Option<Fake>> = Mutex::new(None);

    fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
        let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
        f(g.get_or_insert_with(|| Fake {
            tables: HashMap::new(),
            pages: Vec::new(),
            pins: Vec::new(),
        }))
    }

    pub fn install() {
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
        bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
        bufmgr_seams::get_access_strategy::set(|_| None);
        bufmgr_seams::free_access_strategy::set(|_| {});
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|rel, _fork| {
            with_fake(|f| Ok(f.tables[&rel.rd_id].len() as u32))
        });

        heapam_visibility_seams::heap_tuple_satisfies_visibility::set(|_h, _s, _b| Ok(true));
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_h, _v| Ok(false));
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_h| Ok(false));
        predicate_seams::check_for_serializable_conflict_out_needed::set(|_r, _s| false);
        predicate_seams::predicate_lock_relation::set(|_r, _s| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_r, _t, _s, _x| Ok(()));
        pruneheap_seams::heap_page_prune_opt::set(|_r, _b| Ok(()));
        procarray_seams::global_vis_test_for::set(|_r| GlobalVisStateHandle::new(0));

        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::object_aclcheck::set(|classid, objid, _roleid, _mode| {
            assert_eq!(classid, ::types_core::catalog::RELATION_RELATION_ID);
            ACLCHECKED_RELID.store(objid, Ordering::Relaxed);
            Ok(0)
        });

        relation_seams::relation_open::set(fake_relation_open);
    }

    fn tuple_image(val: i32) -> Vec<u8> {
        let mut img = vec![0u8; 28];
        img[0..4].copy_from_slice(&10u32.to_ne_bytes());
        img[18..20].copy_from_slice(&1u16.to_ne_bytes());
        img[20..22].copy_from_slice(&HEAP_XMAX_INVALID.to_ne_bytes());
        img[22] = 24;
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

    pub fn register_table(relid: Oid, pages: &[&[i32]]) {
        with_fake(|f| {
            let mut bufs = Vec::new();
            for vals in pages {
                let addr = Box::leak(build_page(vals)).0.as_mut_ptr() as usize;
                f.pages.push(addr);
                f.pins.push(0);
                bufs.push(f.pages.len() as Buffer);
            }
            f.tables.insert(relid, bufs);
        });
    }

    pub fn quiesced() {
        with_fake(|f| {
            assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        });
    }

    fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
        let att = FormData_pg_attribute {
            attnum: 1,
            atttypid: 23,
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

    fn record_close(_relid: Oid, _lockmode: LOCKMODE) -> ::types_error::PgResult<()> {
        CLOSED.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn fake_relation_open<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        _lockmode: LOCKMODE,
    ) -> ::types_error::PgResult<Relation<'mcx>> {
        let mut relname = NameData::default();
        relname.namestrcpy("t");
        let rd_rel = FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: tableam::HEAP_TABLE_AM_OID,
            relfilenode: relid,
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
            rd_id: relid,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: std::cell::Cell::new(true),
            rd_createSubid: std::cell::Cell::new(0),
            rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
            rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
            rd_droppedSubid: std::cell::Cell::new(0),
            rd_lockInfo: LockInfoData {
                lockRelId: LockRelId {
                    relId: relid,
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
            pgstat_enabled: std::cell::Cell::new(true),
            rd_amcache: Default::default(),
            rd_supportinfo: Default::default(),
        };
        Ok(Relation::open(data, Some(record_close)))
    }
}

fn mk_seqscan_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, relid: u32) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan};

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
    let tlist = NodeList::make1(mcx, tle).unwrap();
    let scan_node = Node::mk(
        mcx,
        SeqScan {
            scan: Scan {
                plan: Plan {
                    targetlist: tlist,
                    ..Default::default()
                },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    let rte = Node::mk(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_RELATION,
            relid,
            relkind: ::types_rel::RELKIND_RELATION,
            rellockmode: ::types_rel::AccessShareLock,
            perminfoindex: 1,
            inFromCl: true,
            ..Default::default()
        },
    )
    .unwrap();
    let perminfo = Node::mk(
        mcx,
        RTEPermissionInfo {
            relid,
            requiredPerms: 1 << 1, // ACL_SELECT
            ..Default::default()
        },
    )
    .unwrap();
    let mut unpruned = Bitmapset::empty();
    unpruned.add_member(mcx, 1).unwrap();

    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(scan_node);
    pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
    pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

// InitPlan → ExecInitRangeTable → ExecInitNode(SeqScan) → ExecOpenScanRelation
// → ExecGetRangeTableRelation → table_open, then the per-tuple loop and
// ExecEndPlan's close half; snapshot registration (proc-array lane) bypassed.
#[test]
fn seqscan_end_to_end_through_real_init_path() {
    install_seams();
    scanfix::install();
    let mcx = leaked_mcx();

    let relid: u32 = 70001;
    scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
    let pstmt = mk_seqscan_pstmt(mcx, relid);

    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));

    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_snapshot = Some(snapshot);
        let desc = crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
        assert_eq!(desc.natts, 1);
        assert_eq!(desc.attr(0).atttypid, INT4OID);
        assert_eq!(
            scanfix::ACLCHECKED_RELID.load(std::sync::atomic::Ordering::Relaxed),
            relid
        );
        assert_eq!(data.estate.es_range_table_size, 1);
        assert!(data.estate.es_relations[0].is_some(), "scan relation opened");

        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        let mut vals = Vec::new();
        while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
            let mut isnull = false;
            let v = exectuples::slot_getattr(estate.slot_mut(slot_id), 1, &mut isnull);
            assert!(!isnull);
            vals.push(v.as_i32());
        }
        assert_eq!(vals, vec![1, 2, 3, 4, 5]);

        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
    });
    assert_eq!(scanfix::CLOSED.load(std::sync::atomic::Ordering::Relaxed), 1);
    scanfix::quiesced();
}

#[test]
fn exec_clean_type_from_tl_skips_junk() {
    install_seams();
    let mcx = leaked_mcx();
    let tle1 = Node::mk_target_entry(mcx, mk_int4_const(mcx, 1), 1, Some("a"), false).unwrap();
    let tle2 = Node::mk_target_entry(mcx, mk_int4_const(mcx, 2), 2, Some("junk"), true).unwrap();
    let tlist = NodeList::make2(mcx, tle1, tle2).unwrap();
    let clean = crate::exec_clean_type_from_tl(&tlist).unwrap();
    assert_eq!(clean.natts, 1);
    assert_eq!(clean.attr(0).attname.name_str(), b"a");
    let full = crate::exec_type_from_tl(&tlist).unwrap();
    assert_eq!(full.natts, 2);
}
