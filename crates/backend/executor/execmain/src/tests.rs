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
const INT8OID: u32 = 20;
const INT4_LT: u32 = 97;
const INTEGER_BTREE_FAM: u32 = 1976;
const BTREE_AM: u32 = 403;
const F_BTINT4SORTSUPPORT: u32 = 3130;

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
                INT8OID => Some(PgTypeShape {
                    typlen: 8,
                    typbyval: true,
                    typalign: ::types_tuple::TYPALIGN_DOUBLE,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
        // pg_aggregate.dat rows for count() 2803 / sum(int4) 2108.
        syscache_seams::lookup_pg_aggregate_shape::set(|aggfnoid| {
            Ok(match aggfnoid {
                2803 => Some(::syscache_seams::PgAggregateShape {
                    aggkind: b'n' as i8,
                    aggnumdirectargs: 0,
                    aggtransfn: 1219,
                    aggfinalfn: 0,
                    aggcombinefn: 463,
                    aggserialfn: 0,
                    aggdeserialfn: 0,
                    aggfinalextra: false,
                    aggfinalmodify: b'r' as i8,
                    aggtranstype: INT8OID,
                    aggtransspace: 0,
                }),
                2108 => Some(::syscache_seams::PgAggregateShape {
                    aggkind: b'n' as i8,
                    aggnumdirectargs: 0,
                    aggtransfn: 1841,
                    aggfinalfn: 0,
                    aggcombinefn: 463,
                    aggserialfn: 0,
                    aggdeserialfn: 0,
                    aggfinalextra: false,
                    aggfinalmodify: b'r' as i8,
                    aggtranstype: INT8OID,
                    aggtransspace: 0,
                }),
                _ => None,
            })
        });
        syscache_seams::pg_aggregate_agginitval::set(|mcx, aggfnoid| {
            Ok(match aggfnoid {
                2803 => Some(Some(::mcx::PgString::from_str_in("0", mcx).unwrap())),
                2108 => Some(None),
                _ => None,
            })
        });
        // int4 btree sort-operator + hash grouping lookups.
        syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
            let mut v = ::mcx::PgVec::new_in(mcx);
            match opno {
                INT4_LT => v.push(syscache_seams::PgAmopMemberShape {
                    amopfamily: INTEGER_BTREE_FAM,
                    amoplefttype: INT4OID,
                    amoprighttype: INT4OID,
                    amopstrategy: 1,
                    amopmethod: BTREE_AM,
                }),
                INT4_EQ => v.push(syscache_seams::PgAmopMemberShape {
                    amopfamily: INTEGER_HASH_FAM,
                    amoplefttype: INT4OID,
                    amoprighttype: INT4OID,
                    amopstrategy: 1,
                    amopmethod: HASH_AM,
                }),
                other => panic!("unexpected amop probe for operator {other}"),
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, left, right, procnum| {
            Ok(match (opfamily, left, right, procnum) {
                (INTEGER_BTREE_FAM, INT4OID, INT4OID, 2) => F_BTINT4SORTSUPPORT,
                (INTEGER_HASH_FAM, INT4OID, INT4OID, 1) => F_HASHINT4,
                other => panic!("unexpected amproc probe {other:?}"),
            })
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            Ok((opno == INT4_EQ).then_some(syscache_seams::PgOperatorShape {
                oprleft: INT4OID,
                oprright: INT4OID,
                oprresult: BOOLOID,
                oprcom: INT4_EQ,
                oprnegate: 518,
                oprcode: F_INT4EQ,
                oprrest: 101,
                oprjoin: 105,
                oprcanmerge: true,
                oprcanhash: true,
            }))
        });
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
    });
}

const INT4_EQ: u32 = 96;
const INTEGER_HASH_FAM: u32 = 1977;
const HASH_AM: u32 = 405;
const F_HASHINT4: u32 = 450;
const F_INT4EQ: u32 = 65;

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

#[test]
fn executor_rewind_seam_rescans_plan() {
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
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd), 1);
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd), 0);

    execmain_seams::executor_rewind::call(qd).unwrap();
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd), 1);

    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
}

#[test]
fn exec_supports_backward_scan_arms() {
    use ::types_nodes::plannodes::{Limit, Plan, Scan, SeqScan};

    let mcx = leaked_mcx();
    let seqscan = || {
        Node::mk(mcx, SeqScan { scan: Scan { plan: Plan::default(), scanrelid: 1 } }).unwrap()
    };

    assert!(!crate::exec_supports_backward_scan(None));
    assert!(crate::exec_supports_backward_scan(Some(seqscan())));

    // Result forwards to its outer plan; without one it can't back up.
    let bare_result = Node::build::<ResultPlan>(mcx).unwrap().seal();
    assert!(!crate::exec_supports_backward_scan(Some(bare_result)));
    let mut over_scan = Node::build::<ResultPlan>(mcx).unwrap();
    over_scan.plan.lefttree = Some(seqscan());
    assert!(crate::exec_supports_backward_scan(Some(over_scan.seal())));

    let mut limit = Node::build::<Limit>(mcx).unwrap();
    limit.plan.lefttree = Some(seqscan());
    assert!(crate::exec_supports_backward_scan(Some(limit.seal())));

    let mut parallel = Node::build::<SeqScan>(mcx).unwrap();
    parallel.scan.plan.parallel_aware = true;
    assert!(!crate::exec_supports_backward_scan(Some(parallel.seal())));

    // Agg: C's default arm.
    let agg = Node::build::<::types_nodes::plannodes::Agg>(mcx).unwrap().seal();
    assert!(!crate::exec_supports_backward_scan(Some(agg)));
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
    // Serializes fixture users: quiesced()/CLOSED read fixture-global state.
    pub static TEST_LOCK: Mutex<()> = Mutex::new(());

    struct Fake {
        tables: HashMap<Oid, Vec<Buffer>>,
        pages: Vec<usize>,
        pins: Vec<i32>,
        two_col: std::collections::HashSet<Oid>,
    }

    static FAKE: Mutex<Option<Fake>> = Mutex::new(None);

    fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
        let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
        f(g.get_or_insert_with(|| Fake {
            tables: HashMap::new(),
            pages: Vec::new(),
            pins: Vec::new(),
            two_col: std::collections::HashSet::new(),
        }))
    }

    pub fn install() {
        static INSTALLED: std::sync::Once = std::sync::Once::new();
        INSTALLED.call_once(install_once);
    }

    fn install_once() {
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
            // Relations (scans) and procedures (ExecInitAgg's aggfnoid check).
            if classid == ::types_core::catalog::RELATION_RELATION_ID {
                ACLCHECKED_RELID.store(objid, Ordering::Relaxed);
            } else {
                assert_eq!(classid, ::types_core::catalog::PROCEDURE_RELATION_ID);
            }
            Ok(0)
        });

        relation_seams::relation_open::set(fake_relation_open);
    }

    fn tuple_image(vals: &[i32]) -> Vec<u8> {
        let mut img = vec![0u8; 24 + 4 * vals.len()];
        img[0..4].copy_from_slice(&10u32.to_ne_bytes());
        img[18..20].copy_from_slice(&(vals.len() as u16).to_ne_bytes());
        img[20..22].copy_from_slice(&HEAP_XMAX_INVALID.to_ne_bytes());
        img[22] = 24;
        for (i, val) in vals.iter().enumerate() {
            img[24 + 4 * i..28 + 4 * i].copy_from_slice(&val.to_ne_bytes());
        }
        img
    }

    #[repr(align(8))]
    struct TestPage([u8; BLCKSZ]);

    fn build_page(rows: &[&[i32]]) -> Box<TestPage> {
        let mut page = Box::new(TestPage([0u8; BLCKSZ]));
        let n = rows.len();
        let lower = SizeOfPageHeaderData + n * 4;
        let mut upper = BLCKSZ;
        for (i, row) in rows.iter().enumerate() {
            let img = tuple_image(row);
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
                let rows: Vec<&[i32]> = vals.iter().map(std::slice::from_ref).collect();
                let addr = Box::leak(build_page(&rows)).0.as_mut_ptr() as usize;
                f.pages.push(addr);
                f.pins.push(0);
                bufs.push(f.pages.len() as Buffer);
            }
            f.tables.insert(relid, bufs);
        });
    }

    pub fn register_table_2col(relid: Oid, pages: &[&[(i32, i32)]]) {
        with_fake(|f| {
            let mut bufs = Vec::new();
            for rows in pages {
                let rows: Vec<[i32; 2]> = rows.iter().map(|&(a, b)| [a, b]).collect();
                let rows: Vec<&[i32]> = rows.iter().map(|r| r.as_slice()).collect();
                let addr = Box::leak(build_page(&rows)).0.as_mut_ptr() as usize;
                f.pages.push(addr);
                f.pins.push(0);
                bufs.push(f.pages.len() as Buffer);
            }
            f.tables.insert(relid, bufs);
            f.two_col.insert(relid);
        });
    }

    pub fn quiesced() {
        with_fake(|f| {
            assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        });
    }

    fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>, natts: i16) -> Rc<TupleDescData<'mcx>> {
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        for attnum in 1..=natts {
            let att = FormData_pg_attribute {
                attnum,
                atttypid: 23,
                atttypmod: -1,
                attlen: 4,
                attbyval: true,
                attalign: TYPALIGN_INT,
                attstorage: TYPSTORAGE_PLAIN,
                ..Default::default()
            };
            compact.push(CompactAttribute::populate_from(&att));
            attrs.push(att);
        }
        Rc::new(TupleDescData {
            natts: natts as i32,
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
        let data = RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
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
            rd_att: int4_tupdesc(mcx, if with_fake(|f| f.two_col.contains(&relid)) { 2 } else { 1 }),
            rd_index: None,
            rd_opcintype: PgVec::new_in(mcx),
            rd_opfamily: PgVec::new_in(mcx),
            rd_indoption: PgVec::new_in(mcx),
            rd_indcollation: PgVec::new_in(mcx),
            rd_options: None,
            pgstat_enabled: std::cell::Cell::new(true),
            rd_amcache: Default::default(),
            rd_supportinfo: Default::default(),
            rd_indexlist: Default::default(),
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
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let closed_before = scanfix::CLOSED.load(std::sync::atomic::Ordering::Relaxed);
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
    assert_eq!(
        scanfix::CLOSED.load(std::sync::atomic::Ordering::Relaxed) - closed_before,
        1
    );
    scanfix::quiesced();
}

// C: EXPLAIN ANALYZE's per-node counters — es_instrument wraps every node at
// init, InstrStop counts returned tuples, ExecReScan's InstrEndLoop closes the
// cycle, and the seam hands explain the totals keyed by plan_node_id.
#[test]
fn instrumented_seqscan_counts_tuples_and_loops() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();

    let relid: u32 = 70003;
    scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
    let pstmt = mk_seqscan_pstmt(mcx, relid);

    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));

    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_instrument = ::types_core::instrument::INSTRUMENT_TIMER;
        data.estate.es_snapshot = Some(snapshot);
        crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();

        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        assert!(matches!(ps, crate::PlanStateNode::Instrumented(_)));
        let mut n = 0;
        while exec_proc_node(ps, estate).unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, 5);
        let i = &estate.es_instrumentation[0];
        assert!(i.running && i.need_timer);
        assert_eq!(i.tuplecount, 5.0);
        assert!(i.counter.ticks > 0);

        crate::exec_re_scan(ps, estate).unwrap();
        let i = &estate.es_instrumentation[0];
        assert_eq!((i.ntuples, i.nloops), (5.0, 1.0));
        assert!(i.total > 0.0 && i.startup <= i.total);
        assert!(!i.running);

        while exec_proc_node(ps, estate).unwrap().is_some() {}
        ::instrument::instr_end_loop(&mut estate.es_instrumentation[0]);
        let i = &estate.es_instrumentation[0];
        assert_eq!((i.ntuples, i.nloops), (10.0, 2.0));

        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
    });
    scanfix::quiesced();
}

#[test]
fn instrument_seam_reports_rows_by_plan_node_id() {
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
        ::types_core::instrument::INSTRUMENT_ROWS,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    let mut dest = DestReceiver::DoNothing;
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    execmain_seams::executor_finish::call(qd).unwrap();

    let i = execmain_seams::query_desc_instrument::call(qd, 0).expect("node 0 instrumented");
    assert_eq!((i.ntuples, i.nloops), (1.0, 1.0));
    assert!(!i.need_timer && i.total == 0.0);
    assert!(execmain_seams::query_desc_instrument::call(qd, 7).is_none());

    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
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

// Refcount-ownership proof (lib.rs desc_mcx): a portal-style clone held past
// ExecutorEnd, then dropped, returns every desc byte to the context.
#[test]
fn desc_context_stays_flat_across_statements() {
    install_seams();
    let mcx = leaked_mcx();
    let pstmt = mk_select1_pstmt(mcx, None);
    let cycle = || {
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
        let portal_held = execmain_seams::query_desc_result_tupdesc::call(qd).unwrap();
        let mut dest = DestReceiver::DoNothing;
        execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
        execmain_seams::executor_finish::call(qd).unwrap();
        execmain_seams::executor_end::call(qd).unwrap();
        execmain_seams::free_query_desc::call(qd);
        drop(portal_held);
    };
    cycle();
    let ctx = crate::desc_mcx().context();
    let used_after_first = ctx.used();
    let peak_after_first = ctx.peak();
    for _ in 0..(if cfg!(miri) { 20 } else { 1000 }) {
        cycle();
    }
    assert_eq!(ctx.used(), used_after_first, "desc context grew across statements");
    assert_eq!(ctx.peak(), peak_after_first, "desc context peak grew across statements");
}

#[test]
fn no_movement_run_does_not_mark_already_executed() {
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

    // C sets already_executed inside ExecutePlan (execMain.c), which a
    // NoMovement run never reaches.
    execmain_seams::executor_run::call(qd, NoMovementScanDirection, 0, &mut dest).unwrap();
    assert!(!crate::querydesc::with_qd(qd, |d| d.already_executed));

    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    assert!(crate::querydesc::with_qd(qd, |d| d.already_executed));

    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
}

#[test]
fn abort_path_free_reclaims_registry_entry() {
    install_seams();
    let mcx = leaked_mcx();
    let pstmt = mk_select1_pstmt(mcx, None);
    let before = crate::querydesc::registry_len();
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
    assert_eq!(crate::querydesc::registry_len(), before + 1);

    // Abort semantics: error recovery releases without ExecutorFinish/End
    // (C never runs them on abort; portal context reset frees the memory).
    execmain_seams::release_query_desc::call(qd);
    assert_eq!(crate::querydesc::registry_len(), before);
}

// Agg(AGG_PLAIN) over SeqScan on the fake-heap fixture, through the REAL
// InitPlan path: count(*) child scans with an empty targetlist, sum(a)
// projects the column and the Aggref arg reads it as an OUTER_VAR.
fn mk_agg_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    relid: u32,
    aggfnoid: u32,
    with_arg: bool,
) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Agg, Plan, Scan, SeqScan};
    use ::types_nodes::primnodes::{Aggref, OUTER_VAR};

    let scan_tlist = if with_arg {
        let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
        NodeList::make1(mcx, tle).unwrap()
    } else {
        NodeList::nil()
    };
    let scan_node = Node::mk(
        mcx,
        SeqScan {
            scan: Scan {
                plan: Plan { targetlist: scan_tlist, ..Default::default() },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = aggfnoid;
    aggref.aggtype = INT8OID;
    aggref.aggtranstype = INT8OID;
    aggref.aggstar = !with_arg;
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    if with_arg {
        let arg_var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let arg_tle = Node::mk_target_entry(mcx, arg_var, 1, None, false).unwrap();
        aggref.args = NodeList::make1(mcx, arg_tle).unwrap();
    }
    let agg_tle =
        Node::mk_target_entry(mcx, aggref.seal(), 1, Some("agg"), false).unwrap();
    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = NodeList::make1(mcx, agg_tle).unwrap();
    agg.plan.lefttree = Some(scan_node);
    agg.numGroups = 1;
    let agg_node = agg.seal();

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
            requiredPerms: 1 << 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut unpruned = Bitmapset::empty();
    unpruned.add_member(mcx, 1).unwrap();

    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(agg_node);
    pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
    pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

fn run_agg_pstmt(pstmt: &'static PlannedStmt<'static>) -> (Datum, bool) {
    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));
    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_snapshot = Some(snapshot);
        let desc = crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
        assert_eq!(desc.natts, 1);
        assert_eq!(desc.attr(0).atttypid, INT8OID);

        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        let slot_id = exec_proc_node(ps, estate).unwrap().expect("one agg row");
        let (v, isnull) = {
            let base = estate.slot_mut(slot_id).base();
            (base.tts_values[0], base.tts_isnull[0])
        };
        assert!(exec_proc_node(ps, estate).unwrap().is_none(), "agg emits exactly one row");

        // Rescan re-runs the whole aggregation.
        exec_re_scan(ps, estate).unwrap();
        let again = exec_proc_node(ps, estate).unwrap().expect("one agg row after rescan");
        {
            let base = estate.slot_mut(again).base();
            assert_eq!(base.tts_values[0].as_i64(), v.as_i64());
            assert_eq!(base.tts_isnull[0], isnull);
        }
        assert!(exec_proc_node(ps, estate).unwrap().is_none());

        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
        (v, isnull)
    })
}

#[test]
fn agg_count_star_over_fake_heap_end_to_end() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70002;
    scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
    let (v, isnull) = run_agg_pstmt(mk_agg_pstmt(mcx, relid, 2803, false));
    assert!(!isnull);
    assert_eq!(v.as_i64(), 5);
    scanfix::quiesced();
}

#[test]
fn agg_sum_over_fake_heap_end_to_end() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70003;
    scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
    let (v, isnull) = run_agg_pstmt(mk_agg_pstmt(mcx, relid, 2108, true));
    assert!(!isnull);
    assert_eq!(v.as_i64(), 15);
    scanfix::quiesced();
}

#[test]
fn agg_count_star_of_empty_table_is_zero() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70004;
    scanfix::register_table(relid, &[]);
    let (v, isnull) = run_agg_pstmt(mk_agg_pstmt(mcx, relid, 2803, false));
    assert!(!isnull);
    assert_eq!(v.as_i64(), 0);
    scanfix::quiesced();
}

#[test]
fn agg_sum_of_empty_table_is_null() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70005;
    scanfix::register_table(relid, &[]);
    let (_, isnull) = run_agg_pstmt(mk_agg_pstmt(mcx, relid, 2108, true));
    assert!(isnull);
    scanfix::quiesced();
}

// Sort/Limit dispatch flips (notes/sort-limit-execmain-wiring.md): hand-built
// plans over the fake-heap fixture through the real InitPlan path.
fn mk_sort_limit_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    relid: u32,
    with_sort: bool,
    offset: Option<i64>,
    count: Option<i64>,
) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Limit, Plan, Scan, SeqScan, Sort};
    use ::types_nodes::primnodes::OUTER_VAR;

    let scan_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let scan_tle = Node::mk_target_entry(mcx, scan_var, 1, Some("a"), false).unwrap();
    let mut tree = Node::mk(
        mcx,
        SeqScan {
            scan: Scan {
                plan: Plan {
                    targetlist: NodeList::make1(mcx, scan_tle).unwrap(),
                    ..Default::default()
                },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    let outer_tle = |mcx| {
        let v = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        NodeList::make1(mcx, Node::mk_target_entry(mcx, v, 1, Some("a"), false).unwrap())
            .unwrap()
    };

    if with_sort {
        let mut sort = Node::build::<Sort>(mcx).unwrap();
        sort.plan.targetlist = outer_tle(mcx);
        sort.plan.lefttree = Some(tree);
        sort.numCols = 1;
        sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
        sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT]).unwrap();
        sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();
        tree = sort.seal();
    }

    if offset.is_some() || count.is_some() {
        let mk_i8 = |v: i64| {
            Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::from_i64(v), false, true).unwrap()
        };
        let mut limit = Node::build::<Limit>(mcx).unwrap();
        limit.plan.targetlist = outer_tle(mcx);
        limit.plan.lefttree = Some(tree);
        limit.limitOffset = offset.map(mk_i8);
        limit.limitCount = count.map(mk_i8);
        tree = limit.seal();
    }

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
        RTEPermissionInfo { relid, requiredPerms: 1 << 1, ..Default::default() },
    )
    .unwrap();
    let mut unpruned = Bitmapset::empty();
    unpruned.add_member(mcx, 1).unwrap();

    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(tree);
    pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
    pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

fn drain_int4_rows(pstmt: &'static PlannedStmt<'static>, rescan: bool) -> Vec<Vec<i32>> {
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

        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        let mut runs = Vec::new();
        let passes = if rescan { 2 } else { 1 };
        for pass in 0..passes {
            if pass > 0 {
                exec_re_scan(ps, estate).unwrap();
            }
            let mut vals = Vec::new();
            while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
                let mut isnull = false;
                let v = exectuples::slot_getattr(estate.slot_mut(slot_id), 1, &mut isnull);
                assert!(!isnull);
                vals.push(v.as_i32());
            }
            runs.push(vals);
        }
        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
        runs
    })
}

#[test]
fn sort_over_seqscan_orders_output() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70006;
    scanfix::register_table(relid, &[&[3, 1, 2], &[5, 4]]);
    let runs = drain_int4_rows(mk_sort_limit_pstmt(mcx, relid, true, None, None), false);
    assert_eq!(runs, vec![vec![1, 2, 3, 4, 5]]);
    scanfix::quiesced();
}

#[test]
fn limit_bounds_sort_under_it() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70007;
    scanfix::register_table(relid, &[&[3, 1, 2], &[5, 4]]);
    let runs = drain_int4_rows(mk_sort_limit_pstmt(mcx, relid, true, None, Some(2)), false);
    assert_eq!(runs, vec![vec![1, 2]]);
    scanfix::quiesced();
}

#[test]
fn offset_limit_window_over_seqscan() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70008;
    scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
    let runs = drain_int4_rows(mk_sort_limit_pstmt(mcx, relid, false, Some(1), Some(2)), false);
    assert_eq!(runs, vec![vec![2, 3]]);
    scanfix::quiesced();
}

#[test]
fn rescan_of_sort_under_limit_repeats() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70009;
    scanfix::register_table(relid, &[&[3, 1, 2], &[5, 4]]);
    let runs = drain_int4_rows(mk_sort_limit_pstmt(mcx, relid, true, Some(1), Some(3)), true);
    assert_eq!(runs, vec![vec![2, 3, 4], vec![2, 3, 4]]);
    scanfix::quiesced();
}

#[test]
fn limit_pushes_bound_into_sort_state() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70010;
    scanfix::register_table(relid, &[&[3, 1, 2], &[5, 4]]);
    let pstmt = mk_sort_limit_pstmt(mcx, relid, true, None, Some(2));

    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));
    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_snapshot = Some(snapshot);
        crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        exec_proc_node(ps, estate).unwrap().expect("first sorted row");
        match ps {
            crate::procnode::PlanStateNode::Limit(l) => match &*l.outer {
                crate::procnode::PlanStateNode::Sort(s) => {
                    assert!(s.state.bounded, "recompute_limits pushed the bound");
                    assert_eq!(s.state.bound, 2);
                }
                _ => panic!("expected Sort under Limit"),
            },
            _ => panic!("expected Limit root"),
        }
        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
    });
    scanfix::quiesced();
}

// SELECT a FROM t ORDER BY b LIMIT 2: Limit->Sort->SeqScan with a resjunk sort
// column, through the REAL InitPlan junk-filter arm and ExecutePlan filter.
fn mk_junk_sort_limit_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, relid: u32) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Limit, Plan, Scan, SeqScan, Sort};
    use ::types_nodes::primnodes::OUTER_VAR;

    let mk_tlist = |varno: i32| {
        let a = Node::mk_var(mcx, varno, 1, INT4OID, -1, 0, 0).unwrap();
        let b = Node::mk_var(mcx, varno, 2, INT4OID, -1, 0, 0).unwrap();
        NodeList::make2(
            mcx,
            Node::mk_target_entry(mcx, a, 1, Some("a"), false).unwrap(),
            Node::mk_target_entry(mcx, b, 2, Some("b"), true).unwrap(),
        )
        .unwrap()
    };

    let scan = Node::mk(
        mcx,
        SeqScan {
            scan: Scan {
                plan: Plan { targetlist: mk_tlist(1), ..Default::default() },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    let mut sort = Node::build::<Sort>(mcx).unwrap();
    sort.plan.targetlist = mk_tlist(OUTER_VAR);
    sort.plan.lefttree = Some(scan);
    sort.numCols = 1;
    sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[2i16]).unwrap();
    sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT]).unwrap();
    sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();

    let mut limit = Node::build::<Limit>(mcx).unwrap();
    limit.plan.targetlist = mk_tlist(OUTER_VAR);
    limit.plan.lefttree = Some(sort.seal());
    limit.limitCount =
        Some(Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::from_i64(2), false, true).unwrap());

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
        RTEPermissionInfo { relid, requiredPerms: 1 << 1, ..Default::default() },
    )
    .unwrap();
    let mut unpruned = Bitmapset::empty();
    unpruned.add_member(mcx, 1).unwrap();

    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(limit.seal());
    pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
    pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

#[test]
fn junk_filter_removes_order_by_column_end_to_end() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70011;
    scanfix::register_table_2col(relid, &[&[(3, 30), (1, 10), (2, 20)], &[(5, 50), (4, 5)]]);
    let pstmt = mk_junk_sort_limit_pstmt(mcx, relid);

    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));

    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_snapshot = Some(snapshot);
        let desc = crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
        assert_eq!(desc.natts, 1, "junk column excluded from the result type");
        assert_eq!(desc.attr(0).attname.name_str(), b"a");
        assert!(data.estate.es_junkFilter.is_some());

        let store = tuplestore::Tuplestore::begin_heap(true, false, 1024);
        let h = tuplestore::hold::register(store);
        let mut dr = tstore_receiver::tstore_create_DR();
        tstore_receiver::set_params(&mut dr, h, false);
        let mut dest = DestReceiver::Tuplestore(dr);
        crate::execmain::execute_plan(
            data,
            CmdType::CMD_SELECT,
            true,
            0,
            ForwardScanDirection,
            false,
            &mut dest,
        )
        .unwrap();
        assert_eq!(data.estate.es_processed, 2);

        let read_cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("read")));
        let mut slot = exectuples::make_tuple_table_slot(
            read_cx.mcx(),
            ::types_slot::TupleSlotKind::MinimalTuple,
            Some(desc.clone()),
        );
        let mut rows = Vec::new();
        loop {
            let got = tuplestore::hold::with_store(h, |ts| {
                ts.gettupleslot(true, true, &mut slot, read_cx.mcx())
            })
            .unwrap();
            if !got {
                break;
            }
            assert_eq!(slot.base().tts_values.len(), 1, "only column a in output tuples");
            let mut isnull = false;
            let v = exectuples::slot_getattr(&mut slot, 1, &mut isnull);
            assert!(!isnull);
            rows.push(v.as_i32());
        }
        tuplestore::hold::end(h);
        // b values 30,10,20,50,5 sort to 5,10 -> a = [4, 1].
        assert_eq!(rows, vec![4, 1]);

        let ExecData { estate, planstate } = data;
        crate::exec_end_node(planstate.as_mut().unwrap(), estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
    });
    scanfix::quiesced();
}

// Agg(AGG_HASHED) over SeqScan on the fake-heap fixture: SELECT a, count(*)
// FROM t GROUP BY a, through the REAL InitPlan path.
fn mk_hashed_agg_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, relid: u32) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Agg, Plan, Scan, SeqScan};
    use ::types_nodes::primnodes::{Aggref, OUTER_VAR};

    let scan_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let scan_tle = Node::mk_target_entry(mcx, scan_var, 1, Some("a"), false).unwrap();
    let scan_node = Node::mk(
        mcx,
        SeqScan {
            scan: Scan {
                plan: Plan {
                    targetlist: NodeList::make1(mcx, scan_tle).unwrap(),
                    plan_width: 4,
                    ..Default::default()
                },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    let group_var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let group_tle = Node::mk_target_entry(mcx, group_var, 1, Some("a"), false).unwrap();
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = 2803;
    aggref.aggtype = INT8OID;
    aggref.aggtranstype = INT8OID;
    aggref.aggstar = true;
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let count_tle = Node::mk_target_entry(mcx, aggref.seal(), 2, Some("count"), false).unwrap();
    let mut tlist = NodeList::make1(mcx, group_tle).unwrap();
    tlist.lappend(mcx, count_tle).unwrap();

    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = tlist;
    agg.plan.lefttree = Some(scan_node);
    agg.aggstrategy = 2;
    agg.numCols = 1;
    agg.grpColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    agg.grpOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    agg.grpCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    agg.numGroups = 4;
    let agg_node = agg.seal();

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
        RTEPermissionInfo { relid, requiredPerms: 1 << 1, ..Default::default() },
    )
    .unwrap();
    let mut unpruned = Bitmapset::empty();
    unpruned.add_member(mcx, 1).unwrap();

    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(agg_node);
    pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
    pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

#[test]
fn hashed_group_by_over_fake_heap_end_to_end() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70012;
    scanfix::register_table(relid, &[&[1, 2, 1], &[3, 2, 1]]);
    let pstmt = mk_hashed_agg_pstmt(mcx, relid);

    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));
    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_snapshot = Some(snapshot);
        let desc = crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
        assert_eq!(desc.natts, 2);
        assert_eq!(desc.attr(0).atttypid, INT4OID);
        assert_eq!(desc.attr(1).atttypid, INT8OID);

        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        let mut got: Vec<(i32, i64)> = Vec::new();
        while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            assert!(!base.tts_isnull[0] && !base.tts_isnull[1]);
            got.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i64()));
        }
        got.sort_unstable();
        assert_eq!(got, vec![(1, 3), (2, 2), (3, 1)]);

        // Rescan reuses the filled table.
        exec_re_scan(ps, estate).unwrap();
        let mut again: Vec<(i32, i64)> = Vec::new();
        while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            again.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i64()));
        }
        again.sort_unstable();
        assert_eq!(again, vec![(1, 3), (2, 2), (3, 1)]);

        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
    });
    scanfix::quiesced();
}

// Inner nestloop end-to-end: NestLoop(joinqual a = c) over two fake-heap
// seqscans, result asserted against the hand-computed join; second pass
// exercises ExecReScanNestLoop (outer rescan + per-outer-tuple inner rescans
// through the committed rescan arms).
fn mk_nestloop_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    outer_relid: u32,
    inner_relid: u32,
) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Join, NestLoop, Plan, Scan, SeqScan};
    use ::types_nodes::primnodes::{INNER_VAR, OUTER_VAR};

    let scan_tlist = |varno: i32| {
        let a = Node::mk_var(mcx, varno, 1, INT4OID, -1, 0, 0).unwrap();
        let b = Node::mk_var(mcx, varno, 2, INT4OID, -1, 0, 0).unwrap();
        NodeList::make2(
            mcx,
            Node::mk_target_entry(mcx, a, 1, Some("c1"), false).unwrap(),
            Node::mk_target_entry(mcx, b, 2, Some("c2"), false).unwrap(),
        )
        .unwrap()
    };
    let mk_scan = |scanrelid: u32, varno: i32| {
        Node::mk(
            mcx,
            SeqScan {
                scan: Scan {
                    plan: Plan { targetlist: scan_tlist(varno), ..Default::default() },
                    scanrelid,
                },
            },
        )
        .unwrap()
    };

    let mut join_tlist = NodeList::nil();
    for (i, (varno, attno)) in
        [(OUTER_VAR, 1i16), (OUTER_VAR, 2), (INNER_VAR, 1), (INNER_VAR, 2)]
            .into_iter()
            .enumerate()
    {
        let v = Node::mk_var(mcx, varno, attno, INT4OID, -1, 0, 0).unwrap();
        join_tlist
            .lappend(mcx, Node::mk_target_entry(mcx, v, i as i16 + 1, Some("x"), false).unwrap())
            .unwrap();
    }
    let joinqual = {
        let l = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let r = Node::mk_var(mcx, INNER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        Node::mk(
            mcx,
            ::types_nodes::primnodes::OpExpr {
                opno: 96,      // int4eq
                opfuncid: 65,  // pg_proc int4eq
                opresulttype: BOOLOID,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, l, r).unwrap(),
                location: -1,
            },
        )
        .unwrap()
    };

    let mut nl = Node::build::<NestLoop>(mcx).unwrap();
    nl.join = Join {
        plan: Plan {
            targetlist: join_tlist,
            lefttree: Some(mk_scan(1, 1)),
            righttree: Some(mk_scan(2, 2)),
            ..Default::default()
        },
        jointype: ::types_nodes::JoinType::JOIN_INNER,
        inner_unique: false,
        joinqual: NodeList::make1(mcx, joinqual).unwrap(),
    };
    nl.nestParams = NodeList::nil();

    let mk_rte = |relid: u32, perminfoindex: u32| {
        Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid,
                relkind: ::types_rel::RELKIND_RELATION,
                rellockmode: ::types_rel::AccessShareLock,
                perminfoindex,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let mk_perm = |relid: u32| {
        Node::mk(
            mcx,
            RTEPermissionInfo { relid, requiredPerms: 1 << 1, ..Default::default() },
        )
        .unwrap()
    };
    let mut rtable = NodeList::make1(mcx, mk_rte(outer_relid, 1)).unwrap();
    rtable.lappend(mcx, mk_rte(inner_relid, 2)).unwrap();
    let mut perms = NodeList::make1(mcx, mk_perm(outer_relid)).unwrap();
    perms.lappend(mcx, mk_perm(inner_relid)).unwrap();
    let mut unpruned = Bitmapset::empty();
    unpruned.add_member(mcx, 1).unwrap();
    unpruned.add_member(mcx, 2).unwrap();

    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(nl.seal());
    pstmt.rtable = rtable;
    pstmt.permInfos = perms;
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

fn drain_wide_rows(
    pstmt: &'static PlannedStmt<'static>,
    natts: usize,
    passes: usize,
) -> Vec<Vec<Vec<i32>>> {
    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));
    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_snapshot = Some(snapshot);
        let desc = crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
        assert_eq!(desc.natts as usize, natts);

        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        let mut runs = Vec::new();
        for pass in 0..passes {
            if pass > 0 {
                exec_re_scan(ps, estate).unwrap();
            }
            let mut rows = Vec::new();
            while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
                let mut row = Vec::new();
                for attno in 1..=natts {
                    let mut isnull = false;
                    let v = exectuples::slot_getattr(
                        estate.slot_mut(slot_id),
                        attno as i32,
                        &mut isnull,
                    );
                    assert!(!isnull);
                    row.push(v.as_i32());
                }
                rows.push(row);
            }
            runs.push(rows);
        }
        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
        runs
    })
}

#[test]
fn nestloop_inner_join_over_fake_heaps_end_to_end() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let outer: u32 = 70020;
    let inner: u32 = 70021;
    scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (3, 30)]]);
    scanfix::register_table_2col(inner, &[&[(2, 200), (3, 300), (3, 301), (4, 400)]]);
    // Hand-computed inner join on a = c, nestloop order (outer-major).
    let expected = vec![
        vec![2, 20, 2, 200],
        vec![3, 30, 3, 300],
        vec![3, 30, 3, 301],
    ];
    let runs = drain_wide_rows(mk_nestloop_pstmt(mcx, outer, inner), 4, 2);
    assert_eq!(runs, vec![expected.clone(), expected]);
    scanfix::quiesced();
}

#[test]
fn nestloop_with_empty_inner_returns_nothing() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let outer: u32 = 70022;
    let inner: u32 = 70023;
    scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20)]]);
    scanfix::register_table_2col(inner, &[]);
    let runs = drain_wide_rows(mk_nestloop_pstmt(mcx, outer, inner), 4, 1);
    assert_eq!(runs, vec![Vec::<Vec<i32>>::new()]);
    scanfix::quiesced();
}
