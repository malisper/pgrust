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
use ::types_portal::{CachedPlanHandle, ParamListHandle, QueryEnvHandle};
use ::types_scan::sdir::{ForwardScanDirection, NoMovementScanDirection};
use ::types_slot::EXEC_FLAG_SKIP_TRIGGERS;
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
        // Lane-v2 is on by default (2026-07-14); its per-batch CFI goes
        // through this seam, so the fake-heap end-to-end tests need it.
        postgres_seams::check_for_interrupts::set(|| Ok(()));
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
                // RECORD: the MULTIEXPR SubPlan junk column's dummy type.
                2249 => Some(PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: ::types_tuple::TYPALIGN_DOUBLE,
                    typstorage: ::types_tuple::TYPSTORAGE_EXTENDED,
                    typcollation: 0,
                }),
                // _int8 (int8[]): sum(int4)'s aggMTRANSTYPE (pg_type.dat 1016)
                // — the windows_t2_ab moving-frame units resolve it through
                // initialize_peragg_framed's get_typlenbyval.
                1016 => Some(PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: ::types_tuple::TYPALIGN_DOUBLE,
                    typstorage: ::types_tuple::TYPSTORAGE_EXTENDED,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
        syscache_seams::pg_type_io_shape::set(|typid| {
            Ok((typid == INT8OID).then_some(syscache_seams::PgTypeIoShape {
                oid: INT8OID,
                typinput: 460,
                typoutput: 461,
                typreceive: 2408,
                typsend: 2409,
                typmodin: 0,
                typmodout: 0,
                typelem: 0,
                typlen: 8,
                typbyval: true,
                typalign: ::types_tuple::TYPALIGN_DOUBLE,
                typdelim: b',' as i8,
                typisdefined: true,
            }))
        });
        // pg_aggregate.dat rows for count() 2803 / sum(int4) 2108.
        syscache_seams::lookup_pg_aggregate_shape::set(|aggfnoid| {
            Ok(match aggfnoid {
                // count(*): moving-aggregate columns filled from the REAL
                // pg_aggregate.dat row (PostgreSQL 18.3: aggmtransfn int8inc
                // 1219, aggminvtransfn int8dec 3546 — both present in the
                // fmgr canonical table — aggmtranstype int8), the WS-M TODO-7
                // un-stub, landed by WS-R wave-3 so the windows_t2b_ab moving
                // count(*) units exercise the framed lane's MovingByVal
                // INVERSE kernel exactly as production does (the SQL corpus
                // already covered it end-to-end on the real catalog).
                // Additive fixture fill, same argument as the 2108 row below:
                // UNBOUNDED-PRECEDING starts keep use_ma_code=false, so no
                // pre-existing consumer changes code path or results.
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
                    aggsortop: 0,
                    aggtranstype: INT8OID,
                    aggmtransfn: 1219,
                    aggminvtransfn: 3546,
                    aggmfinalfn: 0,
                    aggmfinalextra: false,
                    aggmfinalmodify: b'r' as i8,
                    aggmtranstype: INT8OID,
                    aggtransspace: 0,
                }),
                // sum(int4): moving-aggregate columns filled from the REAL
                // pg_aggregate.dat row (verified against PostgreSQL 18.3:
                // aggmtransfn int4_avg_accum 1963, aggminvtransfn
                // int4_avg_accum_inv 3571, aggmfinalfn int2int4_sum 3572,
                // aggmtranstype _int8 1016) so the windows_t2_ab moving-frame
                // units exercise the framed lane's MovingIntSum INVERSE
                // kernel exactly as production does. Additive fixture fill
                // (fields were stubbed 0); UNBOUNDED-PRECEDING-start frames —
                // every pre-wave-2 consumer — keep the plain-transition path
                // (initialize_peragg's use_ma_code gate), so no existing test
                // changes code path or results.
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
                    aggsortop: 0,
                    aggtranstype: INT8OID,
                    aggmtransfn: 1963,
                    aggminvtransfn: 3571,
                    aggmfinalfn: 3572,
                    aggmfinalextra: false,
                    aggmfinalmodify: b'r' as i8,
                    aggmtranstype: 1016,
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
        // aggminitval mirror (real catalog values; consumed only by
        // moving-frame window aggs — the windows_t2_ab units).
        syscache_seams::pg_aggregate_aggminitval::set(|mcx, aggfnoid| {
            Ok(match aggfnoid {
                2803 => Some(Some(::mcx::PgString::from_str_in("0", mcx).unwrap())),
                2108 => Some(Some(::mcx::PgString::from_str_in("{0,0}", mcx).unwrap())),
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
                // btree ORDER proc for the express_ab fake-index corpus.
                (INTEGER_BTREE_FAM, INT4OID, INT4OID, 1) => F_BTINT4CMP,
                other => panic!("unexpected amproc probe {other:?}"),
            })
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            Ok((opno == INT4_EQ).then_some(syscache_seams::PgOperatorShape { oprnamespace: 11,
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
        // Every generic cached plan is parkable in this test binary; no test
        // here exercises the one-shot-custom-plan rejection.
        plancache_portal_seams::is_source_generic_plan::set(|_| true);
    });
}

const INT4_EQ: u32 = 96;
const INT4_GT: u32 = 521;
const INTEGER_HASH_FAM: u32 = 1977;
const HASH_AM: u32 = 405;
const F_HASHINT4: u32 = 450;
const F_INT4EQ: u32 = 65;
const F_BTINT4CMP: u32 = 351;

/// Shared `lookup_pg_amop_by_operator` strategy seam for the fake int4
/// btree opfamily (1976). Seams are process-global and set-once, so every
/// test module that needs a pg_amop strategy row MUST come through here —
/// express_ab's scan-key probes (INT4_EQ → BTEqual=3, INT4_GT → BTGreater=5)
/// and mergejoin_rowmode_ab's MJExamineQuals probe (INT4_EQ → 3) both do.
/// (Phase-1 integration glue: pre-merge each branch installed its own copy;
/// composed in one test binary the second install panicked "seam installed
/// twice".)
fn install_amop_strategy_seam() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_amop_by_operator::set(|opno, purpose, opfamily| {
            assert_eq!(purpose, b's');
            assert_eq!(opfamily, INTEGER_BTREE_FAM);
            let strategy = match opno {
                INT4_EQ => 3,
                INT4_GT => 5,
                _ => return Ok(None),
            };
            Ok(Some(syscache_seams::PgAmopShape {
                amopstrategy: strategy,
                amopsortfamily: 0,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
            }))
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

// Ratified 2026-07-08 (docs/design/hook-surface.md section 2 parked-portal
// caveat): a counting tap installed at boot must see one "execution" per
// Bind of a parked prepared statement, whether it's a fresh ExecutorStart or
// a rearm-driven reuse — otherwise a future pgss undercounts extended-query
// reuse. tap_executor_start is process-global and install-once, so this is
// the crate's only test touching it; counting is filtered to this test's own
// QueryDescHandle so it stays correct under the test harness's parallelism
// (every other test's executor_start calls also fire the tap once installed).
static REARM_TAP_TARGET: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static REARM_TAP_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn count_start(h: ::types_portal::QueryDescHandle) {
    if h.0 == REARM_TAP_TARGET.load(std::sync::atomic::Ordering::Relaxed) {
        REARM_TAP_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[test]
fn tap_executor_start_counts_start_and_parked_rearm_reuse() {
    install_seams();
    crate::execmain::tap_executor_start::install(count_start);

    let mcx = leaked_mcx();
    let pstmt = mk_select1_pstmt(mcx, None);
    execmain_seams::note_cplan_for_query_desc::call(CachedPlanHandle(1));
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
    REARM_TAP_TARGET.store(qd.0, std::sync::atomic::Ordering::Relaxed);

    // Fresh start: one Bind's worth of execution.
    execmain_seams::executor_start::call(qd, EXEC_FLAG_SKIP_TRIGGERS).unwrap();
    assert_eq!(REARM_TAP_COUNT.load(std::sync::atomic::Ordering::Relaxed), 1);

    let mut dest = DestReceiver::DoNothing;
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    execmain_seams::executor_run::call(qd, NoMovementScanDirection, 0, &mut dest).unwrap();

    // Park (no C counterpart): ExecutorFinish + in-place skeleton disarm.
    let parked = execmain_seams::executor_finish_and_park::call(qd).unwrap();
    assert!(parked, "select1 over a generic cached plan must be park-eligible");

    // Two more Binds against the parked portal, each a rearm reuse — neither
    // passes through executor_start_seam, so only the ratified tap call
    // inside executor_rearm_seam can see them.
    for n in 2..=3 {
        let reused =
            execmain_seams::executor_rearm::call(qd, None, ParamListHandle::NULL).unwrap();
        assert!(reused, "rearm {n} should reuse the parked executor");
        assert_eq!(REARM_TAP_COUNT.load(std::sync::atomic::Ordering::Relaxed), n);
    }
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

// B10: the predicate is a scroll-POLICY oracle now (which cursors get
// implicit SCROLL - C's ExecSupportsBackwardScan answer set, byte-identical);
// the executor itself never scans backward (deletion-prep B1).
#[test]
fn plan_implicit_scroll_ok_arms() {
    use ::types_nodes::plannodes::{Limit, Plan, Scan, SeqScan};

    let mcx = leaked_mcx();
    let seqscan = || {
        Node::mk(mcx, SeqScan { scan: Scan { plan: Plan::default(), scanrelid: 1 }, cb_scan_cols: None }).unwrap()
    };

    assert!(!crate::plan_implicit_scroll_ok(None));
    assert!(crate::plan_implicit_scroll_ok(Some(seqscan())));

    // Result forwards to its outer plan; without one it can't back up.
    let bare_result = Node::build::<ResultPlan>(mcx).unwrap().seal();
    assert!(!crate::plan_implicit_scroll_ok(Some(bare_result)));
    let mut over_scan = Node::build::<ResultPlan>(mcx).unwrap();
    over_scan.plan.lefttree = Some(seqscan());
    assert!(crate::plan_implicit_scroll_ok(Some(over_scan.seal())));

    let mut limit = Node::build::<Limit>(mcx).unwrap();
    limit.plan.lefttree = Some(seqscan());
    assert!(crate::plan_implicit_scroll_ok(Some(limit.seal())));

    let mut parallel = Node::build::<SeqScan>(mcx).unwrap();
    parallel.scan.plan.parallel_aware = true;
    assert!(!crate::plan_implicit_scroll_ok(Some(parallel.seal())));

    // Agg: C's default arm.
    let agg = Node::build::<::types_nodes::plannodes::Agg>(mcx).unwrap().seal();
    assert!(!crate::plan_implicit_scroll_ok(Some(agg)));
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
        // WS-J express_ab: fake btree indexes (index oid -> indexed heap
        // oid); fake_relation_open serves these as RELKIND_INDEX btree
        // relations over a 1-col int4 key.
        indexes: HashMap<Oid, Oid>,
    }

    static FAKE: Mutex<Option<Fake>> = Mutex::new(None);

    fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
        let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
        f(g.get_or_insert_with(|| Fake {
            tables: HashMap::new(),
            pages: Vec::new(),
            pins: Vec::new(),
            two_col: std::collections::HashSet::new(),
            indexes: HashMap::new(),
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
        // WS-J express_ab: the btree read path's extra seams (the
        // nodeindexscan test-fixture set, verbatim semantics).
        bufmgr_seams::release_and_read_buffer::set(|buf, rel, blkno| {
            if buf != ::types_core::InvalidBuffer {
                let same =
                    with_fake(|f| f.tables[&rel.rd_id].get(blkno as usize) == Some(&buf));
                if same {
                    return Ok(buf);
                }
                bufmgr_seams::release_buffer::call(buf)?;
            }
            bufmgr_seams::read_buffer::call(rel, blkno)
        });
        bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
        bufmgr_seams::buffer_get_lsn_atomic::set(|_buf| 0x1234);
        transam_xlog_seams::xlog_standby_info_active::set(|| false);
        predicate_seams::predicate_lock_page::set(|_rel, _blkno, _snap| Ok(()));
        bufmgr_seams::get_access_strategy::set(|_| None);
        bufmgr_seams::free_access_strategy::set(|_| {});
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|rel, _fork| {
            with_fake(|f| Ok(f.tables[&rel.rd_id].len() as u32))
        });

        heapam_visibility_seams::heap_tuple_satisfies_visibility::set(|_h, _s, _b| Ok(true));
        heapam_visibility_seams::heap_tuple_satisfies_mvcc_page::set(|_h, _s, _b, _m| Ok(true));
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_h, _v| Ok(false));
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_h| Ok(false));
        predicate_seams::check_for_serializable_conflict_out_needed::set(|_r, _s| Ok(false));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        predicate_seams::predicate_lock_relation::set(|_r, _s| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_r, _t, _s, _x| Ok(()));
        pruneheap_seams::heap_page_prune_opt::set(|_r, _b| Ok(()));
        procarray_seams::global_vis_test_for::set(|_r| GlobalVisStateHandle::new(0));

        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::pg_class_aclmask::set(|objid, _roleid, mask, _how_all| {
            ACLCHECKED_RELID.store(objid, Ordering::Relaxed);
            Ok(mask)
        });
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

    // ---- WS-J express_ab: fake single-leaf btree over a 2-col heap -------
    // Page shapes lifted from nodeindexscan/src/tests.rs (the canonical fake
    // btree fixture): a BTP_META metapage pointing at one BTP_LEAF|BTP_ROOT
    // leaf whose 16-byte int4 index tuples TID-point into heap page 0.

    fn put_u16(p: &mut TestPage, off: usize, v: u16) {
        p.0[off..off + 2].copy_from_slice(&v.to_ne_bytes());
    }

    fn new_bt_page(special_flags: u16, level: u32) -> Box<TestPage> {
        use ::types_nbtree::{BTPageOpaqueData, P_NONE};
        let mut p = Box::new(TestPage([0u8; BLCKSZ]));
        let special = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
        put_u16(&mut p, 12, SizeOfPageHeaderData as u16); // pd_lower
        put_u16(&mut p, 14, special as u16); // pd_upper
        put_u16(&mut p, 16, special as u16); // pd_special
        let opaque = BTPageOpaqueData {
            btpo_prev: P_NONE,
            btpo_next: P_NONE,
            btpo_level: level,
            btpo_flags: special_flags,
            btpo_cycleid: 0,
        };
        // SAFETY: in-bounds, aligned special area write on an owned page.
        unsafe {
            p.0.as_mut_ptr()
                .add(special)
                .cast::<::types_nbtree::BTPageOpaqueData>()
                .write(opaque)
        };
        p
    }

    fn bt_meta_page(root: u32, level: u32) -> Box<TestPage> {
        use ::types_nbtree::{BTMetaPageData, BTP_META, BTREE_MAGIC, BTREE_VERSION};
        let mut p = new_bt_page(BTP_META, 0);
        let metad = BTMetaPageData {
            btm_magic: BTREE_MAGIC,
            btm_version: BTREE_VERSION,
            btm_root: root,
            btm_level: level,
            btm_fastroot: root,
            btm_fastlevel: level,
            btm_last_cleanup_num_delpages: 0,
            btm_last_cleanup_num_heap_tuples: -1.0,
            btm_allequalimage: true,
        };
        // SAFETY: metapage contents at +SizeOfPageHeaderData on an owned page.
        unsafe {
            p.0.as_mut_ptr()
                .add(SizeOfPageHeaderData)
                .cast::<::types_nbtree::BTMetaPageData>()
                .write(metad)
        };
        p
    }

    // One 16-byte int4 index tuple (t_info alt-TID bits unset).
    fn add_index_tuple(
        p: &mut TestPage,
        tid: ::types_tuple::itemptr::ItemPointerData,
        value: i32,
    ) {
        let itupsz = 16usize;
        let pd_lower = u16::from_ne_bytes([p.0[12], p.0[13]]) as usize;
        let pd_upper = u16::from_ne_bytes([p.0[14], p.0[15]]) as usize;
        let off = pd_upper - itupsz;
        // SAFETY: owned page bytes; ItemPointerData is a 6B POD.
        unsafe {
            p.0.as_mut_ptr()
                .add(off)
                .cast::<::types_tuple::itemptr::ItemPointerData>()
                .write_unaligned(tid);
        }
        p.0[off + 6..off + 8].copy_from_slice(&(itupsz as u16).to_ne_bytes());
        p.0[off + 8..off + 12].copy_from_slice(&value.to_ne_bytes());
        let mut iid = ItemIdData::new(0, 0, 0);
        iid.set_normal(off as u16, itupsz as u16);
        // SAFETY: line-pointer slot in the owned page.
        unsafe {
            p.0.as_mut_ptr()
                .add(pd_lower)
                .cast::<ItemIdData>()
                .write(iid)
        };
        put_u16(p, 12, (pd_lower + 4) as u16);
        put_u16(p, 14, off as u16);
    }

    /// The express_ab kv fixture: a 2-col `(k int4, v int4)` heap page plus a
    /// single-leaf btree over column 1. Heap row `i` (0-based) sits at
    /// offset `i+1` on page 0; the leaf indexes keys in ascending order.
    pub fn register_indexed_table_2col(heap_oid: Oid, index_oid: Oid, rows: &[(i32, i32)]) {
        register_table_2col(heap_oid, &[rows]);
        let mut keyed: Vec<(i32, u16)> = rows
            .iter()
            .enumerate()
            .map(|(i, &(k, _))| (k, (i + 1) as u16))
            .collect();
        keyed.sort_unstable();
        let mut leaf = new_bt_page(
            ::types_nbtree::BTP_LEAF | ::types_nbtree::BTP_ROOT,
            0,
        );
        for (k, off) in keyed {
            add_index_tuple(
                &mut leaf,
                ::types_tuple::itemptr::ItemPointerData::new(0, off),
                k,
            );
        }
        with_fake(|f| {
            let mut bufs = Vec::new();
            for p in [bt_meta_page(1, 0), leaf] {
                let addr = Box::leak(p).0.as_mut_ptr() as usize;
                f.pages.push(addr);
                f.pins.push(0);
                bufs.push(f.pages.len() as Buffer);
            }
            f.tables.insert(index_oid, bufs);
            f.indexes.insert(index_oid, heap_oid);
        });
    }

    pub fn quiesced() {
        with_fake(|f| {
            assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        });
    }

    /// Outstanding pin census (SE-R41 v2 posture teeth): the total page-pin
    /// count across the fixture — the hold-pin pin asserts it is NONZERO at
    /// a cursor-fill suspension (the C-parity Volcano posture holds the
    /// staged page) where the parked posture asserted zero.
    pub fn held_pins() -> i32 {
        with_fake(|f| f.pins.iter().sum())
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
        if let Some(heap_oid) = with_fake(|f| f.indexes.get(&relid).copied()) {
            return Ok(fake_index_relation(mcx, relid, heap_oid));
        }
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
        Ok(Relation::open(data, Some(record_close)))
    }

    /// WS-J express_ab: the fake btree index relation over `heap_oid`'s
    /// column 1 (int4), served by `fake_relation_open` for oids registered
    /// via `register_indexed_table_2col` (shape: nodeindexscan tests'
    /// `index_relation`).
    fn fake_index_relation<'mcx>(mcx: Mcx<'mcx>, relid: Oid, heap_oid: Oid) -> Relation<'mcx> {
        const INT4_BTREE_OPFAMILY: Oid = 1976;
        let mut relname = NameData::default();
        relname.namestrcpy("t_idx");
        let one_oid = |v: Oid| {
            let mut vec = PgVec::new_in(mcx);
            vec.push(v);
            vec
        };
        let mut indkey = PgVec::new_in(mcx);
        indkey.push(1);
        let mut indoption = PgVec::new_in(mcx);
        indoption.push(0i16);
        let data = RelationData {
            rd_locator: Default::default(),
            rd_smgr: Default::default(),
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
            rd_rel: FormData_pg_class {
                relname,
                relnamespace: 2200,
                reltype: 0,
                relowner: 10,
                relam: ::types_core::BTREE_AM_OID,
                relfilenode: relid,
                reltablespace: 0,
                relpages: 0,
                reltuples: -1.0,
                relallvisible: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relisshared: false,
                relpersistence: RELPERSISTENCE_PERMANENT,
                relkind: ::types_rel::RELKIND_INDEX,
                relhassubclass: false,
                relrowsecurity: false,
                relispopulated: true,
                relreplident: b'd',
                relispartition: false,
                relfrozenxid: 3,
                relminmxid: 1,
            },
            rd_att: int4_tupdesc(mcx, 1),
            rd_index: Some(::types_rel::FormData_pg_index {
                indexrelid: relid,
                indrelid: heap_oid,
                indnatts: 1,
                indnkeyatts: 1,
                indisunique: true,
                indnullsnotdistinct: false,
                indisprimary: true,
                indisexclusion: false,
                indimmediate: true,
                indisvalid: true,
                indisready: true,
                indkey,
                has_indpred: false,
                indexprs_src: None,
                indpred_src: None,
            }),
            rd_opcintype: one_oid(23),
            rd_opfamily: one_oid(INT4_BTREE_OPFAMILY),
            rd_indoption: indoption,
            rd_indcollation: one_oid(0),
            rd_options: None,
            pgstat_enabled: std::cell::Cell::new(false),
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
        Relation::open(data, Some(record_close))
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
            cb_scan_cols: None,
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
            cb_scan_cols: None,
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
            cb_scan_cols: None,
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

// SORTFEED-RA policy pin (the AD2 letter's documented shave): once a
// randomAccess sort is DONE, `try_own_sort`'s refused-memo branch must exit
// on the `sort_Done` load BEFORE the RA knob OnceLock + the
// `sort_randomaccess_memo` probe. Two arms over identical Sort-over-SeqScan
// plans inited with EXEC_FLAG_REWIND (randomAccess):
//   * control (plain-first): the first pull reaches the RA branch
//     pre-done and computes the RA side memo — proves the RA admission
//     path is live in this test world, so the pin arm cannot pass
//     vacuously (e.g. under a knob-OFF environment).
//   * pin (EPQ-first): the first pull refuses at the EPQ gate (before any
//     memo) and the row-path `exec_sort` feeds the tuplesort, so the node
//     reaches sort_Done with the RA side memo still unset. Every later
//     non-EPQ pull is post-done and must leave the side memo UNSET while
//     rows flow from the bare drain leg. If the policy regresses (the
//     sort_Done check moves back below the memo probe), those pulls
//     compute the memo and the final assert fails.
//     (Backward-execution wave B6 re-spell, runbook RB-B1 item 4: the
//     original pin arm drove the first pull BACKWARD to hit the lane's
//     direction gate — the row-path backward feed it exercised died with
//     B6, and the direction gate itself dies with B11. The EPQ gate is
//     the surviving refuse-before-memo trigger; the policy under pin is
//     unchanged.)
#[test]
fn sortfeed_ra_postdone_pull_exits_before_ra_memo() {
    use ::types_slot::EXEC_FLAG_REWIND;
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();

    let ra_memo_of = |ps: &crate::procnode::PlanStateNode| -> (bool, Option<bool>) {
        match ps {
            crate::procnode::PlanStateNode::Sort(s) => {
                (s.state.sort_done(), ::nodesort::sort_lane_ra_fusible(&s.state))
            }
            _ => panic!("expected Sort root"),
        }
    };

    let drive = |relid: u32, epq_first: bool| {
        scanfix::register_table(relid, &[&[3, 1, 2], &[5, 4]]);
        let pstmt = mk_sort_limit_pstmt(mcx, relid, true, None, None);
        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, EXEC_FLAG_REWIND)
                .unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();

            let mut vals = Vec::new();
            if epq_first {
                // Row-path feed: the EPQ gate refuses before any memo;
                // exec_sort builds + finalizes the tuplesort and serves the
                // first sorted row from its own drain leg.
                estate.es_epq_active = true;
                let slot_id = exec_proc_node(ps, estate).unwrap().expect("first sorted row");
                let mut isnull = false;
                let v = exectuples::slot_getattr(estate.slot_mut(slot_id), 1, &mut isnull);
                assert!(!isnull);
                vals.push(v.as_i32());
                let (done, memo) = ra_memo_of(ps);
                assert!(done, "EPQ-first pull must feed the sort");
                assert_eq!(memo, None, "row-path feed must not touch the RA side memo");
                estate.es_epq_active = false;
            }

            while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
                let mut isnull = false;
                let v = exectuples::slot_getattr(estate.slot_mut(slot_id), 1, &mut isnull);
                assert!(!isnull);
                vals.push(v.as_i32());
            }
            assert_eq!(vals, vec![1, 2, 3, 4, 5]);

            let (done, memo) = ra_memo_of(ps);
            assert!(done);
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
            memo
        })
    };

    // Control arm: pre-done plain pull computes the RA side memo.
    let control = drive(70061, false);
    assert!(
        control.is_some(),
        "control arm: the RA admission path must be live (side memo computed pre-done)"
    );
    // Pin arm: every lane pull was post-done — the memo must still be unset.
    let pinned = drive(70062, true);
    assert_eq!(
        pinned, None,
        "post-done pulls must exit before the RA memo probe (SORTFEED-RA shave)"
    );
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
            cb_scan_cols: None,
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
            cb_scan_cols: None,
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
    jointype: ::types_nodes::JoinType,
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
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan { targetlist: scan_tlist(varno), ..Default::default() },
                    scanrelid,
                },
            },
        )
        .unwrap()
    };

    // SEMI/ANTI project only the outer side, as the planner emits.
    let tl_cols: &[(i32, i16)] = if matches!(
        jointype,
        ::types_nodes::JoinType::JOIN_SEMI | ::types_nodes::JoinType::JOIN_ANTI
    ) {
        &[(OUTER_VAR, 1), (OUTER_VAR, 2)]
    } else {
        &[(OUTER_VAR, 1), (OUTER_VAR, 2), (INNER_VAR, 1), (INNER_VAR, 2)]
    };
    let mut join_tlist = NodeList::nil();
    for (i, &(varno, attno)) in tl_cols.iter().enumerate() {
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
        jointype,
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
    drain_wide_rows_nullable(pstmt, natts, passes)
        .into_iter()
        .map(|rows| {
            rows.into_iter()
                .map(|row| row.into_iter().map(|v| v.expect("unexpected NULL")).collect())
                .collect()
        })
        .collect()
}

fn drain_wide_rows_nullable(
    pstmt: &'static PlannedStmt<'static>,
    natts: usize,
    passes: usize,
) -> Vec<Vec<Vec<Option<i32>>>> {
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
                    row.push(if isnull { None } else { Some(v.as_i32()) });
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
    let runs = drain_wide_rows(
        mk_nestloop_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_INNER),
        4,
        2,
    );
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
    let runs = drain_wide_rows(
        mk_nestloop_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_INNER),
        4,
        1,
    );
    assert_eq!(runs, vec![Vec::<Vec<i32>>::new()]);
    scanfix::quiesced();
}

// SEMI: outer 3 matches two inners but is emitted once (single_match advance);
// ANTI: only the never-matched outer 1 is emitted. Second pass covers rescan.
#[test]
fn nestloop_semi_and_anti_join_over_fake_heaps() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let outer: u32 = 70024;
    let inner: u32 = 70025;
    scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (3, 30)]]);
    scanfix::register_table_2col(inner, &[&[(2, 200), (3, 300), (3, 301), (4, 400)]]);

    let semi = vec![vec![2, 20], vec![3, 30]];
    let runs = drain_wide_rows(
        mk_nestloop_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_SEMI),
        2,
        2,
    );
    assert_eq!(runs, vec![semi.clone(), semi]);

    let anti = vec![vec![1, 10]];
    let runs = drain_wide_rows(
        mk_nestloop_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_ANTI),
        2,
        2,
    );
    assert_eq!(runs, vec![anti.clone(), anti]);
    scanfix::quiesced();
}


// HashJoin(hashclause a = c) over two fake-heap seqscans, in the post-setrefs
// shape: outer keys OUTER_VAR, the Hash inner node carries the inner keys
// (OUTER_VAR of its own child). The equijoin clause is the hashclause, so
// joinqual is empty.
fn mk_hashjoin_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    outer_relid: u32,
    inner_relid: u32,
    jointype: ::types_nodes::JoinType,
) -> &'mcx PlannedStmt<'mcx> {
    mk_hashjoin_pstmt_est(mcx, outer_relid, inner_relid, jointype, None)
}

// inner_est = (plan_rows, plan_width) on the Hash child's SeqScan: it drives
// ExecChooseHashTableSize, so multi-batch tests pin it.
fn mk_hashjoin_pstmt_est<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    outer_relid: u32,
    inner_relid: u32,
    jointype: ::types_nodes::JoinType,
    inner_est: Option<(f64, i32)>,
) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Hash, HashJoin, Join, Plan, Scan, SeqScan};
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
        let (plan_rows, plan_width) = if scanrelid == 2 {
            inner_est.unwrap_or((0.0, 0))
        } else {
            (0.0, 0)
        };
        Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan {
                        targetlist: scan_tlist(varno),
                        plan_rows,
                        plan_width,
                        ..Default::default()
                    },
                    scanrelid,
                },
            },
        )
        .unwrap()
    };

    // SEMI/ANTI project only the outer side, RIGHT_SEMI/RIGHT_ANTI only the
    // inner side, as the planner emits.
    let tl_cols: &[(i32, i16)] = match jointype {
        ::types_nodes::JoinType::JOIN_SEMI | ::types_nodes::JoinType::JOIN_ANTI => {
            &[(OUTER_VAR, 1), (OUTER_VAR, 2)]
        }
        ::types_nodes::JoinType::JOIN_RIGHT_SEMI
        | ::types_nodes::JoinType::JOIN_RIGHT_ANTI => &[(INNER_VAR, 1), (INNER_VAR, 2)],
        _ => &[(OUTER_VAR, 1), (OUTER_VAR, 2), (INNER_VAR, 1), (INNER_VAR, 2)],
    };
    let mut join_tlist = NodeList::nil();
    for (i, &(varno, attno)) in tl_cols.iter().enumerate() {
        let v = Node::mk_var(mcx, varno, attno, INT4OID, -1, 0, 0).unwrap();
        join_tlist
            .lappend(mcx, Node::mk_target_entry(mcx, v, i as i16 + 1, Some("x"), false).unwrap())
            .unwrap();
    }
    let hashclause = {
        let l = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let r = Node::mk_var(mcx, INNER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        Node::mk(
            mcx,
            ::types_nodes::primnodes::OpExpr {
                opno: 96,     // int4eq
                opfuncid: 65, // pg_proc int4eq
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

    // Hash inner node: hashkeys reference its own child (OUTER_VAR att1).
    let inner_hashkey = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let mut hash_node = Node::build::<Hash>(mcx).unwrap();
    hash_node.plan = Plan {
        targetlist: scan_tlist(2),
        lefttree: Some(mk_scan(2, 2)),
        ..Default::default()
    };
    hash_node.hashkeys = NodeList::make1(mcx, inner_hashkey).unwrap();

    let outer_hashkey = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let mut hj = Node::build::<HashJoin>(mcx).unwrap();
    hj.join = Join {
        plan: Plan {
            targetlist: join_tlist,
            lefttree: Some(mk_scan(1, 1)),
            righttree: Some(hash_node.seal()),
            ..Default::default()
        },
        jointype,
        inner_unique: false,
        joinqual: NodeList::nil(),
    };
    hj.hashclauses = NodeList::make1(mcx, hashclause).unwrap();
    let mut hashoperators = ::types_nodes::list::OidList::nil();
    hashoperators.lappend(mcx, 96).unwrap();
    let mut hashcollations = ::types_nodes::list::OidList::nil();
    hashcollations.lappend(mcx, 0).unwrap();
    hj.hashoperators = hashoperators;
    hj.hashcollations = hashcollations;
    hj.hashkeys = NodeList::make1(mcx, outer_hashkey).unwrap();

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
        Node::mk(mcx, RTEPermissionInfo { relid, requiredPerms: 1 << 1, ..Default::default() })
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
    pstmt.planTree = Some(hj.seal());
    pstmt.rtable = rtable;
    pstmt.permInfos = perms;
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

// Same fixtures as the nestloop e2e; the hash join returns the identical set
// (bucket-chain order differs, so compare sorted). Second pass exercises
// ExecReScanHashJoin (single-batch table reuse).
#[test]
fn hashjoin_inner_join_matches_nestloop_result() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let outer: u32 = 70030;
    let inner: u32 = 70031;
    scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (3, 30)]]);
    scanfix::register_table_2col(inner, &[&[(2, 200), (3, 300), (3, 301), (4, 400)]]);
    let mut expected = vec![vec![2, 20, 2, 200], vec![3, 30, 3, 300], vec![3, 30, 3, 301]];
    expected.sort();

    let runs = drain_wide_rows(
        mk_hashjoin_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_INNER),
        4,
        2,
    );
    for run in &runs {
        let mut got = run.clone();
        got.sort();
        assert_eq!(got, expected, "hash join result set must equal the nestloop result set");
    }
    assert_eq!(runs.len(), 2);
    scanfix::quiesced();
}

#[test]
fn hashjoin_with_empty_inner_returns_nothing() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let outer: u32 = 70032;
    let inner: u32 = 70033;
    scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20)]]);
    scanfix::register_table_2col(inner, &[]);
    let runs = drain_wide_rows(
        mk_hashjoin_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_INNER),
        4,
        1,
    );
    assert_eq!(runs, vec![Vec::<Vec<i32>>::new()]);
    scanfix::quiesced();
}

// SEMI dedups the doubly-matched outer 3; ANTI emits only the never-matched
// outer 1. The empty-inner ANTI case must NOT take the empty-hashtable early
// exit (HJ_FILL_OUTER): every outer row comes back. Outer scan order is
// preserved by the probe loop, so no sort. Second pass covers rescan.
#[test]
fn hashjoin_semi_and_anti_join_over_fake_heaps() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let outer: u32 = 70034;
    let inner: u32 = 70035;
    scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (3, 30)]]);
    scanfix::register_table_2col(inner, &[&[(2, 200), (3, 300), (3, 301), (4, 400)]]);

    let semi = vec![vec![2, 20], vec![3, 30]];
    let runs = drain_wide_rows(
        mk_hashjoin_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_SEMI),
        2,
        2,
    );
    assert_eq!(runs, vec![semi.clone(), semi]);

    let anti = vec![vec![1, 10]];
    let runs = drain_wide_rows(
        mk_hashjoin_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_ANTI),
        2,
        2,
    );
    assert_eq!(runs, vec![anti.clone(), anti]);

    let empty_inner: u32 = 70036;
    scanfix::register_table_2col(empty_inner, &[]);
    let all_outer = vec![vec![1, 10], vec![2, 20], vec![3, 30]];
    let runs = drain_wide_rows(
        mk_hashjoin_pstmt(mcx, outer, empty_inner, ::types_nodes::JoinType::JOIN_ANTI),
        2,
        1,
    );
    assert_eq!(runs, vec![all_outer]);
    let runs = drain_wide_rows(
        mk_hashjoin_pstmt(mcx, outer, empty_inner, ::types_nodes::JoinType::JOIN_SEMI),
        2,
        1,
    );
    assert_eq!(runs, vec![Vec::<Vec<i32>>::new()]);
    scanfix::quiesced();
}

// RIGHT_SEMI emits each matched inner once even with duplicate-key outers
// (the already-matched skip); RIGHT_ANTI emits only never-matched inners via
// the unmatched-inner fill. Empty-outer RIGHT_ANTI emits every inner row.
// Second pass covers the rescan match-flag reset (RIGHT_SEMI would emit
// nothing on pass 2 without it).
#[test]
fn hashjoin_right_semi_and_right_anti_join_over_fake_heaps() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let outer: u32 = 70037;
    let inner: u32 = 70038;
    scanfix::register_table_2col(outer, &[&[(2, 20), (3, 30), (3, 31)]]);
    scanfix::register_table_2col(inner, &[&[(2, 200), (3, 300), (3, 301), (4, 400)]]);

    let sorted = |mut rows: Vec<Vec<i32>>| {
        rows.sort();
        rows
    };
    let right_semi = vec![vec![2, 200], vec![3, 300], vec![3, 301]];
    let runs = drain_wide_rows(
        mk_hashjoin_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_RIGHT_SEMI),
        2,
        2,
    );
    assert_eq!(runs.len(), 2);
    for run in runs {
        assert_eq!(sorted(run), right_semi);
    }

    let right_anti = vec![vec![4, 400]];
    let runs = drain_wide_rows(
        mk_hashjoin_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_RIGHT_ANTI),
        2,
        2,
    );
    assert_eq!(runs.len(), 2);
    for run in runs {
        assert_eq!(sorted(run), right_anti);
    }

    let empty_outer: u32 = 70039;
    scanfix::register_table_2col(empty_outer, &[]);
    let all_inner = vec![vec![2, 200], vec![3, 300], vec![3, 301], vec![4, 400]];
    let runs = drain_wide_rows(
        mk_hashjoin_pstmt(mcx, empty_outer, inner, ::types_nodes::JoinType::JOIN_RIGHT_ANTI),
        2,
        1,
    );
    assert_eq!(runs.len(), 1);
    for run in runs {
        assert_eq!(sorted(run), all_inner);
    }
    scanfix::quiesced();
}

// FULL = matched pairs + null-extended unmatched outer AND inner rows; the
// second pass exercises the rescan match-flag reset (unmatched inners would
// vanish on pass 2 without it).
#[test]
fn hashjoin_full_join_over_fake_heaps() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let outer: u32 = 70050;
    let inner: u32 = 70051;
    scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (3, 30)]]);
    scanfix::register_table_2col(inner, &[&[(2, 200), (3, 300), (3, 301), (4, 400)]]);

    let expected = vec![
        vec![None, None, Some(4), Some(400)],
        vec![Some(1), Some(10), None, None],
        vec![Some(2), Some(20), Some(2), Some(200)],
        vec![Some(3), Some(30), Some(3), Some(300)],
        vec![Some(3), Some(30), Some(3), Some(301)],
    ];
    let runs = drain_wide_rows_nullable(
        mk_hashjoin_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_FULL),
        4,
        2,
    );
    assert_eq!(runs.len(), 2);
    for run in runs {
        let mut got = run;
        got.sort();
        assert_eq!(got, expected);
    }
    scanfix::quiesced();
}

// work_mem=64kB + a 4000-row/width-8 inner estimate forces nbatch>1 through
// the real spill path (BufFile batch files, HJ_NEED_NEW_BATCH reload, outer
// routing). Results must equal the single-batch answer; pass 2 exercises the
// multi-batch rescan (destroy + rebuild, Hash child rescanned).
#[test]
fn hashjoin_multibatch_matches_single_batch_results() {
    install_seams();
    scanfix::install();
    if !guc_tables::vars::work_mem.installed() {
        init_small::init_seams();
    }
    // Temp-file substrate: fd VFDs + resowner + a scratch datadir cwd.
    if !guc_tables::vars::temp_file_limit.installed() {
        guc_tables::init_seams();
    }
    resowner::init_seams();
    ipc_seams::before_shmem_exit::set(|_cb, _arg| Ok(()));
    ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    pgstat_seams::pgstat_report_tempfile::set(|_| {});
    let owner =
        resowner::ResourceOwnerCreate(::types_resowner::ResourceOwner::NULL, "hj-multibatch")
            .unwrap();
    resowner_seams::set_current_resource_owner::call(owner);
    let dir = std::env::temp_dir().join(format!("pgrust_hj_mb_{}", std::process::id()));
    std::fs::create_dir_all(dir.join("base/pgsql_tmp")).unwrap();
    std::env::set_current_dir(&dir).unwrap();
    ::fd::InitFileAccess();
    ::fd::InitTemporaryFileAccess().unwrap();
    if !guc_tables::vars::temp_tablespaces.installed() {
        guc_tables::vars::temp_tablespaces.install(guc_tables::GucVarAccessors {
            get: ::fd::vfd::temp_tablespaces_guc,
            set: ::fd::vfd::set_temp_tablespaces_guc,
        });
    }

    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let outer: u32 = 70052;
    let inner: u32 = 70053;
    let outer_rows: Vec<(i32, i32)> = (3990..=4010).map(|i| (i, i)).collect();
    scanfix::register_table_2col(outer, &[&outer_rows]);
    let inner_rows: Vec<(i32, i32)> = (1..=4000).map(|i| (i, i * 10)).collect();
    let inner_pages: Vec<&[(i32, i32)]> = inner_rows.chunks(200).collect();
    scanfix::register_table_2col(inner, &inner_pages);

    let saved_work_mem = guc_tables::vars::work_mem.read();
    guc_tables::vars::work_mem.write(64);
    let (_b, nbatch, _s) = ::nodehash::exec_choose_hash_table_size(4000.0, 8, true, false, 0);
    assert!(nbatch > 1, "fixture must force a multi-batch table, got nbatch={nbatch}");

    let inner_expected: Vec<Vec<i32>> =
        (3990..=4000).map(|k| vec![k, k, k, k * 10]).collect();
    let runs = drain_wide_rows(
        mk_hashjoin_pstmt_est(
            mcx,
            outer,
            inner,
            ::types_nodes::JoinType::JOIN_INNER,
            Some((4000.0, 8)),
        ),
        4,
        2,
    );
    assert_eq!(runs.len(), 2);
    for run in runs {
        let mut got = run;
        got.sort();
        assert_eq!(got, inner_expected);
    }

    // FULL at the same work_mem: 11 matched + 10 unmatched outer + 3989
    // unmatched inner = 4010 rows; spot-check the null-extension edges.
    let runs = drain_wide_rows_nullable(
        mk_hashjoin_pstmt_est(
            mcx,
            outer,
            inner,
            ::types_nodes::JoinType::JOIN_FULL,
            Some((4000.0, 8)),
        ),
        4,
        2,
    );
    assert_eq!(runs.len(), 2);
    for run in runs {
        let mut got = run;
        got.sort();
        assert_eq!(got.len(), 4010);
        let unmatched_inner: Vec<_> =
            got.iter().filter(|r| r[0].is_none()).collect();
        assert_eq!(unmatched_inner.len(), 3989);
        assert!(unmatched_inner.iter().all(|r| {
            let k = r[2].unwrap();
            (1..=3989).contains(&k) && r[3] == Some(k * 10) && r[1].is_none()
        }));
        let unmatched_outer: Vec<_> =
            got.iter().filter(|r| r[0].is_some() && r[2].is_none()).collect();
        assert_eq!(
            unmatched_outer.iter().map(|r| r[0].unwrap()).collect::<Vec<_>>(),
            (4001..=4010).collect::<Vec<_>>()
        );
        let matched: Vec<_> =
            got.iter().filter(|r| r[0].is_some() && r[2].is_some()).collect();
        assert_eq!(matched.len(), 11);
        assert!(matched
            .iter()
            .all(|r| r[0] == r[2] && r[3] == Some(r[0].unwrap() * 10)));
    }

    guc_tables::vars::work_mem.write(saved_work_mem);
    scanfix::quiesced();
}

fn mk_param_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    kind: ::types_nodes::primnodes::ParamKind,
    paramid: i32,
    n_exec_types: usize,
) -> &'mcx PlannedStmt<'mcx> {
    let param = Node::mk(
        mcx,
        ::types_nodes::primnodes::Param {
            paramkind: kind,
            paramid,
            paramtype: INT4OID,
            paramtypmod: -1,
            paramcollid: 0,
            location: -1,
        },
    )
    .unwrap();
    let tle = Node::mk_target_entry(mcx, param, 1, Some("?column?"), false).unwrap();
    let mut result = Node::build::<ResultPlan>(mcx).unwrap();
    result.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    let plan_node = result.seal();
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(plan_node);
    for _ in 0..n_exec_types {
        pstmt.paramExecTypes.lappend(mcx, INT4OID).unwrap();
    }
    pstmt.seal_ref()
}

fn run_param_qd(pstmt: &'static PlannedStmt<'static>, params: ParamListHandle) {
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        "SELECT $1",
        None,
        None,
        CommandDest::None,
        params,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    let mut dest = DestReceiver::DoNothing;
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd), 1);
    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
}

#[test]
fn executor_start_wires_bound_params_to_estate() {
    use ::types_portal::params::{ParamExternData, PARAM_FLAG_CONST};
    install_seams();
    let mcx = leaked_mcx();
    let pstmt = mk_param_pstmt(mcx, ::types_nodes::primnodes::ParamKind::PARAM_EXTERN, 1, 0);

    let externs: &'static [ParamExternData] = Box::leak(Box::new([ParamExternData {
        value: Datum::from_i32(42),
        isnull: false,
        pflags: PARAM_FLAG_CONST,
        ptype: INT4OID,
    }]));
    // SAFETY: leaked, outlives the registry entry.
    let h = unsafe { ::types_portal::params::register(externs) };
    run_param_qd(pstmt, h);
    ::types_portal::params::free(h);

    // Without the handle, init succeeds and C's ereport surfaces at run
    // (ExecEvalParamExtern) — EXPLAIN (GENERIC_PLAN) relies on init-only.
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        "SELECT $1",
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
    let err = execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest)
        .unwrap_err();
    assert_eq!(err.message, "no value found for parameter 1");
    execmain_seams::release_query_desc::call(qd);
}

#[test]
fn executor_start_sizes_param_exec_vals() {
    install_seams();
    let mcx = leaked_mcx();
    let pstmt = mk_param_pstmt(mcx, ::types_nodes::primnodes::ParamKind::PARAM_EXEC, 1, 2);
    run_param_qd(pstmt, ParamListHandle::NULL);
}

// DISTINCT sorted strategy e2e: Unique over Sort over SeqScan dedups through
// the real InitPlan path (rescan pinned).
#[test]
fn unique_over_sort_dedups_end_to_end() {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan, Sort, Unique};
    use ::types_nodes::primnodes::OUTER_VAR;

    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70090;
    scanfix::register_table(relid, &[&[3, 1, 2, 1], &[3, 2, 1]]);

    let scan_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let scan_tle = Node::mk_target_entry(mcx, scan_var, 1, Some("a"), false).unwrap();
    let scan = Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
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
    let mut sort = Node::build::<Sort>(mcx).unwrap();
    sort.plan.targetlist = outer_tle(mcx);
    sort.plan.lefttree = Some(scan);
    sort.numCols = 1;
    sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT]).unwrap();
    sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();

    let mut uq = Node::build::<Unique>(mcx).unwrap();
    uq.plan.targetlist = outer_tle(mcx);
    uq.plan.lefttree = Some(sort.seal());
    uq.numCols = 1;
    uq.uniqColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    uq.uniqOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    uq.uniqCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();

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
    pstmt.planTree = Some(uq.seal());
    pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
    pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
    pstmt.unprunableRelids = unpruned;
    let pstmt = pstmt.seal_ref();

    let runs = drain_int4_rows(pstmt, true);
    assert_eq!(runs, vec![vec![1, 2, 3], vec![1, 2, 3]]);
    scanfix::quiesced();
}

// --- nodeSubplan.c initplan slice ---

fn mk_initplan_sub_seqscan<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    with_tlist: bool,
) -> Node<'mcx> {
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan};
    let tlist = if with_tlist {
        let var = Node::mk_var(mcx, 2, 1, INT4OID, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, var, 1, Some("b"), false).unwrap();
        NodeList::make1(mcx, tle).unwrap()
    } else {
        NodeList::nil()
    };
    Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan { targetlist: tlist, ..Default::default() },
                scanrelid: 2,
            },
        },
    )
    .unwrap()
}

fn mk_two_rel_pstmt_parts<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    t1: u32,
    t2: u32,
) -> (NodeList<'mcx>, NodeList<'mcx>, ::types_nodes::bitmapset::Bitmapset<'mcx>) {
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    let mk_rte = |relid| {
        Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid,
                relkind: ::types_rel::RELKIND_RELATION,
                rellockmode: ::types_rel::AccessShareLock,
                perminfoindex: if relid == t1 { 1 } else { 2 },
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let mk_pi = |relid| {
        Node::mk(mcx, RTEPermissionInfo { relid, requiredPerms: 1 << 1, ..Default::default() })
            .unwrap()
    };
    let mut rtable = NodeList::make1(mcx, mk_rte(t1)).unwrap();
    rtable.lappend(mcx, mk_rte(t2)).unwrap();
    let mut perms = NodeList::make1(mcx, mk_pi(t1)).unwrap();
    perms.lappend(mcx, mk_pi(t2)).unwrap();
    let mut unpruned = ::types_nodes::bitmapset::Bitmapset::empty();
    unpruned.add_member(mcx, 1).unwrap();
    unpruned.add_member(mcx, 2).unwrap();
    (rtable, perms, unpruned)
}

fn mk_sub_plan_node<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    link: ::types_nodes::SubLinkType,
    first_col_type: u32,
) -> Node<'mcx> {
    Node::mk(
        mcx,
        ::types_nodes::SubPlan {
            subLinkType: link,
            plan_id: 1,
            plan_name: Some("InitPlan 1"),
            firstColType: first_col_type,
            firstColTypmod: -1,
            setParam: ::types_nodes::IntList::make1(mcx, 0).unwrap(),
            ..Default::default()
        },
    )
    .unwrap()
}

// `SELECT a FROM t1 WHERE a < (SELECT b FROM t2)` as an initplan PlannedStmt.
fn mk_expr_initplan_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, t1: u32, t2: u32) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan};
    use ::types_nodes::primnodes::{Param, ParamKind};

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
    let qual_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let prm = Node::mk(
        mcx,
        Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid: 0,
            paramtype: INT4OID,
            paramtypmod: -1,
            paramcollid: 0,
            location: -1,
        },
    )
    .unwrap();
    let qual = Node::mk(
        mcx,
        ::types_nodes::OpExpr {
            opno: INT4_LT,
            opfuncid: 66,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, qual_var, prm).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let scan = Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan {
                    targetlist: NodeList::make1(mcx, tle).unwrap(),
                    qual: NodeList::make1(mcx, qual).unwrap(),
                    initPlan: NodeList::make1(
                        mcx,
                        mk_sub_plan_node(mcx, ::types_nodes::SubLinkType::EXPR_SUBLINK, INT4OID),
                    )
                    .unwrap(),
                    ..Default::default()
                },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    let (rtable, perms, unpruned) = mk_two_rel_pstmt_parts(mcx, t1, t2);
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(scan);
    pstmt.subplans =
        ::types_nodes::list::OptNodeList::make1(mcx, Some(mk_initplan_sub_seqscan(mcx, true)))
            .unwrap();
    pstmt.paramExecTypes = ::types_nodes::list::OidList::make1(mcx, INT4OID).unwrap();
    pstmt.rtable = rtable;
    pstmt.permInfos = perms;
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

// `SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t2)`: gating Result with a
// one-time filter over $0.
fn mk_exists_initplan_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, t1: u32, t2: u32) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::plannodes::{Plan, Result as ResultPlan, Scan, SeqScan};
    use ::types_nodes::primnodes::{Param, ParamKind, OUTER_VAR};

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
    let scan = Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan {
                    targetlist: NodeList::make1(mcx, tle).unwrap(),
                    ..Default::default()
                },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    let out_var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let out_tle = Node::mk_target_entry(mcx, out_var, 1, Some("a"), false).unwrap();
    let prm = Node::mk(
        mcx,
        Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid: 0,
            paramtype: 16,
            paramtypmod: -1,
            paramcollid: 0,
            location: -1,
        },
    )
    .unwrap();
    let rcq = Node::mk_list(mcx, NodeList::make1(mcx, prm).unwrap()).unwrap();
    let mut result = Node::build::<ResultPlan>(mcx).unwrap();
    result.plan.targetlist = NodeList::make1(mcx, out_tle).unwrap();
    result.plan.lefttree = Some(scan);
    result.plan.initPlan = NodeList::make1(
        mcx,
        mk_sub_plan_node(mcx, ::types_nodes::SubLinkType::EXISTS_SUBLINK, 2278),
    )
    .unwrap();
    result.resconstantqual = Some(rcq);
    let top = result.seal();

    let (rtable, perms, unpruned) = mk_two_rel_pstmt_parts(mcx, t1, t2);
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(top);
    pstmt.subplans =
        ::types_nodes::list::OptNodeList::make1(mcx, Some(mk_initplan_sub_seqscan(mcx, false)))
            .unwrap();
    pstmt.paramExecTypes = ::types_nodes::list::OidList::make1(mcx, 16).unwrap();
    pstmt.rtable = rtable;
    pstmt.permInfos = perms;
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

fn run_initplan_pstmt(pstmt: &'static PlannedStmt<'static>) -> Result<Vec<i32>, Box<types_error::PgError>> {
    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));
    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_snapshot = Some(snapshot);
        {
            let n = pstmt.paramExecTypes.len();
            let es = &mut data.estate;
            es.es_param_exec_vals.extend(core::iter::repeat_n(
                ::types_portal::params::ParamExecData::EMPTY,
                n,
            ));
            es.es_param_subplans.extend(core::iter::repeat_n(None, n));
        }
        crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();

        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        let mut out = Vec::new();
        let mut run_err = None;
        loop {
            match exec_proc_node(ps, estate) {
                Ok(Some(slot_id)) => {
                    let base = estate.slot_mut(slot_id).base();
                    out.push(base.tts_values[0].as_i32());
                }
                Ok(None) => break,
                Err(e) => {
                    run_err = Some(e);
                    break;
                }
            }
        }
        crate::exec_end_node(ps, estate).unwrap();
        for i in 0..estate.es_subplanstates.len() {
            let cell = estate.es_subplanstates[i];
            // SAFETY: init_plan's arena cell (standard_executor_end's shape).
            let slot = unsafe {
                &mut *cell.0.cast::<Option<crate::PlanStateNode<'_>>>().as_ptr()
            };
            if let Some(mut sub) = slot.take() {
                crate::exec_end_node(&mut sub, estate).unwrap();
            }
        }
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
        match run_err {
            Some(e) => Err(e),
            None => Ok(out),
        }
    })
}

#[test]
fn expr_initplan_over_fake_heaps_end_to_end() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let (t1, t2) = (70110u32, 70111u32);
    scanfix::register_table(t1, &[&[1, 8, 3, 12, 5]]);
    scanfix::register_table(t2, &[&[6]]);
    // a < (SELECT b FROM t2) = a < 6.
    let rows = run_initplan_pstmt(mk_expr_initplan_pstmt(mcx, t1, t2)).unwrap();
    assert_eq!(rows, vec![1, 3, 5]);
    scanfix::quiesced();
}

#[test]
fn expr_initplan_empty_subquery_yields_null_param() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let (t1, t2) = (70112u32, 70113u32);
    scanfix::register_table(t1, &[&[1, 2, 3]]);
    scanfix::register_table(t2, &[]);
    // $0 is NULL, so the strict `<` never passes.
    let rows = run_initplan_pstmt(mk_expr_initplan_pstmt(mcx, t1, t2)).unwrap();
    assert_eq!(rows, Vec::<i32>::new());
    scanfix::quiesced();
}

#[test]
fn expr_initplan_two_rows_is_cardinality_violation() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let (t1, t2) = (70114u32, 70115u32);
    scanfix::register_table(t1, &[&[1, 2, 3]]);
    scanfix::register_table(t2, &[&[6, 7]]);
    let err = run_initplan_pstmt(mk_expr_initplan_pstmt(mcx, t1, t2)).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_CARDINALITY_VIOLATION);
    assert!(err
        .message()
        .contains("more than one row returned by a subquery used as an expression"));
    scanfix::quiesced();
}

#[test]
fn exists_initplan_gates_scan_end_to_end() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let (t1, t2) = (70116u32, 70117u32);
    scanfix::register_table(t1, &[&[4, 9]]);
    scanfix::register_table(t2, &[&[42]]);
    let rows = run_initplan_pstmt(mk_exists_initplan_pstmt(mcx, t1, t2)).unwrap();
    assert_eq!(rows, vec![4, 9]);
    scanfix::quiesced();
}

#[test]
fn exists_initplan_empty_subquery_gates_to_zero_rows() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let (t1, t2) = (70118u32, 70119u32);
    scanfix::register_table(t1, &[&[4, 9]]);
    scanfix::register_table(t2, &[]);
    let rows = run_initplan_pstmt(mk_exists_initplan_pstmt(mcx, t1, t2)).unwrap();
    assert_eq!(rows, Vec::<i32>::new());
    scanfix::quiesced();
}

// --- ExecSerializePlan NULL-hole subplan transfer (execParallel.c) ---

#[test]
fn worker_pstmt_nulls_parallel_unsafe_subplans() {
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan};
    install_seams();
    let mcx = leaked_mcx();
    let mk_sub = |safe: bool| {
        Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan { parallel_safe: safe, ..Default::default() },
                    scanrelid: 1,
                },
            },
        )
        .unwrap()
    };
    let mut subplans = ::types_nodes::list::OptNodeList::nil();
    subplans.lappend(mcx, Some(mk_sub(true))).unwrap();
    subplans.lappend(mcx, Some(mk_sub(false))).unwrap();
    subplans.lappend(mcx, Some(mk_sub(true))).unwrap();
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(mk_sub(true));
    pstmt.subplans = subplans;
    pstmt.paramExecTypes =
        ::types_nodes::list::OidList::make2(mcx, INT4OID, INT4OID).unwrap();
    let pstmt = pstmt.seal_ref();
    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_plannedstmt = Some(pstmt);
        let worker = crate::execparallel::build_worker_pstmt(
            &data.estate,
            pstmt.planTree.unwrap(),
        )
        .unwrap();
        // The unsafe subplan is a NULL hole; the safe ones keep their
        // plan_id positions.
        assert_eq!(worker.subplans.len(), 3);
        assert!(worker.subplans.nth(0).is_some());
        assert!(worker.subplans.nth(1).is_none());
        assert!(worker.subplans.nth(2).is_some());
        assert_eq!(worker.paramExecTypes.as_slice(), pstmt.paramExecTypes.as_slice());
        assert!(worker.rowMarks.is_nil());
        assert!(worker.resultRelations.is_nil());
        assert!(worker.rewindPlanIDs.is_empty());
    });
}

#[test]
fn initplan_hole_reference_errors_subplan_not_initialized() {
    use ::types_nodes::plannodes::{Result as ResultPlan};
    install_seams();
    let mcx = leaked_mcx();
    let tle = Node::mk_target_entry(mcx, mk_int4_const(mcx, 1), 1, Some("?column?"), false)
        .unwrap();
    let mut result = Node::build::<ResultPlan>(mcx).unwrap();
    result.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    result.plan.initPlan = NodeList::make1(
        mcx,
        mk_sub_plan_node(mcx, ::types_nodes::SubLinkType::EXISTS_SUBLINK, 16),
    )
    .unwrap();
    let top = result.seal();
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(top);
    // ExecSerializePlan's parallel-unsafe hole in the worker copy.
    pstmt.subplans = ::types_nodes::list::OptNodeList::make1(mcx, None).unwrap();
    pstmt.paramExecTypes = ::types_nodes::list::OidList::make1(mcx, 16).unwrap();
    let pstmt = pstmt.seal_ref();
    with_exec_data(pstmt, |data, pstmt| {
        {
            let n = pstmt.paramExecTypes.len();
            let es = &mut data.estate;
            es.es_param_exec_vals.extend(core::iter::repeat_n(
                ::types_portal::params::ParamExecData::EMPTY,
                n,
            ));
            es.es_param_subplans.extend(core::iter::repeat_n(None, n));
        }
        let err =
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap_err();
        assert!(
            err.message().contains("subplan \"InitPlan 1\" was not initialized"),
            "{}",
            err.message()
        );
    });
}

// WindowAgg(part by g, ord by a) over Sort(g,a) over SeqScan: SELECT g, a,
// row_number() OVER w, rank() OVER w, dense_rank() OVER w, sum(a) OVER w
// FROM t WINDOW w AS (PARTITION BY g ORDER BY a).
fn mk_windowagg_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    relid: u32,
    with_order_by: bool,
) -> &'mcx PlannedStmt<'mcx> {
    mk_windowagg_pstmt_ex(mcx, relid, with_order_by, None, true)
}

// The parameterized variant (windows_ab refusal shapes): `frame_options` =
// Some(bits) overrides FRAMEOPTION_DEFAULTS; `with_sort` = false plans the
// WindowAgg straight over the SeqScan (the presorted-plan shape).
fn mk_windowagg_pstmt_ex<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    relid: u32,
    with_order_by: bool,
    frame_options: Option<i32>,
    with_sort: bool,
) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan, Sort, WindowAgg};
    use ::types_nodes::primnodes::{WindowFunc, OUTER_VAR};

    let mk_tlist = |varno: i32| {
        let g = Node::mk_var(mcx, varno, 1, INT4OID, -1, 0, 0).unwrap();
        let a = Node::mk_var(mcx, varno, 2, INT4OID, -1, 0, 0).unwrap();
        NodeList::make2(
            mcx,
            Node::mk_target_entry(mcx, g, 1, Some("g"), false).unwrap(),
            Node::mk_target_entry(mcx, a, 2, Some("a"), false).unwrap(),
        )
        .unwrap()
    };

    let scan = Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan { targetlist: mk_tlist(1), ..Default::default() },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    let child = if with_sort {
        let mut sort = Node::build::<Sort>(mcx).unwrap();
        sort.plan.targetlist = mk_tlist(OUTER_VAR);
        sort.plan.lefttree = Some(scan);
        sort.numCols = 2;
        sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16, 2]).unwrap();
        sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT, INT4_LT]).unwrap();
        sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32, 0]).unwrap();
        sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false, false]).unwrap();
        sort.seal()
    } else {
        scan
    };

    let mk_wfunc = |fnoid: u32, winagg: bool| {
        let mut w = Node::build::<WindowFunc>(mcx).unwrap();
        w.winfnoid = fnoid;
        w.wintype = INT8OID;
        w.winref = 1;
        w.winagg = winagg;
        if winagg {
            w.args = NodeList::make1(
                mcx,
                Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap(),
            )
            .unwrap();
        }
        w.seal()
    };

    let mut tlist = mk_tlist(OUTER_VAR);
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, mk_wfunc(3100, false), 3, Some("rn"), false).unwrap())
        .unwrap();
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, mk_wfunc(3101, false), 4, Some("rank"), false).unwrap())
        .unwrap();
    tlist
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, mk_wfunc(3102, false), 5, Some("dense"), false).unwrap(),
        )
        .unwrap();
    tlist
        .lappend(mcx, Node::mk_target_entry(mcx, mk_wfunc(2108, true), 6, Some("sum"), false).unwrap())
        .unwrap();

    let mut wa = Node::build::<WindowAgg>(mcx).unwrap();
    wa.plan.targetlist = tlist;
    wa.plan.lefttree = Some(child);
    if let Some(fo) = frame_options {
        wa.frameOptions = fo;
    }
    wa.winref = 1;
    wa.partNumCols = 1;
    wa.partColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    wa.partOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    wa.partCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    if with_order_by {
        wa.ordNumCols = 1;
        wa.ordColIdx = ::mcx::slice_borrow_in(mcx, &[2i16]).unwrap();
        wa.ordOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
        wa.ordCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    }
    wa.topWindow = true;

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
    pstmt.planTree = Some(wa.seal());
    pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
    pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

// WindowAgg(partition by g, default frame; row_number + rank) over Sort(g)
// over hashed Agg(count(*) group by g) over SeqScan — W1-admissible in every
// respect EXCEPT the agg-fed sort, pinning the inc-1 STRUCTURAL refusal of
// the hash-agg breaker feed family: its `sort_feed_if_needed` carries the
// lane's one dynamic feed-time refuse (the agg-over-join multi-batch spill),
// which the sticky window drive cannot host (windows.rs
// `window_refuse_reason`; the fixed feed-refuse blocker).
fn mk_window_over_agg_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, relid: u32) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Agg, Plan, Scan, SeqScan, Sort, WindowAgg};
    use ::types_nodes::primnodes::{Aggref, WindowFunc, OUTER_VAR};

    // SeqScan (g, a).
    let g_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let a_var = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
    let scan_tlist = NodeList::make2(
        mcx,
        Node::mk_target_entry(mcx, g_var, 1, Some("g"), false).unwrap(),
        Node::mk_target_entry(mcx, a_var, 2, Some("a"), false).unwrap(),
    )
    .unwrap();
    let scan = Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan { targetlist: scan_tlist, ..Default::default() },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    // Hashed Agg: count(*) GROUP BY g — output (g int4, cnt int8).
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = 2803;
    aggref.aggtype = INT8OID;
    aggref.aggtranstype = INT8OID;
    aggref.aggstar = true;
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let mut agg_tlist = NodeList::make1(
        mcx,
        Node::mk_target_entry(
            mcx,
            Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap(),
            1,
            Some("g"),
            false,
        )
        .unwrap(),
    )
    .unwrap();
    agg_tlist
        .lappend(mcx, Node::mk_target_entry(mcx, aggref.seal(), 2, Some("cnt"), false).unwrap())
        .unwrap();
    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = agg_tlist;
    agg.plan.lefttree = Some(scan);
    agg.aggstrategy = 2;
    agg.numCols = 1;
    agg.grpColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    agg.grpOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    agg.grpCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    agg.numGroups = 4;

    // Sort by g over the agg output.
    let mk_out_tlist = || {
        NodeList::make2(
            mcx,
            Node::mk_target_entry(
                mcx,
                Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap(),
                1,
                Some("g"),
                false,
            )
            .unwrap(),
            Node::mk_target_entry(
                mcx,
                Node::mk_var(mcx, OUTER_VAR, 2, INT8OID, -1, 0, 0).unwrap(),
                2,
                Some("cnt"),
                false,
            )
            .unwrap(),
        )
        .unwrap()
    };
    let mut sort = Node::build::<Sort>(mcx).unwrap();
    sort.plan.targetlist = mk_out_tlist();
    sort.plan.lefttree = Some(agg.seal());
    sort.numCols = 1;
    sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT]).unwrap();
    sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();

    // WindowAgg: partition by g, no ORDER BY, FRAMEOPTION_DEFAULTS.
    let mk_wfunc = |fnoid: u32| {
        let mut w = Node::build::<WindowFunc>(mcx).unwrap();
        w.winfnoid = fnoid;
        w.wintype = INT8OID;
        w.winref = 1;
        w.seal()
    };
    let mut wa_tlist = mk_out_tlist();
    wa_tlist
        .lappend(mcx, Node::mk_target_entry(mcx, mk_wfunc(3100), 3, Some("rn"), false).unwrap())
        .unwrap();
    wa_tlist
        .lappend(mcx, Node::mk_target_entry(mcx, mk_wfunc(3101), 4, Some("rank"), false).unwrap())
        .unwrap();
    let mut wa = Node::build::<WindowAgg>(mcx).unwrap();
    wa.plan.targetlist = wa_tlist;
    wa.plan.lefttree = Some(sort.seal());
    wa.winref = 1;
    wa.partNumCols = 1;
    wa.partColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    wa.partOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    wa.partCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    wa.topWindow = true;

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
    pstmt.planTree = Some(wa.seal());
    pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
    pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

type WinRow = (i32, i32, i64, i64, i64, i64);

fn drain_window_rows<'mcx>(
    ps: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> Vec<WinRow> {
    let mut got = Vec::new();
    loop {
        let Some(slot_id) = exec_proc_node(ps, estate).unwrap() else {
            break;
        };
        let base = estate.slot_mut(slot_id).base();
        assert!(base.tts_isnull.iter().all(|n| !n));
        got.push((
            base.tts_values[0].as_i32(),
            base.tts_values[1].as_i32(),
            base.tts_values[2].as_i64(),
            base.tts_values[3].as_i64(),
            base.tts_values[4].as_i64(),
            base.tts_values[5].as_i64(),
        ));
    }
    got
}

#[test]
fn window_agg_rank_family_and_sum_end_to_end() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70021;
    // (g, a) unsorted on purpose: the Sort below the WindowAgg orders them.
    scanfix::register_table_2col(
        relid,
        &[&[(2, 5), (1, 10), (3, 7), (1, 20)], &[(2, 5), (1, 10), (2, 5)]],
    );
    let pstmt = mk_windowagg_pstmt(mcx, relid, true);
    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));
    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_snapshot = Some(snapshot);
        let desc = crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
        assert_eq!(desc.natts, 6);
        assert_eq!(desc.attr(2).atttypid, INT8OID);

        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        let got = drain_window_rows(ps, estate);
        // Peer groups share rank/sum; rank jumps by peer count, dense by 1.
        let want: Vec<WinRow> = vec![
            (1, 10, 1, 1, 1, 20),
            (1, 10, 2, 1, 1, 20),
            (1, 20, 3, 3, 2, 40),
            (2, 5, 1, 1, 1, 15),
            (2, 5, 2, 1, 1, 15),
            (2, 5, 3, 1, 1, 15),
            (3, 7, 1, 1, 1, 7),
        ];
        assert_eq!(got, want);

        // Rescan replays identically (ExecReScanWindowAgg).
        crate::execami::exec_re_scan(ps, estate).unwrap();
        let again = drain_window_rows(ps, estate);
        assert_eq!(again, want);

        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
    });
    scanfix::quiesced();
}

#[test]
fn window_agg_no_order_by_whole_partition_frame() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70022;
    scanfix::register_table_2col(relid, &[&[(1, 10), (2, 5), (1, 20), (2, 6)]]);
    let pstmt = mk_windowagg_pstmt(mcx, relid, false);
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
        let got = drain_window_rows(ps, estate);
        // No ORDER BY: every partition row is a peer, so rank/dense stay 1
        // and the frame is the whole partition (sum = partition total).
        let want: Vec<WinRow> = vec![
            (1, 10, 1, 1, 1, 30),
            (1, 20, 2, 1, 1, 30),
            (2, 5, 1, 1, 1, 11),
            (2, 6, 2, 1, 1, 11),
        ];
        assert_eq!(got, want);
        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
    });
    scanfix::quiesced();
}

#[test]
fn window_agg_empty_input_end_to_end() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70023;
    scanfix::register_table_2col(relid, &[]);
    let pstmt = mk_windowagg_pstmt(mcx, relid, true);
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
        assert!(exec_proc_node(ps, estate).unwrap().is_none());
        assert!(exec_proc_node(ps, estate).unwrap().is_none());
        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
    });
    scanfix::quiesced();
}

// mk_seqscan_pstmt over a 2-col rel with qual `a = 1` (int4eq).
fn mk_epq_update_subplan_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    relid: u32,
) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan};
    use ::types_nodes::primnodes::OpExpr;
    const INT4OID: u32 = 23;
    const BOOLOID: u32 = 16;
    const F_INT4EQ: u32 = 65;

    let var_a = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let var_b = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
    let tle1 = Node::mk_target_entry(mcx, var_a, 1, Some("a"), false).unwrap();
    let tle2 = Node::mk_target_entry(mcx, var_b, 2, Some("b"), false).unwrap();
    // The junk column forces a projection, like a real UPDATE subplan's ctid.
    let var_j = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle3 = Node::mk_target_entry(mcx, var_j, 3, Some("junk"), true).unwrap();
    let mut tlist = NodeList::make2(mcx, tle1, tle2).unwrap();
    tlist.lappend(mcx, tle3).unwrap();

    let qual_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let qual_const = Node::mk_const(
        mcx, INT4OID, -1, 0, 4, Datum::from_i32(1), false, true,
    )
    .unwrap();
    let args = NodeList::make2(mcx, qual_var, qual_const).unwrap();
    let op = Node::mk(
        mcx,
        OpExpr {
            opno: 96,
            opfuncid: F_INT4EQ,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args,
            location: -1,
        },
    )
    .unwrap();
    let qual = NodeList::make1(mcx, op).unwrap();

    let scan_node = Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan { targetlist: tlist, qual, ..Default::default() },
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
        RTEPermissionInfo { relid, requiredPerms: 1 << 1, ..Default::default() },
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

fn epq_store_test_tuple(
    estate: &mut EStateData<'_>,
    slot: ::executils::ExecSlotId,
    a: i32,
    b: i32,
) {
    let mcx = estate.es_query_cxt;
    let s = estate.slot_mut(slot);
    exectuples::exec_clear_tuple(s, mcx);
    {
        let base = s.base_mut();
        base.tts_values[0] = Datum::from_i32(a);
        base.tts_isnull[0] = false;
        base.tts_values[1] = Datum::from_i32(b);
        base.tts_isnull[1] = false;
    }
    exectuples::exec_store_virtual_tuple(s);
}

fn epq_slot_vals(estate: &mut EStateData<'_>, slot: ::executils::ExecSlotId) -> (i32, i32) {
    let s = estate.slot_mut(slot);
    let mut isnull = false;
    let a = exectuples::slot_getattr(s, 1, &mut isnull).as_i32();
    assert!(!isnull);
    let b = exectuples::slot_getattr(s, 2, &mut isnull).as_i32();
    assert!(!isnull);
    (a, b)
}

// EvalPlanQual over a SeqScan recheck: the test tuple is substituted for the
// scan, the plan qual (a = 1) decides proceed/skip, and the second call
// exercises EvalPlanQualBegin's reset+rescan arm.
#[test]
fn eval_plan_qual_recheck_over_seqscan() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();

    let relid: u32 = 70021;
    scanfix::register_table_2col(relid, &[&[(1, 10), (2, 20)]]);
    let pstmt = mk_epq_update_subplan_pstmt(mcx, relid);

    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));

    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_snapshot = Some(snapshot);
        crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
        let ExecData { estate, planstate } = data;

        let mut epq = crate::epq::EpqState {
            plan: pstmt.planTree,
            recheck: None,
            result_rti: 1,
            lane_verdicts: None,
        };
        let mut subs = None;
        ::executils::ensure_epq_subs(&mut subs, estate.es_query_cxt, estate.epq_rtsize(), 1);
        let desc = estate.es_relations[0].as_ref().unwrap().rd_att.clone();
        let test = estate.exec_init_extra_tuple_slot(
            Some(desc),
            ::types_slot::TupleSlotKind::Virtual,
        );
        subs.as_mut().unwrap().relsubs_slot[0] = Some(test);

        // Latest version still matches the qual: proceed with (1, 99).
        epq_store_test_tuple(estate, test, 1, 99);
        let got = crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test).unwrap();
        let got = got.expect("qual passes; EPQ returns the candidate tuple");
        assert_ne!(got, test, "projection result, not the test slot");
        assert_eq!(epq_slot_vals(estate, got), (1, 99));
        assert!(estate.slot(test).base().is_empty(), "test slot cleared after EPQ");
        assert!(!estate.es_epq_active, "flag dropped outside the recheck run");

        // Reset path: latest version no longer matches -> skip.
        epq_store_test_tuple(estate, test, 2, 99);
        assert!(crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
            .unwrap()
            .is_none());

        // And matches again on a third round.
        epq_store_test_tuple(estate, test, 1, 5);
        let got = crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test).unwrap();
        assert_eq!(epq_slot_vals(estate, got.expect("passes")), (1, 5));

        crate::epq::eval_plan_qual_end(&mut epq, &mut subs, estate).unwrap();
        assert!(epq.recheck.is_none());

        let ps = planstate.as_mut().unwrap();
        crate::exec_end_node(ps, estate).unwrap();
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
    });
    scanfix::quiesced();
}

// Correlated-MULTIEXPR fixture: the top Result projects [$1, junk SubPlan],
// where the SubPlan (parParam [$0] fed by Const 42, setParam [$1]) runs the
// subplans[0] Result whose single column reads $0 back. C shape of
// `UPDATE t SET (a) = (SELECT ... correlated)`'s child projection.
fn mk_multiexpr_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    sub_qual: Option<Node<'mcx>>,
) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::list::{IntList, OidList};
    use ::types_nodes::primnodes::{Param, ParamKind, SubLinkType, SubPlan};

    let mk_exec_param = |paramid: i32| {
        Node::mk(
            mcx,
            Param {
                paramkind: ParamKind::PARAM_EXEC,
                paramid,
                paramtype: INT4OID,
                paramtypmod: -1,
                paramcollid: 0,
                location: -1,
            },
        )
        .unwrap()
    };

    let sub_tle = Node::mk_target_entry(mcx, mk_exec_param(0), 1, None, false).unwrap();
    let mut sub_result = Node::build::<ResultPlan>(mcx).unwrap();
    sub_result.plan.targetlist = NodeList::make1(mcx, sub_tle).unwrap();
    sub_result.resconstantqual = sub_qual;
    let sub_plan_tree = sub_result.seal();

    let subplan_expr = Node::mk(
        mcx,
        SubPlan {
            subLinkType: SubLinkType::MULTIEXPR_SUBLINK,
            testexpr: None,
            paramIds: IntList::nil(),
            plan_id: 1,
            plan_name: Some("SubPlan 1"),
            firstColType: INT4OID,
            firstColTypmod: -1,
            firstColCollation: 0,
            useHashTable: false,
            unknownEqFalse: false,
            parallel_safe: false,
            setParam: IntList::make1(mcx, 1).unwrap(),
            parParam: IntList::make1(mcx, 0).unwrap(),
            args: NodeList::make1(mcx, mk_int4_const(mcx, 42)).unwrap(),
            startup_cost: 0.0,
            per_call_cost: 0.0,
        },
    )
    .unwrap();

    let tle1 = Node::mk_target_entry(mcx, mk_exec_param(1), 1, Some("a"), false).unwrap();
    let tle2 = Node::mk_target_entry(mcx, subplan_expr, 2, None, true).unwrap();
    let mut top = Node::build::<ResultPlan>(mcx).unwrap();
    top.plan.targetlist = NodeList::make2(mcx, tle1, tle2).unwrap();
    let plan_node = top.seal();

    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(plan_node);
    pstmt.subplans = ::types_nodes::list::OptNodeList::make1(mcx, Some(sub_plan_tree)).unwrap();
    pstmt.paramExecTypes = OidList::make2(mcx, INT4OID, INT4OID).unwrap();
    pstmt.seal_ref()
}

fn run_multiexpr_case(pstmt: &'static PlannedStmt<'static>, expect: Option<i32>) {
    use ::types_portal::params::ParamExecData;
    with_exec_data(pstmt, |data, pstmt| {
        // standard_executor_start's param sizing, replayed for init_plan.
        let n = pstmt.paramExecTypes.len();
        data.estate
            .es_param_exec_vals
            .extend(core::iter::repeat_n(ParamExecData::EMPTY, n));
        data.estate
            .es_param_subplans
            .extend(core::iter::repeat_n(None, n));
        crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
        let mut ps = data.planstate.take().unwrap();
        let slot_id = exec_proc_node(&mut ps, &mut data.estate).unwrap().unwrap();
        {
            let base = data.estate.slot(slot_id).base();
            match expect {
                Some(v) => {
                    assert!(!base.tts_isnull[0]);
                    assert_eq!(base.tts_values[0], Datum::from_i32(v));
                }
                None => assert!(base.tts_isnull[0], "empty subplan sets the param to NULL"),
            }
            assert!(base.tts_isnull[1], "MULTIEXPR SubPlan column is a dummy NULL");
        }
        assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_none());
        data.planstate = Some(ps);
    });
}

#[test]
fn correlated_multiexpr_subplan_fills_set_params() {
    install_seams();
    let mcx = leaked_mcx();
    run_multiexpr_case(mk_multiexpr_pstmt(mcx, None), Some(42));
}

#[test]
fn correlated_multiexpr_empty_subplan_sets_params_null() {
    install_seams();
    let mcx = leaked_mcx();
    let qual = Node::mk_list(mcx, NodeList::make1(mcx, mk_bool_const(mcx, false)).unwrap())
        .unwrap();
    run_multiexpr_case(mk_multiexpr_pstmt(mcx, Some(qual)), None);
}

// Correlated EXPR SubPlan whose subplan body carries its own correlated
// initplan — the C plan shape of `select f1, (select distinct min(t1.f1)
// from int4_tbl t1 where t1.f1 = t0.f1) from int4_tbl t0` (planagg turns the
// min into an InitPlan inside the SubPlan; distilled here to the Result that
// projects the initplan's output param). Every outer row's rescan must
// propagate chgParam into the initplan (C ExecReScan's initPlan walk +
// ExecSetParamPlan's first-ExecProcNode rescan); without it the initplan's
// scan stays exhausted and rows after the first read a stale NULL param.
fn mk_correlated_initplan_in_subplan_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    t0: u32,
    t1: u32,
) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::list::{IntList, OidList};
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan};
    use ::types_nodes::primnodes::{Param, ParamKind, SubLinkType, SubPlan};

    let mk_exec_param = |paramid: i32| {
        Node::mk(
            mcx,
            Param {
                paramkind: ParamKind::PARAM_EXEC,
                paramid,
                paramtype: INT4OID,
                paramtypmod: -1,
                paramcollid: 0,
                location: -1,
            },
        )
        .unwrap()
    };
    let mut ext0 = Bitmapset::empty();
    ext0.add_member(mcx, 0).unwrap();

    // subplans[0] (InitPlan 1): Limit 1 -> SeqScan t1 (qual f1 = $0), the
    // planagg minmax shape. The Limit matters: heapam auto-rewinds a spent
    // scan (rs_inited resets at EOF) but LimitState's position does not, so
    // a missed initplan rescan replays doneness (zero rows -> NULL param),
    // not the new param. The planner emits nested initplans before the
    // bodies that reference them, so plan_id order is initplan first
    // (InitPlan's init loop fills es_subplanstates in that order).
    let inner_var = Node::mk_var(mcx, 2, 1, INT4OID, -1, 0, 0).unwrap();
    let inner_tle = Node::mk_target_entry(mcx, inner_var, 1, Some("f1"), false).unwrap();
    let qual_var = Node::mk_var(mcx, 2, 1, INT4OID, -1, 0, 0).unwrap();
    let qual = Node::mk(
        mcx,
        ::types_nodes::OpExpr {
            opno: INT4_EQ,
            opfuncid: 65,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, qual_var, mk_exec_param(0)).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let init_scan = Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan {
                    targetlist: NodeList::make1(mcx, inner_tle).unwrap(),
                    qual: NodeList::make1(mcx, qual).unwrap(),
                    extParam: ext0.clone_in(mcx).unwrap(),
                    allParam: ext0.clone_in(mcx).unwrap(),
                    ..Default::default()
                },
                scanrelid: 2,
            },
        },
    )
    .unwrap();
    let limit_var =
        Node::mk_var(mcx, ::types_nodes::primnodes::OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let limit_tle = Node::mk_target_entry(mcx, limit_var, 1, Some("f1"), false).unwrap();
    let mut init_limit = Node::build::<::types_nodes::plannodes::Limit>(mcx).unwrap();
    init_limit.plan.targetlist = NodeList::make1(mcx, limit_tle).unwrap();
    init_limit.plan.lefttree = Some(init_scan);
    init_limit.plan.extParam = ext0.clone_in(mcx).unwrap();
    init_limit.plan.allParam = ext0.clone_in(mcx).unwrap();
    init_limit.limitCount =
        Some(Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::from_i64(1), false, true).unwrap());
    let init_top = init_limit.seal();

    // subplans[1] (SubPlan 2's body): Result projecting $1, initPlan carrying
    // plan_id 1 with setParam [$1].
    let initplan_ref = Node::mk(
        mcx,
        SubPlan {
            subLinkType: SubLinkType::EXPR_SUBLINK,
            plan_id: 1,
            plan_name: Some("InitPlan 1"),
            firstColType: INT4OID,
            firstColTypmod: -1,
            setParam: IntList::make1(mcx, 1).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let body_tle = Node::mk_target_entry(mcx, mk_exec_param(1), 1, Some("min"), false).unwrap();
    let mut all01 = ext0.clone_in(mcx).unwrap();
    all01.add_member(mcx, 1).unwrap();
    let mut body = Node::build::<ResultPlan>(mcx).unwrap();
    body.plan.targetlist = NodeList::make1(mcx, body_tle).unwrap();
    body.plan.initPlan = NodeList::make1(mcx, initplan_ref).unwrap();
    body.plan.extParam = ext0.clone_in(mcx).unwrap();
    body.plan.allParam = all01;
    let body = body.seal();

    // Outer: SeqScan t0 projecting [f1, SubPlan 2(parParam [$0] <- t0.f1)].
    let subplan_expr = Node::mk(
        mcx,
        SubPlan {
            subLinkType: SubLinkType::EXPR_SUBLINK,
            plan_id: 2,
            plan_name: Some("SubPlan 2"),
            firstColType: INT4OID,
            firstColTypmod: -1,
            parParam: IntList::make1(mcx, 0).unwrap(),
            args: NodeList::make1(mcx, Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap())
                .unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let out_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let out_tle1 = Node::mk_target_entry(mcx, out_var, 1, Some("f1"), false).unwrap();
    let out_tle2 = Node::mk_target_entry(mcx, subplan_expr, 2, Some("min"), false).unwrap();
    let outer = Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan {
                    targetlist: NodeList::make2(mcx, out_tle1, out_tle2).unwrap(),
                    ..Default::default()
                },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    let (rtable, perms, unpruned) = mk_two_rel_pstmt_parts(mcx, t0, t1);
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(outer);
    pstmt.subplans =
        ::types_nodes::list::OptNodeList::make2(mcx, Some(init_top), Some(body)).unwrap();
    pstmt.paramExecTypes = OidList::make2(mcx, INT4OID, INT4OID).unwrap();
    pstmt.rtable = rtable;
    pstmt.permInfos = perms;
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

fn run_two_col_pstmt(
    pstmt: &'static PlannedStmt<'static>,
) -> Vec<(i32, Option<i32>)> {
    let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
    let snapshot: snapmgr::Snapshot = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        snap_ctx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    ));
    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_snapshot = Some(snapshot);
        {
            let n = pstmt.paramExecTypes.len();
            let es = &mut data.estate;
            es.es_param_exec_vals.extend(core::iter::repeat_n(
                ::types_portal::params::ParamExecData::EMPTY,
                n,
            ));
            es.es_param_subplans.extend(core::iter::repeat_n(None, n));
        }
        crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();

        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        let mut out = Vec::new();
        while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            let second =
                if base.tts_isnull[1] { None } else { Some(base.tts_values[1].as_i32()) };
            out.push((base.tts_values[0].as_i32(), second));
        }
        crate::exec_end_node(ps, estate).unwrap();
        for i in 0..estate.es_subplanstates.len() {
            let cell = estate.es_subplanstates[i];
            // SAFETY: init_plan's arena cell (standard_executor_end's shape).
            let slot = unsafe {
                &mut *cell.0.cast::<Option<crate::PlanStateNode<'_>>>().as_ptr()
            };
            if let Some(mut sub) = slot.take() {
                crate::exec_end_node(&mut sub, estate).unwrap();
            }
        }
        estate.exec_reset_tuple_table(false);
        estate.exec_close_range_table_relations().unwrap();
        out
    })
}

#[test]
fn correlated_subplan_reruns_nested_initplan_per_outer_row() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let (t0, t1) = (70140u32, 70141u32);
    scanfix::register_table(t0, &[&[10, 20, 30]]);
    scanfix::register_table(t1, &[&[10, 10, 20, 20, 30, 30]]);
    let rows = run_two_col_pstmt(mk_correlated_initplan_in_subplan_pstmt(mcx, t0, t1));
    assert_eq!(rows, vec![(10, Some(10)), (20, Some(20)), (30, Some(30))]);
    scanfix::quiesced();
}

#[test]
fn correlated_subplan_nested_initplan_null_and_refill_transitions() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let (t0, t1) = (70142u32, 70143u32);
    scanfix::register_table(t0, &[&[10, 20, 30]]);
    scanfix::register_table(t1, &[&[20, 20]]);
    let rows = run_two_col_pstmt(mk_correlated_initplan_in_subplan_pstmt(mcx, t0, t1));
    assert_eq!(rows, vec![(10, None), (20, Some(20)), (30, None)]);
    scanfix::quiesced();
}

// =============================================================================
// Row-mode facility A/B corpus (lanev2/rowmode.rs, PGRUST_LANE_V2_ROWMODE):
// the ProjectSet <- childless Result shape (SELECT generate_series(...)
// no-FROM) driven knob OFF (the unchanged exec_project_set body) vs knob ON
// (the row-mode ResultRowSource -> ProjectSetOp pipeline) in one process —
// same rows, same NULL padding, same errors at the same row, same rescan
// behavior. Plus the childless-Result seam corpus (lane_result_childless_next
// is the select1 hot path's moved body). Two-server byte-compare over psql
// output = scripts/lane-rowmode-e2e.sh.
// =============================================================================
mod rowmode_ab {
    use std::sync::Mutex;

    use super::*;
    use ::types_nodes::plannodes::ProjectSet as ProjectSetPlan;
    use ::types_nodes::primnodes::FuncExpr;

    const F_GENERATE_SERIES_INT4: u32 = 1067;
    const F_GENERATE_SERIES_STEP_INT4: u32 = 1066;

    /// The knob is process-global; every test that flips it holds this lock
    /// and restores OFF (the default) before releasing. `pub(super)` so the
    /// mergejoin row-mode corpus (same knob) serializes against the same
    /// lock — two locks over one global knob would race in parallel runs.
    pub(super) static KNOB: Mutex<()> = Mutex::new(());

    /// `pub(super)` because seams are SET-ONCE process globals
    /// (seam_core's "installed twice" panic): the windows_t2_ab corpus
    /// shares this one `lookup_pg_proc_shape` install (its rows are a
    /// superset — generate_series for the SRF corpus, sum(int4)+int4lt for
    /// the moving-frame volatility probe), the same sharing discipline as
    /// the KNOB lock above.
    pub(super) fn install_rowmode_seams() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            // pg_proc rows: generate_series(int4,int4[,int4]) — the two
            // canonical fmgr builtins the SRF corpus invokes — plus
            // sum(int4) 2108 and int4lt 66 for the windows_t2_ab corpus
            // (initialize_peragg's use_ma_code gate runs
            // contain_volatile_functions over the WindowFunc; values from
            // PostgreSQL 18.3 pg_proc).
            syscache_seams::lookup_pg_proc_shape::set(|funcid| {
                Ok(match funcid {
                    F_GENERATE_SERIES_INT4 | F_GENERATE_SERIES_STEP_INT4 => {
                        Some(syscache_seams::PgProcShape {
                            pronamespace: 11,
                            prorettype: INT4OID,
                            provariadic: 0,
                            prosupport: 0,
                            prolang: 12,
                            pronargs: if funcid == F_GENERATE_SERIES_INT4 { 2 } else { 3 },
                            prokind: b'f' as i8,
                            provolatile: b'i' as i8,
                            proparallel: b's' as i8,
                            proretset: true,
                            proisstrict: true,
                            proleakproof: false,
                            prosecdef: false,
                            proconfig_isnull: true,
                        })
                    }
                    // sum(int4) — windows_t2_ab.
                    2108 => Some(syscache_seams::PgProcShape {
                        pronamespace: 11,
                        prorettype: INT8OID,
                        provariadic: 0,
                        prosupport: 0,
                        prolang: 12,
                        pronargs: 1,
                        prokind: b'a' as i8,
                        provolatile: b'i' as i8,
                        proparallel: b's' as i8,
                        proretset: false,
                        proisstrict: false,
                        proleakproof: false,
                        prosecdef: false,
                        proconfig_isnull: true,
                    }),
                    // count(*) — windows_t2b_ab moving count(*) units
                    // (WS-R wave-3, the same set-once superset discipline
                    // as the 2108 row above; the use_ma_code volatility
                    // probe walks the WindowFunc). PostgreSQL 18.3 pg_proc.
                    2803 => Some(syscache_seams::PgProcShape {
                        pronamespace: 11,
                        prorettype: INT8OID,
                        provariadic: 0,
                        prosupport: 0,
                        prolang: 12,
                        pronargs: 0,
                        prokind: b'a' as i8,
                        provolatile: b'i' as i8,
                        proparallel: b's' as i8,
                        proretset: false,
                        proisstrict: false,
                        proleakproof: false,
                        prosecdef: false,
                        proconfig_isnull: true,
                    }),
                    // int4lt — windows_t2_ab FILTER exprs.
                    66 => Some(syscache_seams::PgProcShape {
                        pronamespace: 11,
                        prorettype: BOOLOID,
                        provariadic: 0,
                        prosupport: 0,
                        prolang: 12,
                        pronargs: 2,
                        prokind: b'f' as i8,
                        provolatile: b'i' as i8,
                        proparallel: b's' as i8,
                        proretset: false,
                        proisstrict: true,
                        proleakproof: true,
                        prosecdef: false,
                        proconfig_isnull: true,
                    }),
                    _ => None,
                })
            });
            // get_type_func_class(INT4) -> Scalar for the SRF result type.
            syscache_seams::pg_type_typtype::set(|typid| {
                Ok((typid == INT4OID).then_some(b'b' as i8))
            });
        });
    }

    fn mk_null_int4_const(mcx: ::mcx::Mcx<'_>) -> Node<'_> {
        Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::null(), true, true).unwrap()
    }

    fn mk_srf<'mcx>(mcx: ::mcx::Mcx<'mcx>, funcid: u32, args: NodeList<'mcx>) -> Node<'mcx> {
        let mut fe = Node::build::<FuncExpr>(mcx).unwrap();
        fe.funcid = funcid;
        fe.funcresulttype = INT4OID;
        fe.funcretset = true;
        fe.args = args;
        fe.seal()
    }

    fn mk_gs<'mcx>(mcx: ::mcx::Mcx<'mcx>, lo: i32, hi: i32) -> Node<'mcx> {
        mk_srf(
            mcx,
            F_GENERATE_SERIES_INT4,
            NodeList::make2(mcx, mk_int4_const(mcx, lo), mk_int4_const(mcx, hi)).unwrap(),
        )
    }

    /// `SELECT <exprs...>` (no FROM, SRF in tlist): ProjectSet over the
    /// childless Result — exactly the plan shape the planner emits and the
    /// only one increment-1 admits.
    fn mk_ps_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        exprs: &[Node<'mcx>],
    ) -> &'mcx PlannedStmt<'mcx> {
        let rtle =
            Node::mk_target_entry(mcx, mk_int4_const(mcx, 1), 1, Some("?column?"), false)
                .unwrap();
        let mut result = Node::build::<ResultPlan>(mcx).unwrap();
        result.plan.targetlist = NodeList::make1(mcx, rtle).unwrap();

        let mut tles: Vec<Node<'mcx>> = Vec::new();
        for (i, e) in exprs.iter().enumerate() {
            tles.push(
                Node::mk_target_entry(mcx, *e, (i + 1) as i16, Some("?column?"), false)
                    .unwrap(),
            );
        }
        let mut tlist = NodeList::make1(mcx, tles[0]).unwrap();
        for tle in &tles[1..] {
            tlist.lappend(mcx, *tle).unwrap();
        }
        let mut pset = Node::build::<ProjectSetPlan>(mcx).unwrap();
        pset.plan.targetlist = tlist;
        pset.plan.lefttree = Some(result.seal());
        let plan_node = pset.seal();

        let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
        pstmt.commandType = CmdType::CMD_SELECT;
        pstmt.canSetTag = true;
        pstmt.planTree = Some(plan_node);
        pstmt.seal_ref()
    }

    /// Drive the plan to completion (or error), collecting per-row datum
    /// columns; `rescan_after` re-scans once after that many fetched rows
    /// and keeps collecting (the mid-expansion rescan probe).
    fn run_ps(
        pstmt: &'static PlannedStmt<'static>,
        ncols: usize,
        rescan_after: Option<usize>,
    ) -> (Vec<Vec<Option<Datum>>>, Option<String>) {
        with_exec_data(pstmt, |data, pstmt| {
            let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
                .unwrap()
                .unwrap();
            let mut rows: Vec<Vec<Option<Datum>>> = Vec::new();
            let mut rescan = rescan_after;
            loop {
                if rescan == Some(rows.len()) {
                    rescan = None;
                    exec_re_scan(&mut ps, &mut data.estate).unwrap();
                }
                match exec_proc_node(&mut ps, &mut data.estate) {
                    Ok(Some(slot_id)) => {
                        let base = data.estate.slot(slot_id).base();
                        let row = (0..ncols)
                            .map(|c| (!base.tts_isnull[c]).then(|| base.tts_values[c]))
                            .collect();
                        rows.push(row);
                    }
                    Ok(None) => return (rows, None),
                    Err(e) => return (rows, Some(e.to_string())),
                }
            }
        })
    }

    fn d(v: i32) -> Option<Datum> {
        Some(Datum::from_i32(v))
    }

    /// One A/B round: build the SAME plan twice (fresh state per arm), run
    /// knob OFF then knob ON, demand identical (rows, error) — and that the
    /// ON arm actually engaged the row-mode drive.
    fn ab(
        mk: impl Fn(::mcx::Mcx<'static>) -> &'static PlannedStmt<'static>,
        ncols: usize,
        rescan_after: Option<usize>,
    ) -> (Vec<Vec<Option<Datum>>>, Option<String>) {
        install_seams();
        install_rowmode_seams();
        let guard = KNOB.lock().unwrap_or_else(|p| p.into_inner());
        crate::lanev2::rowmode_set_for_tests(false);
        let off = run_ps(mk(leaked_mcx()), ncols, rescan_after);
        crate::lanev2::rowmode_set_for_tests(true);
        let owned_before =
            crate::lanev2::ROWMODE_OWNED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed);
        let on = run_ps(mk(leaked_mcx()), ncols, rescan_after);
        let owned_after =
            crate::lanev2::ROWMODE_OWNED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed);
        crate::lanev2::rowmode_set_for_tests(false);
        drop(guard);
        assert_eq!(off, on, "knob OFF vs ON must be identical");
        assert!(owned_after > owned_before, "ON arm never engaged the row-mode drive");
        off
    }

    #[test]
    fn ab_generate_series_basic() {
        let (rows, err) = ab(|mcx| mk_ps_pstmt(mcx, &[mk_gs(mcx, 1, 3)]), 1, None);
        assert_eq!(err, None);
        assert_eq!(rows, vec![vec![d(1)], vec![d(2)], vec![d(3)]]);
    }

    #[test]
    fn ab_generate_series_empty_set() {
        let (rows, err) = ab(|mcx| mk_ps_pstmt(mcx, &[mk_gs(mcx, 1, 0)]), 1, None);
        assert_eq!(err, None);
        assert!(rows.is_empty());
    }

    #[test]
    fn ab_two_srfs_null_padding() {
        // Different lengths: the exhausted SRF pads with NULLs (C's
        // ExecProjectSRF continuing arm).
        let (rows, err) =
            ab(|mcx| mk_ps_pstmt(mcx, &[mk_gs(mcx, 1, 3), mk_gs(mcx, 10, 11)]), 2, None);
        assert_eq!(err, None);
        assert_eq!(
            rows,
            vec![vec![d(1), d(10)], vec![d(2), d(11)], vec![d(3), None]]
        );
    }

    #[test]
    fn ab_scalar_and_srf_mix() {
        let (rows, err) = ab(
            |mcx| mk_ps_pstmt(mcx, &[mk_int4_const(mcx, 42), mk_gs(mcx, 1, 2)]),
            2,
            None,
        );
        assert_eq!(err, None);
        assert_eq!(rows, vec![vec![d(42), d(1)], vec![d(42), d(2)]]);
    }

    #[test]
    fn ab_strict_null_arg_empty_set() {
        // generate_series is strict: a NULL arg yields the empty set (the
        // ExecMakeFunctionResultSet strict arm), not a NULL row.
        let (rows, err) = ab(
            |mcx| {
                let args =
                    NodeList::make2(mcx, mk_null_int4_const(mcx), mk_int4_const(mcx, 3))
                        .unwrap();
                mk_ps_pstmt(mcx, &[mk_srf(mcx, F_GENERATE_SERIES_INT4, args)])
            },
            1,
            None,
        );
        assert_eq!(err, None);
        assert!(rows.is_empty());
    }

    #[test]
    fn ab_erroring_srf_same_error_same_row() {
        // step=0 raises "step size cannot equal zero" on the first
        // expansion call — zero rows out, identical error, both arms.
        let (rows, err) = ab(
            |mcx| {
                let args = NodeList::make3(
                    mcx,
                    mk_int4_const(mcx, 1),
                    mk_int4_const(mcx, 5),
                    mk_int4_const(mcx, 0),
                )
                .unwrap();
                mk_ps_pstmt(mcx, &[mk_srf(mcx, F_GENERATE_SERIES_STEP_INT4, args)])
            },
            1,
            None,
        );
        assert!(rows.is_empty());
        assert!(
            err.as_deref().is_some_and(|e| e.contains("step size cannot equal zero")),
            "expected the zero-step SRF error, got {err:?}"
        );
    }

    #[test]
    fn ab_rescan_mid_expansion() {
        // Rescan after one emitted row of a 3-row expansion: the SRF state
        // (pending_srf_tuples, args_valid, fn_extra, elemdone) resets and
        // the stream restarts from 1 — identical in both drives.
        let (rows, err) = ab(|mcx| mk_ps_pstmt(mcx, &[mk_gs(mcx, 1, 3)]), 1, Some(1));
        assert_eq!(err, None);
        assert_eq!(
            rows,
            vec![vec![d(1)], vec![d(1)], vec![d(2)], vec![d(3)]]
        );
    }

    #[test]
    fn ab_early_stop_mid_expansion_teardown() {
        // The LIMIT shape: stop pulling mid-expansion and tear down — no
        // panic, no error, both arms (SRF cross-call state is node-owned;
        // nothing in the lane needs unwinding).
        install_seams();
        install_rowmode_seams();
        let guard = KNOB.lock().unwrap_or_else(|p| p.into_inner());
        for on in [false, true] {
            crate::lanev2::rowmode_set_for_tests(on);
            let pstmt = mk_ps_pstmt(leaked_mcx(), &[mk_gs(leaked_mcx(), 1, 1000)]);
            with_exec_data(pstmt, |data, pstmt| {
                let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
                    .unwrap()
                    .unwrap();
                let slot = exec_proc_node(&mut ps, &mut data.estate).unwrap().unwrap();
                assert_eq!(data.estate.slot(slot).base().tts_values[0], Datum::from_i32(1));
                // Walk away mid-expansion (pending_srf_tuples = true).
            });
        }
        crate::lanev2::rowmode_set_for_tests(false);
        drop(guard);
    }

    /// The childless-Result seam corpus (lane_result_childless_next): the
    /// rowmode knob does not gate Result, so this pins the CODE MOVE — the
    /// moved body must behave exactly as the inline arm did on the shapes
    /// the select1 hot path exercises (one row, drained tail, one-time
    /// filter, rescan) with the knob in BOTH positions (see express_ab
    /// below for the WS-J point-path A/B corpus).
    #[test]
    fn childless_result_seam_knob_positions() {
        install_seams();
        let guard = KNOB.lock().unwrap_or_else(|p| p.into_inner());
        for on in [false, true] {
            crate::lanev2::rowmode_set_for_tests(on);
            let mcx = leaked_mcx();
            // SELECT 1: one row, value 1, drained, rescan re-emits.
            let pstmt = mk_select1_pstmt(mcx, None);
            with_exec_data(pstmt, |data, pstmt| {
                let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
                    .unwrap()
                    .unwrap();
                let slot_id = exec_proc_node(&mut ps, &mut data.estate).unwrap().unwrap();
                assert_eq!(data.estate.slot(slot_id).base().tts_values[0], Datum::from_i32(1));
                assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_none());
                assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_none());
                exec_re_scan(&mut ps, &mut data.estate).unwrap();
                assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_some());
            });
            // One-time filter false: zero rows, stays drained.
            let qual =
                Node::mk_list(mcx, NodeList::make1(mcx, mk_bool_const(mcx, false)).unwrap())
                    .unwrap();
            let pstmt = mk_select1_pstmt(mcx, Some(qual));
            with_exec_data(pstmt, |data, pstmt| {
                let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
                    .unwrap()
                    .unwrap();
                assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_none());
                assert!(exec_proc_node(&mut ps, &mut data.estate).unwrap().is_none());
            });
            // One-time filter true: exactly one row.
            let qual =
                Node::mk_list(mcx, NodeList::make1(mcx, mk_bool_const(mcx, true)).unwrap())
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
        crate::lanev2::rowmode_set_for_tests(false);
        drop(guard);
    }
}


// =============================================================================
// WindowAgg lane A/B corpus (lanev2/windows.rs, PGRUST_LANE_V2_WINDOWS):
// the W1 shape — WindowAgg(FRAMEOPTION_DEFAULTS, row_number/rank/dense_rank/
// sum) over Sort over SeqScan — driven knob OFF (the unchanged
// exec_window_agg pull loop) vs knob ON (the lane's group-at-a-time machine
// over the sort breaker) in one process: same rows, same rescan behavior,
// and the ON arm must actually engage (sticky drive). The classic
// wrong-results traps are pinned: peer rows (ties) under the default frame
// step running aggregates by PEER GROUP (every tie member sees the whole
// group's sum), rank jumps by peer-group size while dense_rank steps by 1.
// NULL-key ordering and spill-sized partitions ride the dualexec corpus
// (scripts/dualexec/corpus-windows.sql) — the scanfix fixture is
// non-null int4 only. Two-server byte-compare = scripts/lane-windows-e2e.sh.
//
// Locking: the scanfix TEST_LOCK serializes every scanfix-fixture test, and
// the knob flips happen strictly inside it — no other test can ever observe
// the process-global knob ON.
// =============================================================================
mod windows_ab {
    use super::*;

    fn run_window(pstmt: &'static PlannedStmt<'static>, rescan: bool) -> Vec<Vec<WinRow>> {
        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            let mut runs = vec![drain_window_rows(ps, estate)];
            if rescan {
                crate::execami::exec_re_scan(ps, estate).unwrap();
                runs.push(drain_window_rows(ps, estate));
            }
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
            runs
        })
    }

    /// One A/B round over an already-registered relid: knob OFF then knob ON
    /// (fresh plan state per arm), demand identical runs. `expect_engaged`
    /// additionally demands the ON arm drove the lane (the sticky drive) —
    /// refusal shapes pass `false` and demand it did NOT. Caller holds the
    /// scanfix TEST_LOCK.
    fn ab(
        relid: u32,
        with_order_by: bool,
        rescan: bool,
        expect_engaged: bool,
    ) -> Vec<Vec<WinRow>> {
        ab_mk(|mcx| mk_windowagg_pstmt(mcx, relid, with_order_by), rescan, expect_engaged)
    }

    fn ab_mk(
        mk: impl Fn(::mcx::Mcx<'static>) -> &'static PlannedStmt<'static>,
        rescan: bool,
        expect_engaged: bool,
    ) -> Vec<Vec<WinRow>> {
        crate::lanev2::windows_set_for_tests(false);
        let off = run_window(mk(leaked_mcx()), rescan);
        crate::lanev2::windows_set_for_tests(true);
        let owned_before =
            crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed);
        let on = run_window(mk(leaked_mcx()), rescan);
        let owned_after =
            crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed);
        crate::lanev2::windows_set_for_tests(false);
        assert_eq!(off, on, "knob OFF vs ON must be identical");
        if expect_engaged {
            assert!(owned_after > owned_before, "ON arm never engaged the windows lane");
        } else {
            assert_eq!(owned_after, owned_before, "refusal shape engaged the windows lane");
        }
        off
    }

    /// Ties + rank gaps + peer-group aggregate stepping, multi-partition,
    /// including a whole-partition tie (g=2) and a single-row partition
    /// (g=3). The peer-group trap: rows (1,10),(1,10) both see sum=20 (the
    /// whole peer group), then (1,20) sees rank 3 (gap) / dense 2 (no gap).
    #[test]
    fn windows_ab_rank_family_and_sum_ties() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70140;
        scanfix::register_table_2col(
            relid,
            &[&[(2, 5), (1, 10), (3, 7), (1, 20)], &[(2, 5), (1, 10), (2, 5)]],
        );
        let runs = ab(relid, true, false, true);
        let want: Vec<WinRow> = vec![
            (1, 10, 1, 1, 1, 20),
            (1, 10, 2, 1, 1, 20),
            (1, 20, 3, 3, 2, 40),
            (2, 5, 1, 1, 1, 15),
            (2, 5, 2, 1, 1, 15),
            (2, 5, 3, 1, 1, 15),
            (3, 7, 1, 1, 1, 7),
        ];
        assert_eq!(runs, vec![want]);
        scanfix::quiesced();
    }

    /// No ORDER BY: one peer group per partition — rank/dense stay 1 and the
    /// default frame is the whole partition.
    #[test]
    fn windows_ab_no_order_by_whole_partition() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70141;
        scanfix::register_table_2col(relid, &[&[(1, 10), (2, 5), (1, 20), (2, 6)]]);
        let runs = ab(relid, false, false, true);
        let want: Vec<WinRow> = vec![
            (1, 10, 1, 1, 1, 30),
            (1, 20, 2, 1, 1, 30),
            (2, 5, 1, 1, 1, 11),
            (2, 6, 2, 1, 1, 11),
        ];
        assert_eq!(runs, vec![want]);
        scanfix::quiesced();
    }

    /// Rescan replays identically under the sticky drive (the drive is reset
    /// by ExecReScanWindowAgg's lane hook, the sort re-feeds, ownership
    /// persists per-(re)scan-life).
    #[test]
    fn windows_ab_rescan_replays() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70142;
        scanfix::register_table_2col(relid, &[&[(2, 1), (1, 3), (1, 3), (2, 2)]]);
        let runs = ab(relid, true, true, true);
        assert_eq!(runs[0], runs[1], "rescan must replay the first run exactly");
        scanfix::quiesced();
    }

    /// Empty input: zero rows, node drains, no hoist, no panic — both arms.
    #[test]
    fn windows_ab_empty_input() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70143;
        scanfix::register_table_2col(relid, &[]);
        let runs = ab(relid, true, false, true);
        assert_eq!(runs, vec![Vec::<WinRow>::new()]);
        scanfix::quiesced();
    }

    /// Every row its own partition: partition-boundary parking on every
    /// accept (the boundary_saved lane path), single-row groups.
    #[test]
    fn windows_ab_single_row_partitions() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70144;
        scanfix::register_table_2col(relid, &[&[(3, 1), (1, 2), (2, 3)]]);
        let runs = ab(relid, true, false, true);
        let want: Vec<WinRow> = vec![
            (1, 2, 1, 1, 1, 2),
            (2, 3, 1, 1, 1, 3),
            (3, 1, 1, 1, 1, 1),
        ];
        assert_eq!(runs, vec![want]);
        scanfix::quiesced();
    }

    /// REFUSAL SHAPE: a non-default frame (ROWS UNBOUNDED PRECEDING..CURRENT
    /// ROW) — the framed row engine owns it in both arms (note the sum now
    /// steps per ROW through the tie, not per peer group), and the lane must
    /// not engage (ShapeQualProj).
    #[test]
    fn windows_ab_framed_refuses() {
        use ::types_nodes::rawnodes::{
            FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_NONDEFAULT, FRAMEOPTION_ROWS,
            FRAMEOPTION_START_UNBOUNDED_PRECEDING,
        };
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70145;
        scanfix::register_table_2col(relid, &[&[(1, 10), (1, 10), (1, 20)]]);
        let fo = FRAMEOPTION_NONDEFAULT
            | FRAMEOPTION_ROWS
            | FRAMEOPTION_START_UNBOUNDED_PRECEDING
            | FRAMEOPTION_END_CURRENT_ROW;
        let runs = ab_mk(
            |mcx| mk_windowagg_pstmt_ex(mcx, relid, true, Some(fo), true),
            false,
            false,
        );
        // ROWS frame: the tie rows see per-row running sums (10, 20), unlike
        // the default frame's peer-group 20/20 — the framed lane's own
        // semantics, identical in both arms.
        let want: Vec<WinRow> = vec![
            (1, 10, 1, 1, 1, 10),
            (1, 10, 2, 1, 1, 20),
            (1, 20, 3, 3, 2, 40),
        ];
        assert_eq!(runs, vec![want]);
        scanfix::quiesced();
    }

    /// REFUSAL SHAPE (the fixed feed-refuse blocker's family): WindowAgg
    /// over Sort over hashed Agg over SeqScan — W1-admissible in every
    /// respect except the AGG-FED sort, which inc-1 refuses STRUCTURALLY:
    /// the hash-agg breaker feed is the one `sort_feed_if_needed` family
    /// with a dynamic feed-time refuse (agg-over-join multi-batch spill),
    /// and the sticky window drive cannot host a feed verdict that can
    /// flip (first-pull refuse stranding the admit memo; chgParam rescans
    /// flipping a rebuilt join's nbatch). Both arms must be byte-identical
    /// and the lane must never engage.
    #[test]
    fn windows_ab_agg_fed_sort_refuses() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70147;
        // (g, a): group counts g=1 -> 2, g=2 -> 1, g=3 -> 3.
        scanfix::register_table_2col(
            relid,
            &[&[(1, 10), (3, 7), (1, 20), (2, 5), (3, 8), (3, 9)]],
        );
        type AggWinRow = (i32, i64, i64, i64);
        fn drain<'mcx>(
            ps: &mut crate::procnode::PlanStateNode<'mcx>,
            estate: &mut EStateData<'mcx>,
        ) -> Vec<(i32, i64, i64, i64)> {
            let mut got = Vec::new();
            while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
                let base = estate.slot_mut(slot_id).base();
                assert!(base.tts_isnull.iter().all(|n| !n));
                got.push((
                    base.tts_values[0].as_i32(),
                    base.tts_values[1].as_i64(),
                    base.tts_values[2].as_i64(),
                    base.tts_values[3].as_i64(),
                ));
            }
            got
        }
        let run = |rescan: bool| -> Vec<Vec<AggWinRow>> {
            let pstmt = mk_window_over_agg_pstmt(leaked_mcx(), relid);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let ExecData { estate, planstate } = data;
                let ps = planstate.as_mut().unwrap();
                let mut runs = vec![drain(ps, estate)];
                if rescan {
                    crate::execami::exec_re_scan(ps, estate).unwrap();
                    runs.push(drain(ps, estate));
                }
                crate::exec_end_node(ps, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
                runs
            })
        };
        crate::lanev2::windows_set_for_tests(false);
        let off = run(true);
        crate::lanev2::windows_set_for_tests(true);
        let owned_before =
            crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed);
        let on = run(true);
        let owned_after =
            crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed);
        crate::lanev2::windows_set_for_tests(false);
        assert_eq!(off, on, "knob OFF vs ON must be identical");
        assert_eq!(
            owned_after, owned_before,
            "agg-fed sort shape engaged the windows lane (structural refusal broken)"
        );
        let want: Vec<AggWinRow> = vec![(1, 2, 1, 1), (2, 1, 1, 1), (3, 3, 1, 1)];
        assert_eq!(off, vec![want.clone(), want], "rescan must replay identically");
        scanfix::quiesced();
    }

    /// REFUSAL SHAPE: no Sort child (WindowAgg straight over the scan, the
    /// presorted-plan shape) — ChildNotLaneOwned; the row engine owns both
    /// arms. Input registered pre-sorted so the window semantics are sane.
    #[test]
    fn windows_ab_no_sort_child_refuses() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70146;
        scanfix::register_table_2col(relid, &[&[(1, 10), (1, 10), (2, 5)]]);
        let runs =
            ab_mk(|mcx| mk_windowagg_pstmt_ex(mcx, relid, true, None, false), false, false);
        let want: Vec<WinRow> = vec![
            (1, 10, 1, 1, 1, 20),
            (1, 10, 2, 1, 1, 20),
            (2, 5, 1, 1, 1, 5),
        ];
        assert_eq!(runs, vec![want]);
        scanfix::quiesced();
    }
}

// ============================================================================
// WS-J express-lane A/B corpus (lanev2/express.rs; single-executor Phase 1,
// docs/design/rowmode-operators.md §5–§6): the point-path IndexScan shape
// driven through the REAL dispatch (init_plan → procnode → try_own_index_scan
// → express hook) over the scanfix fake btree, with the knob in all three
// positions. Byte-identity is asserted OFF vs EXPRESS vs STRUCTURED per
// shape; engagement via EXPRESS_OWNED_FOR_TESTS; refusal shapes must leave
// the counter untouched while still returning the incumbent's rows.
// ============================================================================
mod express_ab {
    use super::*;
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{IndexScan, Plan, Scan};
    use ::types_nodes::primnodes::{OpExpr, Param, ParamKind};

    const OP_INT4GT: u32 = 521;
    const F_INT4GT: u32 = 147;

    fn install_express_seams() {
        // Scan-key strategy lookup for the fake int4 btree opfamily: the
        // process-shared installer (mergejoin_rowmode_ab needs the same
        // seam; seams are set-once).
        install_amop_strategy_seam();
    }

    /// The point key's right-hand side: a Const (the `k = 20` literal probe)
    /// or a PARAM_EXEC runtime key (the prepared/nestloop probe — express's
    /// defining admitted shape), or a `>` range probe (NOT pksel: the static
    /// shape refusal arm).
    #[derive(Clone, Copy)]
    enum PointKey {
        ConstEq(i32),
        ExecParamEq,
        ConstGt(i32),
    }

    fn mk_key_qual<'mcx>(mcx: ::mcx::Mcx<'mcx>, varno: i32, key: PointKey) -> NodeList<'mcx> {
        let var = Node::mk_var(mcx, varno, 1, INT4OID, -1, 0, 0).unwrap();
        let (opno, opfuncid, rhs) = match key {
            PointKey::ConstEq(k) => (INT4_EQ, F_INT4EQ, mk_int4_const(mcx, k)),
            PointKey::ConstGt(k) => (OP_INT4GT, F_INT4GT, mk_int4_const(mcx, k)),
            PointKey::ExecParamEq => (
                INT4_EQ,
                F_INT4EQ,
                Node::mk(
                    mcx,
                    Param {
                        paramkind: ParamKind::PARAM_EXEC,
                        paramid: 0,
                        paramtype: INT4OID,
                        paramtypmod: -1,
                        paramcollid: 0,
                        location: -1,
                    },
                )
                .unwrap(),
            ),
        };
        let op = Node::mk(
            mcx,
            OpExpr {
                opno,
                opfuncid,
                opresulttype: BOOLOID,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args: NodeList::make2(mcx, var, rhs).unwrap(),
                location: -1,
            },
        )
        .unwrap();
        NodeList::make1(mcx, op).unwrap()
    }

    /// `SELECT v FROM kv WHERE k <op> <key>` as an IndexScan PlannedStmt —
    /// the stmt-attrib point corpus's plan shape (projection Some: the tlist
    /// selects column 2 over the 2-col scan tuple).
    fn mk_point_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        relid: u32,
        index_oid: u32,
        key: PointKey,
    ) -> &'mcx PlannedStmt<'mcx> {
        let var = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, var, 1, Some("v"), false).unwrap();
        let scan_node = Node::mk(
            mcx,
            IndexScan {
                scan: Scan {
                    plan: Plan {
                        targetlist: NodeList::make1(mcx, tle).unwrap(),
                        ..Default::default()
                    },
                    scanrelid: 1,
                },
                indexid: index_oid,
                indexqual: mk_key_qual(mcx, ::execexpr::INDEX_VAR, key),
                indexqualorig: mk_key_qual(mcx, 1, key),
                indexorderby: NodeList::nil(),
                indexorderbyorig: NodeList::nil(),
                indexorderbyops: Default::default(),
                indexorderdir: 1,
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
        if matches!(key, PointKey::ExecParamEq) {
            pstmt.paramExecTypes = ::types_nodes::list::OidList::make1(mcx, INT4OID).unwrap();
        }
        pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
        pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    /// One execution: init the plan, run the scripted step list, tear down.
    /// Steps: `SetParam(v)` writes exec param 0 (None = NULL), `Rescan`
    /// replays the nestloop cadence, `Drain` pulls to end-of-scan collecting
    /// column 1, `PullOne` pulls a single row (the LIMIT-1 / early-teardown
    /// shape). Returns collected values or the first error string.
    #[derive(Clone, Copy)]
    enum Step {
        SetParam(Option<i32>),
        Rescan,
        Drain,
        PullOne,
    }

    fn run_point(
        pstmt: &'static PlannedStmt<'static>,
        steps: &[Step],
    ) -> (Vec<i32>, Option<String>) {
        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            {
                let n = pstmt.paramExecTypes.len();
                let es = &mut data.estate;
                es.es_param_exec_vals.extend(core::iter::repeat_n(
                    ::types_portal::params::ParamExecData::EMPTY,
                    n,
                ));
                es.es_param_subplans.extend(core::iter::repeat_n(None, n));
            }
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            let mut out = Vec::new();
            let mut run_err = None;
            'steps: for step in steps {
                match *step {
                    Step::SetParam(v) => {
                        estate.es_param_exec_vals[0] =
                            ::types_portal::params::ParamExecData {
                                value: v.map_or(Datum::null(), Datum::from_i32),
                                isnull: v.is_none(),
                                exec_plan: false,
                            };
                    }
                    Step::Rescan => exec_re_scan(ps, estate).unwrap(),
                    Step::Drain | Step::PullOne => loop {
                        match exec_proc_node(ps, estate) {
                            Ok(Some(slot_id)) => {
                                let mut isnull = false;
                                let v = exectuples::slot_getattr(
                                    estate.slot_mut(slot_id),
                                    1,
                                    &mut isnull,
                                );
                                assert!(!isnull);
                                out.push(v.as_i32());
                                if matches!(step, Step::PullOne) {
                                    continue 'steps;
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                run_err = Some(e.to_string());
                                break 'steps;
                            }
                        }
                    },
                }
            }
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
            scanfix::quiesced();
            (out, run_err)
        })
    }

    /// One A/B/B round over the same logical run: knob OFF, then EXPRESS
    /// (mode 1), then STRUCTURED (mode 2) — fresh plan/table per arm, all
    /// three (rows, error) results byte-identical; both ON arms must have
    /// ENGAGED (`expect_owned`) or must NOT have (refusal shapes).
    fn ab(
        key: PointKey,
        rows: &[(i32, i32)],
        steps: &[Step],
        expect_owned: bool,
    ) -> (Vec<i32>, Option<String>) {
        install_seams();
        scanfix::install();
        install_express_seams();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut results = Vec::new();
        for mode in [
            crate::lanev2::EXPRESS_OFF,
            crate::lanev2::EXPRESS_POINT,
            crate::lanev2::EXPRESS_STRUCTURED,
        ] {
            crate::lanev2::express_set_for_tests(mode);
            let mcx = leaked_mcx();
            let relid = 71000 + NEXT_EXPRESS_OID.fetch_add(2, std::sync::atomic::Ordering::Relaxed);
            let index_oid = relid + 1;
            scanfix::register_indexed_table_2col(relid, index_oid, rows);
            let pstmt = mk_point_pstmt(mcx, relid, index_oid, key);
            let owned_before = crate::lanev2::EXPRESS_OWNED_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed);
            let r = run_point(pstmt, steps);
            let owned_after = crate::lanev2::EXPRESS_OWNED_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed);
            let engaged = owned_after > owned_before;
            match mode {
                crate::lanev2::EXPRESS_OFF => {
                    assert!(!engaged, "knob OFF must never own a pull")
                }
                _ => assert_eq!(
                    engaged, expect_owned,
                    "mode {mode}: engagement mismatch (expected {expect_owned})"
                ),
            }
            results.push(r);
        }
        crate::lanev2::express_set_for_tests(crate::lanev2::EXPRESS_OFF);
        assert_eq!(results[0], results[1], "OFF vs EXPRESS must be identical");
        assert_eq!(results[0], results[2], "OFF vs STRUCTURED must be identical");
        results.pop().unwrap()
    }

    static NEXT_EXPRESS_OID: std::sync::atomic::AtomicU32 =
        std::sync::atomic::AtomicU32::new(0);

    const KV: &[(i32, i32)] = &[(30, 300), (10, 100), (20, 200)];

    #[test]
    fn ab_point_hit_const() {
        let (rows, err) = ab(PointKey::ConstEq(20), KV, &[Step::Drain], true);
        assert_eq!(err, None);
        assert_eq!(rows, vec![200]);
    }

    #[test]
    fn ab_point_miss_const() {
        let (rows, err) = ab(PointKey::ConstEq(99), KV, &[Step::Drain], true);
        assert_eq!(err, None);
        assert!(rows.is_empty());
    }

    #[test]
    fn ab_point_early_stop_pull_one() {
        // The LIMIT-1 cadence: one pull, then straight to teardown with the
        // scan mid-flight — no panic, no leaked pins, all arms.
        let (rows, err) = ab(PointKey::ConstEq(10), KV, &[Step::PullOne], true);
        assert_eq!(err, None);
        assert_eq!(rows, vec![100]);
    }

    #[test]
    fn ab_runtime_key_rescan_cadence() {
        // The prepared/nestloop shape express exists for: PARAM_EXEC runtime
        // key, evaluated on the first pull's !ready arm, then re-evaluated
        // per rescan with a new binding — including a NULL binding (strict
        // eq: empty result), then a live one again.
        let (rows, err) = ab(
            PointKey::ExecParamEq,
            KV,
            &[
                Step::SetParam(Some(20)),
                Step::Drain,
                Step::SetParam(Some(30)),
                Step::Rescan,
                Step::Drain,
                Step::SetParam(None),
                Step::Rescan,
                Step::Drain,
                Step::SetParam(Some(10)),
                Step::Rescan,
                Step::Drain,
            ],
            true,
        );
        assert_eq!(err, None);
        assert_eq!(rows, vec![200, 300, 100]);
    }

    #[test]
    fn ab_range_probe_refused_not_pksel() {
        // `k > 10` — a forward btree probe but NOT the pksel shape: express
        // must refuse (engagement counter untouched) while the rows flow
        // through the incumbent identically in every knob position.
        let (rows, err) = ab(PointKey::ConstGt(10), KV, &[Step::Drain], false);
        assert_eq!(err, None);
        assert_eq!(rows, vec![200, 300]);
    }

    #[test]
    fn express_epq_pull_refused_dynamically() {
        // es_epq_active is a per-pull gate: with the knob ON, an EPQ-flagged
        // estate must fall through to the incumbent (no engagement), same
        // rows.
        install_seams();
        scanfix::install();
        install_express_seams();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        crate::lanev2::express_set_for_tests(crate::lanev2::EXPRESS_POINT);
        let mcx = leaked_mcx();
        let relid = 72000 + NEXT_EXPRESS_OID.fetch_add(2, std::sync::atomic::Ordering::Relaxed);
        let index_oid = relid + 1;
        scanfix::register_indexed_table_2col(relid, index_oid, KV);
        let pstmt = mk_point_pstmt(mcx, relid, index_oid, PointKey::ConstEq(20));
        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            // EPQ-flagged estate: probe the dispatch hook directly (the
            // incumbent EPQ scan variant is unported in this harness, so a
            // full EPQ pull can't run here) — express must refuse (None,
            // counter untouched) BEFORE any scan work happens.
            estate.es_epq_active = true;
            let owned_before = crate::lanev2::EXPRESS_OWNED_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed);
            {
                let crate::procnode::PlanStateNode::IndexScan(is) = &mut *ps else {
                    panic!("point plan did not init an IndexScan node")
                };
                let r = crate::lanev2::try_own_index_scan(is, estate).unwrap();
                assert!(r.is_none(), "EPQ pull must be refused to the incumbent");
            }
            let owned_after = crate::lanev2::EXPRESS_OWNED_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed);
            assert_eq!(owned_before, owned_after, "EPQ pull must not be express-owned");
            // The gate is per-pull: dropping the flag re-admits, and the
            // node state is untouched — the same node drains correctly.
            estate.es_epq_active = false;
            let mut vals = Vec::new();
            while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
                let mut isnull = false;
                let v = exectuples::slot_getattr(estate.slot_mut(slot_id), 1, &mut isnull);
                assert!(!isnull);
                vals.push(v.as_i32());
            }
            assert_eq!(vals, vec![200]);
            let owned_end = crate::lanev2::EXPRESS_OWNED_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed);
            assert!(owned_end > owned_after, "post-EPQ pulls must re-engage express");
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        crate::lanev2::express_set_for_tests(crate::lanev2::EXPRESS_OFF);
        scanfix::quiesced();
    }
}

// =============================================================================
// MergeJoin row-mode A/B corpus (lanev2/rowmode.rs try_own_merge_join, WS-G
// Phase 1, PGRUST_LANE_V2_ROWMODE): hand-built MergeJoin plans over the
// fake-heap fixture driven knob OFF (the unchanged exec_merge_join Volcano
// drive) vs knob ON (MergeJoinRowSource -> PassthroughOp -> RootAdapter under
// pull_step_rows) in one process — same rows, same NULL padding, same rescan
// behavior, and the ON arm must actually engage. Duplicate-key outers force
// the EXEC_MJ_TESTOUTER inner restore; a Material inner covers the ExtraMarks
// mark cadence, a Sort inner the tuplesort mark/restore leg. The SQL-level
// three-arm byte parity proof is scripts/lane-rowmode-mj-e2e.sh; plan breadth
// (all 7 join types, cross-type clauses, SubPlans, LIMIT, ...) is
// scripts/dualexec/corpus-mergejoin.sql.
// =============================================================================
mod mergejoin_rowmode_ab {
    use super::*;

    /// pg_amop row for int4eq in the integer btree opfamily — what
    /// MJExamineQuals' get_op_opfamily_properties probes (strategy 3 =
    /// BTEqualStrategyNumber / COMPARE_EQ). install_seams covers the rest
    /// (btsortsupport via lookup_pg_amproc, operator shape, type shapes).
    // pub(super): the WS-L rowmode_tail_ab corpus reuses this fixture + the
    // plan builder below for its MergeJoin-over-tail-owned-Material
    // mark/restore leg (the blocking inc-1 composition proof) — the WS-G
    // KNOB-visibility precedent, test-file-only.
    pub(super) fn install_mj_seams() {
        // The process-shared pg_amop strategy installer (express_ab needs
        // the same seam; seams are set-once). MJExamineQuals only probes
        // INT4_EQ in INTEGER_BTREE_FAM → strategy 3, which the shared
        // handler serves identically to the pre-merge module-local copy.
        install_amop_strategy_seam();
    }

    /// Which mark/restore-capable wrapper shields the inner scan.
    #[derive(Clone, Copy, PartialEq)]
    pub(super) enum Inner {
        /// Material over the inner seqscan (order-preserving; exercises the
        /// mj_ExtraMarks cadence — ExecInitMergeJoin sets it for a Material
        /// inner without REWIND).
        Material,
        /// Sort over the inner seqscan (the planner's usual inner shield;
        /// exercises tuplesort mark/restore under EXEC_FLAG_MARK).
        Sort,
    }

    /// MergeJoin(mergeclause: outer.c1 = inner.c1) over two fake-heap
    /// seqscans, the inner behind a mark/restore-capable wrapper — the plan
    /// shape the planner emits for a merge join with a pre-sorted outer.
    pub(super) fn mk_mergejoin_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        outer_relid: u32,
        inner_relid: u32,
        jointype: ::types_nodes::JoinType,
        inner: Inner,
    ) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{
            Join, Material, MergeJoin, Plan, Scan, SeqScan, Sort,
        };
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
                    cb_scan_cols: None,
                    scan: Scan {
                        plan: Plan { targetlist: scan_tlist(varno), ..Default::default() },
                        scanrelid,
                    },
                },
            )
            .unwrap()
        };
        // The wrapper projects its child's two columns through OUTER_VAR.
        let wrapper_tlist = || {
            let a = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
            let b = Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap();
            NodeList::make2(
                mcx,
                Node::mk_target_entry(mcx, a, 1, Some("c1"), false).unwrap(),
                Node::mk_target_entry(mcx, b, 2, Some("c2"), false).unwrap(),
            )
            .unwrap()
        };
        let inner_tree = match inner {
            Inner::Material => {
                let mut mat = Node::build::<Material>(mcx).unwrap();
                mat.plan.targetlist = wrapper_tlist();
                mat.plan.lefttree = Some(mk_scan(2, 2));
                mat.seal()
            }
            Inner::Sort => {
                let mut sort = Node::build::<Sort>(mcx).unwrap();
                sort.plan.targetlist = wrapper_tlist();
                sort.plan.lefttree = Some(mk_scan(2, 2));
                sort.numCols = 1;
                sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
                sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT]).unwrap();
                sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
                sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();
                sort.seal()
            }
        };

        // SEMI/ANTI project only the outer side, as the planner emits.
        let tl_cols: &[(i32, i16)] = if matches!(
            jointype,
            ::types_nodes::JoinType::JOIN_SEMI | ::types_nodes::JoinType::JOIN_ANTI
        ) {
            &[(OUTER_VAR, 1), (OUTER_VAR, 2)]
        } else {
            &[(OUTER_VAR, 1), (OUTER_VAR, 2), (INNER_VAR, 1), (INNER_VAR, 2)]
        };
        let mut join_tlist = NodeList::nil();
        for (i, &(varno, attno)) in tl_cols.iter().enumerate() {
            let v = Node::mk_var(mcx, varno, attno, INT4OID, -1, 0, 0).unwrap();
            join_tlist
                .lappend(
                    mcx,
                    Node::mk_target_entry(mcx, v, i as i16 + 1, Some("x"), false).unwrap(),
                )
                .unwrap();
        }
        let mergeclause = {
            let l = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
            let r = Node::mk_var(mcx, INNER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
            Node::mk(
                mcx,
                ::types_nodes::primnodes::OpExpr {
                    opno: INT4_EQ,
                    opfuncid: 65, // pg_proc int4eq
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

        let mut mj = Node::build::<MergeJoin>(mcx).unwrap();
        mj.join = Join {
            plan: Plan {
                targetlist: join_tlist,
                lefttree: Some(mk_scan(1, 1)),
                righttree: Some(inner_tree),
                ..Default::default()
            },
            jointype,
            inner_unique: false,
            joinqual: NodeList::nil(),
        };
        mj.skip_mark_restore = false;
        mj.mergeclauses = NodeList::make1(mcx, mergeclause).unwrap();
        mj.mergeFamilies = ::mcx::slice_borrow_in(mcx, &[INTEGER_BTREE_FAM]).unwrap();
        mj.mergeCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        mj.mergeReversals = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();
        mj.mergeNullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();

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
        pstmt.planTree = Some(mj.seal());
        pstmt.rtable = rtable;
        pstmt.permInfos = perms;
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    /// One A/B round: build the SAME plan twice (fresh state per arm), run
    /// knob OFF then knob ON (both with `passes` drains, pass 2+ = rescan),
    /// demand identical rows — and that the ON arm engaged the MergeJoin
    /// row-mode drive specifically.
    fn ab_mj(
        mk: impl Fn(::mcx::Mcx<'static>) -> &'static PlannedStmt<'static>,
        natts: usize,
        passes: usize,
    ) -> Vec<Vec<Vec<Option<i32>>>> {
        install_seams();
        install_mj_seams();
        scanfix::install();
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());
        // Wave-2 knob split: MergeJoin hosting now gates on
        // PGRUST_LANE_V2_MERGEJOIN, not the rowmode facility knob.
        crate::lanev2::mergejoin_set_for_tests(false);
        let off = drain_wide_rows_nullable(mk(leaked_mcx()), natts, passes);
        crate::lanev2::mergejoin_set_for_tests(true);
        let owned_before = crate::lanev2::ROWMODE_MJ_OWNED_FOR_TESTS
            .load(std::sync::atomic::Ordering::Relaxed);
        let on = drain_wide_rows_nullable(mk(leaked_mcx()), natts, passes);
        let owned_after = crate::lanev2::ROWMODE_MJ_OWNED_FOR_TESTS
            .load(std::sync::atomic::Ordering::Relaxed);
        crate::lanev2::mergejoin_set_for_tests(false);
        drop(guard);
        assert_eq!(off, on, "knob OFF vs ON must be identical");
        assert!(
            owned_after > owned_before,
            "ON arm never engaged the MergeJoin row-mode drive"
        );
        off
    }

    fn row(vals: &[i32]) -> Vec<Option<i32>> {
        vals.iter().map(|&v| Some(v)).collect()
    }

    // Duplicate-key OUTER over a Material inner: every second same-key outer
    // forces EXEC_MJ_TESTOUTER to restore the inner to the marked group
    // start (with the ExtraMarks cadence), and the inner group itself has
    // two rows — the full mark/restore replay, knob OFF vs ON, plus a
    // rescan pass through exec_rescan_merge_join under the hook.
    #[test]
    fn mj_ab_inner_join_dup_outer_material_inner_mark_restore() {
        let outer: u32 = 73001;
        let inner: u32 = 73002;
        scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (2, 21), (4, 40)]]);
        scanfix::register_table_2col(inner, &[&[(2, 200), (2, 201), (3, 300), (5, 500)]]);
        let expected = vec![
            row(&[2, 20, 2, 200]),
            row(&[2, 20, 2, 201]),
            row(&[2, 21, 2, 200]), // <- replay after restore
            row(&[2, 21, 2, 201]),
        ];
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let runs = ab_mj(
            move |mcx| {
                mk_mergejoin_pstmt(
                    mcx,
                    outer,
                    inner,
                    ::types_nodes::JoinType::JOIN_INNER,
                    Inner::Material,
                )
            },
            4,
            2,
        );
        assert_eq!(runs, vec![expected.clone(), expected]);
        scanfix::quiesced();
    }

    // The same duplicate-outer replay against a Sort inner (unsorted
    // registration order, unique keys): tuplesort mark/restore under
    // EXEC_FLAG_MARK, both knob positions.
    #[test]
    fn mj_ab_inner_join_dup_outer_sort_inner() {
        let outer: u32 = 73003;
        let inner: u32 = 73004;
        scanfix::register_table_2col(outer, &[&[(1, 10), (3, 30), (3, 31)]]);
        scanfix::register_table_2col(inner, &[&[(5, 500), (3, 300), (1, 100)]]);
        let expected = vec![
            row(&[1, 10, 1, 100]),
            row(&[3, 30, 3, 300]),
            row(&[3, 31, 3, 300]),
        ];
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let runs = ab_mj(
            move |mcx| {
                mk_mergejoin_pstmt(
                    mcx,
                    outer,
                    inner,
                    ::types_nodes::JoinType::JOIN_INNER,
                    Inner::Sort,
                )
            },
            4,
            2,
        );
        assert_eq!(runs, vec![expected.clone(), expected]);
        scanfix::quiesced();
    }

    // FULL join: null extension on BOTH sides (fill-outer for unmatched
    // outers, ENDOUTER/ENDINNER fill-inner for unmatched inners), the
    // merge-only-plannable shape the hosting exists for.
    #[test]
    fn mj_ab_full_join_fills_both_sides() {
        let outer: u32 = 73005;
        let inner: u32 = 73006;
        scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (6, 60)]]);
        scanfix::register_table_2col(inner, &[&[(2, 200), (3, 300)]]);
        let n = || None::<i32>;
        let expected = vec![
            vec![Some(1), Some(10), n(), n()],
            vec![Some(2), Some(20), Some(2), Some(200)],
            vec![n(), n(), Some(3), Some(300)],
            vec![Some(6), Some(60), n(), n()],
        ];
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let runs = ab_mj(
            move |mcx| {
                mk_mergejoin_pstmt(
                    mcx,
                    outer,
                    inner,
                    ::types_nodes::JoinType::JOIN_FULL,
                    Inner::Material,
                )
            },
            4,
            2,
        );
        assert_eq!(runs, vec![expected.clone(), expected]);
        scanfix::quiesced();
    }

    // LEFT (fill-outer only), SEMI (single-match advance), ANTI
    // (never-matched outers) over the same tables, one rescan pass each.
    #[test]
    fn mj_ab_left_semi_anti_joins() {
        let outer: u32 = 73007;
        let inner: u32 = 73008;
        scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (3, 30)]]);
        scanfix::register_table_2col(inner, &[&[(2, 200), (2, 201), (4, 400)]]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let n = || None::<i32>;
        let left = vec![
            vec![Some(1), Some(10), n(), n()],
            vec![Some(2), Some(20), Some(2), Some(200)],
            vec![Some(2), Some(20), Some(2), Some(201)],
            vec![Some(3), Some(30), n(), n()],
        ];
        let runs = ab_mj(
            move |mcx| {
                mk_mergejoin_pstmt(
                    mcx,
                    outer,
                    inner,
                    ::types_nodes::JoinType::JOIN_LEFT,
                    Inner::Material,
                )
            },
            4,
            2,
        );
        assert_eq!(runs, vec![left.clone(), left]);

        let semi = vec![row(&[2, 20])];
        let runs = ab_mj(
            move |mcx| {
                mk_mergejoin_pstmt(
                    mcx,
                    outer,
                    inner,
                    ::types_nodes::JoinType::JOIN_SEMI,
                    Inner::Material,
                )
            },
            2,
            2,
        );
        assert_eq!(runs, vec![semi.clone(), semi]);

        let anti = vec![row(&[1, 10]), row(&[3, 30])];
        let runs = ab_mj(
            move |mcx| {
                mk_mergejoin_pstmt(
                    mcx,
                    outer,
                    inner,
                    ::types_nodes::JoinType::JOIN_ANTI,
                    Inner::Material,
                )
            },
            2,
            2,
        );
        assert_eq!(runs, vec![anti.clone(), anti]);
        scanfix::quiesced();
    }

    // Empty inner: LEFT null-extends every outer; INNER emits nothing.
    #[test]
    fn mj_ab_empty_inner() {
        let outer: u32 = 73009;
        let inner: u32 = 73010;
        scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20)]]);
        scanfix::register_table_2col(inner, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let n = || None::<i32>;
        let left = vec![
            vec![Some(1), Some(10), n(), n()],
            vec![Some(2), Some(20), n(), n()],
        ];
        let runs = ab_mj(
            move |mcx| {
                mk_mergejoin_pstmt(
                    mcx,
                    outer,
                    inner,
                    ::types_nodes::JoinType::JOIN_LEFT,
                    Inner::Material,
                )
            },
            4,
            1,
        );
        assert_eq!(runs, vec![left]);
        let runs = ab_mj(
            move |mcx| {
                mk_mergejoin_pstmt(
                    mcx,
                    outer,
                    inner,
                    ::types_nodes::JoinType::JOIN_INNER,
                    Inner::Material,
                )
            },
            4,
            1,
        );
        assert_eq!(runs, vec![Vec::<Vec<Option<i32>>>::new()]);
        scanfix::quiesced();
    }

    // Early stop: pull exactly one row knob-ON, then tear down mid-join (the
    // LIMIT shape) — no panic, no error; all cross-call state is the FSM's
    // own, so abandoning the drive is byte-safe.
    #[test]
    fn mj_ab_early_stop_teardown() {
        install_seams();
        install_mj_seams();
        scanfix::install();
        let outer: u32 = 73011;
        let inner: u32 = 73012;
        scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (3, 30)]]);
        scanfix::register_table_2col(inner, &[&[(1, 100), (2, 200), (3, 300)]]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());
        for on in [false, true] {
            crate::lanev2::mergejoin_set_for_tests(on);
            let pstmt = mk_mergejoin_pstmt(
                leaked_mcx(),
                outer,
                inner,
                ::types_nodes::JoinType::JOIN_INNER,
                Inner::Material,
            );
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let ExecData { estate, planstate } = data;
                let ps = planstate.as_mut().unwrap();
                let slot = exec_proc_node(ps, estate).unwrap().unwrap();
                let mut isnull = false;
                let v = exectuples::slot_getattr(estate.slot_mut(slot), 1, &mut isnull);
                assert!(!isnull);
                assert_eq!(v.as_i32(), 1);
                // Walk away mid-join.
                crate::exec_end_node(ps, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
        }
        crate::lanev2::mergejoin_set_for_tests(false);
        drop(guard);
        scanfix::quiesced();
    }
}

// ---------------------------------------------------------------------------
// WS-P node census (wave-2 flip machinery; lanev2/census.rs). Fixture oid
// band: 74001+ (fixture bands are a shared namespace — phase-1 integration
// precedent; wave-2 integrated map: 70xxx shared/windows incl. T2, 71xxx/
// 72xxx express, 73xxx mergejoin + rowmode-tail fixture reuse, 74xxx census,
// 75xxx dml — dml_ab was moved off 74001+ at wave-2 integration because
// register_table_2col's two_col membership is process-global and permanent,
// which poisoned census's 1-col 74002 registration when dml ran first).
// ---------------------------------------------------------------------------
mod census_tests {
    use super::*;

    /// A census-shaped runnable fixture: Limit(0) <- Sort(1) <- SeqScan(2),
    /// with plan_node_ids ASSIGNED (the shared mk_* fixtures leave every id
    /// 0; the census join and the instrumented-universe cross-check need
    /// the real dense ids setrefs would assign).
    fn mk_census_sort_limit_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        relid: u32,
    ) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{Limit, Plan, Scan, SeqScan, Sort};
        use ::types_nodes::primnodes::OUTER_VAR;

        let scan_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
        let scan_tle = Node::mk_target_entry(mcx, scan_var, 1, Some("a"), false).unwrap();
        let scan = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan {
                        plan_node_id: 2,
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

        let mut sort = Node::build::<Sort>(mcx).unwrap();
        sort.plan.plan_node_id = 1;
        sort.plan.targetlist = outer_tle(mcx);
        sort.plan.lefttree = Some(scan);
        sort.numCols = 1;
        sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
        sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT]).unwrap();
        sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();
        let sort = sort.seal();

        let mut limit = Node::build::<Limit>(mcx).unwrap();
        limit.plan.plan_node_id = 0;
        limit.plan.targetlist = outer_tle(mcx);
        limit.plan.lefttree = Some(sort);
        limit.limitCount = Some(
            Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::from_i64(2), false, true).unwrap(),
        );
        let tree = limit.seal();

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

    /// Walker vs EXPLAIN's traversal, generic-edge shape: the lefttree chain
    /// visits every node exactly once, parent-first, with the census kind
    /// vocabulary.
    #[test]
    fn census_walk_visits_lefttree_chain_once_each() {
        install_seams();
        let mcx = leaked_mcx();
        let pstmt = mk_census_sort_limit_pstmt(mcx, 74001);
        let rows = crate::lanev2::census_rows_for_tests(pstmt);
        assert_eq!(rows, vec![(0, "limit"), (1, "sort"), (2, "seqscan")]);
    }

    /// Walker vs EXPLAIN's traversal, righttree + Hash: a HashJoin subtree
    /// counts the Hash node (EXPLAIN prints it; the census denominator keeps
    /// it — its lane story rides the join flip, docs/design/flip-ladder.md).
    #[test]
    fn census_walk_counts_hash_under_hashjoin() {
        install_seams();
        let mcx = leaked_mcx();
        use ::types_nodes::plannodes::{Hash, HashJoin, Plan, Scan, SeqScan};

        let mk_scan = |id: i32| {
            Node::mk(
                mcx,
                SeqScan {
                    cb_scan_cols: None,
                    scan: Scan {
                        plan: Plan { plan_node_id: id, ..Default::default() },
                        scanrelid: 1,
                    },
                },
            )
            .unwrap()
        };
        let mut hash = Node::build::<Hash>(mcx).unwrap();
        hash.plan.plan_node_id = 2;
        hash.plan.lefttree = Some(mk_scan(3));
        let hash = hash.seal();
        let mut hj = Node::build::<HashJoin>(mcx).unwrap();
        hj.join.plan.plan_node_id = 0;
        hj.join.plan.lefttree = Some(mk_scan(1));
        hj.join.plan.righttree = Some(hash);
        let tree = hj.seal();

        let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
        pstmt.commandType = CmdType::CMD_SELECT;
        pstmt.planTree = Some(tree);
        let pstmt = pstmt.seal_ref();

        let rows = crate::lanev2::census_rows_for_tests(pstmt);
        assert_eq!(
            rows,
            vec![(0, "hashjoin"), (1, "seqscan"), (2, "hash"), (3, "seqscan")]
        );
    }

    /// Walker vs EXPLAIN's traversal, member-list edge: Append's children
    /// come from appendplans (no lefttree).
    #[test]
    fn census_walk_visits_append_member_list() {
        install_seams();
        let mcx = leaked_mcx();
        use ::types_nodes::plannodes::{Append, Plan, Scan, SeqScan};

        let mk_scan = |id: i32| {
            Node::mk(
                mcx,
                SeqScan {
                    cb_scan_cols: None,
                    scan: Scan {
                        plan: Plan { plan_node_id: id, ..Default::default() },
                        scanrelid: 1,
                    },
                },
            )
            .unwrap()
        };
        let mut append = Node::build::<Append>(mcx).unwrap();
        append.plan.plan_node_id = 0;
        append.appendplans = NodeList::make2(mcx, mk_scan(1), mk_scan(2)).unwrap();
        let tree = append.seal();

        let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
        pstmt.commandType = CmdType::CMD_SELECT;
        pstmt.planTree = Some(tree);
        let pstmt = pstmt.seal_ref();

        let rows = crate::lanev2::census_rows_for_tests(pstmt);
        assert_eq!(rows, vec![(0, "append"), (1, "seqscan"), (2, "seqscan")]);
    }

    /// OQ1 cross-check against the init_plan/EXPLAIN universe: an
    /// INSTRUMENTED execution allocates one es_instrumentation slot per plan
    /// node keyed by plan_node_id (the exact ids EXPLAIN's walker visits);
    /// the census walker must count the same universe. Also pins the live
    /// root cross-check: the exhaustive planstate classifier and the plan-
    /// tree classifier agree through the Instrumented wrapper.
    #[test]
    fn census_count_matches_instrumented_universe() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();

        let relid: u32 = 74002;
        scanfix::register_table(relid, &[&[3, 1, 2]]);
        let pstmt = mk_census_sort_limit_pstmt(mcx, relid);

        let census_rows = crate::lanev2::census_rows_for_tests(pstmt);
        assert_eq!(census_rows.len(), 3);
        let mut ids: Vec<i32> = census_rows.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids, vec![0, 1, 2], "census ids must be the dense plan ids");

        let snap_ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_instrument = ::types_core::instrument::INSTRUMENT_ROWS;
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            assert_eq!(
                crate::lanev2::census_planstate_kind_name_for_tests(ps),
                "limit",
                "planstate root classifies as the plan-tree root (Instrumented-transparent)"
            );
            while exec_proc_node(ps, estate).unwrap().is_some() {}
            assert_eq!(
                estate.es_instrumentation.len(),
                census_rows.len(),
                "census universe == instrumented plan-node universe"
            );
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        scanfix::quiesced();
    }

    /// Engine attribution: the strongest per-node claim wins (lane >
    /// runtime > fused-arm > spine); nodes without events are "none".
    #[test]
    fn census_attribution_prefers_lane_claims() {
        install_seams();
        let mcx = leaked_mcx();
        let pstmt = mk_select1_pstmt(mcx, None);
        with_exec_data(pstmt, |data, _pstmt| {
            let estate = &mut data.estate;
            estate.engine_record(0, ::executils::EngineKind::Spine, "sortfeed", "epq");
            estate.engine_record(0, ::executils::EngineKind::Lane, "aggbuild", "");
            estate.engine_record(1, ::executils::EngineKind::FusedArm, "aggbuild",
                "admission-economics-fused-drive");
            let (engine, class, detail) = crate::lanev2::census_attribution_for_tests(estate, 0);
            assert_eq!((engine, class, detail), ("lane", "aggbuild", ""));
            let (engine, class, detail) = crate::lanev2::census_attribution_for_tests(estate, 1);
            assert_eq!(
                (engine, class, detail),
                ("fused-arm", "aggbuild", "admission-economics-fused-drive")
            );
            assert_eq!(
                crate::lanev2::census_attribution_for_tests(estate, 9),
                ("none", "", "")
            );
        });
    }
}

// ===========================================================================
// WS-L wave-2 row-mode tail — A/B unit corpus (lanev2/rowmode_tail.rs behind
// PGRUST_LANE_V2_ROWMODE; contract §5 Stage 1+). Chunk 1: Material
// delegation with rescan + the BLOCKING mergejoin-over-material mark/restore
// composition leg (contract §6-WS-L(4)) with BOTH knobs on. Per-shape
// engagement is asserted through the ratified ROWMODE_TAIL_OWNED_FOR_TESTS
// probe array (contract §3.4), never the shared stats counters.
// ===========================================================================
mod rowmode_tail_ab {
    use super::*;

    fn tail_probe(name: &str) -> u64 {
        crate::lanev2::tail_owned_probe_for_tests(name)
    }

    fn row(vals: &[i32]) -> Vec<Option<i32>> {
        vals.iter().map(|&v| Some(v)).collect()
    }

    /// Material over a fake-heap seqscan, projecting both columns through
    /// OUTER_VAR — the bare shape the tail hosts as a delegation leaf.
    fn mk_material_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        relid: u32,
    ) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{Material, Plan, Scan, SeqScan};
        use ::types_nodes::primnodes::OUTER_VAR;

        let scan_tlist = {
            let a = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
            let b = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
            NodeList::make2(
                mcx,
                Node::mk_target_entry(mcx, a, 1, Some("c1"), false).unwrap(),
                Node::mk_target_entry(mcx, b, 2, Some("c2"), false).unwrap(),
            )
            .unwrap()
        };
        let scan = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan { targetlist: scan_tlist, ..Default::default() },
                    scanrelid: 1,
                },
            },
        )
        .unwrap();
        let mat_tlist = {
            let a = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
            let b = Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap();
            NodeList::make2(
                mcx,
                Node::mk_target_entry(mcx, a, 1, Some("c1"), false).unwrap(),
                Node::mk_target_entry(mcx, b, 2, Some("c2"), false).unwrap(),
            )
            .unwrap()
        };
        let mut mat = Node::build::<Material>(mcx).unwrap();
        mat.plan.targetlist = mat_tlist;
        mat.plan.lefttree = Some(scan);

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
        let perm = Node::mk(
            mcx,
            RTEPermissionInfo { relid, requiredPerms: 1 << 1, ..Default::default() },
        )
        .unwrap();
        let mut unpruned = Bitmapset::empty();
        unpruned.add_member(mcx, 1).unwrap();

        let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
        pstmt.commandType = CmdType::CMD_SELECT;
        pstmt.canSetTag = true;
        pstmt.planTree = Some(mat.seal());
        pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
        pstmt.permInfos = NodeList::make1(mcx, perm).unwrap();
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    // Material delegation A/B: knob OFF then ON over the same plan, two
    // passes (pass 2 = rescan → tuplestore re-read), byte-equal rows; the
    // OFF arm must not move the Material probe, the ON arm must.
    #[test]
    fn material_tail_ab_with_rescan() {
        install_seams();
        scanfix::install();
        let relid: u32 = 73021;
        scanfix::register_table_2col(relid, &[&[(1, 10), (2, 20), (3, 30)]]);
        let expected = vec![row(&[1, 10]), row(&[2, 20]), row(&[3, 30])];
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        crate::lanev2::rowmode_set_for_tests(false);
        let probe_before_off = tail_probe("material");
        let off = drain_wide_rows_nullable(mk_material_pstmt(leaked_mcx(), relid), 2, 2);
        assert_eq!(
            tail_probe("material"),
            probe_before_off,
            "knob OFF must never engage the tail"
        );

        crate::lanev2::rowmode_set_for_tests(true);
        let probe_before_on = tail_probe("material");
        let on = drain_wide_rows_nullable(mk_material_pstmt(leaked_mcx(), relid), 2, 2);
        let probe_after_on = tail_probe("material");
        crate::lanev2::rowmode_set_for_tests(false);
        drop(guard);

        assert_eq!(off, on, "knob OFF vs ON must be identical");
        assert_eq!(off, vec![expected.clone(), expected]);
        assert!(
            probe_after_on > probe_before_on,
            "ON arm never engaged the Material tail drive"
        );
        scanfix::quiesced();
    }

    // THE BLOCKING inc-1 composition leg (contract §6-WS-L(4)): MergeJoin
    // hosted behind PGRUST_LANE_V2_MERGEJOIN over a Material inner hosted
    // behind PGRUST_LANE_V2_ROWMODE. Duplicate outer keys force
    // EXEC_MJ_TESTOUTER restores: the FSM's mark/restore calls enter the
    // Material node through execami DIRECTLY while the tail owns its pulls —
    // rows must equal the both-knobs-off oracle, replay row included.
    #[test]
    fn mj_over_tail_material_mark_restore_ab() {
        install_seams();
        mergejoin_rowmode_ab::install_mj_seams();
        scanfix::install();
        let outer: u32 = 73023;
        let inner: u32 = 73024;
        scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (2, 21), (4, 40)]]);
        scanfix::register_table_2col(inner, &[&[(2, 200), (2, 201), (3, 300), (5, 500)]]);
        let expected = vec![
            row(&[2, 20, 2, 200]),
            row(&[2, 20, 2, 201]),
            row(&[2, 21, 2, 200]), // <- replay after restore
            row(&[2, 21, 2, 201]),
        ];
        let mk = move |mcx| {
            mergejoin_rowmode_ab::mk_mergejoin_pstmt(
                mcx,
                outer,
                inner,
                ::types_nodes::JoinType::JOIN_INNER,
                mergejoin_rowmode_ab::Inner::Material,
            )
        };
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        crate::lanev2::mergejoin_set_for_tests(false);
        crate::lanev2::rowmode_set_for_tests(false);
        let off = drain_wide_rows_nullable(mk(leaked_mcx()), 4, 2);

        crate::lanev2::mergejoin_set_for_tests(true);
        crate::lanev2::rowmode_set_for_tests(true);
        let mat_before = tail_probe("material");
        let mj_before = crate::lanev2::ROWMODE_MJ_OWNED_FOR_TESTS
            .load(std::sync::atomic::Ordering::Relaxed);
        let on = drain_wide_rows_nullable(mk(leaked_mcx()), 4, 2);
        let mat_after = tail_probe("material");
        let mj_after = crate::lanev2::ROWMODE_MJ_OWNED_FOR_TESTS
            .load(std::sync::atomic::Ordering::Relaxed);
        crate::lanev2::mergejoin_set_for_tests(false);
        crate::lanev2::rowmode_set_for_tests(false);
        drop(guard);

        assert_eq!(off, on, "both-knobs-on must equal the both-off oracle");
        assert_eq!(off, vec![expected.clone(), expected]);
        assert!(mj_after > mj_before, "MergeJoin hosting never engaged");
        assert!(mat_after > mat_before, "Material tail hosting never engaged under the MJ inner");
        scanfix::quiesced();
    }
}

// ============================================================================
// windows_t2_ab — the wave-2 WS-M T2-A A/B unit corpus (contract §6 WS-M).
//
// Every test runs the SAME plan knob-OFF (the row engine) then knob-ON
// (PGRUST_LANE_V2_WINDOWS_T2 — the T2-A row-mode delegation host) and
// demands byte-identical rows plus lane engagement (or non-engagement for
// the interplay shapes). The `want` vectors are C-VERIFIED: each fixture
// was executed against PostgreSQL 18.3 (scratch script t2-fixtures.sql,
// results transcribed verbatim; the OFF arm re-proves them against the
// ported row engine on every run). Frame classes covered: ROWS offset
// PRECEDING/FOLLOWING pairs (the MovingIntSum INVERSE-transition kernel),
// ROWS CURRENT ROW..UNBOUNDED FOLLOWING, ROWS n..m PRECEDING (empty
// frames -> strict-sum NULLs), RANGE offsets (in_range 4128), GROUPS
// offsets, EXCLUDE CURRENT ROW / GROUP / TIES, lead/lag (with offset +
// default), first/last/nth_value, FILTER (the W8-retirement gate,
// contract WS-M amendment 5), ntile, stacked WindowAgg nodes (multiple
// window defs), W1-interplay (hook order), rescan replay, empty input.
// ============================================================================
mod windows_t2_ab {
    use super::*;
    use ::types_nodes::rawnodes::{
        FRAMEOPTION_BETWEEN, FRAMEOPTION_DEFAULTS, FRAMEOPTION_END_CURRENT_ROW,
        FRAMEOPTION_END_OFFSET_FOLLOWING, FRAMEOPTION_END_OFFSET_PRECEDING,
        FRAMEOPTION_END_UNBOUNDED_FOLLOWING, FRAMEOPTION_EXCLUDE_CURRENT_ROW,
        FRAMEOPTION_EXCLUDE_GROUP, FRAMEOPTION_EXCLUDE_TIES, FRAMEOPTION_GROUPS,
        FRAMEOPTION_NONDEFAULT, FRAMEOPTION_RANGE, FRAMEOPTION_ROWS,
        FRAMEOPTION_START_CURRENT_ROW, FRAMEOPTION_START_OFFSET_PRECEDING,
        FRAMEOPTION_START_UNBOUNDED_PRECEDING,
    };

    /// The shared unit relation: (g, a) registered UNSORTED (the Sort under
    /// the WindowAgg orders by (g, a)). Sorted view:
    /// (1,10) (1,10) (1,20) (1,30) | (2,5) (2,6) | (3,7).
    const T2_ROWS: &[(i32, i32)] =
        &[(2, 5), (1, 10), (3, 7), (1, 20), (2, 6), (1, 10), (1, 30)];

    /// Window-function argument shapes for the plan builder.
    #[derive(Clone, Copy)]
    enum T2Args {
        /// No arguments (row_number when winagg=false).
        None,
        /// (a) — the partition's second column.
        A,
        /// (a, Const int4) — lead/lag offset, nth_value n.
        AOff(i32),
        /// (a, Const int4, Const int4) — lead/lag offset + default.
        AOffDef(i32, i32),
        /// (Const int4) — ntile buckets.
        N(i32),
    }

    /// One window function column in the built tlist.
    #[derive(Clone, Copy)]
    struct T2Fn {
        fnoid: u32,
        wintype: u32,
        winagg: bool,
        args: T2Args,
        /// FILTER (WHERE a < k) on a window aggregate.
        filter_a_lt: Option<i32>,
    }

    const SUM_A: &[T2Fn] = &[T2Fn {
        fnoid: 2108,
        wintype: INT8OID,
        winagg: true,
        args: T2Args::A,
        filter_a_lt: None,
    }];

    /// Frame + window-clause spec for the built WindowAgg node.
    struct T2Spec {
        frame_options: i32,
        /// ROWS/GROUPS offsets are int8 Consts (the planner's type).
        start_off_i64: Option<i64>,
        end_off_i64: Option<i64>,
        /// RANGE offsets for the int4 ORDER BY column: int4 Consts +
        /// in_range(int4,int4,int4) = fmgr 4128 as start/endInRangeFunc.
        range_off_i32: Option<(i32, i32)>,
        order_by: bool,
        fns: &'static [T2Fn],
    }

    impl T2Spec {
        const fn framed(frame_options: i32) -> Self {
            T2Spec {
                frame_options,
                start_off_i64: None,
                end_off_i64: None,
                range_off_i32: None,
                order_by: true,
                fns: SUM_A,
            }
        }
    }

    /// Output cell/type spec for the drain (lead/lag emit NULLs; sums are
    /// int8, value functions int4).
    #[derive(Clone, Copy)]
    enum Ty {
        I32,
        I64,
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Cell {
        Null,
        I(i64),
    }
    use Cell::{I, Null};

    /// Build WindowAgg(spec) over Sort(g,a) over SeqScan(relid) — the
    /// windows_t2 mirror of `mk_windowagg_pstmt_ex`, generalized to explicit
    /// frames, offsets, EXCLUDE bits, argument-carrying window functions and
    /// FILTER. tlist = (g, a, <one column per spec.fns entry>).
    fn mk_t2_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        relid: u32,
        spec: &T2Spec,
    ) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{Plan, Scan, SeqScan, Sort, WindowAgg};
        use ::types_nodes::primnodes::{WindowFunc, OUTER_VAR};

        let mk_tlist = |varno: i32| {
            let g = Node::mk_var(mcx, varno, 1, INT4OID, -1, 0, 0).unwrap();
            let a = Node::mk_var(mcx, varno, 2, INT4OID, -1, 0, 0).unwrap();
            NodeList::make2(
                mcx,
                Node::mk_target_entry(mcx, g, 1, Some("g"), false).unwrap(),
                Node::mk_target_entry(mcx, a, 2, Some("a"), false).unwrap(),
            )
            .unwrap()
        };
        let i4 = |v: i32| {
            Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(v), false, true).unwrap()
        };
        let i8c = |v: i64| {
            Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::from_i64(v), false, true).unwrap()
        };

        let scan = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
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
        sort.numCols = 2;
        sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16, 2]).unwrap();
        sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT, INT4_LT]).unwrap();
        sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32, 0]).unwrap();
        sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false, false]).unwrap();

        let mut tlist = mk_tlist(OUTER_VAR);
        let a_var = || Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap();
        for (i, f) in spec.fns.iter().enumerate() {
            let mut w = Node::build::<WindowFunc>(mcx).unwrap();
            w.winfnoid = f.fnoid;
            w.wintype = f.wintype;
            w.winref = 1;
            w.winagg = f.winagg;
            w.args = match f.args {
                T2Args::None => NodeList::nil(),
                T2Args::A => NodeList::make1(mcx, a_var()).unwrap(),
                T2Args::AOff(k) => NodeList::make2(mcx, a_var(), i4(k)).unwrap(),
                T2Args::AOffDef(k, d) => {
                    NodeList::make3(mcx, a_var(), i4(k), i4(d)).unwrap()
                }
                T2Args::N(n) => NodeList::make1(mcx, i4(n)).unwrap(),
            };
            if let Some(k) = f.filter_a_lt {
                w.aggfilter = Some(
                    Node::mk(
                        mcx,
                        ::types_nodes::OpExpr {
                            opno: INT4_LT,
                            opfuncid: 66, // pg_proc int4lt
                            opresulttype: BOOLOID,
                            opretset: false,
                            opcollid: 0,
                            inputcollid: 0,
                            args: NodeList::make2(mcx, a_var(), i4(k)).unwrap(),
                            location: -1,
                        },
                    )
                    .unwrap(),
                );
            }
            tlist
                .lappend(
                    mcx,
                    Node::mk_target_entry(mcx, w.seal(), (3 + i) as i16, Some("w"), false)
                        .unwrap(),
                )
                .unwrap();
        }

        let mut wa = Node::build::<WindowAgg>(mcx).unwrap();
        wa.plan.targetlist = tlist;
        wa.plan.lefttree = Some(sort.seal());
        wa.frameOptions = spec.frame_options;
        if let Some(v) = spec.start_off_i64 {
            wa.startOffset = Some(i8c(v));
        }
        if let Some(v) = spec.end_off_i64 {
            wa.endOffset = Some(i8c(v));
        }
        if let Some((s, e)) = spec.range_off_i32 {
            wa.startOffset = Some(i4(s));
            wa.endOffset = Some(i4(e));
            wa.startInRangeFunc = 4128; // in_range(int4,int4,int4)
            wa.endInRangeFunc = 4128;
            wa.inRangeAsc = true;
        }
        wa.winref = 1;
        wa.partNumCols = 1;
        wa.partColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
        wa.partOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
        wa.partCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        if spec.order_by {
            wa.ordNumCols = 1;
            wa.ordColIdx = ::mcx::slice_borrow_in(mcx, &[2i16]).unwrap();
            wa.ordOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
            wa.ordCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        }
        wa.topWindow = true;

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
        pstmt.planTree = Some(wa.seal());
        pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
        pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    /// Stacked-windows plan (multiple window defs): WindowAgg(winref 2,
    /// ORDER BY g, default frame, sum(a)) over WindowAgg(winref 1,
    /// PARTITION BY g ORDER BY a, row_number) over Sort(g,a) over SeqScan.
    /// tlist = (g, a, rn, s2).
    fn mk_t2_stacked_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, relid: u32) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{Plan, Scan, SeqScan, Sort, WindowAgg};
        use ::types_nodes::primnodes::{WindowFunc, OUTER_VAR};

        let mk_ga = |varno: i32| {
            let g = Node::mk_var(mcx, varno, 1, INT4OID, -1, 0, 0).unwrap();
            let a = Node::mk_var(mcx, varno, 2, INT4OID, -1, 0, 0).unwrap();
            NodeList::make2(
                mcx,
                Node::mk_target_entry(mcx, g, 1, Some("g"), false).unwrap(),
                Node::mk_target_entry(mcx, a, 2, Some("a"), false).unwrap(),
            )
            .unwrap()
        };

        let scan = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan { targetlist: mk_ga(1), ..Default::default() },
                    scanrelid: 1,
                },
            },
        )
        .unwrap();

        let mut sort = Node::build::<Sort>(mcx).unwrap();
        sort.plan.targetlist = mk_ga(OUTER_VAR);
        sort.plan.lefttree = Some(scan);
        sort.numCols = 2;
        sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16, 2]).unwrap();
        sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT, INT4_LT]).unwrap();
        sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32, 0]).unwrap();
        sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false, false]).unwrap();

        // Bottom WindowAgg: winref 1, PARTITION BY g ORDER BY a, row_number.
        let mut rn = Node::build::<WindowFunc>(mcx).unwrap();
        rn.winfnoid = 3100;
        rn.wintype = INT8OID;
        rn.winref = 1;
        let mut bot_tlist = mk_ga(OUTER_VAR);
        bot_tlist
            .lappend(mcx, Node::mk_target_entry(mcx, rn.seal(), 3, Some("rn"), false).unwrap())
            .unwrap();
        let mut bot = Node::build::<WindowAgg>(mcx).unwrap();
        bot.plan.targetlist = bot_tlist;
        bot.plan.lefttree = Some(sort.seal());
        bot.winref = 1;
        bot.partNumCols = 1;
        bot.partColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
        bot.partOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
        bot.partCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        bot.ordNumCols = 1;
        bot.ordColIdx = ::mcx::slice_borrow_in(mcx, &[2i16]).unwrap();
        bot.ordOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
        bot.ordCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();

        // Top WindowAgg: winref 2, ORDER BY g (no partition), default frame,
        // sum(a) — peer-group stepping over the g groups.
        let mut sum = Node::build::<WindowFunc>(mcx).unwrap();
        sum.winfnoid = 2108;
        sum.wintype = INT8OID;
        sum.winref = 2;
        sum.winagg = true;
        sum.args =
            NodeList::make1(mcx, Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap())
                .unwrap();
        let mut top_tlist = mk_ga(OUTER_VAR);
        top_tlist
            .lappend(
                mcx,
                Node::mk_target_entry(
                    mcx,
                    Node::mk_var(mcx, OUTER_VAR, 3, INT8OID, -1, 0, 0).unwrap(),
                    3,
                    Some("rn"),
                    false,
                )
                .unwrap(),
            )
            .unwrap();
        top_tlist
            .lappend(mcx, Node::mk_target_entry(mcx, sum.seal(), 4, Some("s2"), false).unwrap())
            .unwrap();
        let mut top = Node::build::<WindowAgg>(mcx).unwrap();
        top.plan.targetlist = top_tlist;
        top.plan.lefttree = Some(bot.seal());
        top.winref = 2;
        top.ordNumCols = 1;
        top.ordColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
        top.ordOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
        top.ordCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        top.topWindow = true;

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
        pstmt.planTree = Some(top.seal());
        pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
        pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    /// Drain a typed row set: (g, a) then one `Cell` per extra column.
    fn drain_cells<'mcx>(
        ps: &mut crate::procnode::PlanStateNode<'mcx>,
        estate: &mut EStateData<'mcx>,
        tys: &[Ty],
    ) -> Vec<(i32, i32, Vec<Cell>)> {
        let mut got = Vec::new();
        while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            assert!(!base.tts_isnull[0] && !base.tts_isnull[1]);
            let mut cells = Vec::new();
            for (i, ty) in tys.iter().enumerate() {
                let col = 2 + i;
                cells.push(if base.tts_isnull[col] {
                    Cell::Null
                } else {
                    match ty {
                        Ty::I32 => Cell::I(base.tts_values[col].as_i32() as i64),
                        Ty::I64 => Cell::I(base.tts_values[col].as_i64()),
                    }
                });
            }
            got.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i32(), cells));
        }
        got
    }

    fn run_t2(
        mk: &dyn Fn(::mcx::Mcx<'static>) -> &'static PlannedStmt<'static>,
        tys: &[Ty],
        rescan: bool,
    ) -> Vec<Vec<(i32, i32, Vec<Cell>)>> {
        let pstmt = mk(leaked_mcx());
        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            let mut runs = vec![drain_cells(ps, estate, tys)];
            if rescan {
                crate::execami::exec_re_scan(ps, estate).unwrap();
                runs.push(drain_cells(ps, estate, tys));
            }
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
            runs
        })
    }

    /// The T2 A/B round: knob OFF (row engine) vs ON (T2-A delegation),
    /// identical rows demanded; the ON arm must tick the T2 probe and must
    /// NOT tick the W1 probe (the W1 knob stays OFF here — the interplay
    /// test drives both). Caller holds the scanfix TEST_LOCK.
    fn ab_t2(
        mk: impl Fn(::mcx::Mcx<'static>) -> &'static PlannedStmt<'static>,
        tys: &[Ty],
        rescan: bool,
    ) -> Vec<Vec<(i32, i32, Vec<Cell>)>> {
        use std::sync::atomic::Ordering::Relaxed;
        // Seams are set-once process globals: the pg_proc rows the
        // moving-frame volatility probe needs live in the SHARED
        // rowmode_ab installer (superset closure; see its doc).
        super::rowmode_ab::install_rowmode_seams();
        crate::lanev2::windows_set_for_tests(false);
        crate::lanev2::windows_t2_set_for_tests(false);
        let off = run_t2(&mk, tys, rescan);
        crate::lanev2::windows_t2_set_for_tests(true);
        let t2_before = crate::lanev2::WINDOWS_T2_OWNED_FOR_TESTS.load(Relaxed);
        let w1_before = crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(Relaxed);
        let on = run_t2(&mk, tys, rescan);
        let t2_after = crate::lanev2::WINDOWS_T2_OWNED_FOR_TESTS.load(Relaxed);
        let w1_after = crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(Relaxed);
        crate::lanev2::windows_t2_set_for_tests(false);
        assert_eq!(off, on, "knob OFF vs ON must be identical");
        assert!(t2_after > t2_before, "ON arm never engaged the T2 windows lane");
        assert_eq!(w1_after, w1_before, "T2 arm ticked the W1 probe (W1 knob is OFF)");
        off
    }

    /// Sorted-view rows zipped with per-row cells: the shared fixture shape.
    fn want(cells: &[&[Cell]]) -> Vec<(i32, i32, Vec<Cell>)> {
        let sorted: &[(i32, i32)] =
            &[(1, 10), (1, 10), (1, 20), (1, 30), (2, 5), (2, 6), (3, 7)];
        sorted
            .iter()
            .zip(cells.iter())
            .map(|(&(g, a), &c)| (g, a, c.to_vec()))
            .collect()
    }

    const ROWS_SLIDING: i32 = FRAMEOPTION_NONDEFAULT
        | FRAMEOPTION_ROWS
        | FRAMEOPTION_BETWEEN
        | FRAMEOPTION_START_OFFSET_PRECEDING
        | FRAMEOPTION_END_OFFSET_FOLLOWING;

    /// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING sum(a): the moving frame
    /// head drives the MovingIntSum INVERSE kernel (fixture-verified vs
    /// PostgreSQL 18.3).
    #[test]
    fn windows_t2_ab_rows_sliding_inverse() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70160;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let mut spec = T2Spec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(20)], &[I(40)], &[I(60)], &[I(50)], &[I(11)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING sum(a).
    #[test]
    fn windows_t2_ab_rows_current_to_unbounded_following() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70161;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let spec = T2Spec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_ROWS
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_CURRENT_ROW
                | FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
        );
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(70)], &[I(60)], &[I(50)], &[I(30)], &[I(11)], &[I(6)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING: empty head frames — the
    /// strict sum yields NULL on each partition's first row.
    #[test]
    fn windows_t2_ab_rows_offset_preceding_pair() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70162;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let mut spec = T2Spec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_ROWS
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_OFFSET_PRECEDING
                | FRAMEOPTION_END_OFFSET_PRECEDING,
        );
        spec.start_off_i64 = Some(3);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[Null], &[I(10)], &[I(20)], &[I(40)], &[Null], &[I(5)], &[Null]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING (in_range(int4,int4,int4)
    /// = 4128 on the int4 ORDER BY column).
    #[test]
    fn windows_t2_ab_range_offset() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70163;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let mut spec = T2Spec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_RANGE
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_OFFSET_PRECEDING
                | FRAMEOPTION_END_OFFSET_FOLLOWING,
        );
        spec.range_off_i32 = Some((2, 2));
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(20)], &[I(20)], &[I(20)], &[I(30)], &[I(11)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING (peer-group grain).
    #[test]
    fn windows_t2_ab_groups_offset() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70164;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let mut spec = T2Spec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_GROUPS
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_OFFSET_PRECEDING
                | FRAMEOPTION_END_OFFSET_FOLLOWING,
        );
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(40)], &[I(40)], &[I(70)], &[I(50)], &[I(11)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW
    /// (single-row partition -> empty frame -> NULL).
    #[test]
    fn windows_t2_ab_exclude_current_row() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70165;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let mut spec = T2Spec::framed(ROWS_SLIDING | FRAMEOPTION_EXCLUDE_CURRENT_ROW);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(10)], &[I(30)], &[I(40)], &[I(20)], &[I(6)], &[I(5)], &[Null]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE
    /// GROUP (whole-partition frame minus the current peer group).
    #[test]
    fn windows_t2_ab_exclude_group() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70166;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let spec = T2Spec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_ROWS
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_UNBOUNDED_PRECEDING
                | FRAMEOPTION_END_UNBOUNDED_FOLLOWING
                | FRAMEOPTION_EXCLUDE_GROUP,
        );
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(50)], &[I(50)], &[I(50)], &[I(40)], &[I(6)], &[I(5)], &[Null]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES (the
    /// default-frame extent with the current row's peers excluded, current
    /// row kept).
    #[test]
    fn windows_t2_ab_exclude_ties() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70167;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let spec = T2Spec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_RANGE
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_UNBOUNDED_PRECEDING
                | FRAMEOPTION_END_CURRENT_ROW
                | FRAMEOPTION_EXCLUDE_TIES,
        );
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(10)], &[I(10)], &[I(40)], &[I(70)], &[I(5)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// lag(a) + lead(a, 1, -1) under the DEFAULT frame: a W1
    /// shape-census refusal (LeadLag is not in the W1 set) that T2-A hosts —
    /// with head NULLs from lag and the lead default at partition tails.
    #[test]
    fn windows_t2_ab_lead_lag() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70168;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let spec = T2Spec {
            frame_options: FRAMEOPTION_DEFAULTS,
            start_off_i64: None,
            end_off_i64: None,
            range_off_i32: None,
            order_by: true,
            fns: &[
                T2Fn {
                    fnoid: 3106,
                    wintype: INT4OID,
                    winagg: false,
                    args: T2Args::A,
                    filter_a_lt: None,
                },
                T2Fn {
                    fnoid: 3111,
                    wintype: INT4OID,
                    winagg: false,
                    args: T2Args::AOffDef(1, -1),
                    filter_a_lt: None,
                },
            ],
        };
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I32, Ty::I32], false);
        let w = want(&[
            &[Null, I(10)],
            &[I(10), I(20)],
            &[I(10), I(30)],
            &[I(20), I(-1)],
            &[Null, I(6)],
            &[I(5), I(-1)],
            &[Null, I(-1)],
        ]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// first_value/last_value/nth_value(a, 2) over ROWS BETWEEN 1 PRECEDING
    /// AND 1 FOLLOWING (nth NULL where the frame has fewer than 2 rows).
    #[test]
    fn windows_t2_ab_first_last_nth() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70169;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let mut spec = T2Spec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        spec.fns = &[
            T2Fn {
                fnoid: 3112,
                wintype: INT4OID,
                winagg: false,
                args: T2Args::A,
                filter_a_lt: None,
            },
            T2Fn {
                fnoid: 3113,
                wintype: INT4OID,
                winagg: false,
                args: T2Args::A,
                filter_a_lt: None,
            },
            T2Fn {
                fnoid: 3114,
                wintype: INT4OID,
                winagg: false,
                args: T2Args::AOff(2),
                filter_a_lt: None,
            },
        ];
        let runs =
            ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I32, Ty::I32, Ty::I32], false);
        let w = want(&[
            &[I(10), I(10), I(10)],
            &[I(10), I(20), I(10)],
            &[I(10), I(30), I(20)],
            &[I(20), I(30), I(30)],
            &[I(5), I(6), I(6)],
            &[I(5), I(6), I(6)],
            &[I(7), I(7), Null],
        ]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// THE W8-RETIREMENT GATE (contract §6 WS-M amendment 5): sum(a) FILTER
    /// (WHERE a < 15) under the default frame, A/B-identical and
    /// lane-hosted. The stale "FILTER is a loud panic at init" note dies
    /// with this test (nodewindowagg lib.rs:5 fixed in the same commit).
    #[test]
    fn windows_t2_ab_filter() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70170;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let spec = T2Spec {
            frame_options: FRAMEOPTION_DEFAULTS,
            start_off_i64: None,
            end_off_i64: None,
            range_off_i32: None,
            order_by: true,
            fns: &[T2Fn {
                fnoid: 2108,
                wintype: INT8OID,
                winagg: true,
                args: T2Args::A,
                filter_a_lt: Some(15),
            }],
        };
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(20)], &[I(20)], &[I(20)], &[I(20)], &[I(5)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// ntile(2) under the default frame (W2 family, whole-partition count).
    #[test]
    fn windows_t2_ab_ntile() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70171;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let spec = T2Spec {
            frame_options: FRAMEOPTION_DEFAULTS,
            start_off_i64: None,
            end_off_i64: None,
            range_off_i32: None,
            order_by: true,
            fns: &[T2Fn {
                fnoid: 3105,
                wintype: INT4OID,
                winagg: false,
                args: T2Args::N(2),
                filter_a_lt: None,
            }],
        };
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I32], false);
        let w = want(&[&[I(1)], &[I(1)], &[I(2)], &[I(2)], &[I(1)], &[I(2)], &[I(1)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// Stacked WindowAgg nodes (multiple window defs): T2 hosts BOTH nodes
    /// per pull — the top drive's child pull recurses into the bottom
    /// node's own T2-owned arm.
    #[test]
    fn windows_t2_ab_stacked_windows() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70172;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let runs = ab_t2(|mcx| mk_t2_stacked_pstmt(mcx, relid), &[Ty::I64, Ty::I64], false);
        let w = want(&[
            &[I(1), I(70)],
            &[I(2), I(70)],
            &[I(3), I(70)],
            &[I(4), I(70)],
            &[I(1), I(81)],
            &[I(2), I(81)],
            &[I(1), I(88)],
        ]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// Rescan replay under T2 (per-pull ownership; ExecReScanWindowAgg
    /// resets the node's own state — the delegation holds none).
    #[test]
    fn windows_t2_ab_rescan_replays() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70173;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let mut spec = T2Spec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], true);
        assert_eq!(runs[0], runs[1], "rescan must replay the first run exactly");
        scanfix::quiesced();
    }

    /// Empty input under a framed shape: zero rows, both arms.
    #[test]
    fn windows_t2_ab_empty_input() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70174;
        scanfix::register_table_2col(relid, &[]);
        let mut spec = T2Spec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2(|mcx| mk_t2_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        assert_eq!(runs, vec![Vec::new()]);
        scanfix::quiesced();
    }

    /// HOOK-ORDER INTERPLAY: a W1-admissible default-frame shape.
    /// (a) both knobs ON: the sticky W1 batch drive wins — the W1 probe
    ///     ticks, the T2 probe must NOT (T2 never hijacks W1's shapes);
    /// (b) W1 OFF + T2 ON: the delegation hosts the same shape;
    /// all arms byte-identical to knob-OFF.
    #[test]
    fn windows_t2_ab_w1_owns_first() {
        use std::sync::atomic::Ordering::Relaxed;
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 70175;
        scanfix::register_table_2col(relid, &[T2_ROWS]);
        let run = || {
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            let pstmt = mk_windowagg_pstmt(leaked_mcx(), relid, true);
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let ExecData { estate, planstate } = data;
                let ps = planstate.as_mut().unwrap();
                let rows = drain_window_rows(ps, estate);
                crate::exec_end_node(ps, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
                rows
            })
        };
        crate::lanev2::windows_set_for_tests(false);
        crate::lanev2::windows_t2_set_for_tests(false);
        let off = run();
        // (a) Both ON: W1 wins, T2 silent.
        crate::lanev2::windows_set_for_tests(true);
        crate::lanev2::windows_t2_set_for_tests(true);
        let t2_before = crate::lanev2::WINDOWS_T2_OWNED_FOR_TESTS.load(Relaxed);
        let w1_before = crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(Relaxed);
        let both = run();
        assert!(
            crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(Relaxed) > w1_before,
            "W1 must own its admitted shape with both knobs on"
        );
        assert_eq!(
            crate::lanev2::WINDOWS_T2_OWNED_FOR_TESTS.load(Relaxed),
            t2_before,
            "T2 hijacked a W1-owned shape (hook order broken)"
        );
        // (b) W1 OFF, T2 ON: the delegation hosts it.
        crate::lanev2::windows_set_for_tests(false);
        let t2_before = crate::lanev2::WINDOWS_T2_OWNED_FOR_TESTS.load(Relaxed);
        let t2_only = run();
        assert!(
            crate::lanev2::WINDOWS_T2_OWNED_FOR_TESTS.load(Relaxed) > t2_before,
            "T2 must host the shape once W1 is off"
        );
        crate::lanev2::windows_t2_set_for_tests(false);
        assert_eq!(off, both, "both-knobs arm diverged");
        assert_eq!(off, t2_only, "T2-only arm diverged");
        scanfix::quiesced();
    }
}

// ===========================================================================
// Wave-2 WS-N inc-1: DML lane A/B corpus (lanev2/dml.rs try_own_modify_table
// behind PGRUST_LANE_V2_DML). Fake-oid band 75001+ (this module's claim per
// the Phase-1 integration precedent — fixture oid bands are a shared
// namespace).
//
// SCOPE HONESTY (contract cross-cutting law: mutation-class shapes prove via
// serial e2e): the in-process fixtures are READ-ONLY fake heaps — the write
// seams (bufmgr dirty path, WAL, xact) are deliberately absent, and
// importing nodemodifytable's write fixture here would collide with
// scanfix's set-once read seams (the composition hazard the integration
// record documents). These units therefore prove the HOST — gate order,
// knob-OFF silence, engagement, DmlShape refusal accounting, end-of-set
// parity — on ZERO-ROW insert plans (empty source ⇒ no write path is ever
// entered, on either engine). Real-write parity (rows, WAL, command tags,
// RETURNING) is scripts/lane-dml-e2e.sh + scripts/dualexec/corpus-dml-insert
// .sql (post-mutation content SELECTs per the silent-row-loss law) plus the
// nodemodifytable integration suite run knob-ON (notes/se-ws-n-dml-inc1.md).
// ===========================================================================
mod dml_ab {
    use super::*;

    /// Serializes this module's tests: they mutate the process-global DML
    /// knob and read the shared engagement/refusal probe deltas.
    static KNOB: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// CheckValidResultRel probes replica identity on the result relation;
    /// the fixture has no publications, so every command is valid. Shared
    /// set-once installer (integration-record precedent: any module needing
    /// this seam MUST come through here).
    /// [pub(super): visibility-only WS-T wave-3 edit so the dml_ab_wave3
    /// region comes through THIS installer instead of double-setting the
    /// seam — flagged to the reconciler in notes/se-ws-t-dml-inc2.md.]
    pub(super) fn install_replica_identity_seam() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            execreplication_seams::check_cmd_replica_identity::set(|_mcx, _rel, _cmd| Ok(()));
            // exec_get_range_table_relation's lock-held debug probe: the
            // fixture's fake result relation carries no real lock table.
            lmgr_seams::check_relation_locked_by_me::set(|_relid, _mode, _orstronger| true);
        });
    }

    /// `INSERT INTO target SELECT c1, c2 FROM source` — the planner's
    /// INSERT..SELECT plan shape: `ModifyTable(INSERT)` over a SeqScan;
    /// rtable = [result rel (RowExclusiveLock, INSERT perms), source rel
    /// (AccessShareLock, SELECT perms)]. `on_conflict_nothing` decorates the
    /// same plan with ONCONFLICT_NOTHING (arbiter-less DO NOTHING) for the
    /// DmlShape refusal leg.
    /// [pub(super): visibility-only WS-T wave-3 edit — the dml_ab_wave3
    /// region reuses this exact fixture for the lane-fed feed A/B; flagged
    /// to the reconciler in notes/se-ws-t-dml-inc2.md.]
    pub(super) fn mk_insert_select_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        target: u32,
        source: u32,
        on_conflict_nothing: bool,
    ) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{ModifyTable, Plan, Scan, SeqScan};

        let scan_tlist = {
            let a = Node::mk_var(mcx, 2, 1, INT4OID, -1, 0, 0).unwrap();
            let b = Node::mk_var(mcx, 2, 2, INT4OID, -1, 0, 0).unwrap();
            NodeList::make2(
                mcx,
                Node::mk_target_entry(mcx, a, 1, Some("c1"), false).unwrap(),
                Node::mk_target_entry(mcx, b, 2, Some("c2"), false).unwrap(),
            )
            .unwrap()
        };
        let scan = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan { targetlist: scan_tlist, ..Default::default() },
                    scanrelid: 2,
                },
            },
        )
        .unwrap();

        let mut mt = Node::build::<ModifyTable>(mcx).unwrap();
        // No RETURNING: the node's own targetlist stays nil (setrefs leaves
        // it empty for plain DML).
        mt.plan.lefttree = Some(scan);
        mt.operation = CmdType::CMD_INSERT;
        mt.canSetTag = true;
        mt.nominalRelation = 1;
        mt.resultRelations = ::types_nodes::IntList::make1(mcx, 1).unwrap();
        if on_conflict_nothing {
            mt.onConflictAction =
                ::types_nodes::OnConflictAction::ONCONFLICT_NOTHING as u32;
        }

        let result_rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid: target,
                relkind: ::types_rel::RELKIND_RELATION,
                rellockmode: ::types_rel::RowExclusiveLock,
                perminfoindex: 1,
                inFromCl: false,
                ..Default::default()
            },
        )
        .unwrap();
        let source_rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid: source,
                relkind: ::types_rel::RELKIND_RELATION,
                rellockmode: ::types_rel::AccessShareLock,
                perminfoindex: 2,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap();
        let insert_perm = Node::mk(
            mcx,
            RTEPermissionInfo {
                relid: target,
                requiredPerms: ::types_nodes::parsenodes::ACL_INSERT,
                ..Default::default()
            },
        )
        .unwrap();
        let select_perm = Node::mk(
            mcx,
            RTEPermissionInfo {
                relid: source,
                requiredPerms: ::types_nodes::parsenodes::ACL_SELECT,
                ..Default::default()
            },
        )
        .unwrap();
        let mut rtable = NodeList::make1(mcx, result_rte).unwrap();
        rtable.lappend(mcx, source_rte).unwrap();
        let mut perms = NodeList::make1(mcx, insert_perm).unwrap();
        perms.lappend(mcx, select_perm).unwrap();
        let mut unpruned = Bitmapset::empty();
        unpruned.add_member(mcx, 1).unwrap();
        unpruned.add_member(mcx, 2).unwrap();

        let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
        pstmt.commandType = CmdType::CMD_INSERT;
        pstmt.canSetTag = true;
        pstmt.planTree = Some(mt.seal());
        pstmt.rtable = rtable;
        pstmt.permInfos = perms;
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    /// Drive the INSERT plan to completion (exec_proc_node until None — the
    /// ExecutePlan cadence for a RETURNING-less DML) and return
    /// es_processed. Both knob arms run these identical statements.
    fn run_insert(pstmt: &'static PlannedStmt<'static>) -> u64 {
        let snap_ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_INSERT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            while exec_proc_node(ps, estate).unwrap().is_some() {}
            let processed = estate.es_processed;
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
            processed
        })
    }

    fn probes() -> (u64, u64) {
        (
            crate::lanev2::DML_OWNED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed),
            crate::lanev2::DML_SHAPE_REFUSED_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Knob OFF vs ON over the admitted INSERT..SELECT shape (empty source):
    /// identical end-of-set behavior, es_processed 0 on both arms; the OFF
    /// arm ticks NOTHING (no pre-existing ModifyTable wholesale refuse —
    /// contract §2d) and the ON arm engages the DML drive.
    #[test]
    fn dml_ab_insert_select_owned() {
        install_seams();
        scanfix::install();
        install_replica_identity_seam();
        let target: u32 = 75001;
        let source: u32 = 75002;
        scanfix::register_table_2col(target, &[]);
        scanfix::register_table_2col(source, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = KNOB.lock().unwrap_or_else(|p| p.into_inner());

        crate::lanev2::dml_set_for_tests(false);
        let (owned0, refused0) = probes();
        let off = run_insert(mk_insert_select_pstmt(leaked_mcx(), target, source, false));
        let (owned1, refused1) = probes();
        assert_eq!(off, 0);
        assert_eq!(owned1, owned0, "knob OFF must not own");
        assert_eq!(refused1, refused0, "knob OFF must tick NOTHING (contract §2d)");

        crate::lanev2::dml_set_for_tests(true);
        let on = run_insert(mk_insert_select_pstmt(leaked_mcx(), target, source, false));
        let (owned2, refused2) = probes();
        crate::lanev2::dml_set_for_tests(false);
        assert_eq!(on, off, "knob OFF vs ON must behave identically");
        assert!(owned2 > owned1, "ON arm never engaged the DML drive");
        assert_eq!(refused2, refused1, "the admitted shape must not tick DmlShape");

        drop(guard);
        scanfix::quiesced();
    }

    /// The DmlShape refusal leg: the SAME plan decorated with an
    /// arbiter-less ON CONFLICT DO NOTHING must refuse loudly under the ON
    /// knob (owned stays flat, DmlShape ticks) and fall through to the
    /// unchanged Volcano arm.
    #[test]
    fn dml_ab_on_conflict_refuses_dml_shape() {
        install_seams();
        scanfix::install();
        install_replica_identity_seam();
        let target: u32 = 75003;
        let source: u32 = 75004;
        scanfix::register_table_2col(target, &[]);
        scanfix::register_table_2col(source, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = KNOB.lock().unwrap_or_else(|p| p.into_inner());

        crate::lanev2::dml_set_for_tests(true);
        let (owned0, refused0) = probes();
        let n = run_insert(mk_insert_select_pstmt(leaked_mcx(), target, source, true));
        let (owned1, refused1) = probes();
        crate::lanev2::dml_set_for_tests(false);
        assert_eq!(n, 0);
        assert_eq!(owned1, owned0, "ON CONFLICT must not be owned in inc-1");
        assert!(refused1 > refused0, "ON CONFLICT must tick DmlShape");

        drop(guard);
        scanfix::quiesced();
    }

    // --- WS-AA wave-7 sub-region (fusion inc-1a): rowchain admission A/B ---
    // DELETED at RB-R1 (SE18) with the stitched trigger-INSERT chain: the
    // trigger-arm truth table, the nested-knob inertness A/B, and the
    // chain-dispatch guard pins died with the machinery they pinned
    // (`mt_rowchain_trigger_admission` / the dml.rs chain dispatch).
    // Trigger-bearing DML now refuses the lane unconditionally again —
    // pinned by the shape-refusal probes above (the "triggers" arm).
    // --- end WS-AA wave-7 sub-region ---------------------------------------
}

// ============================================================================
// --- WS-Q wave-3 append region (se/wave3-refusals) --------------------------
// scans_t3_ab — the T3 SOURCE-form A/B unit corpus (wave-3 contract §6.Q;
// lanev2/tail_source.rs behind PGRUST_LANE_V2_SCANS_T3).
//
// Every A/B runs the SAME plan knob-OFF (oracle) then knob-ON and demands
// byte-identical rows plus MECHANISM-attributed engagement (the T3 probe
// arrays — the delegation tail's probe must NOT move when the source form
// owned the pull). Knob mutation serializes on the shared rowmode_ab::KNOB
// mutex (wave-2 precedent 3: one lock per process-global knob family).
// Fake-oid band 76001+ is WS-Q's (contract §3.3); the boarded units consume
// none of it (the FunctionScan fixtures have no relation RTE) — recorded in
// notes/se-ws-q-refusals.md. The five other shapes' A/B coverage rides
// scripts/dualexec/corpus-scans-t3.sql + scripts/lane-scans-t3-e2e.sh (the
// wave-2 WS-L split: units where hand-built plans are cheap, corpus + e2e
// for the AM-backed shapes).
// ============================================================================
mod scans_t3_ab {
    use super::*;
    use ::types_nodes::parsenodes::RangeTblFunction;
    use ::types_nodes::plannodes::{FunctionScan as FunctionScanPlan, Plan, Scan, Sort};
    use ::types_nodes::primnodes::{FuncExpr, OUTER_VAR};

    const F_GENERATE_SERIES_INT4: u32 = 1067;
    const F_GENERATE_SERIES_STEP_INT4: u32 = 1066;

    fn mk_gs_expr<'mcx>(mcx: ::mcx::Mcx<'mcx>, lo: i32, hi: i32, step: Option<i32>) -> Node<'mcx> {
        let mut fe = Node::build::<FuncExpr>(mcx).unwrap();
        fe.funcid = if step.is_some() { F_GENERATE_SERIES_STEP_INT4 } else { F_GENERATE_SERIES_INT4 };
        fe.funcresulttype = INT4OID;
        fe.funcretset = true;
        let mut args =
            NodeList::make2(mcx, mk_int4_const(mcx, lo), mk_int4_const(mcx, hi)).unwrap();
        if let Some(s) = step {
            args.lappend(mcx, mk_int4_const(mcx, s)).unwrap();
        }
        fe.args = args;
        fe.seal()
    }

    /// A bare `FunctionScan` over generate_series — the top-ranked T3 shape
    /// (inc-0 census), tlist = the scan's own column. No relation RTE: the
    /// ported init/exec bodies never consult the range table for a
    /// function RTE, so the fixture needs no rtable (the rowmode_ab no-FROM
    /// pattern).
    fn mk_fscan_node<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        lo: i32,
        hi: i32,
        step: Option<i32>,
    ) -> Node<'mcx> {
        let scan_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
        let scan_tle = Node::mk_target_entry(mcx, scan_var, 1, Some("g"), false).unwrap();
        let mut rtf = Node::build::<RangeTblFunction>(mcx).unwrap();
        rtf.funcexpr = Some(mk_gs_expr(mcx, lo, hi, step));
        rtf.funccolcount = 1;
        Node::mk(
            mcx,
            FunctionScanPlan {
                scan: Scan {
                    plan: Plan {
                        targetlist: NodeList::make1(mcx, scan_tle).unwrap(),
                        ..Default::default()
                    },
                    scanrelid: 1,
                },
                functions: NodeList::make1(mcx, rtf.seal()).unwrap(),
                funcordinality: false,
            },
        )
        .unwrap()
    }

    fn mk_fscan_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        lo: i32,
        hi: i32,
    ) -> &'mcx PlannedStmt<'mcx> {
        let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
        pstmt.commandType = CmdType::CMD_SELECT;
        pstmt.canSetTag = true;
        pstmt.planTree = Some(mk_fscan_node(mcx, lo, hi, None));
        pstmt.seal_ref()
    }

    /// Sort over a DESCENDING generate_series — the §6.Q inc-final
    /// composition shape: the memoized sort verdict must admit the T3
    /// child and the breaker feed must drain it (input 10,7,4,1 → output
    /// 1,4,7,10, so an unsorted pass-through cannot fake a pass).
    fn mk_sort_over_fscan_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>) -> &'mcx PlannedStmt<'mcx> {
        let outer_v = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let outer_tle = Node::mk_target_entry(mcx, outer_v, 1, Some("g"), false).unwrap();
        let mut sort = Node::build::<Sort>(mcx).unwrap();
        sort.plan.targetlist = NodeList::make1(mcx, outer_tle).unwrap();
        sort.plan.lefttree = Some(mk_fscan_node(mcx, 10, 1, Some(-3)));
        sort.numCols = 1;
        sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
        sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT]).unwrap();
        sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();

        let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
        pstmt.commandType = CmdType::CMD_SELECT;
        pstmt.canSetTag = true;
        pstmt.planTree = Some(sort.seal());
        pstmt.seal_ref()
    }

    /// Drain to completion collecting column-1 int4s; `rescan_after` fires
    /// one `exec_re_scan` mid-stream (the delegation-cadence replay probe).
    fn drain_g(
        pstmt: &'static PlannedStmt<'static>,
        rescan_after: Option<usize>,
    ) -> Vec<i32> {
        with_exec_data(pstmt, |data, pstmt| {
            let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
                .unwrap()
                .unwrap();
            let mut out = Vec::new();
            let mut rescan = rescan_after;
            loop {
                if rescan == Some(out.len()) {
                    rescan = None;
                    exec_re_scan(&mut ps, &mut data.estate).unwrap();
                }
                match exec_proc_node(&mut ps, &mut data.estate).unwrap() {
                    Some(slot_id) => {
                        let mut isnull = false;
                        let v = exectuples::slot_getattr(
                            data.estate.slot_mut(slot_id),
                            1,
                            &mut isnull,
                        );
                        assert!(!isnull);
                        out.push(v.as_i32());
                    }
                    None => break,
                }
            }
            crate::exec_end_node(&mut ps, &mut data.estate).unwrap();
            out
        })
    }

    fn t3_probe(name: &str) -> u64 {
        crate::lanev2::t3_owned_probe_for_tests(name)
    }

    fn t3_sort_probe(name: &str) -> u64 {
        crate::lanev2::t3_sort_child_probe_for_tests(name)
    }

    fn tail_probe(name: &str) -> u64 {
        crate::lanev2::tail_owned_probe_for_tests(name)
    }

    /// The per-shape force-off mask default-arms every T3 shape and the
    /// same-process levers roundtrip (the §2.1 per-shape-knob-before-A/B
    /// rule's unit face).
    #[test]
    fn t3_shape_mask_levers_roundtrip() {
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());
        for cls in [
            "functionscan",
            "tablefuncscan",
            "samplescan",
            "tidscan",
            "tidrangescan",
            "namedtuplestorescan",
        ] {
            crate::lanev2::scans_t3_shape_set_for_tests(cls, false);
            crate::lanev2::scans_t3_shape_set_for_tests(cls, true);
        }
        crate::lanev2::scans_t3_set_for_tests(true);
        crate::lanev2::scans_t3_set_for_tests(false);
        drop(guard);
    }

    /// Standalone source-form A/B (+ mid-stream rescan): knob OFF = Volcano
    /// oracle; knob ON = the batch-size-1 source drive owns every pull and
    /// the delegation tail's probe does NOT move (mechanism attribution).
    #[test]
    fn functionscan_t3_source_ab_with_rescan() {
        install_seams();
        rowmode_ab::install_rowmode_seams();
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        crate::lanev2::scans_t3_set_for_tests(false);
        let t3_before_off = t3_probe("functionscan");
        let off = drain_g(mk_fscan_pstmt(leaked_mcx(), 1, 5), Some(3));
        assert_eq!(
            t3_probe("functionscan"),
            t3_before_off,
            "knob OFF must never engage the T3 source form"
        );

        crate::lanev2::scans_t3_set_for_tests(true);
        let t3_before_on = t3_probe("functionscan");
        let tail_before_on = tail_probe("functionscan");
        let on = drain_g(mk_fscan_pstmt(leaked_mcx(), 1, 5), Some(3));
        let t3_after_on = t3_probe("functionscan");
        let tail_after_on = tail_probe("functionscan");
        crate::lanev2::scans_t3_set_for_tests(false);
        drop(guard);

        assert_eq!(off, on, "knob OFF vs ON must be identical");
        assert_eq!(off, vec![1, 2, 3, 1, 2, 3, 4, 5], "rescan replay from row 3");
        assert!(t3_after_on > t3_before_on, "ON arm never engaged the T3 source drive");
        assert_eq!(
            tail_after_on, tail_before_on,
            "the delegation tail must not tick when the source form owns"
        );
    }

    /// Per-shape force-off under an armed facility knob: T3 stays out, the
    /// ROWMODE delegation fallback owns instead (rollback semantics), rows
    /// byte-identical.
    #[test]
    fn functionscan_t3_force_off_falls_back_to_delegation() {
        install_seams();
        rowmode_ab::install_rowmode_seams();
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        crate::lanev2::scans_t3_set_for_tests(false);
        crate::lanev2::rowmode_set_for_tests(false);
        let off = drain_g(mk_fscan_pstmt(leaked_mcx(), 2, 6), None);

        crate::lanev2::scans_t3_set_for_tests(true);
        crate::lanev2::rowmode_set_for_tests(true);
        crate::lanev2::scans_t3_shape_set_for_tests("functionscan", false);
        let t3_before = t3_probe("functionscan");
        let tail_before = tail_probe("functionscan");
        let on = drain_g(mk_fscan_pstmt(leaked_mcx(), 2, 6), None);
        let t3_after = t3_probe("functionscan");
        let tail_after = tail_probe("functionscan");
        crate::lanev2::scans_t3_shape_set_for_tests("functionscan", true);
        crate::lanev2::scans_t3_set_for_tests(false);
        crate::lanev2::rowmode_set_for_tests(false);
        drop(guard);

        assert_eq!(off, on, "force-off arm must equal the oracle");
        assert_eq!(off, vec![2, 3, 4, 5, 6]);
        assert_eq!(t3_after, t3_before, "forced-off shape must not source-drive");
        assert!(tail_after > tail_before, "delegation fallback never engaged");
    }

    /// inc-final COMPOSITION: Sort over a T3 FunctionScan child. Knob ON,
    /// the memoized sort verdict admits the child (T3 sort-child probe
    /// moves) and the breaker feed drains the batch-size-1 source — output
    /// equals the Volcano oracle, sorted (input arrives descending, so a
    /// non-sorting fake cannot pass). A mid-stream `exec_re_scan` (after 2
    /// rows) exercises the Sort-over-T3 rescan cadence explicitly: the
    /// replay must restart the sorted stream from the top on BOTH arms
    /// (review finding #4's implicit-coverage gap, made direct).
    #[test]
    fn sort_over_functionscan_t3_composition_ab() {
        install_seams();
        rowmode_ab::install_rowmode_seams();
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        crate::lanev2::scans_t3_set_for_tests(false);
        let off = drain_g(mk_sort_over_fscan_pstmt(leaked_mcx()), Some(2));

        crate::lanev2::scans_t3_set_for_tests(true);
        let child_before = t3_sort_probe("functionscan");
        let on = drain_g(mk_sort_over_fscan_pstmt(leaked_mcx()), Some(2));
        let child_after = t3_sort_probe("functionscan");
        crate::lanev2::scans_t3_set_for_tests(false);
        drop(guard);

        assert_eq!(off, on, "sort-over-T3 knob OFF vs ON must be identical");
        assert_eq!(
            off,
            vec![1, 4, 1, 4, 7, 10],
            "descending feed must come back sorted, replayed from the top after rescan"
        );
        assert!(
            child_after > child_before,
            "ON arm never admitted the T3 sort child (composition did not engage)"
        );
    }

    /// EPQ law (§4.2) direct unit: `es_epq_active` is the FIRST dynamic
    /// gate after the knob gates — an EPQ-flagged estate must be refused
    /// to the incumbent BEFORE any scan work (T3 probe untouched; the
    /// delegation tail — knob OFF here — must not tick either). The gate is
    /// per-pull: dropping the flag re-admits and the SAME node source-drives
    /// to completion. Direct-hook probe form: the express
    /// `express_epq_pull_refused_dynamically` precedent (a full EPQ pull
    /// can't run in this harness; the refusal must happen before it would
    /// matter).
    #[test]
    fn functionscan_t3_epq_pull_refused_dynamically() {
        install_seams();
        rowmode_ab::install_rowmode_seams();
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        crate::lanev2::scans_t3_set_for_tests(true);
        let pstmt = mk_fscan_pstmt(leaked_mcx(), 1, 4);
        with_exec_data(pstmt, |data, pstmt| {
            let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
                .unwrap()
                .unwrap();
            data.estate.es_epq_active = true;
            let t3_before = t3_probe("functionscan");
            let tail_before = tail_probe("functionscan");
            {
                let crate::procnode::PlanStateNode::FunctionScan(fs) = &mut ps else {
                    panic!("fscan plan did not init a FunctionScan node")
                };
                let r = crate::lanev2::try_own_function_scan(fs, &mut data.estate).unwrap();
                assert!(r.is_none(), "EPQ pull must be refused to the incumbent");
            }
            assert_eq!(
                t3_probe("functionscan"),
                t3_before,
                "EPQ pull must not be T3-source-owned"
            );
            assert_eq!(
                tail_probe("functionscan"),
                tail_before,
                "EPQ pull must not fall through into the delegation tail"
            );
            data.estate.es_epq_active = false;
            let mut out = Vec::new();
            while let Some(slot_id) = exec_proc_node(&mut ps, &mut data.estate).unwrap() {
                let mut isnull = false;
                let v = exectuples::slot_getattr(
                    data.estate.slot_mut(slot_id),
                    1,
                    &mut isnull,
                );
                assert!(!isnull);
                out.push(v.as_i32());
            }
            assert_eq!(out, vec![1, 2, 3, 4]);
            assert!(
                t3_probe("functionscan") > t3_before,
                "post-EPQ pulls must re-engage the T3 source drive"
            );
            crate::exec_end_node(&mut ps, &mut data.estate).unwrap();
        });
        crate::lanev2::scans_t3_set_for_tests(false);
        drop(guard);
    }
}
// --- end WS-Q wave-3 append region ------------------------------------------

// ===========================================================================
// --- WS-R wave-3 (T2-B) A/B corpus — append-only region ---
// Wave-3 WS-R inc-2: the sealed FRAMED batch drive (lanev2/windows.rs
// try_own_window_agg_t2b behind PGRUST_LANE_V2_WINDOWS_T2B). Fake-oid band
// 77001+ (the contract §3.3 WS-R claim). The plan builders are deliberate
// self-contained mirrors of windows_t2_ab's (shared-append law: no edits in
// another WS's region); the `want` vectors for identical specs are REUSED
// from that module — same fixture rows, same plans, C-VERIFIED against
// PostgreSQL 18.3 by WS-M.
// ===========================================================================
mod windows_t2b_ab {
    use super::*;
    use ::types_nodes::rawnodes::{
        FRAMEOPTION_BETWEEN, FRAMEOPTION_DEFAULTS, FRAMEOPTION_END_CURRENT_ROW,
        FRAMEOPTION_END_OFFSET_FOLLOWING, FRAMEOPTION_END_OFFSET_PRECEDING,
        FRAMEOPTION_END_UNBOUNDED_FOLLOWING, FRAMEOPTION_EXCLUDE_CURRENT_ROW,
        FRAMEOPTION_EXCLUDE_GROUP, FRAMEOPTION_EXCLUDE_TIES, FRAMEOPTION_GROUPS,
        FRAMEOPTION_NONDEFAULT, FRAMEOPTION_RANGE, FRAMEOPTION_ROWS,
        FRAMEOPTION_START_CURRENT_ROW, FRAMEOPTION_START_OFFSET_PRECEDING,
        FRAMEOPTION_START_UNBOUNDED_PRECEDING,
    };

    /// The shared unit relation: (g, a) registered UNSORTED (the Sort under
    /// the WindowAgg orders by (g, a)). Sorted view:
    /// (1,10) (1,10) (1,20) (1,30) | (2,5) (2,6) | (3,7).
    const B_ROWS: &[(i32, i32)] =
        &[(2, 5), (1, 10), (3, 7), (1, 20), (2, 6), (1, 10), (1, 30)];

    #[derive(Clone, Copy)]
    enum BArgs {
        /// No arguments (count(*)).
        NoArgs,
        A,
        AOff(i32),
        AOffDef(i32, i32),
        N(i32),
    }

    #[derive(Clone, Copy)]
    struct BFn {
        fnoid: u32,
        wintype: u32,
        winagg: bool,
        args: BArgs,
        filter_a_lt: Option<i32>,
    }

    const SUM_A: &[BFn] = &[BFn {
        fnoid: 2108,
        wintype: INT8OID,
        winagg: true,
        args: BArgs::A,
        filter_a_lt: None,
    }];

    struct BSpec {
        frame_options: i32,
        start_off_i64: Option<i64>,
        end_off_i64: Option<i64>,
        range_off_i32: Option<(i32, i32)>,
        order_by: bool,
        /// PARTITION BY g (the module default); `false` = no PARTITION BY —
        /// the whole input is ONE partition (partNumCols == 0 accept path).
        partition: bool,
        /// Always-true plan qual `g < k` (rows identical either way): the
        /// T2-B SEAL refusal probe (ShapeQualProj; review finding 2).
        qual_g_lt: Option<i32>,
        fns: &'static [BFn],
    }

    impl BSpec {
        const fn framed(frame_options: i32) -> Self {
            BSpec {
                frame_options,
                start_off_i64: None,
                end_off_i64: None,
                range_off_i32: None,
                order_by: true,
                partition: true,
                qual_g_lt: None,
                fns: SUM_A,
            }
        }
    }

    #[derive(Clone, Copy)]
    enum Ty {
        I32,
        I64,
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Cell {
        Null,
        I(i64),
    }
    use Cell::{I, Null};

    /// Build WindowAgg(spec) over Sort(g,a) over SeqScan(relid) — the T2-B
    /// mirror of windows_t2_ab's builder (self-contained per the
    /// shared-append law). tlist = (g, a, <one column per spec.fns entry>).
    fn mk_t2b_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        relid: u32,
        spec: &BSpec,
    ) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{Plan, Scan, SeqScan, Sort, WindowAgg};
        use ::types_nodes::primnodes::{WindowFunc, OUTER_VAR};

        let mk_tlist = |varno: i32| {
            let g = Node::mk_var(mcx, varno, 1, INT4OID, -1, 0, 0).unwrap();
            let a = Node::mk_var(mcx, varno, 2, INT4OID, -1, 0, 0).unwrap();
            NodeList::make2(
                mcx,
                Node::mk_target_entry(mcx, g, 1, Some("g"), false).unwrap(),
                Node::mk_target_entry(mcx, a, 2, Some("a"), false).unwrap(),
            )
            .unwrap()
        };
        let i4 = |v: i32| {
            Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(v), false, true).unwrap()
        };
        let i8c = |v: i64| {
            Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::from_i64(v), false, true).unwrap()
        };

        let scan = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
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
        sort.numCols = 2;
        sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16, 2]).unwrap();
        sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT, INT4_LT]).unwrap();
        sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32, 0]).unwrap();
        sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false, false]).unwrap();

        let mut tlist = mk_tlist(OUTER_VAR);
        let a_var = || Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap();
        for (i, f) in spec.fns.iter().enumerate() {
            let mut w = Node::build::<WindowFunc>(mcx).unwrap();
            w.winfnoid = f.fnoid;
            w.wintype = f.wintype;
            w.winref = 1;
            w.winagg = f.winagg;
            w.args = match f.args {
                BArgs::NoArgs => NodeList::nil(),
                BArgs::A => NodeList::make1(mcx, a_var()).unwrap(),
                BArgs::AOff(k) => NodeList::make2(mcx, a_var(), i4(k)).unwrap(),
                BArgs::AOffDef(k, d) => {
                    NodeList::make3(mcx, a_var(), i4(k), i4(d)).unwrap()
                }
                BArgs::N(n) => NodeList::make1(mcx, i4(n)).unwrap(),
            };
            if let Some(k) = f.filter_a_lt {
                w.aggfilter = Some(
                    Node::mk(
                        mcx,
                        ::types_nodes::OpExpr {
                            opno: INT4_LT,
                            opfuncid: 66, // pg_proc int4lt
                            opresulttype: BOOLOID,
                            opretset: false,
                            opcollid: 0,
                            inputcollid: 0,
                            args: NodeList::make2(mcx, a_var(), i4(k)).unwrap(),
                            location: -1,
                        },
                    )
                    .unwrap(),
                );
            }
            tlist
                .lappend(
                    mcx,
                    Node::mk_target_entry(mcx, w.seal(), (3 + i) as i16, Some("w"), false)
                        .unwrap(),
                )
                .unwrap();
        }

        let mut wa = Node::build::<WindowAgg>(mcx).unwrap();
        wa.plan.targetlist = tlist;
        wa.plan.lefttree = Some(sort.seal());
        wa.frameOptions = spec.frame_options;
        if let Some(v) = spec.start_off_i64 {
            wa.startOffset = Some(i8c(v));
        }
        if let Some(v) = spec.end_off_i64 {
            wa.endOffset = Some(i8c(v));
        }
        if let Some((s, e)) = spec.range_off_i32 {
            wa.startOffset = Some(i4(s));
            wa.endOffset = Some(i4(e));
            wa.startInRangeFunc = 4128; // in_range(int4,int4,int4)
            wa.endInRangeFunc = 4128;
            wa.inRangeAsc = true;
        }
        wa.winref = 1;
        if spec.partition {
            wa.partNumCols = 1;
            wa.partColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
            wa.partOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
            wa.partCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        }
        if let Some(k) = spec.qual_g_lt {
            // WindowAgg plan qual over the OUTER (spooled-row) tuple; the
            // qual tail asserts topWindow, which the builder always sets.
            let g = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
            wa.plan.qual = NodeList::make1(
                mcx,
                Node::mk(
                    mcx,
                    ::types_nodes::OpExpr {
                        opno: INT4_LT,
                        opfuncid: 66, // pg_proc int4lt
                        opresulttype: BOOLOID,
                        opretset: false,
                        opcollid: 0,
                        inputcollid: 0,
                        args: NodeList::make2(mcx, g, i4(k)).unwrap(),
                        location: -1,
                    },
                )
                .unwrap(),
            )
            .unwrap();
        }
        if spec.order_by {
            wa.ordNumCols = 1;
            wa.ordColIdx = ::mcx::slice_borrow_in(mcx, &[2i16]).unwrap();
            wa.ordOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
            wa.ordCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        }
        wa.topWindow = true;

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
        pstmt.planTree = Some(wa.seal());
        pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
        pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    /// Drain a typed row set: (g, a) then one `Cell` per extra column.
    fn drain_cells<'mcx>(
        ps: &mut crate::procnode::PlanStateNode<'mcx>,
        estate: &mut EStateData<'mcx>,
        tys: &[Ty],
    ) -> Vec<(i32, i32, Vec<Cell>)> {
        let mut got = Vec::new();
        while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            assert!(!base.tts_isnull[0] && !base.tts_isnull[1]);
            let mut cells = Vec::new();
            for (i, ty) in tys.iter().enumerate() {
                let col = 2 + i;
                cells.push(if base.tts_isnull[col] {
                    Cell::Null
                } else {
                    match ty {
                        Ty::I32 => Cell::I(base.tts_values[col].as_i32() as i64),
                        Ty::I64 => Cell::I(base.tts_values[col].as_i64()),
                    }
                });
            }
            got.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i32(), cells));
        }
        got
    }

    fn run_t2b(
        mk: &dyn Fn(::mcx::Mcx<'static>) -> &'static PlannedStmt<'static>,
        tys: &[Ty],
        rescan: bool,
    ) -> Vec<Vec<(i32, i32, Vec<Cell>)>> {
        let pstmt = mk(leaked_mcx());
        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            let mut runs = vec![drain_cells(ps, estate, tys)];
            if rescan {
                crate::execami::exec_re_scan(ps, estate).unwrap();
                runs.push(drain_cells(ps, estate, tys));
            }
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
            runs
        })
    }

    /// The T2-B A/B round: knob OFF (row engine) vs ON (framed batch drive),
    /// identical rows demanded; the ON arm must tick the T2-B probe and must
    /// NOT tick the W1 or T2-A probes (both those knobs stay OFF here — the
    /// interplay tests drive the combinations). Caller holds the scanfix
    /// TEST_LOCK.
    fn ab_t2b(
        mk: impl Fn(::mcx::Mcx<'static>) -> &'static PlannedStmt<'static>,
        tys: &[Ty],
        rescan: bool,
    ) -> Vec<Vec<(i32, i32, Vec<Cell>)>> {
        use std::sync::atomic::Ordering::Relaxed;
        // Seams are set-once process globals: the pg_proc/pg_aggregate rows
        // the framed lane needs live in the SHARED rowmode_ab installer.
        super::rowmode_ab::install_rowmode_seams();
        crate::lanev2::windows_set_for_tests(false);
        crate::lanev2::windows_t2_set_for_tests(false);
        crate::lanev2::windows_t2b_set_for_tests(false);
        let off = run_t2b(&mk, tys, rescan);
        crate::lanev2::windows_t2b_set_for_tests(true);
        let t2b_before = crate::lanev2::WINDOWS_T2B_OWNED_FOR_TESTS.load(Relaxed);
        let t2_before = crate::lanev2::WINDOWS_T2_OWNED_FOR_TESTS.load(Relaxed);
        let w1_before = crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(Relaxed);
        let on = run_t2b(&mk, tys, rescan);
        let t2b_after = crate::lanev2::WINDOWS_T2B_OWNED_FOR_TESTS.load(Relaxed);
        let t2_after = crate::lanev2::WINDOWS_T2_OWNED_FOR_TESTS.load(Relaxed);
        let w1_after = crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(Relaxed);
        crate::lanev2::windows_t2b_set_for_tests(false);
        assert_eq!(off, on, "knob OFF vs ON must be identical");
        assert!(t2b_after > t2b_before, "ON arm never engaged the T2-B framed lane");
        assert_eq!(t2_after, t2_before, "T2-B arm ticked the T2-A probe (that knob is OFF)");
        assert_eq!(w1_after, w1_before, "T2-B arm ticked the W1 probe (that knob is OFF)");
        off
    }

    /// Sorted-view rows zipped with per-row cells: the shared fixture shape.
    fn want(cells: &[&[Cell]]) -> Vec<(i32, i32, Vec<Cell>)> {
        let sorted: &[(i32, i32)] =
            &[(1, 10), (1, 10), (1, 20), (1, 30), (2, 5), (2, 6), (3, 7)];
        sorted
            .iter()
            .zip(cells.iter())
            .map(|(&(g, a), &c)| (g, a, c.to_vec()))
            .collect()
    }

    const ROWS_SLIDING: i32 = FRAMEOPTION_NONDEFAULT
        | FRAMEOPTION_ROWS
        | FRAMEOPTION_BETWEEN
        | FRAMEOPTION_START_OFFSET_PRECEDING
        | FRAMEOPTION_END_OFFSET_FOLLOWING;

    /// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING sum(a): the moving frame
    /// head drives the node's MovingIntSum INVERSE kernel over the
    /// lane-buffered partition.
    #[test]
    fn windows_t2b_ab_rows_sliding_inverse() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77001;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(20)], &[I(40)], &[I(60)], &[I(50)], &[I(11)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING sum(a).
    #[test]
    fn windows_t2b_ab_rows_current_to_unbounded_following() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77002;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let spec = BSpec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_ROWS
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_CURRENT_ROW
                | FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
        );
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(70)], &[I(60)], &[I(50)], &[I(30)], &[I(11)], &[I(6)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING: empty head frames — the
    /// strict sum yields NULL on each partition's first row.
    #[test]
    fn windows_t2b_ab_rows_offset_preceding_pair() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77003;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_ROWS
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_OFFSET_PRECEDING
                | FRAMEOPTION_END_OFFSET_PRECEDING,
        );
        spec.start_off_i64 = Some(3);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[Null], &[I(10)], &[I(20)], &[I(40)], &[Null], &[I(5)], &[Null]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING (in_range(int4,int4,int4)
    /// = 4128 on the int4 ORDER BY column).
    #[test]
    fn windows_t2b_ab_range_offset() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77004;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_RANGE
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_OFFSET_PRECEDING
                | FRAMEOPTION_END_OFFSET_FOLLOWING,
        );
        spec.range_off_i32 = Some((2, 2));
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(20)], &[I(20)], &[I(20)], &[I(30)], &[I(11)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING (peer-group grain — the
    /// currentgroup tracking block of the transcribed loop body).
    #[test]
    fn windows_t2b_ab_groups_offset() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77005;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_GROUPS
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_OFFSET_PRECEDING
                | FRAMEOPTION_END_OFFSET_FOLLOWING,
        );
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(40)], &[I(40)], &[I(70)], &[I(50)], &[I(11)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW
    /// (single-row partition -> empty frame -> NULL).
    #[test]
    fn windows_t2b_ab_exclude_current_row() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77006;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(ROWS_SLIDING | FRAMEOPTION_EXCLUDE_CURRENT_ROW);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(10)], &[I(30)], &[I(40)], &[I(20)], &[I(6)], &[I(5)], &[Null]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE
    /// GROUP (whole-partition frame minus the current peer group).
    #[test]
    fn windows_t2b_ab_exclude_group() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77007;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let spec = BSpec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_ROWS
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_UNBOUNDED_PRECEDING
                | FRAMEOPTION_END_UNBOUNDED_FOLLOWING
                | FRAMEOPTION_EXCLUDE_GROUP,
        );
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(50)], &[I(50)], &[I(50)], &[I(40)], &[I(6)], &[I(5)], &[Null]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES (the
    /// default-frame extent with the current row's peers excluded, current
    /// row kept).
    #[test]
    fn windows_t2b_ab_exclude_ties() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77008;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let spec = BSpec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_RANGE
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_UNBOUNDED_PRECEDING
                | FRAMEOPTION_END_CURRENT_ROW
                | FRAMEOPTION_EXCLUDE_TIES,
        );
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(10)], &[I(10)], &[I(40)], &[I(70)], &[I(5)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// lag(a) + lead(a, 1, -1) under the DEFAULT frame: a W1 shape-census
    /// refusal that T2-B batch-hosts (value functions over the buffered
    /// partition).
    #[test]
    fn windows_t2b_ab_lead_lag() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77009;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let spec = BSpec {
            frame_options: FRAMEOPTION_DEFAULTS,
            start_off_i64: None,
            end_off_i64: None,
            range_off_i32: None,
            order_by: true,
            partition: true,
            qual_g_lt: None,
            fns: &[
                BFn {
                    fnoid: 3106,
                    wintype: INT4OID,
                    winagg: false,
                    args: BArgs::A,
                    filter_a_lt: None,
                },
                BFn {
                    fnoid: 3111,
                    wintype: INT4OID,
                    winagg: false,
                    args: BArgs::AOffDef(1, -1),
                    filter_a_lt: None,
                },
            ],
        };
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I32, Ty::I32], false);
        let w = want(&[
            &[Null, I(10)],
            &[I(10), I(20)],
            &[I(10), I(30)],
            &[I(20), I(-1)],
            &[Null, I(6)],
            &[I(5), I(-1)],
            &[Null, I(-1)],
        ]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// first_value/last_value/nth_value(a, 2) over ROWS BETWEEN 1 PRECEDING
    /// AND 1 FOLLOWING (nth NULL where the frame has fewer than 2 rows).
    #[test]
    fn windows_t2b_ab_first_last_nth() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77010;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        spec.fns = &[
            BFn {
                fnoid: 3112,
                wintype: INT4OID,
                winagg: false,
                args: BArgs::A,
                filter_a_lt: None,
            },
            BFn {
                fnoid: 3113,
                wintype: INT4OID,
                winagg: false,
                args: BArgs::A,
                filter_a_lt: None,
            },
            BFn {
                fnoid: 3114,
                wintype: INT4OID,
                winagg: false,
                args: BArgs::AOff(2),
                filter_a_lt: None,
            },
        ];
        let runs =
            ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I32, Ty::I32, Ty::I32], false);
        let w = want(&[
            &[I(10), I(10), I(10)],
            &[I(10), I(20), I(10)],
            &[I(10), I(30), I(20)],
            &[I(20), I(30), I(30)],
            &[I(5), I(6), I(6)],
            &[I(5), I(6), I(6)],
            &[I(7), I(7), Null],
        ]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// sum(a) FILTER (WHERE a < 15) under the default frame — T2-B hosts
    /// FILTER shapes W1's census refuses.
    #[test]
    fn windows_t2b_ab_filter() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77011;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let spec = BSpec {
            frame_options: FRAMEOPTION_DEFAULTS,
            start_off_i64: None,
            end_off_i64: None,
            range_off_i32: None,
            order_by: true,
            partition: true,
            qual_g_lt: None,
            fns: &[BFn {
                fnoid: 2108,
                wintype: INT8OID,
                winagg: true,
                args: BArgs::A,
                filter_a_lt: Some(15),
            }],
        };
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(20)], &[I(20)], &[I(20)], &[I(20)], &[I(5)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// ntile(2) under the default frame (whole-partition row count over the
    /// fully-spooled buffer).
    #[test]
    fn windows_t2b_ab_ntile() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77012;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let spec = BSpec {
            frame_options: FRAMEOPTION_DEFAULTS,
            start_off_i64: None,
            end_off_i64: None,
            range_off_i32: None,
            order_by: true,
            partition: true,
            qual_g_lt: None,
            fns: &[BFn {
                fnoid: 3105,
                wintype: INT4OID,
                winagg: false,
                args: BArgs::N(2),
                filter_a_lt: None,
            }],
        };
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I32], false);
        let w = want(&[&[I(1)], &[I(1)], &[I(2)], &[I(2)], &[I(1)], &[I(2)], &[I(1)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// A W1-ADMISSIBLE default-frame sum: with W1 OFF, T2-B batch-hosts it
    /// through the node's own eval_windowaggregates_default over the
    /// lane-buffered partition.
    #[test]
    fn windows_t2b_ab_hosts_default_frame() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77013;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let spec = BSpec {
            frame_options: FRAMEOPTION_DEFAULTS,
            start_off_i64: None,
            end_off_i64: None,
            range_off_i32: None,
            order_by: true,
            partition: true,
            qual_g_lt: None,
            fns: SUM_A,
        };
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let w = want(&[&[I(20)], &[I(20)], &[I(40)], &[I(70)], &[I(5)], &[I(11)], &[I(7)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// Rescan replay under T2-B (sticky drive: exec_rescan_window_agg resets
    /// the node machine, the execami arm forgets the drive phase, the sort
    /// re-feeds, frame offsets re-evaluate).
    #[test]
    fn windows_t2b_ab_rescan_replays() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77014;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], true);
        assert_eq!(runs[0], runs[1], "rescan must replay the first run exactly");
        scanfix::quiesced();
    }

    /// Empty input under a framed shape: zero rows, both arms (the machine
    /// never begins a partition; input_done marks Done directly).
    #[test]
    fn windows_t2b_ab_empty_input() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77015;
        scanfix::register_table_2col(relid, &[]);
        let mut spec = BSpec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        assert_eq!(runs, vec![Vec::new()]);
        scanfix::quiesced();
    }

    /// Moving count(*) + sum(a) over ROWS BETWEEN 1 PRECEDING AND 1
    /// FOLLOWING: the un-stubbed count(*) moving-agg fixture columns
    /// (int8inc 1219 / int8dec 3546 — the WS-M TODO-7 item, contract §6.R
    /// inc-3) drive the MovingByVal INVERSE kernel beside MovingIntSum.
    #[test]
    fn windows_t2b_ab_moving_count_star() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77018;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        spec.fns = &[
            BFn {
                fnoid: 2803,
                wintype: INT8OID,
                winagg: true,
                args: BArgs::NoArgs,
                filter_a_lt: None,
            },
            BFn {
                fnoid: 2108,
                wintype: INT8OID,
                winagg: true,
                args: BArgs::A,
                filter_a_lt: None,
            },
        ];
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64, Ty::I64], false);
        let w = want(&[
            &[I(2), I(20)],
            &[I(3), I(40)],
            &[I(3), I(60)],
            &[I(2), I(50)],
            &[I(2), I(11)],
            &[I(2), I(11)],
            &[I(1), I(7)],
        ]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// Moving count(*) vs strict sum over EMPTY frames (ROWS BETWEEN 3
    /// PRECEDING AND 1 PRECEDING): count answers 0 on an empty frame (its
    /// moving initval), the strict sum answers NULL.
    #[test]
    fn windows_t2b_ab_moving_count_empty_frames() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77019;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(
            FRAMEOPTION_NONDEFAULT
                | FRAMEOPTION_ROWS
                | FRAMEOPTION_BETWEEN
                | FRAMEOPTION_START_OFFSET_PRECEDING
                | FRAMEOPTION_END_OFFSET_PRECEDING,
        );
        spec.start_off_i64 = Some(3);
        spec.end_off_i64 = Some(1);
        spec.fns = &[
            BFn {
                fnoid: 2803,
                wintype: INT8OID,
                winagg: true,
                args: BArgs::NoArgs,
                filter_a_lt: None,
            },
            BFn {
                fnoid: 2108,
                wintype: INT8OID,
                winagg: true,
                args: BArgs::A,
                filter_a_lt: None,
            },
        ];
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64, Ty::I64], false);
        let w = want(&[
            &[I(0), Null],
            &[I(1), I(10)],
            &[I(2), I(20)],
            &[I(3), I(40)],
            &[I(0), Null],
            &[I(1), I(5)],
            &[I(0), Null],
        ]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }

    /// HOOK-ORDER INTERPLAY (W1 side): a W1-admissible default-frame shape
    /// with BOTH the W1 and T2-B knobs ON — the sticky W1 batch drive wins
    /// (it runs first) and T2-B provably does NOT hijack; byte-identical to
    /// knob-OFF.
    #[test]
    fn windows_t2b_ab_w1_owns_first() {
        use std::sync::atomic::Ordering::Relaxed;
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        super::rowmode_ab::install_rowmode_seams();
        let relid: u32 = 77016;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let spec = BSpec {
            frame_options: FRAMEOPTION_DEFAULTS,
            start_off_i64: None,
            end_off_i64: None,
            range_off_i32: None,
            order_by: true,
            partition: true,
            qual_g_lt: None,
            fns: SUM_A,
        };
        let run = || run_t2b(&|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        crate::lanev2::windows_set_for_tests(false);
        crate::lanev2::windows_t2_set_for_tests(false);
        crate::lanev2::windows_t2b_set_for_tests(false);
        let off = run();
        crate::lanev2::windows_set_for_tests(true);
        crate::lanev2::windows_t2b_set_for_tests(true);
        let t2b_before = crate::lanev2::WINDOWS_T2B_OWNED_FOR_TESTS.load(Relaxed);
        let w1_before = crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(Relaxed);
        let both = run();
        assert!(
            crate::lanev2::WINDOWS_OWNED_FOR_TESTS.load(Relaxed) > w1_before,
            "W1 must own its admitted shape with both knobs on"
        );
        assert_eq!(
            crate::lanev2::WINDOWS_T2B_OWNED_FOR_TESTS.load(Relaxed),
            t2b_before,
            "T2-B hijacked a W1-owned shape (hook order broken)"
        );
        crate::lanev2::windows_set_for_tests(false);
        crate::lanev2::windows_t2b_set_for_tests(false);
        assert_eq!(off, both, "both-knobs arm diverged");
        scanfix::quiesced();
    }

    /// HOOK-ORDER INTERPLAY (T2-A side): a framed shape with BOTH the T2-B
    /// and T2-A knobs ON — T2-B batch-hosts it (it runs first) and the
    /// delegation provably does NOT engage; byte-identical to knob-OFF.
    #[test]
    fn windows_t2b_ab_owns_before_t2a() {
        use std::sync::atomic::Ordering::Relaxed;
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        super::rowmode_ab::install_rowmode_seams();
        let relid: u32 = 77017;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        let run = || run_t2b(&|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        crate::lanev2::windows_set_for_tests(false);
        crate::lanev2::windows_t2_set_for_tests(false);
        crate::lanev2::windows_t2b_set_for_tests(false);
        let off = run();
        crate::lanev2::windows_t2_set_for_tests(true);
        crate::lanev2::windows_t2b_set_for_tests(true);
        let t2b_before = crate::lanev2::WINDOWS_T2B_OWNED_FOR_TESTS.load(Relaxed);
        let t2_before = crate::lanev2::WINDOWS_T2_OWNED_FOR_TESTS.load(Relaxed);
        let both = run();
        assert!(
            crate::lanev2::WINDOWS_T2B_OWNED_FOR_TESTS.load(Relaxed) > t2b_before,
            "T2-B must own the framed shape ahead of the delegation"
        );
        assert_eq!(
            crate::lanev2::WINDOWS_T2_OWNED_FOR_TESTS.load(Relaxed),
            t2_before,
            "T2-A engaged on a T2-B-owned shape (hook order broken)"
        );
        crate::lanev2::windows_t2_set_for_tests(false);
        crate::lanev2::windows_t2b_set_for_tests(false);
        assert_eq!(off, both, "both-knobs arm diverged");
        scanfix::quiesced();
    }

    /// REVIEW FINDING 1 REGRESSION: a T2-B-owned drive abandoned
    /// mid-partition with a parked boundary row (`more_partitions=true`,
    /// the LIMIT/LATERAL-style partial drain), rescanned, then re-fed an
    /// EMPTY input must return zero rows exactly like Volcano. Before
    /// `lane_framed_reset` cleared the node's `more_partitions`, the stale
    /// flag resurrected `lane_framed_input_done`'s parked-partition branch
    /// after the rescan cleared `first_part_valid`: a debug_assert panic in
    /// debug, the framed-fetch tripwire PgError in release.
    #[test]
    fn windows_t2b_ab_rescan_after_partial_drain_empty_refeed() {
        use std::sync::atomic::Ordering::Relaxed;
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        super::rowmode_ab::install_rowmode_seams();
        let relid: u32 = 77020;
        let mut spec = BSpec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        // Drain exactly ONE row (partition g=1 is fully spooled with the
        // (2,5) boundary row parked => more_partitions=true), abandon the
        // drive mid-emission, swap the fixture to EMPTY, rescan, re-drain.
        let run = || {
            scanfix::register_table_2col(relid, &[B_ROWS]);
            let pstmt = mk_t2b_pstmt(leaked_mcx(), relid, &spec);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let ExecData { estate, planstate } = data;
                let ps = planstate.as_mut().unwrap();
                let slot_id = exec_proc_node(ps, estate).unwrap().expect("first row");
                let first = {
                    let base = estate.slot_mut(slot_id).base();
                    (
                        base.tts_values[0].as_i32(),
                        base.tts_values[1].as_i32(),
                        base.tts_values[2].as_i64(),
                    )
                };
                scanfix::register_table_2col(relid, &[]);
                crate::execami::exec_re_scan(ps, estate).unwrap();
                let rest = drain_cells(ps, estate, &[Ty::I64]);
                crate::exec_end_node(ps, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
                (first, rest)
            })
        };
        crate::lanev2::windows_set_for_tests(false);
        crate::lanev2::windows_t2_set_for_tests(false);
        crate::lanev2::windows_t2b_set_for_tests(false);
        let off = run();
        crate::lanev2::windows_t2b_set_for_tests(true);
        let t2b_before = crate::lanev2::WINDOWS_T2B_OWNED_FOR_TESTS.load(Relaxed);
        let on = run();
        let t2b_after = crate::lanev2::WINDOWS_T2B_OWNED_FOR_TESTS.load(Relaxed);
        crate::lanev2::windows_t2b_set_for_tests(false);
        assert_eq!(off, on, "knob OFF vs ON must be identical");
        assert!(t2b_after > t2b_before, "ON arm never engaged the T2-B framed lane");
        assert_eq!(off.0, (1, 10, 20), "first drained row");
        assert!(
            off.1.is_empty(),
            "empty re-feed must yield zero rows (Volcano's empty-rescan behavior)"
        );
        scanfix::quiesced();
    }

    /// REVIEW FINDING 2: the T2-B SEAL refusal boundary at unit level (the
    /// W1/T2-A refusal-unit precedent). A framed shape with a plan qual —
    /// always TRUE, so rows are identical either way — must fall through
    /// with the knob ON: the memoized chokepoint refuses ShapeQualProj and
    /// the T2-B probe does NOT tick; the row engine owns both arms.
    #[test]
    fn windows_t2b_ab_qual_seal_refuses() {
        use std::sync::atomic::Ordering::Relaxed;
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        super::rowmode_ab::install_rowmode_seams();
        let relid: u32 = 77021;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        spec.qual_g_lt = Some(10);
        crate::lanev2::windows_set_for_tests(false);
        crate::lanev2::windows_t2_set_for_tests(false);
        crate::lanev2::windows_t2b_set_for_tests(false);
        let off = run_t2b(&|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        crate::lanev2::windows_t2b_set_for_tests(true);
        let t2b_before = crate::lanev2::WINDOWS_T2B_OWNED_FOR_TESTS.load(Relaxed);
        let on = run_t2b(&|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        let t2b_after = crate::lanev2::WINDOWS_T2B_OWNED_FOR_TESTS.load(Relaxed);
        crate::lanev2::windows_t2b_set_for_tests(false);
        assert_eq!(off, on, "knob OFF vs ON must be identical");
        assert_eq!(
            t2b_after, t2b_before,
            "T2-B engaged on a sealed-out qual shape (the seal is broken)"
        );
        let w = want(&[&[I(20)], &[I(40)], &[I(60)], &[I(50)], &[I(11)], &[I(11)], &[I(7)]]);
        assert_eq!(off, vec![w]);
        scanfix::quiesced();
    }

    /// REVIEW FINDING 3: the `partNumCols == 0` accept path — no PARTITION
    /// BY, the whole input is ONE partition closed only by end-of-stream
    /// (`lane_framed_accept` skips the part_eq block entirely; the parked
    /// boundary row never occurs).
    #[test]
    fn windows_t2b_ab_no_partition_whole_input() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 77022;
        scanfix::register_table_2col(relid, &[B_ROWS]);
        let mut spec = BSpec::framed(ROWS_SLIDING);
        spec.start_off_i64 = Some(1);
        spec.end_off_i64 = Some(1);
        spec.partition = false;
        let runs = ab_t2b(|mcx| mk_t2b_pstmt(mcx, relid, &spec), &[Ty::I64], false);
        // One 7-row partition in sort order (a: 10,10,20,30,5,6,7), ROWS
        // BETWEEN 1 PRECEDING AND 1 FOLLOWING.
        let w = want(&[&[I(20)], &[I(40)], &[I(60)], &[I(55)], &[I(41)], &[I(18)], &[I(13)]]);
        assert_eq!(runs, vec![w]);
        scanfix::quiesced();
    }
}
// --- end WS-R wave-3 (T2-B) region ---

// --- WS-T wave-3 (dml inc-2 / inc-2b / inc-3a) --------------------------------
// A/B unit corpus for the TupleOp decomposition (DmlInsertOp over bare child
// rows), the lane-fed SeqScan feed, the LockRows TupleOp host, and the
// UPDATE/DELETE verdict widening behind the nested PGRUST_LANE_V2_DML_UD
// knob. Serialization: every test holds scanfix::TEST_LOCK for its full
// span (the wave-2 precedent-3 discipline — dml_ab's tests hold the same
// lock, so the shared DML knob atomics never race across modules). Fake
// oids: the WS-T band continues dml at 75005+ (wave-3 contract §3.3).
mod dml_ab_wave3 {
    use super::*;

    /// `UPDATE target SET c1 = 42` / `DELETE FROM target` — the planner's
    /// plain single-rel shapes: `ModifyTable(op)` over a SeqScan of the
    /// target itself; UPDATE subplan tlist = [new c1 value, junk ctid],
    /// updateColnosLists = [[1]]; DELETE subplan tlist = [junk ctid].
    /// Fixture note: the junk "ctid" entry is FOUND BY NAME at init
    /// (init_result_rel's rowid_attno lookup) and its datum is only read
    /// per fetched row — the source is empty, so a plain int4 Var stands in
    /// for the TID system column (the test seams carry no TID type shape).
    fn mk_update_delete_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        target: u32,
        op: CmdType,
    ) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{ModifyTable, Plan, Scan, SeqScan};

        let scan_tlist = if op == CmdType::CMD_UPDATE {
            let junk = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
            NodeList::make2(
                mcx,
                Node::mk_target_entry(mcx, mk_int4_const(mcx, 42), 1, Some("c1"), false)
                    .unwrap(),
                Node::mk_target_entry(mcx, junk, 2, Some("ctid"), true).unwrap(),
            )
            .unwrap()
        } else {
            let junk = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
            NodeList::make1(
                mcx,
                Node::mk_target_entry(mcx, junk, 1, Some("ctid"), true).unwrap(),
            )
            .unwrap()
        };
        let scan = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan { targetlist: scan_tlist, ..Default::default() },
                    scanrelid: 1,
                },
            },
        )
        .unwrap();

        let mut mt = Node::build::<ModifyTable>(mcx).unwrap();
        mt.plan.lefttree = Some(scan);
        mt.operation = op;
        mt.canSetTag = true;
        mt.nominalRelation = 1;
        mt.resultRelations = ::types_nodes::IntList::make1(mcx, 1).unwrap();
        if op == CmdType::CMD_UPDATE {
            let colnos = ::types_nodes::IntList::make1(mcx, 1).unwrap();
            mt.updateColnosLists =
                NodeList::make1(mcx, Node::mk_int_list(mcx, colnos).unwrap()).unwrap();
        }

        let required = if op == CmdType::CMD_UPDATE {
            ::types_nodes::parsenodes::ACL_UPDATE
        } else {
            ::types_nodes::parsenodes::ACL_DELETE
        };
        let rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid: target,
                relkind: ::types_rel::RELKIND_RELATION,
                rellockmode: ::types_rel::RowExclusiveLock,
                perminfoindex: 1,
                inFromCl: false,
                ..Default::default()
            },
        )
        .unwrap();
        let perm = Node::mk(
            mcx,
            RTEPermissionInfo { relid: target, requiredPerms: required, ..Default::default() },
        )
        .unwrap();
        let mut unpruned = Bitmapset::empty();
        unpruned.add_member(mcx, 1).unwrap();

        let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
        pstmt.commandType = op;
        pstmt.canSetTag = true;
        pstmt.planTree = Some(mt.seal());
        pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
        pstmt.permInfos = NodeList::make1(mcx, perm).unwrap();
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    /// `SELECT c1, c2 FROM target FOR UPDATE` — LockRows over a SeqScan
    /// with the rowmark's junk "ctid1" column (found by name at
    /// exec_init_lock_rows; the same empty-source int4 stand-in as above).
    fn mk_lockrows_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, target: u32) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{LockRows, Plan, PlanRowMark, Scan, SeqScan};

        let scan_tlist = {
            let a = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
            let b = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
            let junk = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
            let mut tl = NodeList::make2(
                mcx,
                Node::mk_target_entry(mcx, a, 1, Some("c1"), false).unwrap(),
                Node::mk_target_entry(mcx, b, 2, Some("c2"), false).unwrap(),
            )
            .unwrap();
            tl.lappend(mcx, Node::mk_target_entry(mcx, junk, 3, Some("ctid1"), true).unwrap())
                .unwrap();
            tl
        };
        let scan = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan { targetlist: scan_tlist, ..Default::default() },
                    scanrelid: 1,
                },
            },
        )
        .unwrap();

        // ROW_MARK_EXCLUSIVE is RowMarkType's default; prti == rti (no
        // inheritance), so no tableoid junk is looked up.
        let rowmark = Node::mk(
            mcx,
            PlanRowMark { rti: 1, prti: 1, rowmarkId: 1, ..Default::default() },
        )
        .unwrap();
        let rowmarks = NodeList::make1(mcx, rowmark).unwrap();

        let mut lr = Node::build::<LockRows>(mcx).unwrap();
        lr.plan.lefttree = Some(scan);
        lr.rowMarks = rowmarks.clone_in(mcx).unwrap();

        let rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid: target,
                relkind: ::types_rel::RELKIND_RELATION,
                rellockmode: ::types_rel::RowShareLock,
                perminfoindex: 1,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap();
        let perm = Node::mk(
            mcx,
            RTEPermissionInfo {
                relid: target,
                requiredPerms: ::types_nodes::parsenodes::ACL_SELECT,
                ..Default::default()
            },
        )
        .unwrap();
        let mut unpruned = Bitmapset::empty();
        unpruned.add_member(mcx, 1).unwrap();

        let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
        pstmt.commandType = CmdType::CMD_SELECT;
        pstmt.canSetTag = true;
        pstmt.planTree = Some(lr.seal());
        pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
        pstmt.permInfos = NodeList::make1(mcx, perm).unwrap();
        pstmt.rowMarks = rowmarks;
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    /// Drive a plan to completion (exec_proc_node until None — the
    /// ExecutePlan cadence) and return es_processed. Both knob arms run
    /// these identical statements. The generalized form of dml_ab's
    /// run_insert (operation-parameterized).
    fn run_stmt(pstmt: &'static PlannedStmt<'static>, op: CmdType) -> u64 {
        let snap_ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, op, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            while exec_proc_node(ps, estate).unwrap().is_some() {}
            let processed = estate.es_processed;
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
            processed
        })
    }

    fn probes() -> (u64, u64) {
        (
            crate::lanev2::DML_OWNED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed),
            crate::lanev2::DML_SHAPE_REFUSED_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    fn lanefed_probe() -> u64 {
        crate::lanev2::DML_LANEFED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn lockrows_probe() -> u64 {
        crate::lanev2::DML_LOCKROWS_OWNED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// inc-2: the op-form drive over the admitted INSERT..SELECT shape with
    /// a SeqScan child — knob OFF ticks NOTHING and owns nothing; knob ON
    /// engages the DmlInsertOp drive AND selects the lane-fed (dispatch-
    /// hoisted) SeqScan feed; behavior identical on both arms.
    #[test]
    fn dml_w3_insert_select_op_form_lanefed() {
        install_seams();
        scanfix::install();
        dml_ab::install_replica_identity_seam();
        let target: u32 = 75005;
        let source: u32 = 75006;
        scanfix::register_table_2col(target, &[]);
        scanfix::register_table_2col(source, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        crate::lanev2::dml_set_for_tests(false);
        let (owned0, refused0) = probes();
        let fed0 = lanefed_probe();
        let off = run_stmt(
            dml_ab::mk_insert_select_pstmt(leaked_mcx(), target, source, false),
            CmdType::CMD_INSERT,
        );
        let (owned1, refused1) = probes();
        assert_eq!(off, 0);
        assert_eq!(owned1, owned0, "knob OFF must not own");
        assert_eq!(refused1, refused0, "knob OFF must tick NOTHING (contract §2.2)");
        assert_eq!(lanefed_probe(), fed0, "knob OFF must not select a lane feed");

        crate::lanev2::dml_set_for_tests(true);
        let on = run_stmt(
            dml_ab::mk_insert_select_pstmt(leaked_mcx(), target, source, false),
            CmdType::CMD_INSERT,
        );
        let (owned2, refused2) = probes();
        crate::lanev2::dml_set_for_tests(false);
        assert_eq!(on, off, "knob OFF vs ON must behave identically");
        assert!(owned2 > owned1, "ON arm never engaged the DML drive");
        assert_eq!(refused2, refused1, "the admitted shape must not tick DmlShape");
        assert!(lanefed_probe() > fed0, "SeqScan child must take the lane-fed feed");

        scanfix::quiesced();
    }

    /// inc-3a nested-knob law: UPDATE refuses (detail `update`) while the
    /// UD stretch knob is off, even with the DML host knob on.
    #[test]
    fn dml_w3_update_refused_without_ud() {
        install_seams();
        scanfix::install();
        dml_ab::install_replica_identity_seam();
        let target: u32 = 75007;
        scanfix::register_table_2col(target, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        crate::lanev2::dml_set_for_tests(true);
        crate::lanev2::dml_ud_set_for_tests(false);
        let (owned0, refused0) = probes();
        let n = run_stmt(
            mk_update_delete_pstmt(leaked_mcx(), target, CmdType::CMD_UPDATE),
            CmdType::CMD_UPDATE,
        );
        let (owned1, refused1) = probes();
        crate::lanev2::dml_set_for_tests(false);
        assert_eq!(n, 0);
        assert_eq!(owned1, owned0, "UPDATE must not be owned with _UD off");
        assert!(refused1 > refused0, "UPDATE at _UD-off must tick DmlShape");

        scanfix::quiesced();
    }

    /// inc-3a nested-knob law, other arm: `_UD` alone flips NOTHING — with
    /// the DML host knob off the arm gate short-circuits and no wave-3 code
    /// runs (no ticks, no ownership).
    #[test]
    fn dml_w3_ud_alone_flips_nothing() {
        install_seams();
        scanfix::install();
        dml_ab::install_replica_identity_seam();
        let target: u32 = 75008;
        scanfix::register_table_2col(target, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        crate::lanev2::dml_set_for_tests(false);
        crate::lanev2::dml_ud_set_for_tests(true);
        let (owned0, refused0) = probes();
        let n = run_stmt(
            mk_update_delete_pstmt(leaked_mcx(), target, CmdType::CMD_UPDATE),
            CmdType::CMD_UPDATE,
        );
        let (owned1, refused1) = probes();
        crate::lanev2::dml_ud_set_for_tests(false);
        assert_eq!(n, 0);
        assert_eq!(owned1, owned0, "_UD alone must not own");
        assert_eq!(refused1, refused0, "_UD alone must tick NOTHING");

        scanfix::quiesced();
    }

    /// inc-3a: plain single-rel no-trigger UPDATE is owned under DML+UD —
    /// the widened verdict routes it through the SAME DmlInsertOp/
    /// mt_accept_row machinery (empty source: admission + drive + the
    /// mt_source_exhausted epilogue, identical on both arms).
    #[test]
    fn dml_w3_update_owned_with_ud() {
        install_seams();
        scanfix::install();
        dml_ab::install_replica_identity_seam();
        let target: u32 = 75009;
        scanfix::register_table_2col(target, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // Volcano oracle arm first (both knobs off).
        crate::lanev2::dml_set_for_tests(false);
        crate::lanev2::dml_ud_set_for_tests(false);
        let off = run_stmt(
            mk_update_delete_pstmt(leaked_mcx(), target, CmdType::CMD_UPDATE),
            CmdType::CMD_UPDATE,
        );

        crate::lanev2::dml_set_for_tests(true);
        crate::lanev2::dml_ud_set_for_tests(true);
        let (owned0, refused0) = probes();
        let on = run_stmt(
            mk_update_delete_pstmt(leaked_mcx(), target, CmdType::CMD_UPDATE),
            CmdType::CMD_UPDATE,
        );
        let (owned1, refused1) = probes();
        crate::lanev2::dml_set_for_tests(false);
        crate::lanev2::dml_ud_set_for_tests(false);
        assert_eq!(on, off, "knob arms must behave identically");
        assert!(owned1 > owned0, "UD arm never engaged on the admitted UPDATE");
        assert_eq!(refused1, refused0, "the admitted UPDATE must not tick DmlShape");

        scanfix::quiesced();
    }

    /// inc-3a: same for DELETE.
    #[test]
    fn dml_w3_delete_owned_with_ud() {
        install_seams();
        scanfix::install();
        dml_ab::install_replica_identity_seam();
        let target: u32 = 75010;
        scanfix::register_table_2col(target, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        crate::lanev2::dml_set_for_tests(false);
        crate::lanev2::dml_ud_set_for_tests(false);
        let off = run_stmt(
            mk_update_delete_pstmt(leaked_mcx(), target, CmdType::CMD_DELETE),
            CmdType::CMD_DELETE,
        );

        crate::lanev2::dml_set_for_tests(true);
        crate::lanev2::dml_ud_set_for_tests(true);
        let (owned0, refused0) = probes();
        let on = run_stmt(
            mk_update_delete_pstmt(leaked_mcx(), target, CmdType::CMD_DELETE),
            CmdType::CMD_DELETE,
        );
        let (owned1, refused1) = probes();
        crate::lanev2::dml_set_for_tests(false);
        crate::lanev2::dml_ud_set_for_tests(false);
        assert_eq!(on, off, "knob arms must behave identically");
        assert!(owned1 > owned0, "UD arm never engaged on the admitted DELETE");
        assert_eq!(refused1, refused0, "the admitted DELETE must not tick DmlShape");

        scanfix::quiesced();
    }

    /// inc-2b: the LockRows TupleOp host engages under the DML knob (the
    /// rowmode-tail delegation hook declines when ROWMODE is off, so the
    /// arm falls to the WS-T hook), owns the pull, and drives the empty
    /// child to the identical end-of-set. Knob OFF: silent.
    #[test]
    fn dml_w3_lockrows_tupleop_owned() {
        install_seams();
        scanfix::install();
        dml_ab::install_replica_identity_seam();
        let target: u32 = 75011;
        scanfix::register_table_2col(target, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // Wave-4 FLIP-1: PGRUST_LANE_V2_ROWMODE is default-ON, and the
        // lockrows_arm hook order (rowmode-tail delegation FIRST, WS-T
        // TupleOp second) means the tail owns LockRows pulls at the new
        // default. Pin the tail OFF so this A/B exercises the DML TupleOp
        // hosting itself — the arm reached whenever the tail is explicitly
        // off (the permanent `=0` spelling) or refuses dynamically.
        crate::lanev2::rowmode_set_for_tests(false);
        crate::lanev2::dml_set_for_tests(false);
        let lr0 = lockrows_probe();
        let off = run_stmt(mk_lockrows_pstmt(leaked_mcx(), target), CmdType::CMD_SELECT);
        assert_eq!(off, 0);
        assert_eq!(lockrows_probe(), lr0, "knob OFF must not own LockRows");

        crate::lanev2::dml_set_for_tests(true);
        let on = run_stmt(mk_lockrows_pstmt(leaked_mcx(), target), CmdType::CMD_SELECT);
        crate::lanev2::dml_set_for_tests(false);
        assert_eq!(on, off, "knob arms must behave identically");
        assert!(lockrows_probe() > lr0, "ON arm never engaged the LockRows TupleOp");

        scanfix::quiesced();
    }
}
// --- end WS-T wave-3 ----------------------------------------------------------

// ===========================================================================
// ===== WAVE-5 APPEND REGION — do not edit above =====
// Wave-5 contract §2: ONE marker, labeled per-WS sub-regions in fixed order
// U, V, W, X. A WS writes ONLY inside its own sub-region; never above the
// marker or inside another WS's sub-region. Fake-oid bands per contract §4:
// U 79001+, V 80001+ (EXCEPT the 5 deferred AM-backed per-shape units,
// which honor the 76xxx band reserved for them by the B1 flip commit
// 6b776d09e), W 81001+, X 82001+ (expected unused).
// ===========================================================================


// --- WS-U wave-5 (EPQ inc-1: seam moves + refuse-all knob) --------------------
// A/B move-equivalence corpus for the epq.rs seam extraction (wave-5
// contract §6.2a — pure code moves, zero behavior delta) and the
// PGRUST_LANE_V2_EPQ refuse-all knob (§6.3). Serialization: every test
// holds scanfix::TEST_LOCK for its full span (the wave-2 precedent-3
// discipline). Fake oids: WS-U band 79001+.
mod epq_seams_w5 {
    use super::*;

    /// Move-equivalence arm A vs arm B (contract §6.2a): the public
    /// `eval_plan_qual` entry (which now composes the extracted seams) vs
    /// a manual composition of those seams in C's EvalPlanQual order
    /// (Begin -> Slot -> availability reset -> Next -> clear/block). Same
    /// 79001 fixture, byte-identical projected values on the pass arm and
    /// the same skip verdict on the qual-fail arm.
    #[test]
    fn epq_w5_seam_composition_matches_entry() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();

        let relid: u32 = 79001;
        scanfix::register_table_2col(relid, &[&[(1, 10), (2, 20)]]);
        let pstmt = mk_epq_update_subplan_pstmt(mcx, relid);

        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));

        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;

            let mut epq = crate::epq::EpqState {
                plan: pstmt.planTree,
                recheck: None,
                result_rti: 1,
                lane_verdicts: None,
            };
            let mut subs = None;
            ::executils::ensure_epq_subs(&mut subs, estate.es_query_cxt, estate.epq_rtsize(), 1);
            let desc = estate.es_relations[0].as_ref().unwrap().rd_att.clone();
            let test = estate.exec_init_extra_tuple_slot(
                Some(desc),
                ::types_slot::TupleSlotKind::Virtual,
            );
            subs.as_mut().unwrap().relsubs_slot[0] = Some(test);

            // Arm A: the public entry (routes through the moved seams).
            epq_store_test_tuple(estate, test, 1, 99);
            let a = crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
                .unwrap()
                .expect("qual passes");
            let a_vals = epq_slot_vals(estate, a);
            assert_eq!(a_vals, (1, 99));

            // Arm B: manual seam composition, C entry-point order. The
            // es_epq swap + active flag mirror the entry's wrapper lines.
            epq_store_test_tuple(estate, test, 1, 99);
            estate.es_epq = subs.take();
            let saved_active = estate.es_epq_active;
            estate.es_epq_active = true;
            crate::epq::eval_plan_qual_begin(&mut epq, estate).unwrap();
            let slot_id = crate::epq::eval_plan_qual_slot(&mut epq, estate).unwrap();
            assert_eq!(slot_id, test, "parked test slot is THE EvalPlanQualSlot");
            {
                let s = estate.es_epq.as_mut().unwrap();
                s.relsubs_done[0] = false;
                s.relsubs_blocked[0] = false;
            }
            let b = crate::epq::eval_plan_qual_next(&mut epq, estate)
                .unwrap()
                .expect("qual passes");
            let b_vals = epq_slot_vals(estate, b);
            let qcx = estate.es_query_cxt;
            exectuples::exec_clear_tuple(estate.slot_mut(test), qcx);
            estate.es_epq.as_mut().unwrap().relsubs_blocked[0] = true;
            estate.es_epq_active = saved_active;
            subs = estate.es_epq.take();

            assert_eq!(b_vals, a_vals, "seam composition == entry (move equivalence)");

            // Skip verdict identical through the entry as before the move.
            epq_store_test_tuple(estate, test, 2, 99);
            assert!(crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
                .unwrap()
                .is_none());

            crate::epq::eval_plan_qual_end(&mut epq, &mut subs, estate).unwrap();
            assert!(epq.recheck.is_none());

            let ps = planstate.as_mut().unwrap();
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        scanfix::quiesced();
    }

    /// `eval_plan_qual_slot` (C EvalPlanQualSlot): made on first use, then
    /// idempotent — the second call returns the same slot id and appends
    /// nothing to the tuple table.
    #[test]
    fn epq_w5_slot_seam_idempotent() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();

        let relid: u32 = 79002;
        scanfix::register_table_2col(relid, &[&[(1, 10)]]);
        let pstmt = mk_epq_update_subplan_pstmt(mcx, relid);

        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));

        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;

            let mut epq = crate::epq::EpqState {
                plan: pstmt.planTree,
                recheck: None,
                result_rti: 1,
                lane_verdicts: None,
            };
            let mut subs = None;
            ::executils::ensure_epq_subs(&mut subs, estate.es_query_cxt, estate.epq_rtsize(), 1);
            // No parked slot: the seam must make one on first use.
            estate.es_epq = subs.take();
            let n0 = estate.es_tupleTable.len();
            let first = crate::epq::eval_plan_qual_slot(&mut epq, estate).unwrap();
            let n1 = estate.es_tupleTable.len();
            assert_eq!(n1, n0 + 1, "first use makes exactly one slot");
            let second = crate::epq::eval_plan_qual_slot(&mut epq, estate).unwrap();
            assert_eq!(second, first, "idempotent per rti");
            assert_eq!(estate.es_tupleTable.len(), n1, "second call appends nothing");
            subs = estate.es_epq.take();

            crate::epq::eval_plan_qual_end(&mut epq, &mut subs, estate).unwrap();
            let ps = planstate.as_mut().unwrap();
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        scanfix::quiesced();
    }

    /// `check_epq_plan` is THE LOUD ADMISSION LIST (wave-5 contract §6.2c;
    /// wave-7 rung Y2): listed shapes pass silently; an unexercised shape
    /// panics LOUDLY. It admits nothing new at wave-7 — Agg stays outside
    /// the list. Wave-7 extension: the positive arm carries a REAL
    /// scanrelid (1) because scanrelid == 0 pushed-down-join scans now
    /// refuse loudly on their own arm (see
    /// `epq_w7_scanrelid_zero_refused_loudly`).
    #[test]
    #[should_panic(expected = "recheck plan")]
    fn epq_w5_check_epq_plan_is_the_loud_admission_list() {
        use ::types_nodes::plannodes::{Plan, Scan, SeqScan};
        let mcx = leaked_mcx();
        // Positive arm first: a whitelist shape passes without panic.
        let seq = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan { plan: Plan::default(), scanrelid: 1 },
            },
        )
        .unwrap();
        crate::epq::check_epq_plan(seq);
        // Negative arm: Agg is not exercised for EPQ rescan — LOUD refuse.
        let agg = Node::build::<::types_nodes::plannodes::Agg>(mcx).unwrap().seal();
        crate::epq::check_epq_plan(agg);
    }

    /// `PGRUST_LANE_V2_EPQ` knob A/B (contract §6.3 + §0.6): OFF ticks
    /// NOTHING through the recheck admission walk; ON refuses the recheck
    /// shape via the existing `epq` carrier and the recheck outcome is
    /// byte-identical on both arms (zero ownership, zero behavior delta).
    #[test]
    fn epq_w5_knob_off_ticks_nothing_on_refuses_everything() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();

        let relid: u32 = 79003;
        scanfix::register_table_2col(relid, &[&[(1, 10), (2, 20)]]);
        let pstmt = mk_epq_update_subplan_pstmt(mcx, relid);

        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));

        let probe = || {
            crate::lanev2::EPQ_ADMISSION_REFUSED_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;

            let mut epq = crate::epq::EpqState {
                plan: pstmt.planTree,
                recheck: None,
                result_rti: 1,
                lane_verdicts: None,
            };
            let mut subs = None;
            ::executils::ensure_epq_subs(&mut subs, estate.es_query_cxt, estate.epq_rtsize(), 1);
            let desc = estate.es_relations[0].as_ref().unwrap().rd_att.clone();
            let test = estate.exec_init_extra_tuple_slot(
                Some(desc),
                ::types_slot::TupleSlotKind::Virtual,
            );
            subs.as_mut().unwrap().relsubs_slot[0] = Some(test);

            // OFF arm: no admission-walk tick, recheck proceeds Volcano.
            crate::lanev2::epq_lane_set_for_tests(false);
            let p0 = probe();
            epq_store_test_tuple(estate, test, 1, 99);
            let off = crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
                .unwrap()
                .expect("qual passes");
            let off_vals = epq_slot_vals(estate, off);
            assert_eq!(probe(), p0, "knob OFF must tick NOTHING (contract §0.6)");

            // ON arm: the admission walk refuses via the epq carrier; the
            // recheck outcome is IDENTICAL (refuse-all = zero behavior).
            crate::lanev2::epq_lane_set_for_tests(true);
            epq_store_test_tuple(estate, test, 1, 99);
            let on = crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
                .unwrap()
                .expect("qual passes");
            let on_vals = epq_slot_vals(estate, on);
            crate::lanev2::epq_lane_set_for_tests(false);
            assert!(probe() > p0, "ON arm never ran the refuse-all admission walk");
            assert_eq!(on_vals, off_vals, "knob arms must behave identically");

            crate::epq::eval_plan_qual_end(&mut epq, &mut subs, estate).unwrap();
            let ps = planstate.as_mut().unwrap();
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        scanfix::quiesced();
    }
}
// --- end WS-U wave-5 ----------------------------------------------------------

// --- WS-V wave-5 sub-region (B1 flip preconditions; band 76xxx reserved) -----
// The 5 deferred AM-backed per-shape T3 units (B1 flip commit 6b776d09e's
// outstanding list; WS-Q review finding #4 residual): TableFuncScan,
// SampleScan, TidScan, NamedTuplestoreScan, TidRangeScan — the five T3
// shapes the boarded scans_t3_ab corpus could not reach with FunctionScan
// fixtures. Same A/B contract as scans_t3_ab: knob OFF = the Volcano
// oracle; knob ON = the batch-size-1 T3 source drive owns every pull,
// byte-identical rows, and the delegation tail's probe does NOT move
// (mechanism attribution). This module also carries the TidScan ctid-fetch
// (scanfix) fixture that never existed in this harness: hand-built TidScan
// plans whose tidquals are `ctid = 'const tid'` OpExprs over the scanfix
// fake-heap pages — AM tid-validity (out-of-range block silently dropped),
// sort+dedupe, and heap_fetch through the fake buffer seams are all
// exercised. Zero scanfix-internal edits: the fixture is plan-builders +
// registered pages only (the §1 file-table grant).
// Relids consumed: 76001 (TidScan), 76003 (TidRangeScan), 76004
// (SampleScan). 76002 is intentionally unconsumed — only three of the five
// shapes carry a relation RTE (NamedTuplestoreScan and TableFuncScan
// consume zero relids); the 76xxx band is reserved wholesale, so the gap
// is harmless and stays (wave-5 WS-V review finding 4, cosmetic).
mod scans_t3_am_ab {
    use super::*;
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{
        RTEKind, RTEPermissionInfo, RangeTblEntry, TableSampleClause,
    };
    use ::types_nodes::plannodes::{
        NamedTuplestoreScan as NtsPlan, SampleScan as SampleScanPlan,
        TableFuncScan as TableFuncScanPlan, TidRangeScan as TidRangeScanPlan,
        TidScan as TidScanPlan,
    };
    use ::types_nodes::primnodes::{OpExpr, TableFunc};
    use ::types_tuple::itemptr::ItemPointerData;

    const TIDOID: u32 = 27;
    const TEXTOID: u32 = 25;
    const FLOAT4OID: u32 = 700;
    const FLOAT8OID: u32 = 701;
    /// pg_operator: `=(tid,tid)` 387; `<=(tid,tid)` 2801 (nodeTidrangescan.c
    /// TIDLessEqOperator).
    const TID_EQ_OP: u32 = 387;
    const TID_LE_OP: u32 = 2801;
    /// pg_proc: tsm_system_handler (tablesample crate F_TSM_SYSTEM_HANDLER).
    const TSM_SYSTEM_HANDLER: u32 = 3314;

    fn mk_tid_const(mcx: ::mcx::Mcx<'_>, block: u32, off: u16) -> Node<'_> {
        let tid: &'static ItemPointerData =
            Box::leak(Box::new(ItemPointerData::new(block, off)));
        Node::mk_const(
            mcx,
            TIDOID,
            -1,
            0,
            6,
            Datum::from_usize(tid as *const ItemPointerData as usize),
            false,
            false,
        )
        .unwrap()
    }

    fn mk_ctid_var(mcx: ::mcx::Mcx<'_>) -> Node<'_> {
        // varattno -1 = SelfItemPointerAttributeNumber.
        Node::mk_var(mcx, 1, -1, TIDOID, -1, 0, 0).unwrap()
    }

    fn mk_tid_op(mcx: ::mcx::Mcx<'_>, opno: u32, block: u32, off: u16) -> Node<'_> {
        let mut op = Node::build::<OpExpr>(mcx).unwrap();
        op.opno = opno;
        op.opresulttype = BOOLOID;
        op.args =
            NodeList::make2(mcx, mk_ctid_var(mcx), mk_tid_const(mcx, block, off)).unwrap();
        op.seal()
    }

    /// A leaked 4B-header text varlena Const (consttype is uninspected by
    /// the exec paths these units drive; XMLTABLE consumes the payload via
    /// varlena_payload).
    fn mk_text_const<'mcx>(mcx: ::mcx::Mcx<'mcx>, s: &str) -> Node<'mcx> {
        let total = s.len() + 4;
        let mut img = vec![0u8; total];
        img[0..4].copy_from_slice(
            &::types_tuple::varatt::set_varsize_4b_word(total as u32).to_ne_bytes(),
        );
        img[4..].copy_from_slice(s.as_bytes());
        let leaked: &'static mut [u8] = Vec::leak(img);
        Node::mk_const(
            mcx,
            TEXTOID,
            -1,
            0,
            -1,
            Datum::from_usize(leaked.as_ptr() as usize),
            false,
            false,
        )
        .unwrap()
    }

    /// One-col int4 scan tlist (the mk_seqscan_pstmt shape).
    fn mk_scan_tlist<'mcx>(mcx: ::mcx::Mcx<'mcx>, vartype: u32) -> NodeList<'mcx> {
        let var = Node::mk_var(mcx, 1, 1, vartype, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
        NodeList::make1(mcx, tle).unwrap()
    }

    /// Relation-RTE PlannedStmt around an arbitrary scan node (the
    /// mk_seqscan_pstmt RTE + ACL_SELECT + unprunable shape, generalized).
    fn mk_rel_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        relid: u32,
        scan_node: Node<'mcx>,
    ) -> &'mcx PlannedStmt<'mcx> {
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

    /// Drain a relation-RTE plan through the REAL init path (init_plan →
    /// ExecInitRangeTable → relation open) collecting column-1 int4s;
    /// `rescan_after` fires one mid-stream `exec_re_scan` (the delegation-
    /// cadence replay probe). The seqscan_end_to_end teardown shape.
    fn drain_rel(
        pstmt: &'static PlannedStmt<'static>,
        rescan_after: Option<usize>,
    ) -> Vec<i32> {
        let snap_ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            let mut out = Vec::new();
            let mut rescan = rescan_after;
            loop {
                if rescan == Some(out.len()) {
                    rescan = None;
                    crate::exec_re_scan(ps, estate).unwrap();
                }
                match exec_proc_node(ps, estate).unwrap() {
                    Some(slot_id) => {
                        let mut isnull = false;
                        let v = exectuples::slot_getattr(
                            estate.slot_mut(slot_id),
                            1,
                            &mut isnull,
                        );
                        assert!(!isnull);
                        out.push(v.as_i32());
                    }
                    None => break,
                }
            }
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
            out
        })
    }

    /// Drain a relation-free plan (NamedTuplestoreScan / TableFuncScan)
    /// via exec_init_node, collecting column-1 as i64 (covers int4 + int8
    /// projections); optional QueryEnvironment for the ENR shapes.
    fn drain_free(
        pstmt: &'static PlannedStmt<'static>,
        env: Option<&'static ::queryenvironment::QueryEnvironment<'static>>,
        rescan_after: Option<usize>,
        as_i64: bool,
    ) -> Vec<i64> {
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_queryEnv = env;
            let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
                .unwrap()
                .unwrap();
            let mut out = Vec::new();
            let mut rescan = rescan_after;
            loop {
                if rescan == Some(out.len()) {
                    rescan = None;
                    exec_re_scan(&mut ps, &mut data.estate).unwrap();
                }
                match exec_proc_node(&mut ps, &mut data.estate).unwrap() {
                    Some(slot_id) => {
                        let mut isnull = false;
                        let v = exectuples::slot_getattr(
                            data.estate.slot_mut(slot_id),
                            1,
                            &mut isnull,
                        );
                        assert!(!isnull);
                        out.push(if as_i64 { v.as_i64() } else { v.as_i32() as i64 });
                    }
                    None => break,
                }
            }
            crate::exec_end_node(&mut ps, &mut data.estate).unwrap();
            out
        })
    }

    fn t3_probe(name: &str) -> u64 {
        crate::lanev2::t3_owned_probe_for_tests(name)
    }

    fn tail_probe(name: &str) -> u64 {
        crate::lanev2::tail_owned_probe_for_tests(name)
    }

    /// The shared A/B skeleton: OFF drain (Volcano oracle, probes frozen) →
    /// ON drain (T3 source drive owns; tail probe frozen) → byte-compare.
    fn ab_case(class: &str, want: &[i64], mut drain: impl FnMut() -> Vec<i64>) {
        crate::lanev2::rowmode_set_for_tests(false);
        crate::lanev2::scans_t3_set_for_tests(false);
        let t3_off0 = t3_probe(class);
        let off = drain();
        assert_eq!(
            t3_probe(class),
            t3_off0,
            "knob OFF must never engage the T3 source form ({class})"
        );

        crate::lanev2::scans_t3_set_for_tests(true);
        let t3_on0 = t3_probe(class);
        let tail_on0 = tail_probe(class);
        let on = drain();
        let t3_on1 = t3_probe(class);
        let tail_on1 = tail_probe(class);
        crate::lanev2::scans_t3_set_for_tests(false);

        assert_eq!(off, on, "knob OFF vs ON must be identical ({class})");
        assert_eq!(off, want, "oracle rows mismatch ({class})");
        assert!(t3_on1 > t3_on0, "ON arm never engaged the T3 source drive ({class})");
        assert_eq!(
            tail_on1, tail_on0,
            "the delegation tail must not tick when the source form owns ({class})"
        );
    }

    /// TidScan over the scanfix fake heap — the ctid-fetch fixture that
    /// never existed (WS-Q review finding #4 residual; 76xxx band honored).
    /// tidquals = three `ctid = 'const'` OpExprs (implicit OR) + one
    /// AM-INVALID tid (block past rs_nblocks) that table_tuple_tid_valid
    /// must silently drop (C contract); TidListEval must sort+dedupe to
    /// heap order; heap_fetch rides the fake buffer seams. Mid-stream
    /// rescan replays the re-evaluated TID list from the top.
    #[test]
    fn tidscan_am_ab_ctid_fetch_with_rescan() {
        install_seams();
        scanfix::install();
        rowmode_ab::install_rowmode_seams();
        let relid: u32 = 76001;
        scanfix::register_table(relid, &[&[10, 20, 30], &[40, 50]]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        let mk = || {
            let mcx = leaked_mcx();
            let mut plan = Node::build::<TidScanPlan>(mcx).unwrap();
            plan.scan.plan.targetlist = mk_scan_tlist(mcx, INT4OID);
            plan.scan.scanrelid = 1;
            plan.tidquals = NodeList::from_slice(
                mcx,
                &[
                    mk_tid_op(mcx, TID_EQ_OP, 1, 2),  // (1,2) = 50
                    mk_tid_op(mcx, TID_EQ_OP, 0, 1),  // (0,1) = 10
                    mk_tid_op(mcx, TID_EQ_OP, 0, 3),  // (0,3) = 30
                    mk_tid_op(mcx, TID_EQ_OP, 7, 1),  // AM-invalid: dropped
                ],
            )
            .unwrap();
            mk_rel_pstmt(mcx, relid, plan.seal())
        };

        // Rescan after row 1: TidListEval re-runs, replay from the top.
        ab_case("tidscan", &[10, 10, 30, 50], || {
            drain_rel(mk(), Some(1)).into_iter().map(i64::from).collect()
        });

        drop(guard);
        scanfix::quiesced();
    }

    /// TidRangeScan upper bound: `ctid <= '(1,1)'` (inclusive) walks the
    /// fake heap through block 1 offset 1 via the AM tidrange scan.
    #[test]
    fn tidrangescan_am_ab_upper_bound_with_rescan() {
        install_seams();
        scanfix::install();
        rowmode_ab::install_rowmode_seams();
        let relid: u32 = 76003;
        scanfix::register_table(relid, &[&[10, 20, 30], &[40, 50]]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        let mk = || {
            let mcx = leaked_mcx();
            let mut plan = Node::build::<TidRangeScanPlan>(mcx).unwrap();
            plan.scan.plan.targetlist = mk_scan_tlist(mcx, INT4OID);
            plan.scan.scanrelid = 1;
            plan.tidrangequals =
                NodeList::make1(mcx, mk_tid_op(mcx, TID_LE_OP, 1, 1)).unwrap();
            mk_rel_pstmt(mcx, relid, plan.seal())
        };

        ab_case("tidrangescan", &[10, 20, 10, 20, 30, 40], || {
            drain_rel(mk(), Some(2)).into_iter().map(i64::from).collect()
        });

        drop(guard);
        scanfix::quiesced();
    }

    /// SampleScan TABLESAMPLE SYSTEM (100) REPEATABLE (0): deterministic
    /// (hashfloat8-seeded) full sample over the fake heap in block order —
    /// a 100% system sample must return every row on both arms.
    #[test]
    fn samplescan_am_ab_system_full_with_rescan() {
        install_seams();
        scanfix::install();
        rowmode_ab::install_rowmode_seams();
        let relid: u32 = 76004;
        scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        let mk = || {
            let mcx = leaked_mcx();
            let percent = Node::mk_const(
                mcx,
                FLOAT4OID,
                -1,
                0,
                4,
                Datum::from_f32(100.0),
                false,
                true,
            )
            .unwrap();
            let repeatable = Node::mk_const(
                mcx,
                FLOAT8OID,
                -1,
                0,
                8,
                Datum::from_f64(0.0),
                false,
                true,
            )
            .unwrap();
            let tsc = Node::mk(
                mcx,
                TableSampleClause {
                    tsmhandler: TSM_SYSTEM_HANDLER,
                    args: NodeList::make1(mcx, percent).unwrap(),
                    repeatable: Some(repeatable),
                },
            )
            .unwrap();
            let mut plan = Node::build::<SampleScanPlan>(mcx).unwrap();
            plan.scan.plan.targetlist = mk_scan_tlist(mcx, INT4OID);
            plan.scan.scanrelid = 1;
            plan.tablesample = Some(tsc);
            mk_rel_pstmt(mcx, relid, plan.seal())
        };

        ab_case("samplescan", &[1, 2, 1, 2, 3, 4, 5], || {
            drain_rel(mk(), Some(2)).into_iter().map(i64::from).collect()
        });

        drop(guard);
        scanfix::quiesced();
    }

    /// NamedTuplestoreScan over a registered ENR: the estate's
    /// QueryEnvironment carries the tuplestore (the trigger transition-
    /// table shape); the scan's private read pointer replays on rescan.
    /// No relation RTE — zero 76xxx oids consumed (the WS-Q honesty note's
    /// pattern).
    #[test]
    fn namedtuplestorescan_am_ab_enr_with_rescan() {
        install_seams();
        rowmode_ab::install_rowmode_seams();
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        let env_mcx = leaked_mcx();
        // 1-col int4 descriptor (the scanfix int4_tupdesc shape, module-local
        // per the shared-append law).
        let tupdesc = {
            let att = ::types_tuple::FormData_pg_attribute {
                attnum: 1,
                atttypid: INT4OID,
                atttypmod: -1,
                attlen: 4,
                attbyval: true,
                attalign: TYPALIGN_INT,
                attstorage: TYPSTORAGE_PLAIN,
                ..Default::default()
            };
            let mut attrs = ::mcx::PgVec::new_in(env_mcx);
            let mut compact = ::mcx::PgVec::new_in(env_mcx);
            compact.push(::types_tuple::CompactAttribute::populate_from(&att));
            attrs.push(att);
            std::rc::Rc::new(::types_tuple::TupleDescData {
                natts: 1,
                tdtypeid: 0,
                tdtypmod: -1,
                tdrefcount: -1,
                constr: None,
                compact_attrs: compact,
                attrs,
            })
        };
        let store = ::tuplestore::Tuplestore::begin_heap(false, false, 1024);
        let handle = ::tuplestore::hold::register(store);
        for v in [7, 8, 9] {
            ::tuplestore::hold::putvalues(
                handle,
                &tupdesc,
                &[Datum::from_i32(v)],
                &[false],
            )
            .unwrap();
        }
        let env: &'static mut ::queryenvironment::QueryEnvironment<'static> =
            Box::leak(Box::new(::queryenvironment::create_queryEnv(env_mcx)));
        ::queryenvironment::register_ENR(
            env,
            ::queryenvironment::EphemeralNamedRelationData {
                md: ::queryenvironment::EphemeralNamedRelationMetadataData {
                    name: ::mcx::PgString::from_str_in("t3_enr_v", env_mcx).unwrap(),
                    reliddesc: 0,
                    tupdesc: Some(tupdesc.clone()),
                    enrtype: ::queryenvironment::ENR_NAMED_TUPLESTORE,
                    enrtuples: 3.0,
                },
                reldata: handle,
            },
        )
        .unwrap();
        let env: &'static ::queryenvironment::QueryEnvironment<'static> = env;

        let mk = || {
            let mcx = leaked_mcx();
            let mut plan = Node::build::<NtsPlan>(mcx).unwrap();
            plan.scan.plan.targetlist = mk_scan_tlist(mcx, INT4OID);
            plan.scan.scanrelid = 1;
            plan.enrname = Some("t3_enr_v");
            let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
            pstmt.commandType = CmdType::CMD_SELECT;
            pstmt.canSetTag = true;
            pstmt.planTree = Some(plan.seal());
            pstmt.seal_ref()
        };

        ab_case("namedtuplestorescan", &[7, 7, 8, 9], || {
            drain_free(mk(), Some(env), Some(1), false)
        });

        drop(guard);
        ::tuplestore::hold::end(handle);
    }

    /// TableFuncScan XMLTABLE over an inline document: libxml row/column
    /// paths feed int8 columns through the real input-function path
    /// (int8in via the pg_type_io_shape fixture row — the one io shape the
    /// shared seam serves). No relation RTE — zero 76xxx oids consumed.
    /// SET-ONCE process-global (seam_core "installed twice" panic): the one
    /// `pg_type_category` install in this test binary — XMLTABLE's
    /// get_value consults it per column (xmltable.rs:212). pg_type.dat
    /// values: int8 'N', text 'S', bool 'B', xml 'U'; none preferred.
    fn install_type_category_seam() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            syscache_seams::pg_type_category::set(|typid| {
                Ok(match typid {
                    INT8OID => Some((b'N' as i8, false)),
                    TEXTOID => Some((b'S' as i8, false)),
                    BOOLOID => Some((b'B' as i8, false)),
                    142 => Some((b'U' as i8, false)), // xml
                    _ => None,
                })
            });
        });
    }

    #[test]
    fn tablefuncscan_am_ab_xmltable_with_rescan() {
        install_seams();
        rowmode_ab::install_rowmode_seams();
        install_type_category_seam();
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        let mk = || {
            let mcx = leaked_mcx();
            let mut tf = Node::build::<TableFunc>(mcx).unwrap();
            tf.docexpr = Some(mk_text_const(mcx, "<r><e>7</e><e>8</e><e>9</e></r>"));
            tf.rowexpr = Some(mk_text_const(mcx, "/r/e"));
            tf.colnames =
                NodeList::make1(mcx, Node::mk_string(mcx, "v").unwrap()).unwrap();
            tf.coltypes = ::types_nodes::list::OidList::make1(mcx, INT8OID).unwrap();
            tf.coltypmods = ::types_nodes::list::IntList::make1(mcx, -1).unwrap();
            tf.colcollations = ::types_nodes::list::OidList::make1(mcx, 0).unwrap();
            tf.colexprs = ::types_nodes::list::OptNodeList::make1(
                mcx,
                Some(mk_text_const(mcx, ".")),
            )
            .unwrap();
            tf.coldefexprs =
                ::types_nodes::list::OptNodeList::make1(mcx, None).unwrap();
            tf.colvalexprs =
                ::types_nodes::list::OptNodeList::make1(mcx, None).unwrap();
            tf.ordinalitycol = -1;

            let mut plan = Node::build::<TableFuncScanPlan>(mcx).unwrap();
            plan.scan.plan.targetlist = mk_scan_tlist(mcx, INT8OID);
            plan.scan.scanrelid = 1;
            plan.tablefunc = Some(tf.seal());
            let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
            pstmt.commandType = CmdType::CMD_SELECT;
            pstmt.canSetTag = true;
            pstmt.planTree = Some(plan.seal());
            pstmt.seal_ref()
        };

        ab_case("tablefuncscan", &[7, 7, 8, 9], || {
            drain_free(mk(), None, Some(1), true)
        });

        drop(guard);
    }
}
// --- end WS-V wave-5 sub-region -------------------------------------------------

// --- WS-W (wave-5): dml inc-4 OC admission ------------------------------------
// A/B unit corpus for the PGRUST_LANE_V2_DML_OC nested knob (wave-5 contract
// §8.3): admission + engagement + nested-knob law + MERGE-stays-refused.
// Serialization: every test holds scanfix::TEST_LOCK for its full span (the
// wave-2 precedent-3 discipline; the shared DML knob atomics never race
// across modules). Fake oids: the WS-W band is 81001+ (contract §4).
//
// SCOPE HONESTY (the wave-2 dml_ab header law, verbatim posture): these
// fixtures are READ-ONLY fake heaps with NO indexes — an arbiter-less ON
// CONFLICT DO NOTHING plan proves the HOST (admission verdict, gate order,
// knob-OFF silence, engagement, refusal accounting) while `exec_insert`
// skips the oc_* ceremony (onconflict != 0 && num_indices == 0) and the
// empty source means no write path runs on either engine. The ceremony
// itself (arbiter pre-check, speculative token, DO UPDATE dispatch, EPQ
// interplay) proves on a real server: the isolation battery's
// insert-conflict family + scripts/dualexec/corpus-dml-oc.sql +
// scripts/lane-dml-oc-e2e.sh (post-mutation content SELECTs + command-tag
// legs per the lane-dml-epq.md §10 proof channel).
mod dml_ab_wave5 {
    use super::*;

    /// Drive a plan to completion (exec_proc_node until None — the
    /// ExecutePlan cadence) and return es_processed (the dml_ab_wave3
    /// run_stmt shape, module-local per the append-region discipline).
    fn run_stmt(pstmt: &'static PlannedStmt<'static>, op: CmdType) -> u64 {
        let snap_ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, op, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            while exec_proc_node(ps, estate).unwrap().is_some() {}
            let processed = estate.es_processed;
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
            processed
        })
    }

    fn probes() -> (u64, u64) {
        (
            crate::lanev2::DML_OWNED_FOR_TESTS.load(std::sync::atomic::Ordering::Relaxed),
            crate::lanev2::DML_SHAPE_REFUSED_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// `MERGE INTO target USING ... ` skeleton: ModifyTable(CMD_MERGE) over
    /// a SeqScan of the target with the junk "ctid" row-id column
    /// (init_result_rel's rowid_attno lookup, the mk_update_delete_pstmt
    /// stand-in idiom) and one empty per-rel cell in mergeJoinConditions /
    /// mergeActionLists (nil join condition, zero WHEN actions). The empty
    /// source means no action ever dispatches — the unit exercises ONLY the
    /// admission verdict (`merge` must refuse even under DML+OC).
    fn mk_merge_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, target: u32) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{ModifyTable, Plan, Scan, SeqScan};

        let junk = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
        let scan_tlist = NodeList::make2(
            mcx,
            Node::mk_target_entry(mcx, Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap(), 1, Some("c1"), false)
                .unwrap(),
            Node::mk_target_entry(mcx, junk, 2, Some("ctid"), true).unwrap(),
        )
        .unwrap();
        let scan = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan { targetlist: scan_tlist, ..Default::default() },
                    scanrelid: 1,
                },
            },
        )
        .unwrap();

        let mut mt = Node::build::<ModifyTable>(mcx).unwrap();
        mt.plan.lefttree = Some(scan);
        mt.operation = CmdType::CMD_MERGE;
        mt.canSetTag = true;
        mt.nominalRelation = 1;
        mt.resultRelations = ::types_nodes::IntList::make1(mcx, 1).unwrap();
        mt.mergeJoinConditions =
            NodeList::make1(mcx, Node::mk_list(mcx, NodeList::nil()).unwrap()).unwrap();
        mt.mergeActionLists =
            NodeList::make1(mcx, Node::mk_list(mcx, NodeList::nil()).unwrap()).unwrap();

        let rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid: target,
                relkind: ::types_rel::RELKIND_RELATION,
                rellockmode: ::types_rel::RowExclusiveLock,
                perminfoindex: 1,
                inFromCl: false,
                ..Default::default()
            },
        )
        .unwrap();
        let perm = Node::mk(
            mcx,
            RTEPermissionInfo {
                relid: target,
                requiredPerms: ::types_nodes::parsenodes::ACL_UPDATE,
                ..Default::default()
            },
        )
        .unwrap();
        let mut unpruned = Bitmapset::empty();
        unpruned.add_member(mcx, 1).unwrap();

        let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
        pstmt.commandType = CmdType::CMD_MERGE;
        pstmt.canSetTag = true;
        pstmt.planTree = Some(mt.seal());
        pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
        pstmt.permInfos = NodeList::make1(mcx, perm).unwrap();
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    /// The OC admission arm: the arbiter-less ON CONFLICT DO NOTHING
    /// INSERT..SELECT (the exact wave-2 refusal fixture) is OWNED under
    /// DML+OC — owned ticks, DmlShape stays flat, behavior identical to the
    /// all-off Volcano oracle arm.
    #[test]
    fn dml_w5_oc_nothing_owned_with_oc() {
        install_seams();
        scanfix::install();
        dml_ab::install_replica_identity_seam();
        let target: u32 = 81001;
        let source: u32 = 81002;
        scanfix::register_table_2col(target, &[]);
        scanfix::register_table_2col(source, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        crate::lanev2::dml_set_for_tests(false);
        crate::lanev2::dml_oc_set_for_tests(false);
        let off = run_stmt(
            dml_ab::mk_insert_select_pstmt(leaked_mcx(), target, source, true),
            CmdType::CMD_INSERT,
        );

        crate::lanev2::dml_set_for_tests(true);
        crate::lanev2::dml_oc_set_for_tests(true);
        let (owned0, refused0) = probes();
        let on = run_stmt(
            dml_ab::mk_insert_select_pstmt(leaked_mcx(), target, source, true),
            CmdType::CMD_INSERT,
        );
        let (owned1, refused1) = probes();
        crate::lanev2::dml_set_for_tests(false);
        crate::lanev2::dml_oc_set_for_tests(false);
        assert_eq!(on, off, "knob arms must behave identically");
        assert!(owned1 > owned0, "OC arm never engaged on the admitted ON CONFLICT shape");
        assert_eq!(refused1, refused0, "the admitted OC shape must not tick DmlShape");

        scanfix::quiesced();
    }

    /// Nested-knob law, refusal arm: with the host knob on and _OC off the
    /// SAME shape still refuses as DmlShape ('on-conflict' detail) — the
    /// allowlist row stands unshrunk (wave-5 rider).
    #[test]
    fn dml_w5_oc_refused_without_oc() {
        install_seams();
        scanfix::install();
        dml_ab::install_replica_identity_seam();
        let target: u32 = 81003;
        let source: u32 = 81004;
        scanfix::register_table_2col(target, &[]);
        scanfix::register_table_2col(source, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        crate::lanev2::dml_set_for_tests(true);
        crate::lanev2::dml_oc_set_for_tests(false);
        let (owned0, refused0) = probes();
        let n = run_stmt(
            dml_ab::mk_insert_select_pstmt(leaked_mcx(), target, source, true),
            CmdType::CMD_INSERT,
        );
        let (owned1, refused1) = probes();
        crate::lanev2::dml_set_for_tests(false);
        assert_eq!(n, 0);
        assert_eq!(owned1, owned0, "ON CONFLICT must not be owned with _OC off");
        assert!(refused1 > refused0, "ON CONFLICT at _OC-off must tick DmlShape");

        scanfix::quiesced();
    }

    /// Nested-knob law, other arm: `_OC` alone flips NOTHING — with the DML
    /// host knob off the arm gate short-circuits and no wave-5 code runs.
    #[test]
    fn dml_w5_oc_alone_flips_nothing() {
        install_seams();
        scanfix::install();
        dml_ab::install_replica_identity_seam();
        let target: u32 = 81005;
        let source: u32 = 81006;
        scanfix::register_table_2col(target, &[]);
        scanfix::register_table_2col(source, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        crate::lanev2::dml_set_for_tests(false);
        crate::lanev2::dml_oc_set_for_tests(true);
        let (owned0, refused0) = probes();
        let n = run_stmt(
            dml_ab::mk_insert_select_pstmt(leaked_mcx(), target, source, true),
            CmdType::CMD_INSERT,
        );
        let (owned1, refused1) = probes();
        crate::lanev2::dml_oc_set_for_tests(false);
        assert_eq!(n, 0);
        assert_eq!(owned1, owned0, "_OC alone must not own");
        assert_eq!(refused1, refused0, "_OC alone must tick NOTHING");

        scanfix::quiesced();
    }

    /// MERGE stays refused EVEN under DML+OC (contract §8.2: the C-side
    /// trace pin is outstanding; the probe's `merge` arm is unconditional).
    #[test]
    fn dml_w5_merge_refused_even_oc_on() {
        install_seams();
        scanfix::install();
        dml_ab::install_replica_identity_seam();
        let target: u32 = 81007;
        scanfix::register_table_2col(target, &[]);
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        crate::lanev2::dml_set_for_tests(true);
        crate::lanev2::dml_oc_set_for_tests(true);
        let (owned0, refused0) = probes();
        let n = run_stmt(mk_merge_pstmt(leaked_mcx(), target), CmdType::CMD_MERGE);
        let (owned1, refused1) = probes();
        crate::lanev2::dml_set_for_tests(false);
        crate::lanev2::dml_oc_set_for_tests(false);
        assert_eq!(n, 0);
        assert_eq!(owned1, owned0, "MERGE must never be owned (trace pin outstanding)");
        assert!(refused1 > refused0, "MERGE under DML+OC must tick DmlShape");

        scanfix::quiesced();
    }
}
// --- end WS-W (wave-5) ----------------------------------------------------------

// --- WS-X wave-5 sub-region (cursors/SPI design; band 82001+, expected unused) --
// (reserved; WS-X appends here)
// --- end WS-X wave-5 sub-region -------------------------------------------------

// --- WS-Y wave-7 (EPQ inc-5 rungs Y0-Y2; band 83001+) ---------------------------
// Unit corpus for the lane-side EPQ module (lanev2/epq.rs): Y0
// captured-singleton source latch orderings + dark-code refusals, Y1
// per-node verdicts memoized once per recheck plan (wave-5 review finding
// 5's binding law), Y2 loud-admission-list tightenings (scanrelid == 0 +
// SubqueryScan.subplan recursion). Serialization: every exec-fixture test
// holds scanfix::TEST_LOCK for its full span (wave-2 precedent-3).
mod epq_capture_w7 {
    use super::*;
    use crate::lanev2::epq::{EpqCaptureFeed, EpqNodeVerdict};

    /// Y1 memoization law (wave-5 review finding 5, ledgered in
    /// lane-epq.md §6): the classification WALK runs once per recheck
    /// plan; the refusal TICKS keep wave-5 semantics (once per mappable
    /// node per recheck initiation, through the existing `epq` carrier).
    /// Two initiations over one EpqState: walks +1, ticks +2, outcomes
    /// byte-identical to the knob-OFF oracle.
    #[test]
    fn epq_w7_verdict_walk_memoized_once_per_plan() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();

        let relid: u32 = 83001;
        scanfix::register_table_2col(relid, &[&[(1, 10), (2, 20)]]);
        let pstmt = mk_epq_update_subplan_pstmt(mcx, relid);

        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));

        let walks = || {
            crate::lanev2::epq::EPQ_CLASSIFY_WALKS_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        let ticks = || {
            crate::lanev2::EPQ_ADMISSION_REFUSED_FOR_TESTS
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;

            let mut epq = crate::epq::EpqState {
                plan: pstmt.planTree,
                recheck: None,
                result_rti: 1,
                lane_verdicts: None,
            };
            let mut subs = None;
            ::executils::ensure_epq_subs(&mut subs, estate.es_query_cxt, estate.epq_rtsize(), 1);
            let desc = estate.es_relations[0].as_ref().unwrap().rd_att.clone();
            let test = estate.exec_init_extra_tuple_slot(
                Some(desc),
                ::types_slot::TupleSlotKind::Virtual,
            );
            subs.as_mut().unwrap().relsubs_slot[0] = Some(test);

            // Knob-OFF oracle first: the byte-identity baseline; neither
            // counter moves and no cache is built.
            crate::lanev2::epq_lane_set_for_tests(false);
            let (w0, t0) = (walks(), ticks());
            epq_store_test_tuple(estate, test, 1, 99);
            let off = crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
                .unwrap()
                .expect("qual passes");
            let off_vals = epq_slot_vals(estate, off);
            assert_eq!((walks(), ticks()), (w0, t0), "OFF arm must tick NOTHING");
            assert!(epq.lane_verdicts.is_none(), "OFF arm must build no cache");

            // ON arm, initiation 1: ONE classification walk + one tick for
            // the single mappable node (the SeqScan recheck plan).
            crate::lanev2::epq_lane_set_for_tests(true);
            epq_store_test_tuple(estate, test, 1, 99);
            let on1 = crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
                .unwrap()
                .expect("qual passes");
            let on1_vals = epq_slot_vals(estate, on1);
            assert_eq!(walks(), w0 + 1, "first initiation classifies the plan");
            assert_eq!(ticks(), t0 + 1, "one mappable node refuses via the epq carrier");
            assert!(epq.lane_verdicts.is_some(), "verdicts memoized on the EpqState");

            // ON arm, initiation 2 (same EpqState = same recheck plan): the
            // WALK does not re-run; the tick re-fires from the memo.
            epq_store_test_tuple(estate, test, 1, 5);
            let on2 = crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
                .unwrap()
                .expect("qual passes");
            let on2_vals = epq_slot_vals(estate, on2);
            crate::lanev2::epq_lane_set_for_tests(false);
            assert_eq!(walks(), w0 + 1, "ONE classification per recheck plan (memo law)");
            assert_eq!(ticks(), t0 + 2, "ticks stay per-initiation (wave-5 census semantics)");

            assert_eq!(on1_vals, off_vals, "knob arms behave identically (drive stays Volcano)");
            assert_eq!(on2_vals, (1, 5));

            crate::epq::eval_plan_qual_end(&mut epq, &mut subs, estate).unwrap();
            let ps = planstate.as_mut().unwrap();
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        scanfix::quiesced();
    }

    /// Y1 verdict vocabulary: per-node verdicts reuse the EXISTING
    /// engagement classes (mint count zero) and the walk visits exactly
    /// the loud admission list's edges. Sort-over-SeqScan classifies as
    /// [RescanComposed, CaptureScan]; Material/TidScan are structurally
    /// Short (no try_own_* surface — Y3 gate-delta rows); Hash is glue
    /// and contributes no entry.
    #[test]
    fn epq_w7_per_node_verdicts_reuse_engagement_classes() {
        use ::types_nodes::plannodes::{Hash, Material, Plan, Scan, SeqScan, Sort, TidScan};
        let mcx = leaked_mcx();

        let seq = Node::mk(
            mcx,
            SeqScan { cb_scan_cols: None, scan: Scan { plan: Plan::default(), scanrelid: 1 } },
        )
        .unwrap();
        let sort = Node::mk(
            mcx,
            Sort {
                plan: Plan { lefttree: Some(seq), ..Default::default() },
                ..Default::default()
            },
        )
        .unwrap();
        // The loud list admits the same tree the verdict walk classifies.
        crate::epq::check_epq_plan(sort);
        let rows = crate::lanev2::epq::epq_classify_for_tests(Some(sort));
        assert_eq!(
            rows,
            vec![
                ("sortfeed", EpqNodeVerdict::RescanComposed),
                ("seqscan", EpqNodeVerdict::CaptureScan),
            ],
            "existing engagement classes, walk order == loud-list order"
        );

        let material = Node::mk(mcx, Material { plan: Plan::default() }).unwrap();
        assert_eq!(
            crate::lanev2::epq::epq_classify_for_tests(Some(material)),
            vec![("material", EpqNodeVerdict::Short)],
            "no try_own_* surface => structurally Short (Y3 gate delta)"
        );

        let tid = Node::mk(
            mcx,
            TidScan {
                scan: Scan { plan: Plan::default(), scanrelid: 1 },
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            crate::lanev2::epq::epq_classify_for_tests(Some(tid)),
            vec![("tidscan", EpqNodeVerdict::Short)],
            "tid fallthrough + recheckMtd unwired => Short"
        );

        let hash = Node::mk(mcx, Hash::default()).unwrap();
        assert_eq!(
            crate::lanev2::epq::epq_classify_for_tests(Some(hash)),
            vec![],
            "glue tags tick nothing (vocab law: no new classes/reasons)"
        );
    }

    /// Y0 exactly-once latch: the captured-singleton source stages the
    /// parked test tuple ONCE (relsubs_done latched at the handout, like C
    /// ExecScanFetch), drains to zero, refuses emit after end_claim with a
    /// loud PgError, and a SECOND source over the latched rel stages
    /// nothing until the latch is reloaded (EvalPlanQualBegin's rescan
    /// arm, mimicked by hand).
    #[test]
    fn epq_w7_captured_source_exactly_once_latch() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();

        let relid: u32 = 83002;
        scanfix::register_table_2col(relid, &[&[(1, 10)]]);
        let pstmt = mk_epq_update_subplan_pstmt(mcx, relid);

        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));

        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;

            let mut subs = None;
            ::executils::ensure_epq_subs(&mut subs, estate.es_query_cxt, estate.epq_rtsize(), 1);
            let desc = estate.es_relations[0].as_ref().unwrap().rd_att.clone();
            let test = estate.exec_init_extra_tuple_slot(
                Some(desc),
                ::types_slot::TupleSlotKind::Virtual,
            );
            subs.as_mut().unwrap().relsubs_slot[0] = Some(test);
            epq_store_test_tuple(estate, test, 1, 99);

            // Swap in like eval_plan_qual's wrapper (capture model: the ONE
            // parent estate, the owner's subs), then mirror EvalPlanQual's
            // availability reset: the rel UNDER TEST is transiently
            // unblocked (ensure_epq_subs starts result_rti blocked+done,
            // per C EvalPlanQualStart).
            estate.es_epq = subs.take();
            estate.es_epq_active = true;
            {
                let s = estate.es_epq.as_mut().unwrap();
                s.relsubs_done[0] = false;
                s.relsubs_blocked[0] = false;
            }
            crate::lanev2::epq_lane_set_for_tests(true);

            let p = crate::lanev2::epq::epq_captured_probe_for_tests(
                estate,
                1,
                EpqCaptureFeed::TestSlot,
            )
            .unwrap()
            .expect("constructed: knob ON, active recheck, slot parked");
            assert_eq!(p.granule_total, 1, "ONE granule: the captured row");
            assert_eq!(p.first_batch, 1, "the singleton batch stages");
            assert_eq!(p.emitted, Some(test), "emit(0) hands out the parked test slot");
            assert_eq!(p.second_batch, 0, "drained after the handout");
            assert!(p.done_latched, "relsubs_done latched at the handout (exactly-once)");
            assert!(p.reemit_refused, "emit after end_claim = loud PgError, never a panic");
            assert!(
                p.empty_claim_refused,
                "empty claim window (0..0) refuses loudly — only the exact \
                 singleton window positions (wave-7 review finding 3)"
            );

            // Latched rel: a fresh source constructs (slot still parked)
            // but stages nothing — the done latch IS source exhaustion.
            let p2 = crate::lanev2::epq::epq_captured_probe_for_tests(
                estate,
                1,
                EpqCaptureFeed::TestSlot,
            )
            .unwrap()
            .expect("constructed");
            assert_eq!(p2.first_batch, 0, "done-latched rel stages nothing");
            assert_eq!(p2.emitted, None);

            // Begin's rescan arm reloads done from blocked (both false
            // here): the reloaded latch re-arms the singleton.
            estate.es_epq.as_mut().unwrap().relsubs_done[0] = false;
            let p3 = crate::lanev2::epq::epq_captured_probe_for_tests(
                estate,
                1,
                EpqCaptureFeed::TestSlot,
            )
            .unwrap()
            .expect("constructed");
            assert_eq!(p3.first_batch, 1, "latch reload re-arms the captured row");

            crate::lanev2::epq_lane_set_for_tests(false);
            estate.es_epq_active = false;
            subs = estate.es_epq.take();

            let mut epq = crate::epq::EpqState {
                plan: pstmt.planTree,
                recheck: None,
                result_rti: 1,
                lane_verdicts: None,
            };
            crate::epq::eval_plan_qual_end(&mut epq, &mut subs, estate).unwrap();
            let ps = planstate.as_mut().unwrap();
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        scanfix::quiesced();
    }

    /// Y0 dark-code + blocked/origslot arms: the constructor refuses knob
    /// OFF, refuses outside an active recheck, refuses an unparked feed
    /// cell; a blocked rel (done reloaded true by Begin) stages nothing;
    /// the OrigSlot flavor stages the row under recheck from
    /// `EpqSubs::origslot` (the rowmark feed of lane-epq.md §2).
    #[test]
    fn epq_w7_captured_source_dark_blocked_origslot_arms() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();

        let relid: u32 = 83003;
        scanfix::register_table_2col(relid, &[&[(1, 10)]]);
        let pstmt = mk_epq_update_subplan_pstmt(mcx, relid);

        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));

        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;

            let mut subs = None;
            ::executils::ensure_epq_subs(&mut subs, estate.es_query_cxt, estate.epq_rtsize(), 1);
            let desc = estate.es_relations[0].as_ref().unwrap().rd_att.clone();
            let test = estate.exec_init_extra_tuple_slot(
                Some(desc),
                ::types_slot::TupleSlotKind::Virtual,
            );
            subs.as_mut().unwrap().relsubs_slot[0] = Some(test);
            epq_store_test_tuple(estate, test, 1, 99);
            estate.es_epq = subs.take();

            // Dark-code arm 1: knob OFF refuses even inside a recheck.
            estate.es_epq_active = true;
            crate::lanev2::epq_lane_set_for_tests(false);
            assert!(crate::lanev2::epq::epq_captured_probe_for_tests(
                estate,
                1,
                EpqCaptureFeed::TestSlot
            )
            .unwrap()
            .is_none());

            // Dark-code arm 2: knob ON outside an active recheck refuses.
            crate::lanev2::epq_lane_set_for_tests(true);
            estate.es_epq_active = false;
            assert!(crate::lanev2::epq::epq_captured_probe_for_tests(
                estate,
                1,
                EpqCaptureFeed::TestSlot
            )
            .unwrap()
            .is_none());

            // Unparked feed cell refuses (the plain-rescannable rel is not
            // this source's shape).
            estate.es_epq_active = true;
            assert!(
                crate::lanev2::epq::epq_captured_probe_for_tests(
                    estate,
                    1,
                    EpqCaptureFeed::OrigSlot
                )
                .unwrap()
                .is_none(),
                "no origslot parked => refuse"
            );

            // Blocked rel (writep4a/writep4b inheritance class): Begin
            // reloads done from blocked; the source stages nothing.
            {
                let s = estate.es_epq.as_mut().unwrap();
                s.relsubs_blocked[0] = true;
                s.relsubs_done[0] = true;
            }
            let pb = crate::lanev2::epq::epq_captured_probe_for_tests(
                estate,
                1,
                EpqCaptureFeed::TestSlot,
            )
            .unwrap()
            .expect("constructed: slot parked");
            assert_eq!(pb.first_batch, 0, "blocked rel stages nothing");

            // OrigSlot flavor: the row under recheck feeds the singleton.
            {
                let s = estate.es_epq.as_mut().unwrap();
                s.relsubs_blocked[0] = false;
                s.relsubs_done[0] = false;
                s.origslot = Some(test);
            }
            let po = crate::lanev2::epq::epq_captured_probe_for_tests(
                estate,
                1,
                EpqCaptureFeed::OrigSlot,
            )
            .unwrap()
            .expect("constructed: origslot parked");
            assert_eq!(po.first_batch, 1);
            assert_eq!(po.emitted, Some(test), "origslot feeds the captured row");
            assert!(po.done_latched, "rowmark handout latches done too (C 18 semantics)");

            crate::lanev2::epq_lane_set_for_tests(false);
            estate.es_epq_active = false;
            subs = estate.es_epq.take();

            let mut epq = crate::epq::EpqState {
                plan: pstmt.planTree,
                recheck: None,
                result_rti: 1,
                lane_verdicts: None,
            };
            crate::epq::eval_plan_qual_end(&mut epq, &mut subs, estate).unwrap();
            let ps = planstate.as_mut().unwrap();
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        scanfix::quiesced();
    }

    /// Y2: `scanrelid == 0` pushed-down-join scans refuse LOUDLY until a
    /// spec exercises them (lane-epq.md §2's recorded FDW gap, now pinned
    /// for every ADMITTED scan tag as well).
    #[test]
    #[should_panic(expected = "scanrelid == 0")]
    fn epq_w7_scanrelid_zero_refused_loudly() {
        use ::types_nodes::plannodes::{Plan, Scan, SeqScan};
        let mcx = leaked_mcx();
        let seq = Node::mk(
            mcx,
            SeqScan { cb_scan_cols: None, scan: Scan { plan: Plan::default(), scanrelid: 0 } },
        )
        .unwrap();
        crate::epq::check_epq_plan(seq);
    }

    /// Y2: the loud list recurses into SubqueryScan.subplan — an admitted
    /// SubqueryScan can no longer silently admit an unexercised shape
    /// underneath (honesty gap closed; positive arm proves the admitted
    /// composition still passes).
    #[test]
    #[should_panic(expected = "recheck plan")]
    fn epq_w7_subqueryscan_subplan_recursed_loudly() {
        use ::types_nodes::plannodes::{Plan, Scan, SeqScan, SubqueryScan};
        let mcx = leaked_mcx();
        let seq = Node::mk(
            mcx,
            SeqScan { cb_scan_cols: None, scan: Scan { plan: Plan::default(), scanrelid: 1 } },
        )
        .unwrap();
        let ok = Node::mk(
            mcx,
            SubqueryScan {
                scan: Scan { plan: Plan::default(), scanrelid: 1 },
                subplan: Some(seq),
                scanstatus: 0,
            },
        )
        .unwrap();
        crate::epq::check_epq_plan(ok);
        // Negative arm: an Agg UNDER an admitted SubqueryScan now refuses.
        let agg = Node::build::<::types_nodes::plannodes::Agg>(mcx).unwrap().seal();
        let bad = Node::mk(
            mcx,
            SubqueryScan {
                scan: Scan { plan: Plan::default(), scanrelid: 1 },
                subplan: Some(agg),
                scanstatus: 0,
            },
        )
        .unwrap();
        crate::epq::check_epq_plan(bad);
    }
}
// --- end WS-Y wave-7 ------------------------------------------------------------

// ============================================================================
// ===== WAVE-9 SHARED TEST REGION (contract §7) — sub-regions in AG, AH, AI,
// AJ order; each WS fills ONLY its own block; integration splices verbatim.
// ============================================================================
// --- WS-AG (wave-9): the fusion D1a per-mask chain-program pins — DELETED
// at RB-R1 (SE18) with the stitched trigger-INSERT chain (the per-mask
// program builder and lanestitch chain classification no longer exist).
// --- end WS-AG (wave-9) ------------------------------------------------------
// --- WS-AH (wave-9): reserved ---
// --- WS-AI wave-9 (forward-pull cursors inc-1; contract §3, band 92001+) -------
// Unit pins for the §1 budget-N emit-sink substrate: the run-seam budget
// install gates (incl. the §3 serial-law fail-closed arm), the estate-
// resident install + None-overwrite law, count-exact stop, and forward
// resume across two count-limited `execute_plan` drives on one ExecData —
// the portal FETCH cadence at the unit level, byte-compared against the
// knob-OFF oracle. Serialization: the exec-fixture test holds
// scanfix::TEST_LOCK for its full span (wave-2 precedent-3); the knob
// lever is set explicitly at every test start (process-global static).
mod cursors_wave9 {
    use super::*;

    /// The install gate is the §3.1 count-exact suspension shape and
    /// nothing else: knob-ON AND count != 0 AND forward AND SELECT AND
    /// serial. The `use_parallel_mode` arm is the §3 serial-law pin at
    /// this seam — FAIL-CLOSED (no budget over a gang, ever), unreachable
    /// in production by the ported execmain.rs:978 gate (count != 0
    /// forces serial).
    #[test]
    fn cursors_w9_budget_gate_semantics() {
        // The knob lever is a process-global static: serialize every test
        // that flips it (inc-1b grew a second unlocked flipper — the
        // fixture lock is the module's knob lock too).
        let _knob = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let install = crate::lanev2::cursor_run_budget_install;

        // Knob OFF (default posture): nothing installs, any shape.
        crate::lanev2::cursors_set_for_tests(false);
        assert_eq!(install(true, true, 92001, false, 0), None, "knob-OFF installs nothing");
        assert_eq!(install(true, true, 1, false, 0), None);

        // Knob ON: exactly the count-limited forward serial SELECT shape.
        crate::lanev2::cursors_set_for_tests(true);
        assert_eq!(install(true, true, 92001, false, 0), Some(92001), "the §3.1 shape installs");
        assert_eq!(install(true, true, 0, false, 0), None, "count-0 (FETCH_ALL) never installs");
        assert_eq!(install(false, true, 92001, false, 0), None, "non-SELECT never installs");
        assert_eq!(install(true, false, 92001, false, 0), None, "non-forward never installs");
        assert_eq!(
            install(true, true, 92001, true, 0),
            None,
            "serial-law pin: a parallel run NEVER carries a budget (fail-closed)"
        );

        crate::lanev2::cursors_set_for_tests(false);
    }

    /// inc-1b ADMISSION TAXONOMY (contract item 2): every non-admit at the
    /// cursor seam is a NAMED refusal class — the R-VOCAB registry strings
    /// pinned here are the corpus cells' labels and the allowlist rows'
    /// spelling. SCROLL detection = the REWIND|BACKWARD|MARK top eflags
    /// PortalStart writes for scrollable portals (declared AND
    /// free-upgraded arrive identically — one class covers both corpus
    /// shapes). The serial-law arm keeps the EXISTING `parallel-gate`
    /// vocabulary (it is the §3 pin, not a taxonomy row).
    #[test]
    fn cursors_w95_admission_taxonomy_named_classes() {
        use ::types_slot::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
        // Serialize with every other knob-flipping test (process-global
        // static; the fixture lock doubles as the module's knob lock).
        let _knob = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let name = crate::lanev2::cursor_admission_refusal_name;

        // Admit: forward, serial.
        assert_eq!(name(true, 0, false), None, "the forward-only shape admits");

        // SUNSET EXECUTED (se/seam-wiring, SE10-GATES item 1): the
        // cursor-scroll eflags arm is REMOVED — scroll-capable eflags no
        // longer refuse the budget (store-served portals are
        // lane-ADMITTED; eligible row-chain fills carry these eflags and
        // refuse per-scan via batch_allowed, rolling up as
        // cursor-plan-refused). The classifier ADMITS these shapes now.
        assert_eq!(name(true, EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD, false), None);
        assert_eq!(name(true, EXEC_FLAG_REWIND, false), None);
        assert_eq!(name(true, EXEC_FLAG_MARK, false), None);

        // cursor-backward: the direction demand outranks the portal
        // capability (the corpus's explicit-backward cell naming).
        assert_eq!(
            name(false, EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD, false),
            Some("cursor-backward")
        );
        assert_eq!(name(false, 0, false), Some("cursor-backward"));

        // Serial-law fail-closed arm (NOT a cursor taxonomy row).
        assert_eq!(name(true, 0, true), Some("parallel-gate"));

        // The install half post-SUNSET: scroll-capable eflags no longer
        // refuse — the budget INSTALLS (store-served portals are
        // lane-admitted; eligible row-chain fills refuse per-scan via
        // batch_allowed and nothing lane-stages). A named refusal
        // (backward direction) still refuses the WHOLE run: no budget ⇒
        // no cursor machinery ⇒ Volcano byte-identical (fail-open).
        crate::lanev2::cursors_set_for_tests(true);
        assert_eq!(
            crate::lanev2::cursor_run_budget_install(
                true,
                true,
                92010,
                false,
                EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD
            ),
            Some(92010),
            "post-SUNSET: scroll-capable eflags install the budget"
        );
        assert_eq!(
            crate::lanev2::cursor_run_budget_install(true, false, 92010, false, 0),
            None,
            "a backward run still refuses wholesale (cursor-backward)"
        );
        assert_eq!(
            crate::lanev2::cursor_run_budget_install(true, true, 92010, false, 0),
            Some(92010)
        );
        crate::lanev2::cursors_set_for_tests(false);
    }

    /// The run seam writes `es_cursor_run_budget` UNCONDITIONALLY at every
    /// `execute_plan` entry: a knob-ON count-2 drive installs Some(2),
    /// stops count-exact, and the SAME ExecData resumes forward from the
    /// node-resident position on the next drive (the portal FETCH cadence);
    /// the count-0 follow-up run overwrites the budget to None (no stale
    /// budget survives). Rows across both drives are byte-identical to the
    /// knob-OFF oracle pair below (fail-open discipline: the budget is a
    /// signal, never a semantics change).
    #[test]
    fn cursors_w9_budgeted_run_installs_stops_exact_and_resumes() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();

        // Two knob arms over the identical table shape (band-92001 relids).
        let mut arm_rows: Vec<Vec<i32>> = Vec::new();
        for (arm_on, relid) in [(false, 92001u32), (true, 92002u32)] {
            crate::lanev2::cursors_set_for_tests(arm_on);
            scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
            let pstmt = mk_seqscan_pstmt(mcx, relid);

            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));

            let mut rows: Vec<i32> = Vec::new();
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                let desc =
                    crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                assert_eq!(
                    data.estate.es_cursor_run_budget, None,
                    "fresh estate carries no budget"
                );

                let store = tuplestore::Tuplestore::begin_heap(true, false, 1024);
                let h = tuplestore::hold::register(store);
                let mut dr = tstore_receiver::tstore_create_DR();
                tstore_receiver::set_params(&mut dr, h, false);
                let mut dest = DestReceiver::Tuplestore(dr);

                // Drive 1: FETCH 2 (count-limited forward SELECT, serial).
                data.estate.es_processed = 0;
                crate::execmain::execute_plan(
                    data,
                    CmdType::CMD_SELECT,
                    true,
                    2,
                    ForwardScanDirection,
                    false,
                    &mut dest,
                )
                .unwrap();
                assert_eq!(data.estate.es_processed, 2, "count-exact stop");
                assert_eq!(
                    crate::lanev2::cursor_run_budget(&data.estate),
                    if arm_on { Some(2) } else { None },
                    "budget installed iff knob-ON (read through the consumer face)"
                );

                // Drive 2: FETCH ALL (count 0) resumes from the node-resident
                // position and overwrites the budget to None on BOTH arms.
                data.estate.es_processed = 0;
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
                assert_eq!(data.estate.es_processed, 3, "resume served the remainder");
                assert_eq!(
                    data.estate.es_cursor_run_budget, None,
                    "count-0 run overwrote the budget (no stale Some survives)"
                );

                let read_cx: &'static MemoryContext =
                    Box::leak(Box::new(MemoryContext::new("read")));
                let mut slot = exectuples::make_tuple_table_slot(
                    read_cx.mcx(),
                    ::types_slot::TupleSlotKind::MinimalTuple,
                    Some(desc.clone()),
                );
                loop {
                    let got = tuplestore::hold::with_store(h, |ts| {
                        ts.gettupleslot(true, true, &mut slot, read_cx.mcx())
                    })
                    .unwrap();
                    if !got {
                        break;
                    }
                    let mut isnull = false;
                    let v = exectuples::slot_getattr(&mut slot, 1, &mut isnull);
                    assert!(!isnull);
                    rows.push(v.as_i32());
                }
                tuplestore::hold::end(h);

                let ExecData { estate, planstate } = data;
                crate::exec_end_node(planstate.as_mut().unwrap(), estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
            assert_eq!(rows, vec![1, 2, 3, 4, 5], "both drives together read the whole table");
            arm_rows.push(rows);
        }
        assert_eq!(arm_rows[0], arm_rows[1], "knob arms byte-identical (fail-open law)");
        crate::lanev2::cursors_set_for_tests(false);
        scanfix::quiesced();
    }

    /// inc-1b PARK SHAPE (contract item 1; lane-cursors.md §2): a suspended
    /// lane-staged page batch SETTLES — claim released through the chain
    /// (R3 zero pins, `scanfix::quiesced()` is the teeth), reposition
    /// recorded node-resident — and the next run's resume walk restages the
    /// SAME visible set with the consume cursor restored, so the emitted
    /// remainder is byte-identical to an unsuspended drive (the oracle
    /// arm). Also pins: the pos==n boundary (fully-consumed staged batch
    /// still parks — resume restages, then the walk advances normally),
    /// settle/resume idempotence across TWO cycles (the remainder-window
    /// arithmetic survives a resumed scan's re-park), and the EPQ law (a
    /// budgeted estate inside an EPQ recheck settles NOTHING — the budget
    /// belongs to the outer run, the inc-1a §5 note).
    #[test]
    fn cursors_w95_park_settle_releases_pin_and_resume_restages() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();
        crate::lanev2::cursors_set_for_tests(true);

        // The lane consume face over a staged batch, as the standalone
        // pipeline drives it (stage → per-row emit → cursor advance).
        fn emit_rows<'m>(
            ss: &mut ::nodeseqscan::SeqScanState<'m>,
            estate: &mut EStateData<'m>,
            upto: u32,
            out: &mut Vec<i32>,
        ) {
            let (mut pos, n) = ss.lane_cursor();
            while pos < upto.min(n) {
                let slot_id = ::nodeseqscan::seq_scan_batch_emit(ss, estate, pos)
                    .unwrap()
                    .expect("staged row emits");
                let mut isnull = false;
                let v =
                    exectuples::slot_getattr(estate.slot_mut(slot_id), 1, &mut isnull);
                assert!(!isnull);
                out.push(v.as_i32());
                pos += 1;
                ss.set_lane_cursor(pos, n);
            }
        }

        // Two arms over identical two-page tables: oracle = straight lane
        // drive, park arm = the same drive suspended twice mid-flight.
        let mut arm_rows: Vec<Vec<i32>> = Vec::new();
        for (parked_arm, relid) in [(false, 92003u32), (true, 92004u32)] {
            scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
            let pstmt = mk_seqscan_pstmt(mcx, relid);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            let mut rows: Vec<i32> = Vec::new();
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let ExecData { estate, planstate } = data;
                let planstate = planstate.as_mut().unwrap();

                // Stage page 0 and consume one row.
                {
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                    else {
                        panic!("bare seqscan plan");
                    };
                    let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate).unwrap();
                    assert_eq!(n, 3);
                    ss.set_lane_cursor(0, n);
                    emit_rows(ss, estate, 1, &mut rows);
                }

                if parked_arm {
                    // EPQ law first: a budgeted estate inside an EPQ
                    // recheck settles NOTHING.
                    estate.es_epq_active = true;
                    assert!(
                        !crate::lanev2::cursor_run_park(planstate, estate).unwrap(),
                        "EPQ drive must not park"
                    );
                    estate.es_epq_active = false;
                    {
                        let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                        else {
                            unreachable!()
                        };
                        assert_eq!(ss.lane_cursor(), (1, 3), "EPQ arm left state untouched");
                        assert!(!::nodeseqscan::seq_scan_cursor_parked(ss));
                    }

                    // SUSPENSION 1 (mid-batch): settle releases the staged
                    // claim's pin — R3 zero-pins-at-settle, asserted by the
                    // fixture's pin census.
                    assert!(crate::lanev2::cursor_run_park(planstate, estate).unwrap(), "parks");
                    scanfix::quiesced(); // R3: ZERO pins while suspended
                    {
                        let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                        else {
                            unreachable!()
                        };
                        assert!(::nodeseqscan::seq_scan_cursor_parked(ss));
                        assert_eq!(ss.lane_cursor(), (0, 0), "staged state settled");
                    }
                    // RESUME: restage + cursor restore, byte-identical set.
                    crate::lanev2::cursor_park_resume(planstate, estate).unwrap();
                    {
                        let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                        else {
                            unreachable!()
                        };
                        assert!(!::nodeseqscan::seq_scan_cursor_parked(ss));
                        assert_eq!(ss.lane_cursor(), (1, 3), "consume cursor restored");
                    }
                }

                // Drain the rest of page 0.
                {
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                    else {
                        unreachable!()
                    };
                    emit_rows(ss, estate, 3, &mut rows);
                    assert_eq!(ss.lane_cursor(), (3, 3));
                }

                if parked_arm {
                    // SUSPENSION 2 (pos == n boundary — the fully-consumed
                    // staged batch still holds its pin, so it still parks;
                    // the second cycle also pins the resumed-window
                    // remainder arithmetic).
                    assert!(crate::lanev2::cursor_run_park(planstate, estate).unwrap(), "re-parks");
                    scanfix::quiesced();
                    crate::lanev2::cursor_park_resume(planstate, estate).unwrap();
                    {
                        let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                        else {
                            unreachable!()
                        };
                        assert_eq!(ss.lane_cursor(), (3, 3), "boundary cursor restored");
                    }
                }

                // Advance to page 1 and drain it (the walk continues from
                // the restaged position on both arms).
                {
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                    else {
                        unreachable!()
                    };
                    let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate).unwrap();
                    assert_eq!(n, 2);
                    ss.set_lane_cursor(0, n);
                    emit_rows(ss, estate, 2, &mut rows);
                    let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate).unwrap();
                    assert_eq!(n, 0, "end of scan");
                }

                crate::exec_end_node(planstate, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
            assert_eq!(rows, vec![1, 2, 3, 4, 5], "the whole table, in order");
            arm_rows.push(rows);
        }
        assert_eq!(
            arm_rows[0], arm_rows[1],
            "suspended+resumed drive byte-identical to the straight drive"
        );
        crate::lanev2::cursors_set_for_tests(false);
        scanfix::quiesced();
    }

    /// BRIN-BUDGET P1 pin (band 99301): the settle walk's slot hygiene
    /// MATERIALIZES the parked scan's emitted slot instead of clearing it.
    /// A node above the seam (a lane join probe holding `ecxt_outertuple`
    /// for the whole inner iteration — the brin/brin_bloom/brin_multi
    /// DO-loop shape, plpgsql FOR over `brinopers JOIN unnest(op)`) reads
    /// the suspended run's last emitted slot on the NEXT fetch; the old
    /// clear made that read `heap slot without tuple`
    /// (deform.rs:420/535), deterministic under EITHER budget flavor.
    /// Pinned here: after a park the slot is (a) NON-EMPTY with the
    /// emitted row's values readable — the join-holder read — and (b)
    /// pin-free (R3 zero-pins across the suspension, `scanfix::quiesced`),
    /// and the resumed drive stays byte-identical.
    #[test]
    fn cursors_brinfix_park_retains_emitted_slot_values() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();
        crate::lanev2::cursors_set_for_tests(true);

        scanfix::register_table(99301, &[&[7, 8, 9], &[10, 11]]);
        let pstmt = mk_seqscan_pstmt(mcx, 99301);
        let snap_ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        let mut rows: Vec<i32> = Vec::new();
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let planstate = planstate.as_mut().unwrap();

            // Stage page 0 and emit one row (the suspended run's last
            // emitted tuple — the slot a join probe above still holds).
            let scan_slot_id = {
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                else {
                    panic!("bare seqscan plan");
                };
                let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate).unwrap();
                assert_eq!(n, 3);
                ss.set_lane_cursor(0, n);
                let slot_id = ::nodeseqscan::seq_scan_batch_emit(ss, estate, 0)
                    .unwrap()
                    .expect("staged row emits");
                let mut isnull = false;
                let v = exectuples::slot_getattr(estate.slot_mut(slot_id), 1, &mut isnull);
                assert!(!isnull);
                rows.push(v.as_i32());
                ss.set_lane_cursor(1, n);
                slot_id
            };
            assert_eq!(rows, vec![7]);

            // Park. R3: zero pins while suspended.
            assert!(crate::lanev2::cursor_run_park(planstate, estate).unwrap(), "parks");
            scanfix::quiesced();

            // THE PIN: the emitted slot survived the settle — non-empty,
            // values intact (the read that used to die with
            // "heap slot without tuple" on the next fetch's join
            // projection).
            {
                let slot = estate.slot_mut(scan_slot_id);
                assert!(
                    !slot.base().is_empty(),
                    "parked scan's emitted slot must retain its tuple (materialized, not cleared)"
                );
                let mut isnull = false;
                let v = exectuples::slot_getattr(slot, 1, &mut isnull);
                assert!(!isnull);
                assert_eq!(v.as_i32(), 7, "materialized slot serves the emitted row's values");
            }

            // Resume and drain the remainder — byte-identical continuation.
            crate::lanev2::cursor_park_resume(planstate, estate).unwrap();
            {
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                else {
                    unreachable!()
                };
                assert_eq!(ss.lane_cursor(), (1, 3), "consume cursor restored");
                loop {
                    let (mut pos, n) = ss.lane_cursor();
                    while pos < n {
                        let slot_id = ::nodeseqscan::seq_scan_batch_emit(ss, estate, pos)
                            .unwrap()
                            .expect("staged row emits");
                        let mut isnull = false;
                        let v = exectuples::slot_getattr(
                            estate.slot_mut(slot_id),
                            1,
                            &mut isnull,
                        );
                        assert!(!isnull);
                        rows.push(v.as_i32());
                        pos += 1;
                        ss.set_lane_cursor(pos, n);
                    }
                    let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate).unwrap();
                    if n == 0 {
                        break;
                    }
                    ss.set_lane_cursor(0, n);
                }
            }

            crate::exec_end_node(planstate, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        assert_eq!(rows, vec![7, 8, 9, 10, 11], "the whole table, in order");
        crate::lanev2::cursors_set_for_tests(false);
        scanfix::quiesced();
    }

    /// inc-1b run-seam pin: a knob-ON budgeted VOLCANO run (heap standalone
    /// refuses lane ownership) settles nothing, never sets the estate
    /// resume flag, and stays byte-identical across the FETCH cadence —
    /// the settle walk is engagement-gated, and Volcano's own cross-FETCH
    /// pin posture (C parity) is untouched.
    #[test]
    fn cursors_w95_budgeted_volcano_run_parks_nothing() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();
        crate::lanev2::cursors_set_for_tests(true);
        scanfix::register_table(92005, &[&[1, 2, 3], &[4, 5]]);
        let pstmt = mk_seqscan_pstmt(mcx, 92005);
        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            let desc = crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();

            let store = tuplestore::Tuplestore::begin_heap(true, false, 1024);
            let h = tuplestore::hold::register(store);
            let mut dr = tstore_receiver::tstore_create_DR();
            tstore_receiver::set_params(&mut dr, h, false);
            let mut dest = DestReceiver::Tuplestore(dr);

            // FETCH 2 then FETCH ALL — the inc-1a resume cadence, now over
            // the settle/resume-bearing run seam.
            for (count, want) in [(2u64, 2u64), (0, 3)] {
                data.estate.es_processed = 0;
                crate::execmain::execute_plan(
                    data,
                    CmdType::CMD_SELECT,
                    true,
                    count,
                    ForwardScanDirection,
                    false,
                    &mut dest,
                )
                .unwrap();
                assert_eq!(data.estate.es_processed, want);
                assert!(
                    !data.estate.es_lane_cursor_parked,
                    "a Volcano-refused plan never parks (nothing lane-staged)"
                );
            }

            let read_cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("read")));
            let mut slot = exectuples::make_tuple_table_slot(
                read_cx.mcx(),
                ::types_slot::TupleSlotKind::MinimalTuple,
                Some(desc.clone()),
            );
            let mut rows: Vec<i32> = Vec::new();
            loop {
                let got = tuplestore::hold::with_store(h, |ts| {
                    ts.gettupleslot(true, true, &mut slot, read_cx.mcx())
                })
                .unwrap();
                if !got {
                    break;
                }
                let mut isnull = false;
                let v = exectuples::slot_getattr(&mut slot, 1, &mut isnull);
                assert!(!isnull);
                rows.push(v.as_i32());
            }
            tuplestore::hold::end(h);
            assert_eq!(rows, vec![1, 2, 3, 4, 5]);

            let ExecData { estate, planstate } = data;
            crate::exec_end_node(planstate.as_mut().unwrap(), estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        crate::lanev2::cursors_set_for_tests(false);
        scanfix::quiesced();
    }
}
// --- end WS-AI wave-9 -----------------------------------------------------------
// --- WS-AJ wave-9 sub-region (se/wave9-spi-inc1): Stage A count-seam pins ---
// Fake-oid band 93001+ (wave-9 contract §5). Evidence/corpus-only increment
// (contract §4 re-scope: WS-AI's budget sink absent at branch-open — no seam
// code this increment). These pins freeze the VOLCANO-WORLD `executor_run`
// count semantics that `_SPI_pquery` consumes (spi/src/execute.rs:562 —
// executor_run(qd, Forward, tcount, dest); :563 — SPI_processed reads
// es_processed), per docs/design/lane-spi.md §1. The future Stage A lane
// seam (PGRUST_LANE_V2_SPI) must keep every assertion green byte-for-byte:
//   * tcount=N stops after exactly N emitted rows; es_processed == N
//     (SPI_processed correct BY CONSTRUCTION);
//   * the `_SPI_pquery` shape is STOP-then-END: ExecutorFinish/End run
//     directly after the single count-limited run — no park, no resume —
//     and teardown returns the fixture to zero pins (the settle face,
//     scanfix::quiesced()). (SPI portal fetches are the second, RESUMABLE
//     producer of count-limited Spi-dest runs — review re-baseline,
//     notes/se-spi-stage-a.md §8; pinned in spi_stage_a_aj_w95.)
//   * tcount=0 runs to completion; tcount > available saturates.
mod spi_inc1_aj_w9 {
    use super::*;

    /// `_SPI_pquery`'s exact seam cadence (spi/src/execute.rs:560-575):
    /// start(eflags) → ONE run(Forward, tcount) → read es_processed →
    /// finish → end. No second run, no park, no resume.
    ///
    /// The QueryDesc carries NO snapshot: the scanfix fixture AM serves rows
    /// itself (table_beginscan takes Option<Snapshot>; the fixture ignores
    /// it), and a Some(snapshot) here would drag the resowner + snapmgr
    /// substrate into the fixture (RegisterSnapshot at querydesc.rs:230
    /// needs current_resource_owner, whose real seams hashjoin_multibatch
    /// installs unconditionally — a second installer would panic the suite).
    /// What these pins freeze is the seam CADENCE and count semantics, not
    /// MVCC; the select1 seam tests take the same None-snapshot shape.
    fn run_spi_shape(relid: u32, tcount: u64) -> u64 {
        let mcx = leaked_mcx();
        let pstmt = mk_seqscan_pstmt(mcx, relid);
        let qd = execmain_seams::create_query_desc::call(
            pstmt,
            "SELECT a FROM spi_inc1_fixture",
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
        execmain_seams::executor_run::call(qd, ForwardScanDirection, tcount, &mut dest)
            .unwrap();
        let n = execmain_seams::query_desc_es_processed::call(qd);
        execmain_seams::executor_finish::call(qd).unwrap();
        execmain_seams::executor_end::call(qd).unwrap();
        execmain_seams::free_query_desc::call(qd);
        n
    }

    /// tcount=N (N < rows): count-exact stop with es_processed == N, and the
    /// STOP-then-END teardown releases every pin mid-scan (early stop inside
    /// a page and at a page boundary).
    #[test]
    fn spi_shape_tcount_stops_exactly_and_settles() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 93001; // WS-AJ band
        scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
        assert_eq!(run_spi_shape(relid, 2), 2, "tcount=2 must emit exactly 2");
        scanfix::quiesced();
        assert_eq!(run_spi_shape(relid, 4), 4, "tcount=4 crosses the page boundary");
        scanfix::quiesced();
    }

    /// tcount=0: run to completion — the SPI no-limit arm.
    #[test]
    fn spi_shape_tcount_zero_runs_to_completion() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 93002; // WS-AJ band
        scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
        assert_eq!(run_spi_shape(relid, 0), 5);
        scanfix::quiesced();
    }

    /// tcount > available: es_processed reports what was emitted, never the
    /// request.
    #[test]
    fn spi_shape_tcount_saturates_at_available() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 93003; // WS-AJ band
        scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
        assert_eq!(run_spi_shape(relid, 99), 5);
        scanfix::quiesced();
    }
}

// WS-AJ wave-9.5 (SPI Stage-A seam, se/spi-stage-a; append-only growth of
// this same WS-AJ sub-region): the seam-half pins — knob gate order, the
// NAMED admission taxonomy, the knob-ON re-ride of the frozen Volcano
// cadence pins above through a REAL `CommandDest::Spi` receiver (the budget
// installs, the settle runs, and every count/pin assertion holds
// byte-for-byte), and the review-re-baseline park/resume pin (a budgeted
// SPI-dest run that parks arms the SHARED es_lane_cursor_parked resume
// signal — the portal-fetch producer resumes; notes/se-spi-stage-a.md §8).
// Knob lever = process-global static: every flipping test serializes on
// scanfix::TEST_LOCK (the cursors_wave9 precedent) and restores OFF.
mod spi_stage_a_aj_w95 {
    use super::*;

    /// The SpiPrintTup receiver's owner callbacks live in spi.c (the spi
    /// crate installs them at backend boot); the unit world installs
    /// counting stubs ONCE (seams are set-once process-globals — the
    /// hashjoin_multibatch double-installer lesson, so this helper is the
    /// only installer in the suite and it is Once-guarded).
    fn install_spi_dest_seams() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            ::spi_seams::spi_dest_startup::set(spi_dest_startup_stub);
            ::spi_seams::spi_printtup::set(spi_printtup_stub);
        });
    }
    fn spi_dest_startup_stub(
        _operation: i32,
        _typeinfo: &::types_tuple::TupleDescData<'_>,
    ) -> ::types_error::PgResult<()> {
        Ok(())
    }
    fn spi_printtup_stub(
        _slot: &mut ::types_slot::SlotData<'_>,
    ) -> ::types_error::PgResult<bool> {
        Ok(true)
    }

    /// `_SPI_pquery`'s exact cadence (the `run_spi_shape` shape above) but
    /// through the REAL SPI receiver: dest = SpiPrintTup, CommandDest::Spi
    /// — the seam-visible SPI signal the install half keys on.
    fn run_spi_shape_spi_dest(relid: u32, tcount: u64) -> u64 {
        let mcx = leaked_mcx();
        let pstmt = mk_seqscan_pstmt(mcx, relid);
        let qd = execmain_seams::create_query_desc::call(
            pstmt,
            "SELECT a FROM spi_stage_a_fixture",
            None,
            None,
            CommandDest::Spi,
            ParamListHandle::NULL,
            QueryEnvHandle::NULL,
            0,
        )
        .unwrap();
        execmain_seams::executor_start::call(qd, 0).unwrap();
        let mut dest = DestReceiver::SpiPrintTup;
        execmain_seams::executor_run::call(qd, ForwardScanDirection, tcount, &mut dest)
            .unwrap();
        let n = execmain_seams::query_desc_es_processed::call(qd);
        execmain_seams::executor_finish::call(qd).unwrap();
        execmain_seams::executor_end::call(qd).unwrap();
        execmain_seams::free_query_desc::call(qd);
        n
    }

    /// Install gate order + knob-OFF-zero (R-KNOBS row semantics): the
    /// count/select/dest register tests answer BEFORE the knob cell loads;
    /// knob-OFF installs nothing for ANY shape; knob-ON installs exactly
    /// `_SPI_pquery`'s count-exact STOP shape.
    #[test]
    fn spi_w95_budget_gate_semantics() {
        let _knob = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let install = crate::lanev2::spi_run_budget_install;

        // Knob OFF (default posture): nothing installs, any shape.
        crate::lanev2::spi_set_for_tests(false);
        assert_eq!(install(true, true, true, 93050, false, 0), None, "knob-OFF installs nothing");
        assert_eq!(install(true, true, true, 1, false, 0), None);

        // Knob ON: exactly the tcount-limited forward serial SELECT into
        // the SPI receiver.
        crate::lanev2::spi_set_for_tests(true);
        assert_eq!(install(true, true, true, 93050, false, 0), Some(93050));
        assert_eq!(install(true, true, true, 0, false, 0), None, "tcount-0 never installs");
        assert_eq!(install(false, true, true, 93050, false, 0), None, "non-SELECT never installs");
        assert_eq!(
            install(true, false, true, 93050, false, 0),
            None,
            "a non-SPI dest never installs (the seam-visibility gate)"
        );
        assert_eq!(
            install(true, true, true, 93050, true, 0),
            None,
            "serial-law pin: a parallel run NEVER carries a budget (fail-closed)"
        );

        crate::lanev2::spi_set_for_tests(false);
    }

    /// The NAMED admission taxonomy (`ShapeClass::Spi`), reusing the
    /// GENERIC registry vocabulary (scroll-mark / parallel-gate).
    /// Backward-execution wave B11: the classifier's backward arm is
    /// DELETED — a backward demand cannot reach a budgeted run (the store
    /// serves backward fetches above the seam; kill-world backward runs
    /// die 0A000 at the forward-only run seam, B1). scroll-mark stays as
    /// the defensive random-access-eflags fence (its wave-9.5 producer
    /// died in B2); parallel-gate stays the fail-closed serial-law pin. A
    /// refusal refuses the WHOLE statement to Volcano (refusal-not-error).
    #[test]
    fn spi_w95_admission_taxonomy_named_classes() {
        use ::types_slot::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
        let _knob = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let name = crate::lanev2::spi_admission_refusal_name;

        // Admit: forward, no random-access demand, serial.
        assert_eq!(name(true, 0, false), None, "the _SPI_pquery shape admits");

        // B11: the direction argument is SUNSET-kept in the signature; a
        // backward value no longer classifies (the seam error owns it).
        assert_eq!(name(false, 0, false), None);
        assert_eq!(name(true, EXEC_FLAG_REWIND, false), Some("scroll-mark"));
        assert_eq!(name(true, EXEC_FLAG_BACKWARD, false), Some("scroll-mark"));
        assert_eq!(name(true, EXEC_FLAG_MARK, false), Some("scroll-mark"));
        assert_eq!(name(true, 0, true), Some("parallel-gate"));

        // The install half refuses the WHOLE statement on a named refusal:
        // no budget ⇒ no SPI count-seam machinery ⇒ Volcano byte-identical.
        crate::lanev2::spi_set_for_tests(true);
        assert_eq!(
            crate::lanev2::spi_run_budget_install(
                true,
                true,
                true,
                93051,
                false,
                EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD
            ),
            None,
            "a random-access-eflags run refuses wholesale"
        );
        crate::lanev2::spi_set_for_tests(false);
    }

    /// Knob-ON re-ride of the frozen cadence pins through the REAL SPI
    /// receiver: the budget installs (dest = Spi, tcount != 0), the
    /// settle runs below the drive loop, and every count
    /// assertion of the Volcano pins above holds byte-for-byte — with the
    /// fixture back to zero pins after each run (the settle face +
    /// teardown, scanfix::quiesced). The knob-OFF arm re-rides the same
    /// cells first: OFF and ON must be indistinguishable at this seam
    /// (values move NEVER; the budget is settle discipline + accounting).
    #[test]
    fn spi_w95_shape_knob_on_byte_identity() {
        install_seams();
        install_spi_dest_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let relid: u32 = 93052; // WS-AJ band
        scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);

        // Knob-OFF arm (the default posture the dualexec off-arm rides).
        crate::lanev2::spi_set_for_tests(false);
        assert_eq!(run_spi_shape_spi_dest(relid, 2), 2);
        scanfix::quiesced();
        assert_eq!(run_spi_shape_spi_dest(relid, 0), 5);
        scanfix::quiesced();

        // Knob-ON arm: budget installed (tcount-limited SPI-dest SELECT),
        // settle at the count-limited stop; identical counts.
        crate::lanev2::spi_set_for_tests(true);
        assert_eq!(run_spi_shape_spi_dest(relid, 2), 2, "tcount=2 stops exactly, knob-ON");
        scanfix::quiesced();
        assert_eq!(run_spi_shape_spi_dest(relid, 4), 4, "tcount=4 crosses the page boundary");
        scanfix::quiesced();
        assert_eq!(run_spi_shape_spi_dest(relid, 0), 5, "tcount=0 completeness (no budget)");
        scanfix::quiesced();
        assert_eq!(run_spi_shape_spi_dest(relid, 99), 5, "tcount>rows saturates");
        scanfix::quiesced();

        crate::lanev2::spi_set_for_tests(false);
    }

    /// REVIEW RE-BASELINE PIN (notes/se-spi-stage-a.md §8, finding 1): a
    /// budgeted SPI-dest run whose scan holds a lane-staged page batch at
    /// the count-limited stop SETTLES (claim released — R3 zero pins,
    /// scanfix::quiesced is the teeth) and REPORTS parked=true — the
    /// execute_plan caller then arms `es_lane_cursor_parked`, the SHARED
    /// WS-AI resume signal — and the entry-side resume walk restages the
    /// batch with the consume cursor restored. Load-bearing because the
    /// portal-fetch producer (SPI_cursor_fetch → PortalRunFetch →
    /// PortalRunSelect; the plpgsql FOR-loop fetch(10)/fetch(50) cadence)
    /// RESUMES the same QueryDesc/estate: dropping the parked bit (the
    /// original STOP-ONLY shape) would resume an un-inited scan the moment
    /// a budgeted SPI run carries a lane-staged batch. Also pins the EPQ
    /// law (settle refuses under es_epq_active, budget belongs to the
    /// outer run).
    #[test]
    fn spi_w95_settle_arms_resume_signal_for_portal_fetch_producer() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();
        crate::lanev2::spi_set_for_tests(true);

        let relid: u32 = 93053; // WS-AJ band
        scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
        let pstmt = mk_seqscan_pstmt(mcx, relid);
        let snap_ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let planstate = planstate.as_mut().unwrap();

            // Stage page 0 and park the consume cursor mid-batch (the
            // standalone-pipeline shape a lane-engaged scan holds when a
            // tcount-limited fetch stops).
            {
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                else {
                    panic!("bare seqscan plan");
                };
                let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate).unwrap();
                assert_eq!(n, 3);
                ss.set_lane_cursor(1, n);
            }

            // EPQ law: a budgeted estate inside an EPQ recheck settles
            // NOTHING (the budget belongs to the outer run).
            estate.es_epq_active = true;
            assert!(
                !crate::lanev2::spi_run_settle(planstate, estate).unwrap(),
                "EPQ drive must not settle/park"
            );
            estate.es_epq_active = false;

            // The count-limited stop: settle releases the staged claim
            // and reports parked (the caller arms es_lane_cursor_parked).
            assert!(
                crate::lanev2::spi_run_settle(planstate, estate).unwrap(),
                "a lane-staged batch parks (the bit the caller must arm)"
            );
            scanfix::quiesced(); // R3: ZERO pins while suspended
            {
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                else {
                    unreachable!()
                };
                assert!(::nodeseqscan::seq_scan_cursor_parked(ss));
                assert_eq!(ss.lane_cursor(), (0, 0), "staged state settled");
            }

            // The next fetch's entry-side repossession (execute_plan runs
            // the SHARED WS-AI resume walk off es_lane_cursor_parked):
            // batch restaged, consume cursor restored.
            crate::lanev2::cursor_park_resume(planstate, estate).unwrap();
            {
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                else {
                    unreachable!()
                };
                assert!(!::nodeseqscan::seq_scan_cursor_parked(ss));
                assert_eq!(ss.lane_cursor(), (1, 3), "consume cursor restored");
            }

            crate::exec_end_node(planstate, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        crate::lanev2::spi_set_for_tests(false);
        scanfix::quiesced();
    }
}
// --- end WS-AJ wave-9 sub-region --------------------------------------------
// --- WS-CB wave-10 (cursors inc-2: batch store fill; contract §2.1, band 95001+) --
// Unit pins for the TuplestoreBatchSink protocol + the run-seam dispatch:
// per-accept budget decrement across BATCH boundaries, Full⇒Paused at
// exhaustion with node-resident position, settle (R3 zero-pins) + resume-
// reposition through the EXISTING inc-1b walkers, budget-None (count-0
// drain) breaker posture, overfill hard error, the EPQ/dest/admission
// dispatch gates, row-chain fallback parity at the seam (§2.3
// fetch-invisibility at the unit level), and the §6 forward-only staging
// faces. Same serialization discipline as cursors_wave9 (scanfix::TEST_LOCK
// held for the fixture span; knob lever set explicitly per test).
mod cursors_wave10_cb {
    use super::*;

    fn read_store_i32s(
        h: ::types_portal::TuplestoreHandle,
        desc: &std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    ) -> Vec<i32> {
        let read_cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("read")));
        let mut slot = exectuples::make_tuple_table_slot(
            read_cx.mcx(),
            ::types_slot::TupleSlotKind::MinimalTuple,
            Some(desc.clone()),
        );
        let mut rows: Vec<i32> = Vec::new();
        loop {
            let got = tuplestore::hold::with_store(h, |ts| {
                ts.gettupleslot(true, true, &mut slot, read_cx.mcx())
            })
            .unwrap();
            if !got {
                break;
            }
            let mut isnull = false;
            let v = exectuples::slot_getattr(&mut slot, 1, &mut isnull);
            assert!(!isnull);
            rows.push(v.as_i32());
        }
        rows
    }

    fn mk_store_dest() -> (::types_portal::TuplestoreHandle, DestReceiver<'static>) {
        let store = tuplestore::Tuplestore::begin_heap(true, false, 1024);
        let h = tuplestore::hold::register(store);
        let mut dr = tstore_receiver::tstore_create_DR();
        tstore_receiver::set_params(&mut dr, h, false);
        (h, DestReceiver::Tuplestore(dr))
    }

    /// THE CB-1 protocol pin: a budget-4 batch fill over a 3+2-row two-page
    /// table decrements ACROSS the batch boundary (3 accepts from batch 1,
    /// 1 from batch 2), pauses at exhaustion with the consume position
    /// node-resident, SETTLES through the inc-1b park walker (R3 zero pins
    /// while suspended — `scanfix::quiesced()` is the teeth), resumes with
    /// the position restored, drains the remainder on the next budgeted
    /// drive, and the store bytes equal the ROW-CHAIN arm's over an
    /// identical table (§2.3 fill-strategy invisibility, unit level).
    #[test]
    fn cursors_w10_sink_budget_across_batches_park_resume_and_rowchain_parity() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();
        crate::lanev2::cursors_set_for_tests(true);

        let mut arm_rows: Vec<Vec<i32>> = Vec::new();

        // Arm L: the batch fill (sink + fill_step below the admission
        // verdict — the scanfix heap fixture refuses standalone admission
        // by design, so the pipeline is entered through the test face).
        {
            let relid = 95001u32;
            scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
            let pstmt = mk_seqscan_pstmt(mcx, relid);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            let mut rows: Vec<i32> = Vec::new();
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                let desc =
                    crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let (h, mut dest) = mk_store_dest();
                let ExecData { estate, planstate } = data;
                let planstate = planstate.as_mut().unwrap();

                // Budgeted drive 1: FETCH 4 (crosses the 3-row page batch).
                estate.es_direction = ForwardScanDirection;
                estate.es_processed = 0;
                estate.es_cursor_run_budget =
                    crate::lanev2::cursor_run_budget_install(true, true, 4, false, 0);
                assert_eq!(estate.es_cursor_run_budget, Some(4));
                {
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate else {
                        panic!("bare seqscan plan");
                    };
                    let exhausted =
                        crate::lanev2::cursor_fill_step_seqscan_for_tests(ss, &mut dest, estate)
                            .unwrap();
                    assert!(!exhausted, "budget exhaustion pauses, source not done");
                    assert_eq!(estate.es_processed, 4, "the sink carries the loop accounting");
                    assert_eq!(
                        estate.es_cursor_run_budget,
                        Some(0),
                        "per-accept decrement across the batch boundary (3 + 1)"
                    );
                    assert_eq!(ss.lane_cursor(), (1, 2), "position node-resident mid-batch-2");
                }
                // Settle at the run seam's park point (the EXISTING inc-1b
                // walker — no new machinery): R3 zero pins while suspended.
                assert!(crate::lanev2::cursor_run_park(planstate, estate).unwrap(), "parks");
                scanfix::quiesced();
                // Resume-reposition.
                crate::lanev2::cursor_park_resume(planstate, estate).unwrap();
                {
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate else {
                        unreachable!()
                    };
                    assert_eq!(ss.lane_cursor(), (1, 2), "consume cursor restored");
                }
                // Budgeted drive 2: the remainder exhausts the source.
                estate.es_processed = 0;
                estate.es_cursor_run_budget =
                    crate::lanev2::cursor_run_budget_install(true, true, 99, false, 0);
                {
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate else {
                        unreachable!()
                    };
                    let exhausted =
                        crate::lanev2::cursor_fill_step_seqscan_for_tests(ss, &mut dest, estate)
                            .unwrap();
                    assert!(exhausted, "source exhausted under a slack budget");
                    assert_eq!(estate.es_processed, 1);
                    assert_eq!(estate.es_cursor_run_budget, Some(98));
                }
                rows = read_store_i32s(h, &desc);
                tuplestore::hold::end(h);
                crate::exec_end_node(planstate, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
            assert_eq!(rows, vec![1, 2, 3, 4, 5], "the whole table, in order");
            arm_rows.push(rows);
        }

        // Arm R: the row-chain fill of an identical table through the REAL
        // run seam (execute_plan; knob-ON — the dispatch refuses on heap
        // standalone admission and the per-tuple loop serves the store).
        {
            let relid = 95002u32;
            scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
            let pstmt = mk_seqscan_pstmt(mcx, relid);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            let mut rows: Vec<i32> = Vec::new();
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                let desc =
                    crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let (h, mut dest) = mk_store_dest();
                for (count, want) in [(4u64, 4u64), (0, 1)] {
                    data.estate.es_processed = 0;
                    crate::execmain::execute_plan(
                        data,
                        CmdType::CMD_SELECT,
                        true,
                        count,
                        ForwardScanDirection,
                        false,
                        &mut dest,
                    )
                    .unwrap();
                    assert_eq!(data.estate.es_processed, want);
                }
                rows = read_store_i32s(h, &desc);
                tuplestore::hold::end(h);
                let ExecData { estate, planstate } = data;
                crate::exec_end_node(planstate.as_mut().unwrap(), estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
            arm_rows.push(rows);
        }

        assert_eq!(
            arm_rows[0], arm_rows[1],
            "batch fill and row-chain fill land byte-identical stores (§2.3)"
        );
        crate::lanev2::cursors_set_for_tests(false);
        scanfix::quiesced();
    }

    /// Dispatch gates (§2.1 EPQ pin + the store-face gate) and the §2.3
    /// Volcano-refusal fallback THROUGH the real run seam: a knob-ON
    /// budgeted store-fill run over the heap fixture refuses standalone
    /// admission inside `cursor_store_batch_fill` and the per-tuple loop
    /// fills the same store — byte-identical to the knob-OFF oracle.
    #[test]
    fn cursors_w10_dispatch_gates_and_seam_fallback_parity() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();

        // Direct gate pins first (EPQ, dest kind) on a budgeted estate.
        crate::lanev2::cursors_set_for_tests(true);
        {
            let relid = 95003u32;
            scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
            let pstmt = mk_seqscan_pstmt(mcx, relid);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let ExecData { estate, planstate } = data;
                let planstate = planstate.as_mut().unwrap();
                estate.es_direction = ForwardScanDirection;
                estate.es_cursor_run_budget = Some(2);

                // EPQ pin: the budget belongs to the outer run.
                estate.es_epq_active = true;
                let (h, mut dest) = mk_store_dest();
                assert!(
                    !crate::lanev2::cursor_store_batch_fill(planstate, estate, &mut dest, None)
                        .unwrap(),
                    "EPQ drive never batch-fills"
                );
                estate.es_epq_active = false;

                // Store-face gate: a non-tuplestore receiver keeps the loop.
                let mut nodest = DestReceiver::DoNothing;
                assert!(
                    !crate::lanev2::cursor_store_batch_fill(planstate, estate, &mut nodest, None)
                        .unwrap(),
                    "non-store receivers keep the row loop"
                );

                // Heap standalone refuses admission: dispatch falls through
                // with NOTHING consumed (the row loop then owns the run).
                assert!(
                    !crate::lanev2::cursor_store_batch_fill(planstate, estate, &mut dest, None)
                        .unwrap(),
                    "Volcano-refused plan keeps the row loop"
                );
                assert_eq!(estate.es_processed, 0, "refused dispatch consumed nothing");
                tuplestore::hold::end(h);

                estate.es_cursor_run_budget = None;
                crate::exec_end_node(planstate, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
        }

        // Seam fallback parity: knob arms over identical tables through
        // execute_plan (the dispatch is live inside the seam knob-ON).
        let mut arm_rows: Vec<Vec<i32>> = Vec::new();
        for (arm_on, relid) in [(false, 95004u32), (true, 95005u32)] {
            crate::lanev2::cursors_set_for_tests(arm_on);
            scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
            let pstmt = mk_seqscan_pstmt(mcx, relid);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            let mut rows: Vec<i32> = Vec::new();
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                let desc =
                    crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let (h, mut dest) = mk_store_dest();
                for (count, want) in [(2u64, 2u64), (0, 3)] {
                    data.estate.es_processed = 0;
                    crate::execmain::execute_plan(
                        data,
                        CmdType::CMD_SELECT,
                        true,
                        count,
                        ForwardScanDirection,
                        false,
                        &mut dest,
                    )
                    .unwrap();
                    assert_eq!(data.estate.es_processed, want);
                }
                rows = read_store_i32s(h, &desc);
                tuplestore::hold::end(h);
                let ExecData { estate, planstate } = data;
                crate::exec_end_node(planstate.as_mut().unwrap(), estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
            assert_eq!(rows, vec![1, 2, 3, 4, 5]);
            arm_rows.push(rows);
        }
        assert_eq!(arm_rows[0], arm_rows[1], "knob arms byte-identical at the seam");
        crate::lanev2::cursors_set_for_tests(false);
        scanfix::quiesced();
    }

    /// Budget-None breaker posture (the §2.4 count-0 persist drain: never
    /// Full, runs to exhaustion) and the overfill hard error (an operator
    /// ignoring `SinkFeed::Full` must not silently lose rows).
    #[test]
    fn cursors_w10_sink_none_budget_drains_and_overfill_errors() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();
        crate::lanev2::cursors_set_for_tests(true);

        // Budget None: whole-table drain in one drive.
        {
            let relid = 95006u32;
            scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
            let pstmt = mk_seqscan_pstmt(mcx, relid);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                let desc =
                    crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let (h, mut dest) = mk_store_dest();
                let ExecData { estate, planstate } = data;
                let planstate = planstate.as_mut().unwrap();
                estate.es_direction = ForwardScanDirection;
                estate.es_processed = 0;
                estate.es_cursor_run_budget = None;
                {
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate else {
                        panic!("bare seqscan plan");
                    };
                    let exhausted =
                        crate::lanev2::cursor_fill_step_seqscan_for_tests(ss, &mut dest, estate)
                            .unwrap();
                    assert!(exhausted, "no budget: the drain runs to exhaustion");
                }
                assert_eq!(estate.es_processed, 5);
                assert_eq!(read_store_i32s(h, &desc), vec![1, 2, 3, 4, 5]);
                tuplestore::hold::end(h);
                crate::exec_end_node(planstate, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
        }

        // Overfill: accept on a zero budget is a hard protocol error.
        {
            let relid = 95007u32;
            scanfix::register_table(relid, &[&[1, 2, 3]]);
            let pstmt = mk_seqscan_pstmt(mcx, relid);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let (h, mut dest) = mk_store_dest();
                let ExecData { estate, planstate } = data;
                let planstate = planstate.as_mut().unwrap();
                estate.es_direction = ForwardScanDirection;
                estate.es_cursor_run_budget = Some(0);
                {
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate else {
                        panic!("bare seqscan plan");
                    };
                    let err =
                        crate::lanev2::cursor_fill_step_seqscan_for_tests(ss, &mut dest, estate)
                            .unwrap_err();
                    assert!(err.to_string().contains("overfilled"), "got: {err}");
                }
                tuplestore::hold::end(h);
                estate.es_cursor_run_budget = None;
                crate::exec_end_node(planstate, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
        }
        crate::lanev2::cursors_set_for_tests(false);
        scanfix::quiesced();
    }

    /// §6 staging faces, SEAM-WIRING F3 REWORK (SE10-GATES item 7): the
    /// original pin asserted `!cursor_store_ever_armed()` against the
    /// process-global never-cleared static — a test-order hazard once the
    /// armed note went live. The reworked pin is order-independent: it
    /// tests the evidence counter with the assert PROVABLY inert (knob
    /// forced OFF — the assert is knob-scoped, push.rs), then the
    /// monotonic arming transition, then the knob faces. It never asserts
    /// the static's initial state and leaves the knob OFF (the static may
    /// remain armed — harmless under the knob-scoped assert).
    #[test]
    fn cursors_w10_forward_only_staging_faces() {
        // The knob lever is a process-global static: serialize with every
        // other flipping test (the fixture lock is the module's knob lock).
        let _knob = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Evidence counter ticks with the debug assert inert: knob OFF makes
        // the assert's conjunct false regardless of prior arming.
        crate::lanev2::cursors_set_for_tests(false);
        let before = crate::lanev2::run_seam_backward_evidence_count();
        crate::lanev2::run_seam_backward_evidence();
        assert_eq!(crate::lanev2::run_seam_backward_evidence_count(), before + 1);
        // The arming note is monotonic (never clears).
        crate::lanev2::cursor_store_armed_note();
        assert!(crate::lanev2::cursor_store_ever_armed());
        // The pub knob face (the CA-facing seam) mirrors the test lever —
        // one cell serves the portal face and the budget classifier.
        crate::lanev2::cursor_store_fill_set_for_tests(true);
        assert!(crate::lanev2::cursor_store_fill_enabled());
        crate::lanev2::cursor_store_fill_set_for_tests(false);
        assert!(!crate::lanev2::cursor_store_fill_enabled());
    }

    /// §6 deletion rider row 4 EXECUTED (se/deletion-prep B1): the run
    /// seam is FORWARD-ONLY. A backward drive into `execute_plan` ticks
    /// the bake counter, then errors 0A000 BEFORE any plan work. At
    /// defaults this state is unreachable (the portal store serves every
    /// backward fetch — the SE13 flip); the error is the kill-switch
    /// worlds' loud degradation, replacing their old backward plan drive.
    /// Knob forced OFF here for the same reason as the staging-faces pin:
    /// the push.rs debug assert is knob-scoped, and the world that can
    /// reach this seam backward IS the knob-OFF world.
    #[test]
    fn run_seam_backward_errors_forward_only_b1() {
        use ::types_scan::sdir::BackwardScanDirection;
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();
        crate::lanev2::cursors_set_for_tests(false);
        let relid = 96101u32;
        scanfix::register_table(relid, &[&[1, 2, 3]]);
        let pstmt = mk_seqscan_pstmt(mcx, relid);
        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let (h, mut dest) = mk_store_dest();
            let before = crate::lanev2::run_seam_backward_evidence_count();
            let err = crate::execmain::execute_plan(
                data,
                CmdType::CMD_SELECT,
                true,
                0,
                BackwardScanDirection,
                false,
                &mut dest,
            )
            .unwrap_err();
            assert!(
                err.message().contains("backward scan is not supported"),
                "unexpected error: {}",
                err.message()
            );
            assert_eq!(
                crate::lanev2::run_seam_backward_evidence_count(),
                before + 1,
                "the bake counter must still tick on every refused attempt"
            );
            assert_eq!(data.estate.es_processed, 0, "no plan work before the refusal");
            tuplestore::hold::end(h);
            let ExecData { estate, planstate } = data;
            crate::exec_end_node(planstate.as_mut().unwrap(), estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        scanfix::quiesced();
    }
}
// --- end WS-CB wave-10 --------------------------------------------------------

// --- SE-R41 (reason-41 retirement: the capture-batch heap fill; band 99201+) ---
// Unit pins for the in-run §4.2 capture that retires the row-chain fallback
// for heap cursor fills (notes/se-r41-retire.md §3): the capture-batchable
// plan probe, the capture-armed batch fill through the REAL run seam
// (sidecar/store row alignment with GENUINE (block, lineoff) tids off the
// scanfix heap pages), and the capture ROW LOOP fallback when the dispatch
// refuses at run time — whose sidecar must be byte-identical to the sink's
// (the fallback-parity teeth: correctness never rides on the lane
// admitting). Same serialization discipline as the WS-CB region.
mod cursors_r41 {
    use super::*;

    fn mk_sidecar() -> ::types_portal::TuplestoreHandle {
        tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(true, false, 1024))
    }

    fn read_sidecar(h: ::types_portal::TuplestoreHandle) -> Vec<(u32, u64)> {
        let mut rows = Vec::new();
        loop {
            match tuplestore::hold::tidstore_get(h, rows.len() as i64).unwrap() {
                Some(row) => rows.push(row),
                None => break,
            }
        }
        rows
    }

    // Store dest + reader, the cursors_wave10_cb idiom (module-private
    // there; duplicated rather than widened).
    fn mk_store_dest() -> (::types_portal::TuplestoreHandle, DestReceiver<'static>) {
        let store = tuplestore::Tuplestore::begin_heap(true, false, 1024);
        let h = tuplestore::hold::register(store);
        let mut dr = tstore_receiver::tstore_create_DR();
        tstore_receiver::set_params(&mut dr, h, false);
        (h, DestReceiver::Tuplestore(dr))
    }

    fn read_store_i32s(
        h: ::types_portal::TuplestoreHandle,
        desc: &std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    ) -> Vec<i32> {
        let read_cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("read")));
        let mut slot = exectuples::make_tuple_table_slot(
            read_cx.mcx(),
            ::types_slot::TupleSlotKind::MinimalTuple,
            Some(desc.clone()),
        );
        let mut rows: Vec<i32> = Vec::new();
        loop {
            let got = tuplestore::hold::with_store(h, |ts| {
                ts.gettupleslot(true, true, &mut slot, read_cx.mcx())
            })
            .unwrap();
            if !got {
                break;
            }
            let mut isnull = false;
            let v = exectuples::slot_getattr(&mut slot, 1, &mut isnull);
            assert!(!isnull);
            rows.push(v.as_i32());
        }
        rows
    }

    /// §3.1 probe shapes: a bare heap SeqScan top is capture-batchable; a
    /// wrapped top (Limit/Sort over the same scan) is NOT — it keeps the
    /// D-CA-2 fence and the row-chain capture loop verbatim.
    #[test]
    fn cursors_r41_capture_probe_admits_bare_seqscan_only() {
        let mcx = leaked_mcx();
        assert!(crate::execcurrent::cursor_plan_capture_batch_fill_seam(mk_seqscan_pstmt(
            mcx, 99201
        )));
        assert!(!crate::execcurrent::cursor_plan_capture_batch_fill_seam(
            mk_sort_limit_pstmt(mcx, 99201, true, None, Some(3))
        ));
    }

    /// THE retirement pin: a capture-armed budgeted store fill over a
    /// two-page heap table, driven through the REAL run seam twice
    /// (budget 4 = pause mid-batch-2 + park, then a slack budget = resume
    /// + drain). The batch fill ENGAGES (lane staging position is the
    /// teeth), the store receives the whole table in order, and the
    /// sidecar is row-aligned with the store carrying the GENUINE page
    /// tids — (0,1)..(0,3), (1,1)..(1,2) — captured per accepted row
    /// inside the run (settle-safe by ordering). Then the FALLBACK arm:
    /// an identical table whose scan init carries the fence eflags
    /// (`batch_allowed=false` — the unit-level stand-in for any run-time
    /// dispatch refusal), so the capture ROW LOOP serves the same fills —
    /// store AND sidecar must land byte-identical to the sink's.
    #[test]
    fn cursors_r41_capture_batch_fill_sidecar_alignment_and_rowloop_fallback_parity() {
        use ::types_slot::EXEC_FLAG_BACKWARD;

        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();
        crate::lanev2::cursors_set_for_tests(true);

        // The genuine page tids of the 3+2-row fixture: block<<16 | lineoff.
        let expect_packed: Vec<u64> = vec![1, 2, 3, (1 << 16) | 1, (1 << 16) | 2];
        let mut arm_store: Vec<Vec<i32>> = Vec::new();
        let mut arm_packed: Vec<Vec<u64>> = Vec::new();

        for (relid, fence_eflags) in [(99202u32, 0), (99203u32, EXEC_FLAG_BACKWARD)] {
            scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
            let pstmt = mk_seqscan_pstmt(mcx, relid);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            let mut rows: Vec<i32> = Vec::new();
            let mut sidecar_rows: Vec<(u32, u64)> = Vec::new();
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                let desc =
                    crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, fence_eflags)
                        .unwrap();
                let (h, mut dest) = mk_store_dest();
                let sidecar = mk_sidecar();
                tcop_dest::SetTuplestoreCaptureSidecar(&mut dest, sidecar);
                // Budgeted drive 1: FETCH 4 (crosses the 3-row page batch;
                // the engaged arm pauses mid-batch-2 and parks).
                data.estate.es_processed = 0;
                crate::execmain::execute_plan(
                    data,
                    CmdType::CMD_SELECT,
                    true,
                    4,
                    ForwardScanDirection,
                    false,
                    &mut dest,
                )
                .unwrap();
                assert_eq!(data.estate.es_processed, 4);
                // Engagement teeth (SE-R41 v2, the pin posture): the engaged
                // batch fill pauses WITHOUT parking — the staged page batch
                // and its pin survive the suspension (C-parity Volcano
                // posture; `seq_scan_cursor_settle` refuses on
                // `lane_hold_pin`), so the resume flag never arms on EITHER
                // arm and the next fill continues from the node-resident
                // cursor with zero restage. The unfenced arm's engagement
                // evidence is the live staged cursor + posture flag + a HELD
                // page pin; the fenced (capture row loop) arm stages nothing
                // and holds only the Volcano scan's own cross-FETCH pin.
                assert!(
                    !data.estate.es_lane_cursor_parked,
                    "hold-pin posture: an engaged cursor fill never parks"
                );
                {
                    let planstate = data.planstate.as_mut().unwrap();
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate
                    else {
                        panic!("bare seqscan plan");
                    };
                    if fence_eflags == 0 {
                        assert!(ss.lane_hold_pin(), "posture set at engagement");
                        assert_eq!(
                            ss.lane_cursor(),
                            (1, 2),
                            "staged batch survives the suspension (mid-batch-2)"
                        );
                        assert!(
                            !::nodeseqscan::seq_scan_cursor_parked(ss),
                            "no park record under the pin posture"
                        );
                        assert!(
                            scanfix::held_pins() > 0,
                            "the staged page's pin is HELD across the suspension"
                        );
                    } else {
                        assert!(!ss.lane_hold_pin(), "fenced arm never engages the fill");
                        assert_eq!(ss.lane_cursor(), (0, 0), "row loop stages nothing");
                    }
                }
                // Budgeted drive 2: slack budget drains the remainder
                // (resume-repossession for the engaged arm).
                data.estate.es_processed = 0;
                crate::execmain::execute_plan(
                    data,
                    CmdType::CMD_SELECT,
                    true,
                    99,
                    ForwardScanDirection,
                    false,
                    &mut dest,
                )
                .unwrap();
                assert_eq!(data.estate.es_processed, 1);
                rows = read_store_i32s(h, &desc);
                sidecar_rows = read_sidecar(sidecar);
                tuplestore::hold::end(sidecar);
                tuplestore::hold::end(h);
                let ExecData { estate, planstate } = data;
                crate::exec_end_node(planstate.as_mut().unwrap(), estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
            assert_eq!(rows, vec![1, 2, 3, 4, 5], "the whole table, in order");
            assert_eq!(
                sidecar_rows.len(),
                rows.len(),
                "sidecar/store row alignment (one identity per accepted row)"
            );
            assert_eq!(
                sidecar_rows.iter().map(|&(o, _)| o).collect::<Vec<_>>(),
                vec![relid; 5],
                "capture stamps the scan relation oid"
            );
            arm_store.push(rows);
            arm_packed.push(sidecar_rows.into_iter().map(|(_, t)| t).collect());
        }

        assert_eq!(
            arm_packed[0], expect_packed,
            "sink capture reads the genuine (block, lineoff) page tids"
        );
        assert_eq!(arm_store[0], arm_store[1], "store parity across fill engines");
        assert_eq!(
            arm_packed[0], arm_packed[1],
            "sidecar parity: sink capture == capture row loop (fallback-parity teeth)"
        );

        crate::lanev2::cursors_set_for_tests(false);
        scanfix::quiesced();
    }

    /// SE-R41 v2 §2 — THE PAGE-REMAINDER PIN (the SE12 budget-floor probe's
    /// es_processed 8-vs-16 loss, made a unit): a batch fill that freshly
    /// engages over a scan the per-tuple row walk left MID-PAGE must adopt
    /// the current page's unconsumed remainder — never advance past it
    /// (`heap_getnextpagebatch` advances pages; the documented no-interleave
    /// invariant). Fixture: 16 rows over two 8-row pages; the row walk
    /// consumes 3, then a fill engages through the test face (below the
    /// memoized-verdict dispatch — exactly the shape the SE12 floor probe
    /// made reachable). RED pre-fix: the fresh staging advanced to page 1
    /// and delivered 8 rows (the page-0 remainder LOST); GREEN: 13 rows,
    /// byte-exact, in order.
    #[test]
    fn cursors_r41v2_midpage_engagement_adopts_page_remainder() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();
        crate::lanev2::cursors_set_for_tests(true);

        let relid = 99204u32;
        scanfix::register_table(
            relid,
            &[&[1, 2, 3, 4, 5, 6, 7, 8], &[9, 10, 11, 12, 13, 14, 15, 16]],
        );
        let pstmt = mk_seqscan_pstmt(mcx, relid);
        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        let mut rows: Vec<i32> = Vec::new();
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            let desc = crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let (h, mut dest) = mk_store_dest();
            let ExecData { estate, planstate } = data;
            let planstate = planstate.as_mut().unwrap();
            estate.es_direction = ForwardScanDirection;

            // The row-chain drive first: three per-tuple pulls leave the AM
            // mid-page-0 (last-returned index 2 of 8).
            for expect in [1, 2, 3] {
                let slot_id = crate::exec_proc_node(planstate, estate)
                    .unwrap()
                    .expect("row walk row");
                let mut isnull = false;
                let v = exectuples::slot_getattr(estate.slot_mut(slot_id), 1, &mut isnull);
                assert!(!isnull);
                assert_eq!(v.as_i32(), expect);
            }

            // Fresh batch engagement MID-PAGE: the fill must adopt rows
            // 4..8 of the pinned page before walking on.
            estate.es_processed = 0;
            estate.es_cursor_run_budget =
                crate::lanev2::cursor_run_budget_install(true, true, 999, false, 0);
            {
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate else {
                    panic!("bare seqscan plan");
                };
                let exhausted =
                    crate::lanev2::cursor_fill_step_seqscan_for_tests(ss, &mut dest, estate)
                        .unwrap();
                assert!(exhausted, "slack budget drains to end of scan");
            }
            assert_eq!(
                estate.es_processed, 13,
                "the page-0 remainder is ADOPTED, not lost (the 8-vs-16 defect class)"
            );
            rows = read_store_i32s(h, &desc);
            tuplestore::hold::end(h);
            crate::exec_end_node(planstate, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        assert_eq!(
            rows,
            (4..=16).collect::<Vec<i32>>(),
            "remainder + following pages, byte-exact, in order"
        );
        crate::lanev2::cursors_set_for_tests(false);
        scanfix::quiesced();
    }

    /// SE-R41 v2 §3 — the pin-posture walker pin: a cursor-fill-owned scan
    /// (`lane_hold_pin`, set by the dispatch at engagement) suspends WITHOUT
    /// parking — the settle walker refuses, the staged page batch and its
    /// pin survive (held_pins > 0 where the parked posture asserted zero),
    /// and the next fill continues from the node-resident cursor with ZERO
    /// restage, byte-identically. Control arm: the SAME shape without the
    /// posture flag still parks with zero pins — R3 for lane claims is
    /// unchanged.
    #[test]
    fn cursors_r41v2_hold_pin_posture_suspends_without_park_and_resumes_in_place() {
        install_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mcx = leaked_mcx();
        crate::lanev2::cursors_set_for_tests(true);

        let mut arm_rows: Vec<Vec<i32>> = Vec::new();
        for (hold_pin, relid) in [(true, 99205u32), (false, 99206u32)] {
            scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
            let pstmt = mk_seqscan_pstmt(mcx, relid);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            let mut rows: Vec<i32> = Vec::new();
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                let desc =
                    crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let (h, mut dest) = mk_store_dest();
                let ExecData { estate, planstate } = data;
                let planstate = planstate.as_mut().unwrap();
                estate.es_direction = ForwardScanDirection;

                // Budgeted fill 1 (FETCH 4): pauses mid-batch-2.
                estate.es_processed = 0;
                estate.es_cursor_run_budget =
                    crate::lanev2::cursor_run_budget_install(true, true, 4, false, 0);
                {
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate else {
                        panic!("bare seqscan plan");
                    };
                    if hold_pin {
                        // As the dispatch does at engagement.
                        ss.set_lane_hold_pin();
                    }
                    let exhausted =
                        crate::lanev2::cursor_fill_step_seqscan_for_tests(ss, &mut dest, estate)
                            .unwrap();
                    assert!(!exhausted, "budget exhaustion pauses");
                    assert_eq!(estate.es_processed, 4);
                }
                // The suspension: settle walk at the run seam's park point.
                // SE14 boarding composition: brinfix (SE13) made the park
                // walk fallible (slot-materialize hygiene) — PgResult<bool>.
                let parked = crate::lanev2::cursor_run_park(planstate, estate).unwrap();
                if hold_pin {
                    assert!(!parked, "posture: the walker refuses to park");
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate else {
                        unreachable!()
                    };
                    assert_eq!(ss.lane_cursor(), (1, 2), "staged cursor survives");
                    assert!(!::nodeseqscan::seq_scan_cursor_parked(ss));
                    assert!(
                        scanfix::held_pins() > 0,
                        "the staged page's pin is HELD across the suspension"
                    );
                } else {
                    assert!(parked, "control: the lane claim parks as before");
                    scanfix::quiesced(); // R3 zero-pins-at-settle unchanged
                    crate::lanev2::cursor_park_resume(planstate, estate).unwrap();
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate else {
                        unreachable!()
                    };
                    assert_eq!(ss.lane_cursor(), (1, 2), "consume cursor restored");
                }
                // Budgeted fill 2: continues in place (posture arm restages
                // NOTHING — there is nothing to restage) and drains.
                estate.es_processed = 0;
                estate.es_cursor_run_budget =
                    crate::lanev2::cursor_run_budget_install(true, true, 99, false, 0);
                {
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *planstate else {
                        unreachable!()
                    };
                    let exhausted =
                        crate::lanev2::cursor_fill_step_seqscan_for_tests(ss, &mut dest, estate)
                            .unwrap();
                    assert!(exhausted, "source exhausted under a slack budget");
                    assert_eq!(estate.es_processed, 1);
                }
                rows = read_store_i32s(h, &desc);
                tuplestore::hold::end(h);
                crate::exec_end_node(planstate, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
            assert_eq!(rows, vec![1, 2, 3, 4, 5], "the whole table, in order");
            arm_rows.push(rows);
        }
        assert_eq!(
            arm_rows[0], arm_rows[1],
            "pin posture is byte-identical to the parked posture"
        );
        crate::lanev2::cursors_set_for_tests(false);
        scanfix::quiesced();
    }
}
// --- end SE-R41 -----------------------------------------------------------------

// =============================================================================
// WS-MJ1 (LANE-MERGEJOIN inc-1) lane-surface pins — band 99101+ (lane band
// 99001+ per the branch-open re-grep, worklog notes/mergejoin-ws-mj1.md §1.2;
// 990xx = FSM-level pins in nodemergejoin/src/tests.rs, 991xx = this region).
// The surface under test: lanev2.rs `try_own_merge_join` (knob
// PGRUST_LANE_V2_MERGEJOIN_NATIVE, default OFF) over the ported EXEC_MJ_*
// FSM with lane feed adapters (lanev2/lane_mergejoin.rs). Fixtures/harness
// reused from the WS-G row-mode corpus (mk_mergejoin_pstmt / scanfix /
// drain_wide_rows_nullable / rowmode_ab::KNOB) — test-file-only reuse, the
// WS-L precedent.
// =============================================================================
mod mergejoin_native_ws_mj1 {
    use super::mergejoin_rowmode_ab::{install_mj_seams, mk_mergejoin_pstmt, Inner};
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    fn owned() -> u64 {
        crate::lanev2::MJ_NATIVE_OWNED_FOR_TESTS.load(Relaxed)
    }
    fn marks() -> u64 {
        crate::lanev2::MJ_NATIVE_MARKS_FOR_TESTS.load(Relaxed)
    }
    fn restores() -> u64 {
        crate::lanev2::MJ_NATIVE_RESTORES_FOR_TESTS.load(Relaxed)
    }
    fn refused(i: usize) -> u64 {
        crate::lanev2::MJ_NATIVE_REFUSED_FOR_TESTS[i].load(Relaxed)
    }

    /// INNER-only, Sort-inner-only plan with the two planner flags the
    /// skip_mark_restore pin needs (mk_mergejoin_pstmt hardcodes both false;
    /// costsize.c:3904-3906: skip_mark_restore iff (SEMI ∨ ANTI ∨
    /// inner_unique) ∧ joinrestrictinfo == mergeclauses — modeled here as
    /// inner_unique with an all-mergeclause restrictlist).
    fn mk_mj99_pstmt<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        outer_relid: u32,
        inner_relid: u32,
        skip_mark_restore: bool,
        inner_unique: bool,
    ) -> &'mcx PlannedStmt<'mcx> {
        use ::types_nodes::bitmapset::Bitmapset;
        use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
        use ::types_nodes::plannodes::{Join, MergeJoin, Plan, Scan, SeqScan, Sort};
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
                    cb_scan_cols: None,
                    scan: Scan {
                        plan: Plan { targetlist: scan_tlist(varno), ..Default::default() },
                        scanrelid,
                    },
                },
            )
            .unwrap()
        };
        let wrapper_tlist = || {
            let a = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
            let b = Node::mk_var(mcx, OUTER_VAR, 2, INT4OID, -1, 0, 0).unwrap();
            NodeList::make2(
                mcx,
                Node::mk_target_entry(mcx, a, 1, Some("c1"), false).unwrap(),
                Node::mk_target_entry(mcx, b, 2, Some("c2"), false).unwrap(),
            )
            .unwrap()
        };
        let inner_tree = {
            let mut sort = Node::build::<Sort>(mcx).unwrap();
            sort.plan.targetlist = wrapper_tlist();
            sort.plan.lefttree = Some(mk_scan(2, 2));
            sort.numCols = 1;
            sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
            sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT]).unwrap();
            sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
            sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();
            sort.seal()
        };
        let mut join_tlist = NodeList::nil();
        for (i, &(varno, attno)) in
            [(OUTER_VAR, 1i16), (OUTER_VAR, 2), (INNER_VAR, 1), (INNER_VAR, 2)].iter().enumerate()
        {
            let v = Node::mk_var(mcx, varno, attno, INT4OID, -1, 0, 0).unwrap();
            join_tlist
                .lappend(
                    mcx,
                    Node::mk_target_entry(mcx, v, i as i16 + 1, Some("x"), false).unwrap(),
                )
                .unwrap();
        }
        let mergeclause = {
            let l = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
            let r = Node::mk_var(mcx, INNER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
            Node::mk(
                mcx,
                ::types_nodes::primnodes::OpExpr {
                    opno: INT4_EQ,
                    opfuncid: 65,
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

        let mut mj = Node::build::<MergeJoin>(mcx).unwrap();
        mj.join = Join {
            plan: Plan {
                targetlist: join_tlist,
                lefttree: Some(mk_scan(1, 1)),
                righttree: Some(inner_tree),
                ..Default::default()
            },
            jointype: ::types_nodes::JoinType::JOIN_INNER,
            inner_unique,
            joinqual: NodeList::nil(),
        };
        mj.skip_mark_restore = skip_mark_restore;
        mj.mergeclauses = NodeList::make1(mcx, mergeclause).unwrap();
        mj.mergeFamilies = ::mcx::slice_borrow_in(mcx, &[INTEGER_BTREE_FAM]).unwrap();
        mj.mergeCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        mj.mergeReversals = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();
        mj.mergeNullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();

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
        pstmt.planTree = Some(mj.seal());
        pstmt.rtable = rtable;
        pstmt.permInfos = perms;
        pstmt.unprunableRelids = unpruned;
        pstmt.seal_ref()
    }

    /// One native A/B round: knob OFF (Volcano) then knob ON (lane-native
    /// drive), same plan built fresh per arm, `passes` drains (pass 2+ =
    /// rescan through exec_rescan_merge_join — the §1.4 RescanComposed
    /// parity duty); identical rows demanded, ON arm must ENGAGE.
    fn ab_mj_native(
        mk: impl Fn(::mcx::Mcx<'static>) -> &'static PlannedStmt<'static>,
        natts: usize,
        passes: usize,
    ) -> Vec<Vec<Vec<Option<i32>>>> {
        install_seams();
        install_mj_seams();
        scanfix::install();
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());
        crate::lanev2::mergejoin_native_set_for_tests(false);
        let off = drain_wide_rows_nullable(mk(leaked_mcx()), natts, passes);
        crate::lanev2::mergejoin_native_set_for_tests(true);
        let owned_before = owned();
        let on = drain_wide_rows_nullable(mk(leaked_mcx()), natts, passes);
        let owned_after = owned();
        crate::lanev2::mergejoin_native_set_for_tests(false);
        drop(guard);
        assert_eq!(off, on, "knob OFF vs ON must be byte-identical");
        assert!(
            owned_after > owned_before,
            "ON arm never engaged the lane-native mergejoin drive"
        );
        off
    }

    fn row(vals: &[i32]) -> Vec<Option<i32>> {
        vals.iter().map(|&v| Some(v)).collect()
    }

    /// §2.3 THE NEST — duplicate-rich keys BOTH sides over the Sort inner:
    /// every second same-key outer forces the TESTOUTER cmp==0 restore to
    /// the marked group start (restore repositions to AFTER the marked
    /// tuple; the marked COPY stands in for the current inner — off-by-one
    /// either way changes these rows). Exact mark/restore CADENCE pinned via
    /// the lane adapter probes: marks fire in SKIP_TEST cmp==0 ONLY (one per
    /// key group: 2), restores in TESTOUTER cmp==0 ONLY (one for the
    /// duplicate outer: 1) — and NEVER in NEXTINNER (the §2.4 counter-trap:
    /// "NB: must NOT do 'extraMarks' here"; mj_ExtraMarks is also
    /// init-false for a Sort inner). Two passes pin rescan parity.
    /// (Inner dup payloads are identical: tuplesort is not stable, so
    /// distinct payloads would make the expected vector nondeterministic.)
    #[test]
    fn mj99101_lane_testouter_restore_dup_keys_both_sides_sort_inner() {
        let outer: u32 = 99101;
        let inner: u32 = 99102;
        scanfix::register_table_2col(outer, &[&[(1, 10), (1, 11), (2, 20)]]);
        scanfix::register_table_2col(inner, &[&[(2, 200), (1, 100), (1, 100)]]);
        let expected = vec![
            row(&[1, 10, 1, 100]),
            row(&[1, 10, 1, 100]),
            row(&[1, 11, 1, 100]), // <- replay after TESTOUTER restore
            row(&[1, 11, 1, 100]),
            row(&[2, 20, 2, 200]),
        ];
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (m0, r0) = (marks(), restores());
        let runs = ab_mj_native(
            move |mcx| mk_mj99_pstmt(mcx, outer, inner, false, false),
            4,
            2,
        );
        assert_eq!(runs, vec![expected.clone(), expected]);
        // Cadence over the ON arm (2 passes): 2 marks + 1 restore per pass.
        assert_eq!(marks() - m0, 4, "mark cadence: SKIP_TEST cmp==0 only, 2 key groups x 2 passes");
        assert_eq!(restores() - r0, 2, "restore cadence: TESTOUTER cmp==0 only, 1 dup outer x 2 passes");
        scanfix::quiesced();
    }

    /// §2.3 skip_mark_restore (provenance costsize.c:3904-3906): the lane
    /// surface honors the planner flag identically — probe-pinned: NO
    /// mark/restore call is ever issued under it (the TESTOUTER cmp==0 arm
    /// takes the "current inner is already the first possible match" leg;
    /// SKIP_TEST skips ExecMarkPos). inner_unique also arms js_single_match
    /// (first-match advance).
    #[test]
    fn mj99102_lane_skip_mark_restore_no_mark_calls() {
        let outer: u32 = 99103;
        let inner: u32 = 99104;
        scanfix::register_table_2col(outer, &[&[(1, 10), (1, 11)]]);
        scanfix::register_table_2col(inner, &[&[(1, 100)]]);
        let expected = vec![row(&[1, 10, 1, 100]), row(&[1, 11, 1, 100])];
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (m0, r0) = (marks(), restores());
        let runs = ab_mj_native(
            move |mcx| mk_mj99_pstmt(mcx, outer, inner, true, true),
            4,
            2,
        );
        assert_eq!(runs, vec![expected.clone(), expected]);
        assert_eq!(marks() - m0, 0, "skip_mark_restore: no ExecMarkPos ever");
        assert_eq!(restores() - r0, 0, "skip_mark_restore: no ExecRestrPos ever");
        scanfix::quiesced();
    }

    /// §2.5 (the inc-1 INNER halves of the INITIALIZE arms): empty inputs.
    /// Empty outer = INITIALIZE_OUTER ENDOFJOIN, no fill -> zero rows (the
    /// inner sort is never even fed); empty inner = INITIALIZE_INNER
    /// ENDOFJOIN, no fill -> zero rows. (The fill-mode asymmetry halves —
    /// MatchedInner=true / MatchedOuter=false — are FSM-pinned in
    /// nodemergejoin tests mj99003 + right_join_empty_outer; they become
    /// lane-reachable at inc-2/3.)
    #[test]
    fn mj99103_lane_empty_inputs_initialize_arms() {
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let none: Vec<Vec<Option<i32>>> = Vec::new();
        // empty inner
        scanfix::register_table_2col(99105, &[&[(1, 10), (2, 20)]]);
        scanfix::register_table_2col(99106, &[]);
        let runs =
            ab_mj_native(move |mcx| mk_mj99_pstmt(mcx, 99105, 99106, false, false), 4, 1);
        assert_eq!(runs, vec![none.clone()]);
        // empty outer
        scanfix::register_table_2col(99107, &[]);
        scanfix::register_table_2col(99108, &[&[(1, 100)]]);
        let runs =
            ab_mj_native(move |mcx| mk_mj99_pstmt(mcx, 99107, 99108, false, false), 4, 1);
        assert_eq!(runs, vec![none]);
        scanfix::quiesced();
    }

    /// §1.2/§1.3 NAMED refusals, knob-ON: (a) a non-INNER face refuses
    /// `mergejoin-jointype` (JoinShape carrier) and falls through to the
    /// Volcano drive byte-identically; (b) a non-Sort inner (Material)
    /// refuses `mergejoin-inner-feed` (ChildNotLaneOwned carrier). The
    /// owned probe must NOT move in either case.
    #[test]
    fn mj99104_lane_named_refusals_fall_through_byte_identically() {
        install_seams();
        install_mj_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        // (a) LEFT jointype, Sort inner.
        scanfix::register_table_2col(99109, &[&[(1, 10), (2, 20)]]);
        scanfix::register_table_2col(99110, &[&[(2, 200)]]);
        let mk_left = move |mcx| {
            mk_mergejoin_pstmt(mcx, 99109, 99110, ::types_nodes::JoinType::JOIN_LEFT, Inner::Sort)
        };
        crate::lanev2::mergejoin_native_set_for_tests(false);
        let off = drain_wide_rows_nullable(mk_left(leaked_mcx()), 4, 1);
        crate::lanev2::mergejoin_native_set_for_tests(true);
        let (o0, j0) = (owned(), refused(3));
        let on = drain_wide_rows_nullable(mk_left(leaked_mcx()), 4, 1);
        assert_eq!(off, on, "jointype refusal must fall through byte-identically");
        assert!(refused(3) > j0, "mergejoin-jointype (join-shape) never ticked");
        assert_eq!(owned(), o0, "refused shape must not be owned");

        // (b) INNER jointype, Material inner.
        let mk_mat = move |mcx| {
            mk_mergejoin_pstmt(
                mcx,
                99109,
                99110,
                ::types_nodes::JoinType::JOIN_INNER,
                Inner::Material,
            )
        };
        crate::lanev2::mergejoin_native_set_for_tests(false);
        let off = drain_wide_rows_nullable(mk_mat(leaked_mcx()), 4, 1);
        crate::lanev2::mergejoin_native_set_for_tests(true);
        let (o1, f0) = (owned(), refused(4));
        let on = drain_wide_rows_nullable(mk_mat(leaked_mcx()), 4, 1);
        assert_eq!(off, on, "inner-feed refusal must fall through byte-identically");
        assert!(refused(4) > f0, "mergejoin-inner-feed (child-not-lane-owned) never ticked");
        assert_eq!(owned(), o1, "refused shape must not be owned");

        crate::lanev2::mergejoin_native_set_for_tests(false);
        drop(guard);
        scanfix::quiesced();
    }

    /// §1.4 es_epq_active HARD LAW, pinned INSIDE a DRIVEN recheck (the
    /// AF-rung pattern, mandatory at inc-1): EvalPlanQual over the MergeJoin
    /// plan with the knob ON must (i) tick the `epq` refusal from this
    /// surface at recheck initiation, (ii) NEVER own a recheck pull, and
    /// (iii) produce the recheck verdict byte-identically to knob OFF. The
    /// lift is Y3's, one step, census-gated — never here.
    #[test]
    fn mj99105_lane_epq_hard_law_inside_driven_recheck() {
        install_seams();
        install_mj_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());

        scanfix::register_table_2col(99111, &[&[(5, 50)]]);
        scanfix::register_table_2col(99112, &[&[(5, 500)]]);

        let run_recheck = |native_on: bool| -> Vec<i32> {
            crate::lanev2::mergejoin_native_set_for_tests(native_on);
            let pstmt = mk_mj99_pstmt(leaked_mcx(), 99111, 99112, false, false);
            let snap_ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            let snapshot: snapmgr::Snapshot =
                std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                    snap_ctx.mcx(),
                    ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
                ));
            let mut vals = Vec::new();
            with_exec_data(pstmt, |data, pstmt| {
                data.estate.es_snapshot = Some(snapshot);
                crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
                let ExecData { estate, planstate } = data;
                let mut epq = crate::epq::EpqState {
                    plan: pstmt.planTree,
                    recheck: None,
                    result_rti: 1,
                    lane_verdicts: None,
                };
                let mut subs = None;
                ::executils::ensure_epq_subs(
                    &mut subs,
                    estate.es_query_cxt,
                    estate.epq_rtsize(),
                    1,
                );
                let desc = estate.es_relations[0].as_ref().unwrap().rd_att.clone();
                let test = estate.exec_init_extra_tuple_slot(
                    Some(desc),
                    ::types_slot::TupleSlotKind::Virtual,
                );
                subs.as_mut().unwrap().relsubs_slot[0] = Some(test);
                // Substituted outer row (5, 77): keyed to match the inner.
                {
                    let mcx = estate.es_query_cxt;
                    let s = estate.slot_mut(test);
                    exectuples::exec_clear_tuple(s, mcx);
                    {
                        let base = s.base_mut();
                        base.tts_values[0] = Datum::from_i32(5);
                        base.tts_isnull[0] = false;
                        base.tts_values[1] = Datum::from_i32(77);
                        base.tts_isnull[1] = false;
                    }
                    exectuples::exec_store_virtual_tuple(s);
                }
                let got = crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
                    .unwrap()
                    .expect("recheck joins the substituted row");
                let s = estate.slot_mut(got);
                for att in 1..=4 {
                    let mut isnull = false;
                    let v = exectuples::slot_getattr(s, att, &mut isnull);
                    assert!(!isnull);
                    vals.push(v.as_i32());
                }
                crate::epq::eval_plan_qual_end(&mut epq, &mut subs, estate).unwrap();
                let ps = planstate.as_mut().unwrap();
                crate::exec_end_node(ps, estate).unwrap();
                estate.exec_reset_tuple_table(false);
                estate.exec_close_range_table_relations().unwrap();
            });
            vals
        };

        let off = run_recheck(false);
        assert_eq!(off, vec![5, 77, 5, 500]);
        let (o0, e0) = (owned(), refused(0));
        let on = run_recheck(true);
        assert_eq!(on, off, "recheck verdict must be knob-invariant (HARD LAW)");
        assert!(refused(0) > e0, "epq refusal never ticked from the lane surface");
        assert_eq!(owned(), o0, "the surface must NEVER own inside a recheck pre-Y3");

        crate::lanev2::mergejoin_native_set_for_tests(false);
        drop(guard);
        scanfix::quiesced();
    }

    /// Mixed-arm coherence (worklog §1.6, pinned by construction claim):
    /// lane and Volcano share ONE MergeJoinState, so a mid-stream knob flip
    /// hands the drive over without loss or replay.
    #[test]
    fn mj99106_lane_mixed_arm_pull_coherence() {
        install_seams();
        install_mj_seams();
        scanfix::install();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let guard = rowmode_ab::KNOB.lock().unwrap_or_else(|p| p.into_inner());
        scanfix::register_table_2col(99113, &[&[(1, 10), (2, 20), (3, 30)]]);
        scanfix::register_table_2col(99114, &[&[(3, 300), (1, 100), (2, 200)]]);
        let pstmt = mk_mj99_pstmt(leaked_mcx(), 99113, 99114, false, false);
        let snap_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap")));
        let snapshot: snapmgr::Snapshot =
            std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
                snap_ctx.mcx(),
                ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
            ));
        with_exec_data(pstmt, |data, pstmt| {
            data.estate.es_snapshot = Some(snapshot);
            crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap();
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().unwrap();
            let mut keys = Vec::new();
            // Rows 1-2 lane-owned, rest Volcano: same join, same state.
            crate::lanev2::mergejoin_native_set_for_tests(true);
            let o0 = owned();
            for _ in 0..2 {
                let slot = exec_proc_node(ps, estate).unwrap().unwrap();
                let mut isnull = false;
                keys.push(exectuples::slot_getattr(estate.slot_mut(slot), 1, &mut isnull).as_i32());
            }
            assert!(owned() > o0, "first pulls must be lane-owned");
            crate::lanev2::mergejoin_native_set_for_tests(false);
            while let Some(slot) = exec_proc_node(ps, estate).unwrap() {
                let mut isnull = false;
                keys.push(exectuples::slot_getattr(estate.slot_mut(slot), 1, &mut isnull).as_i32());
            }
            assert_eq!(keys, vec![1, 2, 3], "handover must neither drop nor replay rows");
            crate::exec_end_node(ps, estate).unwrap();
            estate.exec_reset_tuple_table(false);
            estate.exec_close_range_table_relations().unwrap();
        });
        crate::lanev2::mergejoin_native_set_for_tests(false);
        drop(guard);
        scanfix::quiesced();
    }
}
// --- end WS-MJ1 (LANE-MERGEJOIN inc-1) sub-region ----------------------------
