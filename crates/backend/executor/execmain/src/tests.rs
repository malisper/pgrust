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
const TEXTOID: u32 = 25;
/// C collation (pg_collation.dat 950) — the memcmp tier lanefold's
/// `str_collation_safe` admits without a locale seam.
const C_COLLATION: u32 = 950;
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
                // text (pg_type.dat 25): max(text)'s transtype — the
                // GL-SINKCRASH-3 str-transvalue fold regression unit.
                TEXTOID => Some(PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: TYPALIGN_INT,
                    typstorage: ::types_tuple::TYPSTORAGE_EXTENDED,
                    typcollation: 100,
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
        // pg_proc.proowner for initialize_peragg's component-fn ACL checks
        // (nodeWindowAgg.c:2911): the fixture aggregates are catalog-owned.
        syscache_seams::lookup_pg_proc_secdef::set(|_funcoid| {
            Ok(Some(::syscache_seams::PgProcSecdefShape {
                proowner: 10,
                prosecdef: false,
                proconfig: None,
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
                // max(text): the REAL pg_aggregate.dat row (PostgreSQL 18.3:
                // aggtransfn/aggcombinefn text_larger 458, aggsortop 666
                // (text >), aggtranstype text, NULL initval, no moving
                // columns) — the GL-SINKCRASH-3 str-transvalue fold
                // regression unit's aggregate.
                2129 => Some(::syscache_seams::PgAggregateShape {
                    aggkind: b'n' as i8,
                    aggnumdirectargs: 0,
                    aggtransfn: 458,
                    aggfinalfn: 0,
                    aggcombinefn: 458,
                    aggserialfn: 0,
                    aggdeserialfn: 0,
                    aggfinalextra: false,
                    aggfinalmodify: b'r' as i8,
                    aggsortop: 666,
                    aggtranstype: TEXTOID,
                    aggmtransfn: 0,
                    aggminvtransfn: 0,
                    aggmfinalfn: 0,
                    aggmfinalextra: false,
                    aggmfinalmodify: b'r' as i8,
                    aggmtranstype: 0,
                    aggtransspace: 0,
                }),
                _ => None,
            })
        });
        syscache_seams::pg_aggregate_agginitval::set(|mcx, aggfnoid| {
            Ok(match aggfnoid {
                2803 => Some(Some(::mcx::PgString::from_str_in("0", mcx).unwrap())),
                2108 | 2129 => Some(None),
                _ => None,
            })
        });
        // aggminitval mirror (real catalog values; consumed only by
        // moving-frame window aggs — the windows_t2_ab units).
        syscache_seams::pg_aggregate_aggminitval::set(|mcx, aggfnoid| {
            Ok(match aggfnoid {
                2803 => Some(Some(::mcx::PgString::from_str_in("0", mcx).unwrap())),
                2108 => Some(Some(::mcx::PgString::from_str_in("{0,0}", mcx).unwrap())),
                2129 => Some(None),
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
            Ok(
                (opno == INT4_EQ).then_some(syscache_seams::PgOperatorShape {
                    oprnamespace: 11,
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
                }),
            )
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
    let tle =
        Node::mk_target_entry(mcx, mk_int4_const(mcx, 1), 1, Some("?column?"), false).unwrap();
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

fn mk_limit_select1_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::plannodes::Limit;
    use ::types_nodes::primnodes::OUTER_VAR;

    let tle =
        Node::mk_target_entry(mcx, mk_int4_const(mcx, 1), 1, Some("?column?"), false).unwrap();
    let mut result = Node::build::<ResultPlan>(mcx).unwrap();
    result.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    let inner = result.seal();

    let v = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let mut limit = Node::build::<Limit>(mcx).unwrap();
    limit.plan.targetlist = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, v, 1, Some("?column?"), false).unwrap(),
    )
    .unwrap();
    limit.plan.lefttree = Some(inner);

    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(limit.seal());
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
// the crate's only test touching it. The COUNT lives in a thread-local:
// query-desc handles are small per-test sequence numbers, so the handle
// filter alone is near-vacuous under the harness's parallelism — a sibling
// test's desc routinely carries the same number (CI flake 2026-08-05,
// count 2 != 1). Every executor call this test makes runs on the test's
// own thread, so sibling taps land in sibling storage by construction; the
// handle filter stays as defense against same-thread helper calls.
static REARM_TAP_TARGET: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
thread_local! {
    static REARM_TAP_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn count_start(h: ::types_portal::QueryDescHandle) {
    if h.0 == REARM_TAP_TARGET.load(std::sync::atomic::Ordering::Relaxed) {
        REARM_TAP_COUNT.with(|c| c.set(c.get() + 1));
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
    assert_eq!(REARM_TAP_COUNT.with(std::cell::Cell::get), 1);

    let mut dest = DestReceiver::DoNothing;
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    execmain_seams::executor_run::call(qd, NoMovementScanDirection, 0, &mut dest).unwrap();

    // Park (no C counterpart): ExecutorFinish + in-place skeleton disarm.
    let parked = execmain_seams::executor_finish_and_park::call(qd).unwrap();
    assert!(
        parked,
        "select1 over a generic cached plan must be park-eligible"
    );

    // Two more Binds against the parked portal, each a rearm reuse — neither
    // passes through executor_start_seam, so only the ratified tap call
    // inside executor_rearm_seam can see them.
    for n in 2..=3 {
        let reused = execmain_seams::executor_rearm::call(qd, None, ParamListHandle::NULL).unwrap();
        assert!(reused, "rearm {n} should reuse the parked executor");
        assert_eq!(REARM_TAP_COUNT.with(std::cell::Cell::get), n);
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
        Node::mk(
            mcx,
            SeqScan {
                scan: Scan {
                    plan: Plan::default(),
                    scanrelid: 1,
                },
                cb_scan_cols: None,
            },
        )
        .unwrap()
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
    let agg = Node::build::<::types_nodes::plannodes::Agg>(mcx)
        .unwrap()
        .seal();
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
fn unsupported_mark_pos_is_noop_restr_errors() {
    install_seams();
    let mcx = leaked_mcx();
    let pstmt = mk_limit_select1_pstmt(mcx);
    with_exec_data(pstmt, |data, pstmt| {
        let mut ps = exec_init_node(pstmt.planTree, &mut data.estate, 0)
            .unwrap()
            .unwrap();
        crate::execami::exec_mark_pos(&mut ps, &mut data.estate).unwrap();
        let err = crate::execami::exec_restr_pos(&mut ps, &mut data.estate).unwrap_err();
        assert!(
            err.to_string().contains("unrecognized node type: 437"),
            "{err}"
        );
    });
}

#[test]
fn false_constant_qual_yields_zero_rows() {
    install_seams();
    let mcx = leaked_mcx();
    let qual = Node::mk_list(
        mcx,
        NodeList::make1(mcx, mk_bool_const(mcx, false)).unwrap(),
    )
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
    let qual = Node::mk_list(mcx, NodeList::make1(mcx, mk_bool_const(mcx, true)).unwrap()).unwrap();
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
        // GL-SINKCRASH-3 regression fixture: `(k int4, v text)` relations.
        kv_text: std::collections::HashSet<Oid>,
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
            kv_text: std::collections::HashSet::new(),
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
                let same = with_fake(|f| f.tables[&rel.rd_id].get(blkno as usize) == Some(&buf));
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

    /// One heap tuple image for the GL-SINKCRASH-3 `(k int4, v text)`
    /// fixture: 2 attributes, no nulls, the text value as a SHORT (1-byte
    /// header) varlena — alignment-free, exactly what an untoasted small
    /// text lands as on a real heap page.
    fn tuple_image_kv_text(k: i32, v: &str) -> Vec<u8> {
        use ::types_tuple::varatt::VARATT_SHORT_MAX;
        assert!(v.len() + 1 < VARATT_SHORT_MAX, "short-varlena fixture");
        let mut img = vec![0u8; 24 + 4 + 1 + v.len()];
        img[0..4].copy_from_slice(&10u32.to_ne_bytes());
        img[18..20].copy_from_slice(&2u16.to_ne_bytes());
        img[20..22]
            .copy_from_slice(&(HEAP_XMAX_INVALID | ::types_tuple::HEAP_HASVARWIDTH).to_ne_bytes());
        img[22] = 24;
        img[24..28].copy_from_slice(&k.to_ne_bytes());
        // SAFETY: in-bounds write of the 1-byte short-varlena header + payload.
        unsafe { ::types_tuple::varatt::set_varsize_short(img.as_mut_ptr().add(28), 1 + v.len()) };
        img[29..29 + v.len()].copy_from_slice(v.as_bytes());
        img
    }

    fn build_page(rows: &[&[i32]]) -> Box<TestPage> {
        let imgs: Vec<Vec<u8>> = rows.iter().map(|row| tuple_image(row)).collect();
        build_page_imgs(&imgs)
    }

    fn build_page_imgs(imgs: &[Vec<u8>]) -> Box<TestPage> {
        let mut page = Box::new(TestPage([0u8; BLCKSZ]));
        let n = imgs.len();
        let lower = SizeOfPageHeaderData + n * 4;
        let mut upper = BLCKSZ;
        for (i, img) in imgs.iter().enumerate() {
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

    /// GL-SINKCRASH-3 regression fixture: a `(k int4, v text)` heap.
    pub fn register_table_kv_text(relid: Oid, pages: &[&[(i32, &str)]]) {
        with_fake(|f| {
            let mut bufs = Vec::new();
            for rows in pages {
                let imgs: Vec<Vec<u8>> = rows
                    .iter()
                    .map(|&(k, v)| tuple_image_kv_text(k, v))
                    .collect();
                let addr = Box::leak(build_page_imgs(&imgs)).0.as_mut_ptr() as usize;
                f.pages.push(addr);
                f.pins.push(0);
                bufs.push(f.pages.len() as Buffer);
            }
            f.tables.insert(relid, bufs);
            f.kv_text.insert(relid);
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
    fn add_index_tuple(p: &mut TestPage, tid: ::types_tuple::itemptr::ItemPointerData, value: i32) {
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
        let mut leaf = new_bt_page(::types_nbtree::BTP_LEAF | ::types_nbtree::BTP_ROOT, 0);
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

    /// The `(k int4, v text)` tupdesc (GL-SINKCRASH-3 fixture). Text is the
    /// real pg_attribute shape: varlena (attlen -1), byref, 'i' align,
    /// EXTENDED storage (⇒ attispackable — short-header images deform),
    /// default collation 100.
    fn kv_text_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        let k = FormData_pg_attribute {
            attnum: 1,
            atttypid: 23,
            atttypmod: -1,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        let v = FormData_pg_attribute {
            attnum: 2,
            atttypid: 25,
            atttypmod: -1,
            attlen: -1,
            attbyval: false,
            attalign: TYPALIGN_INT,
            attstorage: ::types_tuple::TYPSTORAGE_EXTENDED,
            attcollation: 100,
            ..Default::default()
        };
        for att in [k, v] {
            compact.push(CompactAttribute::populate_from(&att));
            attrs.push(att);
        }
        Rc::new(TupleDescData {
            natts: 2,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
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
            rd_rel,
            rd_att: if with_fake(|f| f.kv_text.contains(&relid)) {
                kv_text_tupdesc(mcx)
            } else {
                int4_tupdesc(
                    mcx,
                    if with_fake(|f| f.two_col.contains(&relid)) {
                        2
                    } else {
                        1
                    },
                )
            },
            rd_index: None,
            rd_opcintype: PgVec::new_in(mcx),
            rd_opfamily: PgVec::new_in(mcx),
            rd_indoption: PgVec::new_in(mcx),
            rd_indcollation: PgVec::new_in(mcx),
            rd_options: None,
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
        assert!(
            data.estate.es_relations[0].is_some(),
            "scan relation opened"
        );

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
    assert_eq!(
        ctx.used(),
        used_after_first,
        "desc context grew across statements"
    );
    assert_eq!(
        ctx.peak(),
        peak_after_first,
        "desc context peak grew across statements"
    );
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
                plan: Plan {
                    targetlist: scan_tlist,
                    ..Default::default()
                },
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
    let agg_tle = Node::mk_target_entry(mcx, aggref.seal(), 1, Some("agg"), false).unwrap();
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
        assert!(
            exec_proc_node(ps, estate).unwrap().is_none(),
            "agg emits exactly one row"
        );

        // Rescan re-runs the whole aggregation.
        exec_re_scan(ps, estate).unwrap();
        let again = exec_proc_node(ps, estate)
            .unwrap()
            .expect("one agg row after rescan");
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
        NodeList::make1(
            mcx,
            Node::mk_target_entry(mcx, v, 1, Some("a"), false).unwrap(),
        )
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
    let runs = drain_int4_rows(
        mk_sort_limit_pstmt(mcx, relid, false, Some(1), Some(2)),
        false,
    );
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
    let runs = drain_int4_rows(
        mk_sort_limit_pstmt(mcx, relid, true, Some(1), Some(3)),
        true,
    );
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
        exec_proc_node(ps, estate)
            .unwrap()
            .expect("first sorted row");
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
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan {
                    targetlist: mk_tlist(1),
                    ..Default::default()
                },
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
        tstore_receiver::set_params(&mut dr, h, false, None, None);
        let mut dest = DestReceiver::Tuplestore(dr);
        crate::execmain::execute_plan(
            data,
            CmdType::CMD_SELECT,
            true,
            0,
            ForwardScanDirection,
            false,
                None,
            &mut dest,
                ::types_portal::CachedPlanHandle::NULL,
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
            assert_eq!(
                slot.base().tts_values.len(),
                1,
                "only column a in output tuples"
            );
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

// GL-SINKCRASH-3 regression: Agg(AGG_HASHED) with a BY-REF STR TRANSVALUE —
// SELECT k, max(v COLLATE "C") FROM t GROUP BY k over a (k int4, v text)
// fake heap — through the REAL InitPlan path and the lane-v2 SERIAL staged
// fold (verified engaged: `[lanev2] agg-over-seqscan: staged fold feed
// engaged (compact table)` under PGRUST_LANE_V2_TRACE=1).
//
// The defect this pins: `agg_fold_staged_mm`'s GL-SINKCRASH-2 fail-closed
// guard spelled "is a sink build" as `agg_sink_state_bytes(agg).is_some()`,
// which is true for EVERY hashed build (perhash always reports a state
// size), while the store-arming side keys on `sink_cap` — so this perfectly
// sound SERIAL grouped max(text) fold (aggcontext home; table and context
// die together) errored "aggregation sink shape violation: byref str
// transvalue folded on a sink build with no table-owned state store"
// (SQLSTATE XX000 to the client; found by the SQL differential fuzzer).
// The guard must read `agg_sink_mode` — the arming side's own predicate.
fn mk_hashed_strmax_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, relid: u32) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Agg, Plan, Scan, SeqScan};
    use ::types_nodes::primnodes::{Aggref, OUTER_VAR};

    // Unprojected physical scan tlist (k, v) — the serial K2 staged-fold
    // shape (a projected scan would route the expr-key decide instead).
    let k_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let v_var = Node::mk_var(mcx, 1, 2, TEXTOID, -1, C_COLLATION, 0).unwrap();
    let mut scan_tlist = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, k_var, 1, Some("k"), false).unwrap(),
    )
    .unwrap();
    scan_tlist
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, v_var, 2, Some("v"), false).unwrap(),
        )
        .unwrap();
    let scan_node = Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan {
                    targetlist: scan_tlist,
                    plan_width: 8,
                    ..Default::default()
                },
                scanrelid: 1,
            },
        },
    )
    .unwrap();

    let group_var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let group_tle = Node::mk_target_entry(mcx, group_var, 1, Some("k"), false).unwrap();
    let mut aggref = Node::build::<Aggref>(mcx).unwrap();
    aggref.aggfnoid = 2129; // max(text)
    aggref.aggtype = TEXTOID;
    aggref.aggtranstype = TEXTOID;
    aggref.aggcollid = C_COLLATION;
    aggref.inputcollid = C_COLLATION;
    aggref.aggargtypes = ::types_nodes::list::OidList::make1(mcx, TEXTOID).unwrap();
    aggref.aggno = 0;
    aggref.aggtransno = 0;
    let arg_var = Node::mk_var(mcx, OUTER_VAR, 2, TEXTOID, -1, C_COLLATION, 0).unwrap();
    aggref.args = NodeList::make1(
        mcx,
        Node::mk_target_entry(mcx, arg_var, 1, None, false).unwrap(),
    )
    .unwrap();
    let max_tle = Node::mk_target_entry(mcx, aggref.seal(), 2, Some("max"), false).unwrap();
    let mut tlist = NodeList::make1(mcx, group_tle).unwrap();
    tlist.lappend(mcx, max_tle).unwrap();

    let mut agg = Node::build::<Agg>(mcx).unwrap();
    agg.plan.targetlist = tlist;
    agg.plan.lefttree = Some(scan_node);
    agg.aggstrategy = 2; // AGG_HASHED
    agg.numCols = 1;
    agg.grpColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    agg.grpOperators = ::mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
    agg.grpCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    agg.numGroups = 2;
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

/// Decode a text datum (short- or 4-byte-header varlena) into a String.
fn text_datum_str(d: Datum) -> String {
    let p = d.as_usize() as *const u8;
    // SAFETY: the datum is a live in-slot varlena pointer for the duration
    // of this call (the caller copies before the next slot fill).
    unsafe {
        let total = ::types_tuple::varatt::varsize_any(p);
        let (off, len) = if ::types_tuple::varatt::varatt_is_1b(p) {
            (1, total - 1)
        } else {
            (4, total - 4)
        };
        String::from_utf8(std::slice::from_raw_parts(p.add(off), len).to_vec()).unwrap()
    }
}

#[test]
fn hashed_group_by_str_max_over_fake_heap_end_to_end() {
    install_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let relid: u32 = 70019;
    scanfix::register_table_kv_text(
        relid,
        &[
            &[(1, "apple"), (2, "zeta"), (1, "pear")],
            &[(2, "anchor"), (1, "banana"), (2, "kiwi")],
        ],
    );
    let pstmt = mk_hashed_strmax_pstmt(mcx, relid);

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
        assert_eq!(desc.attr(1).atttypid, TEXTOID);

        let ExecData { estate, planstate } = data;
        let ps = planstate.as_mut().unwrap();
        let mut got: Vec<(i32, String)> = Vec::new();
        while let Some(slot_id) = exec_proc_node(ps, estate).unwrap() {
            let base = estate.slot_mut(slot_id).base();
            assert!(!base.tts_isnull[0] && !base.tts_isnull[1]);
            got.push((
                base.tts_values[0].as_i32(),
                text_datum_str(base.tts_values[1]),
            ));
        }
        got.sort();
        assert_eq!(got, vec![(1, "pear".to_string()), (2, "zeta".to_string())]);

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
                    plan: Plan {
                        targetlist: scan_tlist(varno),
                        ..Default::default()
                    },
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
        &[
            (OUTER_VAR, 1),
            (OUTER_VAR, 2),
            (INNER_VAR, 1),
            (INNER_VAR, 2),
        ]
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
    let joinqual = {
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
            RTEPermissionInfo {
                relid,
                requiredPerms: 1 << 1,
                ..Default::default()
            },
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
                .map(|row| {
                    row.into_iter()
                        .map(|v| v.expect("unexpected NULL"))
                        .collect()
                })
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
        ::types_nodes::JoinType::JOIN_RIGHT_SEMI | ::types_nodes::JoinType::JOIN_RIGHT_ANTI => {
            &[(INNER_VAR, 1), (INNER_VAR, 2)]
        }
        _ => &[
            (OUTER_VAR, 1),
            (OUTER_VAR, 2),
            (INNER_VAR, 1),
            (INNER_VAR, 2),
        ],
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
        Node::mk(
            mcx,
            RTEPermissionInfo {
                relid,
                requiredPerms: 1 << 1,
                ..Default::default()
            },
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
    // op_strict(int4eq) via lookup_pg_proc_shape (rowmode superset install).
    rowmode_ab::install_rowmode_seams();
    scanfix::install();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mcx = leaked_mcx();
    let outer: u32 = 70030;
    let inner: u32 = 70031;
    scanfix::register_table_2col(outer, &[&[(1, 10), (2, 20), (3, 30)]]);
    scanfix::register_table_2col(inner, &[&[(2, 200), (3, 300), (3, 301), (4, 400)]]);
    let mut expected = vec![
        vec![2, 20, 2, 200],
        vec![3, 30, 3, 300],
        vec![3, 30, 3, 301],
    ];
    expected.sort();

    let runs = drain_wide_rows(
        mk_hashjoin_pstmt(mcx, outer, inner, ::types_nodes::JoinType::JOIN_INNER),
        4,
        2,
    );
    for run in &runs {
        let mut got = run.clone();
        got.sort();
        assert_eq!(
            got, expected,
            "hash join result set must equal the nestloop result set"
        );
    }
    assert_eq!(runs.len(), 2);
    scanfix::quiesced();
}

#[test]
fn hashjoin_with_empty_inner_returns_nothing() {
    install_seams();
    // op_strict(int4eq) via lookup_pg_proc_shape (rowmode superset install).
    rowmode_ab::install_rowmode_seams();
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
    // op_strict(int4eq) via lookup_pg_proc_shape (rowmode superset install).
    rowmode_ab::install_rowmode_seams();
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
    // op_strict(int4eq) via lookup_pg_proc_shape (rowmode superset install).
    rowmode_ab::install_rowmode_seams();
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
        mk_hashjoin_pstmt(
            mcx,
            empty_outer,
            inner,
            ::types_nodes::JoinType::JOIN_RIGHT_ANTI,
        ),
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
    // op_strict(int4eq) via lookup_pg_proc_shape (rowmode superset install).
    rowmode_ab::install_rowmode_seams();
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
    // op_strict(int4eq) via lookup_pg_proc_shape (rowmode superset install).
    rowmode_ab::install_rowmode_seams();
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
    assert!(
        nbatch > 1,
        "fixture must force a multi-batch table, got nbatch={nbatch}"
    );

    let inner_expected: Vec<Vec<i32>> = (3990..=4000).map(|k| vec![k, k, k, k * 10]).collect();
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
        let unmatched_inner: Vec<_> = got.iter().filter(|r| r[0].is_none()).collect();
        assert_eq!(unmatched_inner.len(), 3989);
        assert!(unmatched_inner.iter().all(|r| {
            let k = r[2].unwrap();
            (1..=3989).contains(&k) && r[3] == Some(k * 10) && r[1].is_none()
        }));
        let unmatched_outer: Vec<_> = got
            .iter()
            .filter(|r| r[0].is_some() && r[2].is_none())
            .collect();
        assert_eq!(
            unmatched_outer
                .iter()
                .map(|r| r[0].unwrap())
                .collect::<Vec<_>>(),
            (4001..=4010).collect::<Vec<_>>()
        );
        let matched: Vec<_> = got
            .iter()
            .filter(|r| r[0].is_some() && r[2].is_some())
            .collect();
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
    let err =
        execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap_err();
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
        NodeList::make1(
            mcx,
            Node::mk_target_entry(mcx, v, 1, Some("a"), false).unwrap(),
        )
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

fn mk_initplan_sub_seqscan<'mcx>(mcx: ::mcx::Mcx<'mcx>, with_tlist: bool) -> Node<'mcx> {
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
                plan: Plan {
                    targetlist: tlist,
                    ..Default::default()
                },
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
) -> (
    NodeList<'mcx>,
    NodeList<'mcx>,
    ::types_nodes::bitmapset::Bitmapset<'mcx>,
) {
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
        Node::mk(
            mcx,
            RTEPermissionInfo {
                relid,
                requiredPerms: 1 << 1,
                ..Default::default()
            },
        )
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
fn mk_expr_initplan_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    t1: u32,
    t2: u32,
) -> &'mcx PlannedStmt<'mcx> {
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
fn mk_exists_initplan_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    t1: u32,
    t2: u32,
) -> &'mcx PlannedStmt<'mcx> {
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

fn run_initplan_pstmt(
    pstmt: &'static PlannedStmt<'static>,
) -> Result<Vec<i32>, Box<types_error::PgError>> {
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
            let slot = unsafe { &mut *cell.0.cast::<Option<crate::PlanStateNode<'_>>>().as_ptr() };
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
                    plan: Plan {
                        parallel_safe: safe,
                        ..Default::default()
                    },
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
    pstmt.paramExecTypes = ::types_nodes::list::OidList::make2(mcx, INT4OID, INT4OID).unwrap();
    let pstmt = pstmt.seal_ref();
    with_exec_data(pstmt, |data, pstmt| {
        data.estate.es_plannedstmt = Some(pstmt);
        let worker =
            crate::execparallel::build_worker_pstmt(&data.estate, pstmt.planTree.unwrap()).unwrap();
        // The unsafe subplan is a NULL hole; the safe ones keep their
        // plan_id positions.
        assert_eq!(worker.subplans.len(), 3);
        assert!(worker.subplans.nth(0).is_some());
        assert!(worker.subplans.nth(1).is_none());
        assert!(worker.subplans.nth(2).is_some());
        assert_eq!(
            worker.paramExecTypes.as_slice(),
            pstmt.paramExecTypes.as_slice()
        );
        assert!(worker.rowMarks.is_nil());
        assert!(worker.resultRelations.is_nil());
        assert!(worker.rewindPlanIDs.is_empty());
    });
}

#[test]
fn initplan_hole_reference_errors_subplan_not_initialized() {
    use ::types_nodes::plannodes::Result as ResultPlan;
    install_seams();
    let mcx = leaked_mcx();
    let tle =
        Node::mk_target_entry(mcx, mk_int4_const(mcx, 1), 1, Some("?column?"), false).unwrap();
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
        let err = crate::execmain::init_plan(data, pstmt, CmdType::CMD_SELECT, 0).unwrap_err();
        assert!(
            err.message()
                .contains("subplan \"InitPlan 1\" was not initialized"),
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
                plan: Plan {
                    targetlist: mk_tlist(1),
                    ..Default::default()
                },
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
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, mk_wfunc(3100, false), 3, Some("rn"), false).unwrap(),
        )
        .unwrap();
    tlist
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, mk_wfunc(3101, false), 4, Some("rank"), false).unwrap(),
        )
        .unwrap();
    tlist
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, mk_wfunc(3102, false), 5, Some("dense"), false).unwrap(),
        )
        .unwrap();
    tlist
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, mk_wfunc(2108, true), 6, Some("sum"), false).unwrap(),
        )
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
                plan: Plan {
                    targetlist: scan_tlist,
                    ..Default::default()
                },
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
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, aggref.seal(), 2, Some("cnt"), false).unwrap(),
        )
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
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, mk_wfunc(3100), 3, Some("rn"), false).unwrap(),
        )
        .unwrap();
    wa_tlist
        .lappend(
            mcx,
            Node::mk_target_entry(mcx, mk_wfunc(3101), 4, Some("rank"), false).unwrap(),
        )
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
        &[
            &[(2, 5), (1, 10), (3, 7), (1, 20)],
            &[(2, 5), (1, 10), (2, 5)],
        ],
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
fn mk_epq_update_subplan_pstmt<'mcx>(mcx: ::mcx::Mcx<'mcx>, relid: u32) -> &'mcx PlannedStmt<'mcx> {
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
    let qual_const =
        Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
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
                plan: Plan {
                    targetlist: tlist,
                    qual,
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
        };
        let mut subs = None;
        ::executils::ensure_epq_subs(&mut subs, estate.es_query_cxt, estate.epq_rtsize(), 1);
        let desc = estate.es_relations[0].as_ref().unwrap().rd_att.clone();
        let test =
            estate.exec_init_extra_tuple_slot(Some(desc), ::types_slot::TupleSlotKind::Virtual);
        subs.as_mut().unwrap().relsubs_slot[0] = Some(test);

        // Latest version still matches the qual: proceed with (1, 99).
        epq_store_test_tuple(estate, test, 1, 99);
        let got = crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test).unwrap();
        let got = got.expect("qual passes; EPQ returns the candidate tuple");
        assert_ne!(got, test, "projection result, not the test slot");
        assert_eq!(epq_slot_vals(estate, got), (1, 99));
        assert!(
            estate.slot(test).base().is_empty(),
            "test slot cleared after EPQ"
        );
        assert!(
            !estate.es_epq_active,
            "flag dropped outside the recheck run"
        );

        // Reset path: latest version no longer matches -> skip.
        epq_store_test_tuple(estate, test, 2, 99);
        assert!(
            crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
                .unwrap()
                .is_none()
        );

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
            assert!(
                base.tts_isnull[1],
                "MULTIEXPR SubPlan column is a dummy NULL"
            );
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
    let qual = Node::mk_list(
        mcx,
        NodeList::make1(mcx, mk_bool_const(mcx, false)).unwrap(),
    )
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
    let limit_var = Node::mk_var(
        mcx,
        ::types_nodes::primnodes::OUTER_VAR,
        1,
        INT4OID,
        -1,
        0,
        0,
    )
    .unwrap();
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

fn run_two_col_pstmt(pstmt: &'static PlannedStmt<'static>) -> Vec<(i32, Option<i32>)> {
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
            let second = if base.tts_isnull[1] {
                None
            } else {
                Some(base.tts_values[1].as_i32())
            };
            out.push((base.tts_values[0].as_i32(), second));
        }
        crate::exec_end_node(ps, estate).unwrap();
        for i in 0..estate.es_subplanstates.len() {
            let cell = estate.es_subplanstates[i];
            // SAFETY: init_plan's arena cell (standard_executor_end's shape).
            let slot = unsafe { &mut *cell.0.cast::<Option<crate::PlanStateNode<'_>>>().as_ptr() };
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

// --- p72 D-5: the row-mode facility A/B corpus (lanev2/rowmode.rs,
// PGRUST_LANE_V2_ROWMODE) was DELETED with the lane body. The mod survives
// slimmed: `install_rowmode_seams` is the SHARED pg_proc-shape seam install
// (set-once process global) that the Volcano join/agg tests above ride.
mod rowmode_ab {
    use super::*;

    const F_GENERATE_SERIES_INT4: u32 = 1067;
    const F_GENERATE_SERIES_STEP_INT4: u32 = 1066;

    /// `pub(super)` because seams are SET-ONCE process globals
    /// (seam_core's "installed twice" panic): this one `lookup_pg_proc_shape`
    /// install serves every fixture that needs the pg_proc shape rows
    /// (generate_series, sum(int4), int4lt — PostgreSQL 18.3 values).
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
                            pronargs: if funcid == F_GENERATE_SERIES_INT4 {
                                2
                            } else {
                                3
                            },
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
                    // int4eq — the hashjoin e2e corpus (ExecInitHashJoin
                    // now reads op_strict(96) -> proisstrict of oprcode 65
                    // for the strict build-side NULL-key skip). PostgreSQL
                    // 18.3 pg_proc.
                    65 => Some(syscache_seams::PgProcShape {
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
}

// --- p72 D-5: the WindowAgg W1 lane A/B corpus (lanev2/windows.rs, PGRUST_LANE_V2_WINDOWS) was DELETED with the lane body. ---

// --- p72 D-5: the MergeJoin row-mode A/B corpus (lanev2/rowmode.rs try_own_merge_join) was DELETED with the lane body. ---

// --- p72 D-5: the WS-P node-census corpus (lanev2/census.rs) was DELETED with the lane body. ---

// --- p72 D-5: the WS-L row-mode tail A/B corpus (lanev2/rowmode_tail.rs) was DELETED with the lane body. ---

// --- p72 D-5: the WS-M T2-A windows A/B corpus (lanev2/windows.rs, PGRUST_LANE_V2_WINDOWS_T2) was DELETED with the lane body. ---

// (The wave-2 WS-N dml_ab A/B corpus was DELETED at p72 D-2 with the DML
// hosting island, lanev2/dml.rs.)

// --- p72 D-5: the WS-Q scans-T3 A/B corpus (lanev2/tail_source.rs, PGRUST_LANE_V2_SCANS_T3) was DELETED with the lane body. ---
// --- end WS-Q wave-3 append region ------------------------------------------

// --- p72 D-5: the WS-R T2-B framed-drive A/B corpus (lanev2/windows.rs, PGRUST_LANE_V2_WINDOWS_T2B) was DELETED with the lane body. ---
// --- end WS-R wave-3 (T2-B) region ---

// --- WS-T wave-3 (dml inc-2/2b/3a) A/B corpus — DELETED at p72 D-2 with the
// DML hosting island, lanev2/dml.rs. ---

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
            };
            let mut subs = None;
            ::executils::ensure_epq_subs(&mut subs, estate.es_query_cxt, estate.epq_rtsize(), 1);
            let desc = estate.es_relations[0].as_ref().unwrap().rd_att.clone();
            let test =
                estate.exec_init_extra_tuple_slot(Some(desc), ::types_slot::TupleSlotKind::Virtual);
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

            assert_eq!(
                b_vals, a_vals,
                "seam composition == entry (move equivalence)"
            );

            // Skip verdict identical through the entry as before the move.
            epq_store_test_tuple(estate, test, 2, 99);
            assert!(
                crate::epq::eval_plan_qual(&mut epq, &mut subs, estate, test)
                    .unwrap()
                    .is_none()
            );

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
            assert_eq!(
                estate.es_tupleTable.len(),
                n1,
                "second call appends nothing"
            );
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
    /// panics LOUDLY. The EPQ-unique admission wave
    /// (fix/epq-unique-recheck) admitted the subquery/aggregate family —
    /// Unique/Agg/Group/WindowAgg/SetOp/Memoize/IncrementalSort/
    /// MergeAppend, each exercised by the epq-storm-unique / epq-subq-*
    /// isolation specs — and the whitelist-completion wave
    /// (fix/epq-whitelist-completion) admitted ProjectSet/SampleScan/
    /// TableFuncScan/ForeignScan/NamedTuplestoreScan, so the negative arm
    /// pins a shape that stays outside the list PERMANENTLY (Gather:
    /// structurally unreachable — no locking/DML plan is ever parallel —
    /// so the panic arm is the documented invariant's assertion).
    /// Wave-7 extension: the positive arm carries a REAL scanrelid (1)
    /// because scanrelid == 0 pushed-down-join scans refuse loudly on
    /// their own arm (see `epq_w7_scanrelid_zero_refused_loudly`).
    #[test]
    #[should_panic(expected = "recheck plan")]
    fn epq_w5_check_epq_plan_is_the_loud_admission_list() {
        use ::types_nodes::plannodes::{Plan, Scan, SeqScan, Unique};
        let mcx = leaked_mcx();
        // Positive arm first: a whitelist shape passes without panic.
        let seq = Node::mk(
            mcx,
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan::default(),
                    scanrelid: 1,
                },
            },
        )
        .unwrap();
        crate::epq::check_epq_plan(seq);
        // Positive arm, EPQ-unique admission wave: a Unique over a SeqScan
        // (the live 2026-08-05 refusal shape — SELECT DISTINCT subquery
        // under a FOR UPDATE join) passes, and the walker recurses into
        // its lefttree (an unadmitted child would still refuse).
        let uniq = Node::mk(
            mcx,
            Unique {
                plan: Plan {
                    lefttree: Some(seq),
                    ..Plan::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
        crate::epq::check_epq_plan(uniq);
        // Negative arm: Gather can never appear in a recheck plan (the
        // planner forbids parallel locking/DML plans) — LOUD refuse.
        let g = Node::build::<::types_nodes::plannodes::Gather>(mcx)
            .unwrap()
            .seal();
        crate::epq::check_epq_plan(g);
    }

    // (The PGRUST_LANE_V2_EPQ knob A/B test was DELETED at p72 D-3 with the
    // EPQ-lane island, lanev2/epq.rs.)
}
// --- end WS-U wave-5 ----------------------------------------------------------

// --- p72 D-5: the WS-V wave-5 AM-backed scans-T3 per-shape units (lanev2/tail_source.rs, PGRUST_LANE_V2_SCANS_T3) were DELETED with the lane body. ---
// --- end WS-V wave-5 sub-region -------------------------------------------------

// --- WS-W (wave-5) dml inc-4 OC A/B corpus — DELETED at p72 D-2 with the
// DML hosting island, lanev2/dml.rs. ---

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
    // (The Y0/Y1 capture + verdict corpus — tests of the lanev2/epq.rs
    // module — was DELETED at p72 D-3 with the EPQ-lane island. The two
    // Y2 loud-admission-list pins below test the surviving
    // crate::epq::check_epq_plan and stay.)

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
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan::default(),
                    scanrelid: 0,
                },
            },
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
            SeqScan {
                cb_scan_cols: None,
                scan: Scan {
                    plan: Plan::default(),
                    scanrelid: 1,
                },
            },
        )
        .unwrap();
        let ok = Node::mk(
            mcx,
            SubqueryScan {
                scan: Scan {
                    plan: Plan::default(),
                    scanrelid: 1,
                },
                subplan: Some(seq),
                scanstatus: 0,
            },
        )
        .unwrap();
        crate::epq::check_epq_plan(ok);
        // Negative arm: a Gather UNDER an admitted SubqueryScan refuses
        // (ProjectSet itself was admitted by the whitelist-completion
        // wave; Gather stays outside the list permanently — structurally
        // unreachable, the panic is the invariant's assertion).
        let g = Node::build::<::types_nodes::plannodes::Gather>(mcx)
            .unwrap()
            .seal();
        let bad = Node::mk(
            mcx,
            SubqueryScan {
                scan: Scan {
                    plan: Plan::default(),
                    scanrelid: 1,
                },
                subplan: Some(g),
                scanstatus: 0,
            },
        )
        .unwrap();
        crate::epq::check_epq_plan(bad);
    }
}
// --- end WS-Y wave-7 ------------------------------------------------------------

// --- p72 D-5: the WS-AI wave-9/9.5 cursor budget/park corpus (lanev2/push.rs) was DELETED with the lane body. ---
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
        execmain_seams::executor_run::call(qd, ForwardScanDirection, tcount, &mut dest).unwrap();
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
        assert_eq!(
            run_spi_shape(relid, 4),
            4,
            "tcount=4 crosses the page boundary"
        );
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

// --- p72 D-5: the WS-AJ wave-9.5 SPI Stage-A seam corpus (lanev2/push.rs SPI budget) was DELETED with the lane body. ---
// --- p72 D-5: the WS-CB cursors inc-2 unit corpus was DELETED with the lane
// cursor plane (lanev2/push.rs). The forward-only run-seam pin below SURVIVES:
// the backward 0A000 refusal is executor law, not lane machinery.
mod run_seam_forward_only {
    use super::*;

    fn mk_store_dest() -> (::types_portal::TuplestoreHandle, DestReceiver<'static>) {
        let store = tuplestore::Tuplestore::begin_heap(true, false, 1024);
        let h = tuplestore::hold::register(store);
        let mut dr = tstore_receiver::tstore_create_DR();
        tstore_receiver::set_params(&mut dr, h, false, None, None);
        (h, DestReceiver::Tuplestore(dr))
    }

    /// §6 deletion rider row 4 EXECUTED (se/deletion-prep B1): the run
    /// seam is FORWARD-ONLY. A backward drive into `execute_plan`
    /// errors 0A000 BEFORE any plan work. At
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
            let err = crate::execmain::execute_plan(
                data,
                CmdType::CMD_SELECT,
                true,
                0,
                BackwardScanDirection,
                false,
                None,
                &mut dest,
                ::types_portal::CachedPlanHandle::NULL,
            )
            .unwrap_err();
            assert!(
                err.message().contains("backward scan is not supported"),
                "unexpected error: {}",
                err.message()
            );
            assert_eq!(
                data.estate.es_processed, 0,
                "no plan work before the refusal"
            );
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

// --- p72 D-5: the SE-R41 capture-batch fill corpus (lanev2/push.rs) was DELETED with the lane body. ---
// --- end SE-R41 -----------------------------------------------------------------

// --- p72 D-5: the WS-MJ1 lane-native mergejoin corpus (lanev2/lane_mergejoin.rs) was DELETED with the lane body. ---
// --- end WS-MJ1 (LANE-MERGEJOIN inc-1) sub-region ----------------------------

// --- planner/executor-init stack-budget regression (LD7-F1, TGT-F5/F7/F8) ----
// The 54001 "stack depth limit exceeded" family: exec_init_node's and
// init_expr_rec's giant dispatcher matches used to give every arm's by-value
// temporaries a distinct slot in the DISPATCHER frame (measured 380kB /
// 84kB per recursion level on arm64 dev builds), so a five-level plan or a
// two-dozen-level expression tripped the C-parity max_stack_depth=2048kB
// guard on SQL C inits in a few kB. The arms now run under
// stack_depth_core::with_own_frame; these tests arm the real guard (the
// unit-test default leaves STACK_BASE unset, guard off) at the shipped
// 2048kB budget and init trees deep enough that the pre-fix frames blow the
// budget many times over, in EVERY build profile (no debug_assert reliance).

fn mk_const_tlist(mcx: ::mcx::Mcx<'_>) -> NodeList<'_> {
    let tle =
        Node::mk_target_entry(mcx, mk_int4_const(mcx, 1), 1, Some("?column?"), false).unwrap();
    NodeList::make1(mcx, tle).unwrap()
}

/// depth nested Material nodes over a `SELECT 1` Result leaf; qual (if any)
/// goes on the leaf as resconstantqual.
fn mk_deep_pstmt<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    depth: usize,
    resconstantqual: Option<Node<'mcx>>,
) -> &'mcx PlannedStmt<'mcx> {
    let mut result = Node::build::<ResultPlan>(mcx).unwrap();
    result.plan.targetlist = mk_const_tlist(mcx);
    result.resconstantqual = resconstantqual;
    let mut child = result.seal();
    for _ in 0..depth {
        let mut m = Node::build::<::types_nodes::plannodes::Material>(mcx).unwrap();
        m.plan.targetlist = mk_const_tlist(mcx);
        m.plan.lefttree = Some(child);
        child = m.seal();
    }
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(child);
    pstmt.seal_ref()
}

/// NOT(NOT(...(true))) with `depth` NOT levels.
fn mk_not_chain(mcx: ::mcx::Mcx<'_>, depth: usize) -> Node<'_> {
    let mut e = mk_bool_const(mcx, true);
    for _ in 0..depth {
        let mut b = Node::build::<::types_nodes::primnodes::BoolExpr>(mcx).unwrap();
        b.boolop = ::types_nodes::primnodes::BoolExprType::NOT_EXPR;
        b.args = NodeList::make1(mcx, e).unwrap();
        e = b.seal();
    }
    e
}

/// Runs `f` on a thread whose stack is big enough that only the ARMED guard
/// can fail (a genuine overflow would SIGSEGV, not error), with the guard
/// armed at the shipped default budget from the thread entry frame.
fn with_armed_stack_guard<F: FnOnce() + Send + 'static>(f: F) {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            let old = stack_depth_core::set_stack_base();
            // Production pairing: ceiling clamps the scaled budget inside
            // this thread's real 64 MiB, keeping failure polite (54001).
            stack_depth_core::set_thread_stack_ceiling(64 * 1024 * 1024);
            stack_depth_core::assign_max_stack_depth(2048); // shipped default, kB
            f();
            stack_depth_core::restore_stack_base(old);
        })
        .unwrap()
        .join()
        .unwrap();
}

fn start_and_end(pstmt: &'static PlannedStmt<'static>) {
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        "deep init stack-budget probe",
        None,
        None,
        CommandDest::None,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    // EXPLAIN_ONLY still walks the FULL init recursion (the banked repros
    // fired at plain EXPLAIN) and lets end run without an executor_run.
    if let Err(e) = execmain_seams::executor_start::call(qd, ::types_slot::EXEC_FLAG_EXPLAIN_ONLY) {
        panic!(
            "executor init blew the C-parity 2048kB stack budget \
             (per-level dispatcher frames regressed?): {:?}",
            e
        );
    }
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
}

#[test]
fn deep_plan_tree_inits_within_c_parity_stack_budget() {
    install_seams();
    with_armed_stack_guard(|| {
        let mcx = leaked_mcx();
        // 10 plan levels: pre-fix exec_init_node frames (380kB/level) blow
        // 2048kB at level 6; post-fix per-level cost must keep this green.
        start_and_end(mk_deep_pstmt(mcx, 10, None));
    });
}

#[test]
fn deep_expression_inits_within_c_parity_stack_budget() {
    install_seams();
    with_armed_stack_guard(|| {
        let mcx = leaked_mcx();
        // 150 NOT levels on the leaf qual: pre-fix init_expr_rec frames
        // (84kB/level) blow 2048kB at ~24 levels; C compiles thousands.
        let qual =
            Node::mk_list(mcx, NodeList::make1(mcx, mk_not_chain(mcx, 150)).unwrap()).unwrap();
        start_and_end(mk_deep_pstmt(mcx, 0, Some(qual)));
    });
}

// RB-13 (covdiff seeds 404/505): the run seam must arm the §4.2 capture row
// loop from the RECEIVER's sidecar. PR #1527's lane deletion (p72 D-5)
// hardcoded the run-seam sidecar read to None, so a store-armed
// (implicit-SCROLL, non-FOR-UPDATE) cursor filled its row store but never
// its tid sidecar — and every WHERE CURRENT OF over it then resolved
// through exec_current_of's shortfall arm into a spurious 24000
// `cursor "c" is not a simply updatable scan of table "t"` where C
// succeeds (pg_regress portals pins the end-to-end surface; this pins the
// seam contract the fix restored). A capture-armed tuplestore receiver
// gets EXACTLY one sidecar identity row per emitted row — a scanless
// Result plan captures the (0, 0) inactive placeholder, the point being
// that a row IS appended — and an unarmed receiver's run leaves the
// sidecar untouched.
#[test]
fn executor_run_arms_capture_sidecar_from_receiver() {
    install_seams();
    let mcx = leaked_mcx();
    let mk_store =
        || ::tuplestore::hold::register(::tuplestore::Tuplestore::begin_heap(true, false, 1024));

    // Capture-armed receiver: one identity row per emitted row.
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
    let store = mk_store();
    let sidecar = mk_store();
    let mut dest = ::tcop_dest::CreateDestReceiver(CommandDest::Tuplestore);
    ::tcop_dest::SetTuplestoreDestReceiverParams(&mut dest, store, false, None, None);
    ::tcop_dest::SetTuplestoreCaptureSidecar(&mut dest, sidecar);
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest).unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd), 1);
    assert_eq!(
        ::tuplestore::hold::with_store(store, |s| s.tuple_count()),
        1,
        "the fill's row store gets the emitted row"
    );
    assert_eq!(
        ::tuplestore::hold::tidstore_get(sidecar, 0).unwrap(),
        Some((0, 0)),
        "capture-armed run appends one identity row per emitted row \
         (RB-13: a None here is the starved-sidecar regression)"
    );
    assert_eq!(::tuplestore::hold::tidstore_get(sidecar, 1).unwrap(), None);
    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);

    // Unarmed receiver: same plan, no sidecar — the plain loop runs and the
    // sidecar (a bystander store here) stays empty.
    let pstmt2 = mk_select1_pstmt(mcx, None);
    let qd2 = execmain_seams::create_query_desc::call(
        pstmt2,
        "SELECT 1",
        None,
        None,
        CommandDest::None,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd2, 0).unwrap();
    let store2 = mk_store();
    let bystander = mk_store();
    let mut dest2 = ::tcop_dest::CreateDestReceiver(CommandDest::Tuplestore);
    ::tcop_dest::SetTuplestoreDestReceiverParams(&mut dest2, store2, false, None, None);
    execmain_seams::executor_run::call(qd2, ForwardScanDirection, 0, &mut dest2).unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd2), 1);
    assert_eq!(::tuplestore::hold::with_store(store2, |s| s.tuple_count()), 1);
    assert_eq!(::tuplestore::hold::tidstore_get(bystander, 0).unwrap(), None);
    execmain_seams::executor_finish::call(qd2).unwrap();
    execmain_seams::executor_end::call(qd2).unwrap();
    execmain_seams::free_query_desc::call(qd2);

    for h in [store, sidecar, store2, bystander] {
        ::tuplestore::hold::end(h);
    }
}
