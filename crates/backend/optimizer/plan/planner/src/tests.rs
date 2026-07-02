use std::sync::Once;

use datum::Datum;
use mcx::{alloc_leak_in, Mcx, MemoryContext};
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind};
use types_nodes::primnodes::FromExpr;
use types_nodes::{Node, NodeTag};
use types_portal::{ParamListHandle, CURSOR_OPT_PARALLEL_OK};
use types_tuple::PgTypeShape;

use crate::planner;

fn install_fixtures() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        crate::init_seams();
        backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                23 => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typstorage: b'p' as i8,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
    });
}

fn cx() -> MemoryContext {
    install_fixtures();
    MemoryContext::new_bump("planner-test")
}

// The analyzer's output for `SELECT 1`.
fn select_1_query(mcx: Mcx<'_>) -> Query<'_> {
    let konst = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, konst, 1, Some("?column?"), false).unwrap();
    let jointree =
        alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        targetList: NodeList::make1(mcx, tle).unwrap(),
        stmt_location: 0,
        stmt_len: 8,
        ..Query::default()
    }
}

#[test]
fn select_1_plans_to_a_result_node() {
    let cx = cx();
    let mcx = cx.mcx();
    let stmt = planner(
        mcx,
        select_1_query(mcx),
        "SELECT 1",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    assert_eq!(stmt.commandType, CmdType::CMD_SELECT);
    assert!(stmt.canSetTag);
    assert!(!stmt.hasReturning);
    assert_eq!(stmt.jitFlags, 0);
    assert!(stmt.subplans.is_nil());
    assert!(stmt.relationOids.is_nil());
    assert!(stmt.unprunableRelids.is_empty());
    assert_eq!(stmt.stmt_len, 8);

    // replace_empty_jointree's dummy RTE survives into the flat rtable.
    assert_eq!(stmt.rtable.len(), 1);
    let rte = stmt.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(rte.rtekind, RTEKind::RTE_RESULT);
    assert_eq!(rte.eref.unwrap().aliasname, Some("*RESULT*"));

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_Result);
    let result = plan.as_result().unwrap();
    assert!(result.plan.lefttree.is_none());
    assert!(result.resconstantqual.is_none());
    assert_eq!(result.plan.plan_node_id, 0);
    // EXPLAIN SELECT 1: cost=0.00..0.01 rows=1 width=4.
    assert_eq!(result.plan.startup_cost, 0.0);
    assert_eq!(result.plan.total_cost, 0.01);
    assert_eq!(result.plan.plan_rows, 1.0);
    assert_eq!(result.plan.plan_width, 4);

    assert_eq!(result.plan.targetlist.len(), 1);
    let tle = result.plan.targetlist.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resno, 1);
    assert_eq!(tle.resname, Some("?column?"));
    assert!(!tle.resjunk);
    let c = tle.expr.as_const().unwrap();
    assert_eq!(c.consttype, 23);
    assert_eq!(c.constvalue.as_i32(), 1);
}

#[test]
fn seam_routes_to_standard_planner() {
    let cx = cx();
    let mcx = cx.mcx();
    let stmt = planner_seams::planner::call(
        mcx,
        select_1_query(mcx),
        "SELECT 1",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();
    assert_eq!(stmt.planTree.unwrap().node_tag(), NodeTag::T_Result);
}

#[test]
fn select_arithmetic_folds_before_planning() {
    static PROC: Once = Once::new();
    PROC.call_once(|| {
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok(match funcid {
                177 => Some(syscache_seams::PgProcShape {
                    pronamespace: 11,
                    prorettype: 23,
                    provariadic: 0,
                    prosupport: 0,
                    pronargs: 2,
                    prokind: b'f' as i8,
                    provolatile: b'i' as i8,
                    proparallel: b's' as i8,
                    proretset: false,
                    proisstrict: true,
                    proleakproof: false,
                }),
                _ => None,
            })
        });
        clauses_seams_evaluate_expr_fixture();
    });

    let cx = cx();
    let mcx = cx.mcx();
    let mut parse = select_1_query(mcx);
    let one = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
    let null = Node::mk_const(mcx, 23, -1, 0, 4, Datum::null(), true, true).unwrap();
    let op = Node::mk(
        mcx,
        types_nodes::primnodes::OpExpr {
            opno: 551,
            opfuncid: 177,
            opresulttype: 23,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, one, null).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let tle = Node::mk_target_entry(mcx, op, 1, Some("?column?"), false).unwrap();
    parse.targetList = NodeList::make1(mcx, tle).unwrap();

    // int4pl is strict with a NULL arg: folds to a NULL Const, no executor.
    let stmt = planner(mcx, parse, "SELECT 1 + NULL", CURSOR_OPT_PARALLEL_OK, ParamListHandle::NULL)
        .unwrap();
    let plan = stmt.planTree.unwrap();
    let tle = plan.as_result().unwrap().plan.targetlist.nth(0).as_target_entry().unwrap();
    assert!(tle.expr.as_const().unwrap().constisnull);
}

fn clauses_seams_evaluate_expr_fixture() {
    // Non-const inputs never reach evaluate_expr in these tests.
    clauses_seams::evaluate_expr::set(|_, _, _, _, _| {
        panic!("evaluate_expr not exercised")
    });
}

#[test]
fn guc_boot_values_match_the_settings_tables() {
    use guc_tables::{GucDefaultValue, GucSetting};
    let expect: &[(&str, GucDefaultValue)] = &[
        ("cpu_tuple_cost", GucDefaultValue::Real(crate::gucs::cpu_tuple_cost())),
        ("cursor_tuple_fraction", GucDefaultValue::Real(crate::gucs::cursor_tuple_fraction())),
        ("jit_above_cost", GucDefaultValue::Real(crate::gucs::jit_above_cost())),
        (
            "jit_optimize_above_cost",
            GucDefaultValue::Real(crate::gucs::jit_optimize_above_cost()),
        ),
        ("jit_inline_above_cost", GucDefaultValue::Real(crate::gucs::jit_inline_above_cost())),
        ("jit", GucDefaultValue::Bool(crate::gucs::jit_enabled())),
        ("jit_expressions", GucDefaultValue::Bool(crate::gucs::jit_expressions())),
        ("jit_tuple_deforming", GucDefaultValue::Bool(crate::gucs::jit_tuple_deforming())),
        (
            "max_parallel_workers_per_gather",
            GucDefaultValue::Int(crate::gucs::max_parallel_workers_per_gather()),
        ),
        (
            "debug_parallel_query",
            GucDefaultValue::Enum(crate::gucs::debug_parallel_query()),
        ),
    ];
    for (name, have) in expect {
        let boot = guc_tables::all_settings()
            .find_map(|s| match s {
                GucSetting::Bool(b) if b.name == *name => Some(b.boot_val),
                GucSetting::Int(i) if i.name == *name => Some(i.boot_val),
                GucSetting::Real(r) if r.name == *name => Some(r.boot_val),
                GucSetting::Enum(e) if e.name == *name => Some(e.boot_val),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{name} not in guc tables"));
        assert_eq!(boot, *have, "{name}");
    }
}

#[test]
#[should_panic(expected = "M2")]
fn from_relation_panics_loudly() {
    let cx = cx();
    let mcx = cx.mcx();
    let mut parse = select_1_query(mcx);
    let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = 16384;
    rte.inh = true;
    parse.rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
    let jointree = alloc_leak_in(
        mcx,
        FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: None },
    )
    .unwrap();
    parse.jointree = Some(jointree);

    let _ = planner(mcx, parse, "SELECT 1 FROM t", CURSOR_OPT_PARALLEL_OK, ParamListHandle::NULL);
}
