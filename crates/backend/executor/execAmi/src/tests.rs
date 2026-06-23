//! Logic tests for the execAmi entry points, driving the real dispatchers
//! against mock installs of the unported owners' seams (instrument,
//! nodeSubplan, bitmapset, syscache, amapi, and execTuples for the Material
//! rescan arm); execUtils is the real, directly-depended-on crate. Per-test
//! state is `thread_local!`.

use std::cell::Cell;
use std::sync::Once;

use execTuples_seams as execTuples;
use nodes_core_seams as bms_seams;
use mcx::{alloc_in, vec_with_capacity_in, Mcx, MemoryContext, PgBox, PgVec};
use nodes::bitmapset::Bitmapset;
use nodes::execexpr::SubPlanState;
use nodes::execnodes::ExprContext;
use nodes::instrument::Instrumentation;
use nodes::nodeindexscan::CUSTOMPATH_SUPPORT_BACKWARD_SCAN;
use nodes::pathnodes::{
    AppendPath, CustomPath, GroupResultPath, IndexOptInfo, IndexPath, MergeAppendPath,
    MinMaxAggPath, PathData, ProjectionPath,
};
use nodes::{Material, MaterialState, TupleTableSlot};

use super::*;

thread_local! {
    static INSTR_END_LOOPS: Cell<usize> = const { Cell::new(0) };
    static PARAM_SET_UPDATES: Cell<usize> = const { Cell::new(0) };
    static SET_PARAM_PLAN_RESCANS: Cell<usize> = const { Cell::new(0) };
}

fn mock_instr_end_loop(_instr: &mut Instrumentation) -> PgResult<()> {
    INSTR_END_LOOPS.with(|c| c.set(c.get() + 1));
    Ok(())
}

/// `bms_intersect` mock: the real `UpdateChangedParamSet` (execUtils, a
/// direct dependency) calls this exactly once per invocation, so the counter
/// counts UpdateChangedParamSet calls.
fn mock_bms_intersect<'mcx>(
    _mcx: Mcx<'mcx>,
    _a: Option<&Bitmapset<'_>>,
    _b: Option<&Bitmapset<'_>>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    PARAM_SET_UPDATES.with(|c| c.set(c.get() + 1));
    Ok(None)
}

fn mock_bms_join<'mcx>(
    a: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    b: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
) -> Option<PgBox<'mcx, Bitmapset<'mcx>>> {
    a.or(b)
}

fn mock_exec_re_scan_set_param_plan<'mcx>(
    _node: &mut SubPlanState<'mcx>,
    _parent_chg_param: &mut Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    SET_PARAM_PLAN_RESCANS.with(|c| c.set(c.get() + 1));
    Ok(())
}

fn mock_exec_clear_tuple<'mcx>(
    _estate: &mut nodes::EStateData<'mcx>,
    _slot: nodes::SlotId,
) -> PgResult<()> {
    Ok(())
}

/// `search_relation_relam` mock: relation 1 has relam 403, relation 2 is a
/// cache miss.
fn mock_search_relation_relam(relid: Oid) -> PgResult<Option<Oid>> {
    Ok(match relid {
        1 => Some(403),
        _ => None,
    })
}

/// `index_am_canbackward` mock: AM 403 supports backward scan.
fn mock_index_am_canbackward(amoid: Oid) -> PgResult<bool> {
    Ok(amoid == 403)
}

fn install_mocks() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        crate::init_seams();
        instrument::instr_end_loop::set(mock_instr_end_loop);
        bms_seams::bms_intersect::set(mock_bms_intersect);
        bms_seams::bms_join::set(mock_bms_join);
        nodeSubplan::exec_re_scan_set_param_plan::set(mock_exec_re_scan_set_param_plan);
        execTuples::exec_clear_tuple::set(mock_exec_clear_tuple);
        syscache::search_relation_relam::set(mock_search_relation_relam);
        amapi::index_am_canbackward::set(mock_index_am_canbackward);
    });
}

fn empty_bms(mcx: Mcx<'_>) -> PgResult<PgBox<'_, Bitmapset<'_>>> {
    alloc_in(
        mcx,
        Bitmapset {
            words: PgVec::new_in(mcx),
        },
    )
}

// ===========================================================================
// ExecReScan
// ===========================================================================

#[test]
fn exec_re_scan_walks_params_and_dispatches() {
    install_mocks();
    let cx = MemoryContext::new("exec_re_scan test");
    let mcx = cx.mcx();
    // The InitPlan's subselect plan node: declared before the EState so the
    // state tree's borrowed plan back-link outlives it. plan->extParam is
    // non-NULL to drive UpdateChangedParamSet.
    let mut splan_plan = Material::default();
    splan_plan.plan.extParam = Some(empty_bms(mcx).unwrap());
    let splan_plan = nodes::nodes::Node::mk_material(mcx, splan_plan);
    let child_plan = nodes::nodes::Node::mk_material(mcx, Material::default());

    let mut estate = EStateData::new_in(mcx);
    let slot = estate.make_slot(TupleTableSlot::new_in(estate.es_query_cxt)).unwrap();

    // Outer child: chgParam non-NULL so ExecReScanMaterial leaves the rescan
    // to the next ExecProcNode (no recursive exec_re_scan). The real
    // UpdateChangedParamSet dereferences node->plan, so give it one.
    let mut child = MaterialState::default();
    child.ss.ps.plan = Some(&child_plan);
    child.ss.ps.chgParam = Some(empty_bms(mcx).unwrap());

    // One InitPlan whose subselect state aliases `splan_plan` and has
    // chgParam non-NULL (drives ExecReScanSetParamPlan).
    let mut init_splan = MaterialState::default();
    init_splan.ss.ps.plan = Some(&splan_plan);
    init_splan.ss.ps.chgParam = Some(empty_bms(mcx).unwrap());
    let init_state = SubPlanState {
        planstate: Some(
            alloc_in(
                mcx,
                PlanStateNode::Material(alloc_in(mcx, init_splan).unwrap()),
            )
            .unwrap(),
        ),
        ..Default::default()
    };
    let mut init_plan = vec_with_capacity_in(mcx, 1).unwrap();
    init_plan.push(init_state);

    let mut mat = MaterialState::default();
    mat.eflags = 0;
    mat.ss.ps.ps_ResultTupleSlot = Some(slot);
    mat.ss.ps.instrument = Some(alloc_in(mcx, Instrumentation::default()).unwrap());
    let econtext = ExprContext {
        ecxt_scantuple: None,
        ecxt_innertuple: None,
        ecxt_outertuple: None,
        ecxt_oldtuple: None,
        ecxt_newtuple: None,
        ecxt_per_query_memory: estate.es_query_cxt,
        ecxt_per_tuple_memory: estate.es_query_cxt.context().new_child("ExprContext"),
        ecxt_aggvalues: PgVec::new_in(estate.es_query_cxt),
        ecxt_aggnulls: PgVec::new_in(estate.es_query_cxt),
        caseValue_datum: Default::default(),
        caseValue_isNull: true,
        domainValue_datum: Default::default(),
        domainValue_isNull: true,
        ecxt_callbacks: None,
    };
    mat.ss.ps.ps_ExprContext = Some(estate.add_expr_context(econtext).unwrap());
    mat.ss.ps.chgParam = Some(empty_bms(mcx).unwrap());
    mat.ss.ps.initPlan = Some(init_plan);
    mat.ss.ps.lefttree = Some(
        alloc_in(
            mcx,
            PlanStateNode::Material(alloc_in(mcx, child).unwrap()),
        )
        .unwrap(),
    );

    let mut node = PlanStateNode::Material(alloc_in(mcx, mat).unwrap());

    let instr0 = INSTR_END_LOOPS.with(|c| c.get());
    let upd0 = PARAM_SET_UPDATES.with(|c| c.get());
    let spp0 = SET_PARAM_PLAN_RESCANS.with(|c| c.get());

    exec_re_scan(&mut node, &mut estate).unwrap();

    // InstrEndLoop ran once. (ReScanExprContext is the real execUtils
    // implementation: it ran the — empty — callback list and reset the
    // per-tuple context.)
    assert_eq!(INSTR_END_LOOPS.with(|c| c.get()), instr0 + 1);
    // UpdateChangedParamSet: once for the InitPlan's splan, once for lefttree.
    assert_eq!(PARAM_SET_UPDATES.with(|c| c.get()), upd0 + 2);
    // ExecReScanSetParamPlan: once for the InitPlan.
    assert_eq!(SET_PARAM_PLAN_RESCANS.with(|c| c.get()), spp0 + 1);
    // bms_free(node->chgParam); node->chgParam = NULL;
    assert!(node.ps_head().chgParam.is_none());
}

// ===========================================================================
// ExecMarkPos / ExecRestrPos
// ===========================================================================

#[test]
fn mark_and_restore_dispatch_to_material() {
    install_mocks();
    let cx = MemoryContext::new("mark/restore test");
    let mcx = cx.mcx();

    // EXEC_FLAG_MARK set, no tuplestore yet: both calls are no-ops.
    let mut estate = EStateData::new_in(mcx);
    let mut mat = MaterialState::default();
    mat.eflags = 0x0010;
    let mut node = PlanStateNode::Material(alloc_in(mcx, mat).unwrap());
    exec_mark_pos(&mut node, &mut estate).unwrap();
    exec_restr_pos(&mut node, &mut estate).unwrap();
}

// ===========================================================================
// ExecSupportsMarkRestore
// ===========================================================================

#[test]
fn supports_mark_restore_matches_c_table() {
    let cx = MemoryContext::new("mark/restore path test");
    let mcx = cx.mcx();

    // T_Material / T_Sort: true (the C switch only consults pathtype here).
    assert!(exec_supports_mark_restore(&PathNode::Path(PathData {
        pathtype: T_Material
    })));
    assert!(exec_supports_mark_restore(&PathNode::Path(PathData {
        pathtype: T_Sort
    })));

    // T_IndexScan / T_IndexOnlyScan: indexinfo->amcanmarkpos.
    for (pathtype, can, expected) in [
        (T_IndexScan, true, true),
        (T_IndexScan, false, false),
        (T_IndexOnlyScan, true, true),
    ] {
        let path = PathNode::IndexPath(IndexPath {
            path: PathData { pathtype },
            indexinfo: alloc_in(mcx, IndexOptInfo { amcanmarkpos: can }).unwrap(),
        });
        assert_eq!(exec_supports_mark_restore(&path), expected);
    }

    // T_CustomScan: CUSTOMPATH_SUPPORT_MARK_RESTORE flag.
    assert!(exec_supports_mark_restore(&PathNode::CustomPath(
        CustomPath {
            path: PathData {
                pathtype: T_CustomScan
            },
            flags: CUSTOMPATH_SUPPORT_MARK_RESTORE,
        }
    )));
    assert!(!exec_supports_mark_restore(&PathNode::CustomPath(
        CustomPath {
            path: PathData {
                pathtype: T_CustomScan
            },
            flags: CUSTOMPATH_SUPPORT_BACKWARD_SCAN,
        }
    )));

    // T_Result producers: ProjectionPath recurses into its subpath; the
    // childless Result producers are false.
    let projection = PathNode::ProjectionPath(ProjectionPath {
        path: PathData { pathtype: T_Result },
        subpath: alloc_in(mcx, PathNode::Path(PathData { pathtype: T_Sort })).unwrap(),
    });
    assert!(exec_supports_mark_restore(&projection));
    assert!(!exec_supports_mark_restore(&PathNode::MinMaxAggPath(
        MinMaxAggPath {
            path: PathData { pathtype: T_Result },
        }
    )));
    assert!(!exec_supports_mark_restore(&PathNode::GroupResultPath(
        GroupResultPath {
            path: PathData { pathtype: T_Result },
        }
    )));
    assert!(!exec_supports_mark_restore(&PathNode::Path(PathData {
        pathtype: T_Result
    })));

    // T_Append / T_MergeAppend: single-subpath lists defer to the child.
    let mut subpaths = vec_with_capacity_in(mcx, 1).unwrap();
    subpaths.push(PathNode::Path(PathData {
        pathtype: T_Material,
    }));
    assert!(exec_supports_mark_restore(&PathNode::AppendPath(
        AppendPath {
            path: PathData { pathtype: T_Append },
            subpaths,
        }
    )));
    let mut subpaths = vec_with_capacity_in(mcx, 2).unwrap();
    subpaths.push(PathNode::Path(PathData {
        pathtype: T_Material,
    }));
    subpaths.push(PathNode::Path(PathData {
        pathtype: T_Material,
    }));
    assert!(!exec_supports_mark_restore(&PathNode::MergeAppendPath(
        MergeAppendPath {
            path: PathData {
                pathtype: T_MergeAppend
            },
            subpaths,
        }
    )));

    // default: false.
    assert!(!exec_supports_mark_restore(&PathNode::Path(PathData {
        pathtype: nodes::nodes::NodeTag(9999)
    })));
}

// ===========================================================================
// ExecSupportsBackwardScan / IndexSupportsBackwardScan
// ===========================================================================

#[test]
fn supports_backward_scan() {
    let cx = MemoryContext::new("backward scan test");
    let mcx = cx.mcx();

    // node == NULL.
    assert!(!exec_supports_backward_scan(None).unwrap());

    // Material: in the "don't evaluate tlist" group.
    let mat = nodes::nodes::Node::mk_material(mcx, Material::default());
    assert!(exec_supports_backward_scan(Some(&mat)).unwrap());

    // Parallel-aware nodes can't back up.
    let mut parallel = Material::default();
    parallel.plan.parallel_aware = true;
    let parallel = nodes::nodes::Node::mk_material(mcx, parallel);
    assert!(!exec_supports_backward_scan(Some(&parallel)).unwrap());

    let _ = mcx;
}

#[test]
fn index_backward_scan_consults_the_am() {
    install_mocks();
    assert!(index_supports_backward_scan(1).unwrap());
    let err = index_supports_backward_scan(2).unwrap_err();
    assert!(err.message().contains("cache lookup failed for relation 2"));
}

// ===========================================================================
// ExecMaterializesOutput
// ===========================================================================

#[test]
fn materializes_output_matches_c_table() {
    use nodes::nodes::{
        T_CteScan, T_FunctionScan, T_NamedTuplestoreScan, T_TableFuncScan, T_WorkTableScan,
    };
    for tag in [
        T_Material,
        T_FunctionScan,
        T_TableFuncScan,
        T_CteScan,
        T_NamedTuplestoreScan,
        T_WorkTableScan,
        T_Sort,
    ] {
        assert!(exec_materializes_output(tag));
    }
    for tag in [T_Result, T_Append, T_MergeAppend, T_IndexScan, T_CustomScan] {
        assert!(!exec_materializes_output(tag));
    }
}
